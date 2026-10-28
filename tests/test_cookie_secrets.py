"""账号 Cookie 落库加密（AES-256-GCM）的行为锁定。

这套东西的意义只有一个：**库文件泄露 ≠ 账号泄露**。
所以测试要盯住四件事：
  1. 落库的确实是密文，读出来才是明文；
  2. 老明文数据能在启动时自动迁移，且迁移是幂等的；
  3. 密文被改一个字节、或者换了密钥，都必须解不开（宁可报错也不能给错东西）；
  4. 缺依赖 / 密钥文件坏掉时**大声失败**，不偷偷降级成明文。
"""

from __future__ import annotations

import os
import sqlite3

import pytest

from app import secrets as secrets_mod
from app.db import Database
from app.engine import DeliveryEngine
from app.runner import AccountWorker
from app.secrets import ENCRYPTED_PREFIX, CookieVault, SecretError
from app.web import create_app

COOKIE = "unb=4167468834; sgcookie=sg; _m_h5_tk=tok_1"


def vault(tmp_path, name: str = "cookie.key", **kwargs) -> CookieVault:
    return CookieVault(tmp_path / name, **kwargs)


# ---------------------------------------------------------------- 加解密


def test_round_trip_and_random_nonce(tmp_path):
    v = vault(tmp_path)
    first = v.encrypt(COOKIE)
    second = v.encrypt(COOKIE)

    assert first.startswith(ENCRYPTED_PREFIX)
    assert first != second          # 随机 nonce：同样明文不会得到同样密文
    assert COOKIE not in first      # 密文里不该出现明文片段
    assert v.decrypt(first) == COOKIE
    assert v.decrypt(second) == COOKIE


def test_plaintext_legacy_value_is_passed_through(tmp_path):
    v = vault(tmp_path)
    assert v.decrypt(COOKIE) == COOKIE       # 老数据：明文原样返回，交给迁移去加密
    assert v.encrypt("") == ""               # 空值不加密
    sealed = v.encrypt(COOKIE)
    assert v.encrypt(sealed) == sealed       # 幂等：已经是密文就不再套一层


def test_tampered_ciphertext_is_rejected(tmp_path):
    v = vault(tmp_path)
    sealed = v.encrypt(COOKIE)
    # 改掉 base64 里靠后的一个字符（落在密文或 tag 上）
    tampered = sealed[:-3] + ("A" if sealed[-3] != "A" else "B") + sealed[-2:]
    assert v.decrypt(tampered) == ""
    assert "解密失败" in v.last_error


def test_wrong_key_cannot_decrypt(tmp_path):
    mine = vault(tmp_path, "a.key")
    sealed = mine.encrypt(COOKIE)

    other = vault(tmp_path, "b.key")      # 另一把钥匙
    assert other.decrypt(sealed) == ""
    assert "解密失败" in other.last_error


def test_unknown_version_is_an_error_not_plaintext(tmp_path):
    v = vault(tmp_path)
    assert v.decrypt("enc:v9:whatever") == ""
    assert "版本" in v.last_error         # 不能把不认识的密文当明文发出去


def test_key_file_is_created_private_and_loose_perms_are_fixed(tmp_path):
    v = vault(tmp_path)
    v.ensure_key()
    key_path = tmp_path / "cookie.key"
    assert key_path.exists()
    assert (key_path.stat().st_mode & 0o777) == 0o600

    os.chmod(key_path, 0o644)             # 模拟 cp 过来带了宽权限
    fresh = vault(tmp_path)               # 新实例才会重新读文件
    fresh.ensure_key()
    assert (key_path.stat().st_mode & 0o777) == 0o600


def test_broken_key_file_fails_loudly(tmp_path):
    key_path = tmp_path / "cookie.key"
    key_path.write_text("not-hex-at-all\n", encoding="utf-8")
    with pytest.raises(SecretError) as excinfo:
        vault(tmp_path).ensure_key()
    assert "十六进制" in str(excinfo.value)

    key_path.write_text("00" * 8, encoding="utf-8")   # 长度不够
    with pytest.raises(SecretError) as excinfo2:
        vault(tmp_path).ensure_key()
    assert "长度不对" in str(excinfo2.value)


def test_missing_cryptography_does_not_silently_fall_back(tmp_path, monkeypatch):
    """没装 cryptography 时必须报错并给出「重跑 ./start.sh」，绝不悄悄写明文。"""
    monkeypatch.setattr(secrets_mod, "CRYPTO_OK", False)
    v = vault(tmp_path)
    with pytest.raises(SecretError) as excinfo:
        v.ensure_key()
    assert "cryptography" in str(excinfo.value)
    assert "./start.sh" in str(excinfo.value)

    with pytest.raises(SecretError):
        v.encrypt(COOKIE)
    assert v.decrypt("enc:v1:AAAA") == ""   # 解不开也不能崩


def test_disabled_vault_is_plaintext_but_refuses_ciphertext(tmp_path):
    v = vault(tmp_path, enabled=False)
    assert v.mode.startswith("plaintext")
    assert v.encrypt(COOKIE) == COOKIE          # 显式关掉才写明文
    assert v.decrypt("enc:v1:AAAA") == ""
    assert "关闭" in v.last_error


# ---------------------------------------------------------------- DB 层


async def test_db_stores_ciphertext_and_returns_plaintext(tmp_path):
    db = Database(tmp_path / "t.db")
    await db.init()
    account_id = await db.upsert_account("主号", COOKIE)

    # 直接看库里的原始值：必须是密文
    conn = sqlite3.connect(tmp_path / "t.db")
    try:
        raw = conn.execute("SELECT cookie FROM accounts WHERE id = ?", (account_id,)).fetchone()[0]
    finally:
        conn.close()
    assert raw.startswith(ENCRYPTED_PREFIX)
    assert COOKIE not in raw

    # 读出来是明文，并且能直接喂给客户端
    account = await db.get_account(account_id)
    assert account["cookie"] == COOKIE
    assert account["cookie_error"] == ""
    assert (await db.list_accounts())[0]["cookie"] == COOKIE
    assert await db.get_account_cookie_by_name("主号") == COOKIE

    # 刷新后的 Cookie 同样加密落库
    await db.update_account_cookie(account_id, COOKIE + "; extra=1")
    conn = sqlite3.connect(tmp_path / "t.db")
    try:
        raw2 = conn.execute("SELECT cookie FROM accounts WHERE id = ?", (account_id,)).fetchone()[0]
    finally:
        conn.close()
    assert raw2.startswith(ENCRYPTED_PREFIX)
    assert (await db.get_account(account_id))["cookie"].endswith("extra=1")


async def test_legacy_plaintext_is_migrated_once(tmp_path):
    db = Database(tmp_path / "t.db")
    await db.init()
    # 手工塞一条明文（模拟升级前的老库）
    await db.execute(
        "INSERT INTO accounts (name, cookie, remark, enabled, created_at, updated_at)"
        " VALUES (?, ?, '', 1, '2026-10-01 00:00:00', '2026-10-01 00:00:00')",
        ("老号", COOKIE),
    )

    assert await db._migrate_cookie_encryption() == 1
    assert await db._migrate_cookie_encryption() == 0     # 幂等
    assert (await db.get_account(1))["cookie"] == COOKIE  # 迁移后照样能用


async def test_wrong_key_surfaces_cookie_error(tmp_path):
    db = Database(tmp_path / "t.db")
    await db.init()
    account_id = await db.upsert_account("主号", COOKIE)

    # 换一把钥匙打开同一个库：能读到行，但 Cookie 解不开
    other = Database(tmp_path / "t.db", vault=CookieVault(tmp_path / "other.key"))
    row = await other.get_account(account_id)
    assert row["cookie"] == ""
    assert "解密失败" in row["cookie_error"]


async def test_worker_refuses_to_start_with_unreadable_cookie(tmp_path):
    """解不开的 Cookie 交出去必须是空串：账号不能被当成可用去连闲鱼。"""
    db = Database(tmp_path / "t.db")
    await db.init()
    account_id = await db.upsert_account("主号", COOKIE)

    other = Database(tmp_path / "t.db", vault=CookieVault(tmp_path / "other.key"))
    account = await other.get_account(account_id)
    worker = AccountWorker(other, account, DeliveryEngine(other))
    with pytest.raises(Exception) as excinfo:
        await worker._once()
    assert "unb" in str(excinfo.value)


async def test_api_masks_cookie_and_exposes_cookie_error(tmp_path):
    db = Database(tmp_path / "t.db")
    await db.init()
    await db.upsert_account("主号", COOKIE)

    class StubManager:
        async def sync(self) -> None:
            pass

        async def stop_all(self) -> None:
            pass

        def status(self) -> list[dict]:
            return []

    app = create_app(db=db, manager=StubManager())
    import httpx

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
    ) as ac:
        async with app.router.lifespan_context(app):
            rows = (await ac.get("/api/accounts")).json()
            assert rows[0]["cookie"] == "***"
            assert rows[0]["cookie_error"] == ""
            assert COOKIE not in str(rows)

    # 同一份数据换把钥匙 = 后台要看到「解密失败」，而不是静悄悄的空账号
    broken = Database(tmp_path / "t.db", vault=CookieVault(tmp_path / "wrong.key"))
    app2 = create_app(db=broken, manager=StubManager())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app2), base_url="http://127.0.0.1"
    ) as ac:
        async with app2.router.lifespan_context(app2):
            row = (await ac.get("/api/accounts")).json()[0]
            # 解不开就没有 Cookie 可给：接口返回空串（不打码），但原因必须写清楚
            assert row["cookie"] == ""
            assert "解密失败" in row["cookie_error"]


def test_python_free_environment_fails_at_startup(tmp_path, monkeypatch):
    """init() 阶段就要炸：不能等第一条 Cookie 落库才发现密钥不可用。"""
    monkeypatch.setattr(secrets_mod, "CRYPTO_OK", False)
    db = Database(tmp_path / "t.db")

    import asyncio

    with pytest.raises(SecretError):
        asyncio.run(db.init())


# ---------------------------------------------------------------- 密钥放哪


def test_resolve_cookie_key_path_follows_db_when_not_configured():
    """用 XY_DB_PATH 指到临时库时，密钥要跟着那个库走，不许碰生产密钥。"""
    from dataclasses import replace
    from pathlib import Path

    from app.config import DEFAULT_DB_PATH, resolve_cookie_key_path, settings

    explicit = replace(
        settings, cookie_key_explicit=True,
        cookie_key_path=Path("/k/explicit.key"), db_path=DEFAULT_DB_PATH,
    )
    assert resolve_cookie_key_path(explicit) == Path("/k/explicit.key")

    default_db = replace(
        settings, cookie_key_explicit=False,
        cookie_key_path=Path("/k/prod.key"), db_path=DEFAULT_DB_PATH,
    )
    assert resolve_cookie_key_path(default_db) == Path("/k/prod.key")

    other_db = replace(
        settings, cookie_key_explicit=False,
        cookie_key_path=Path("/k/prod.key"), db_path=Path("/tmp/scratch.db"),
    )
    assert resolve_cookie_key_path(other_db) == Path("/tmp/scratch.db.key")
