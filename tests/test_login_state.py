"""登录态相关的行为锁定：过期判定 / 保活不假成功 / 业务自检 / 到期预警。

背景（线上实测结论，别改回去）：
  * 闲鱼网页登录态是按「登录时刻」算 TTL 的，实测 **24 小时整**就到期，
    期间订单轮询 30 秒一次、_m_h5_tk 刷新 1.5~2.5 小时一次，都延长不了它；
  * passport 的 hasLogin.do **判断不了登录态**：有效 Cookie 和垃圾 Cookie
    （unb=12345678; cookie2=garbage）返回的是同一行
      {"titleMsg":"您的登录态已失效，请重新登录","resultCode":100,...,"hasError":false}
    而且永远没有 Set-Cookie（续不了期）。老代码既认不出它、又把它当成功。
所以：保活只负责合并 Cookie；「还活着吗」只认业务接口（verify_login）。
"""

from __future__ import annotations

import time

import httpx
import pytest

from app.client import (
    CookieExpired,
    XianyuClient,
    _has_login_true,
    _is_session_expired,
    cookie_flags,
)
from app.db import Database, now_iso
from app.engine import DeliveryEngine
from app.qrlogin import QrLoginManager, QrSession
from app.runner import AccountWorker, WorkerManager, _parse_iso_epoch
from app.web import create_app

#: hasLogin.do 对**任何** Cookie 都回这一行（线上原文）
PASSPORT_STUB = (
    '{"content":{"data":{"titleMsg":"您的登录态已失效，请重新登录",'
    '"resultCode":100,"changeView":"default"},"status":0,"success":true},'
    '"hasError":false}'
)


def make_client(handler) -> XianyuClient:
    return XianyuClient("unb=1; _m_h5_tk=tok_1", transport=httpx.MockTransport(handler))


# ---------------------------------------------------------------- 过期判定


def test_session_expired_recognises_passport_wording():
    # passport 说的是「登录态已失效」，mtop 说的是「登录已失效」，两套都得认
    assert _is_session_expired("您的登录态已失效，请重新登录") is True
    assert _is_session_expired("登录已失效") is True
    assert _is_session_expired("FAIL_SYS_SESSION_EXPIRED::Session过期") is True
    assert _is_session_expired("SUCCESS::调用成功") is False
    assert _is_session_expired("FAIL_SYS_TOKEN_EXOIRED::令牌过期") is False


def test_has_login_true_needs_an_explicit_true():
    assert _has_login_true('{"hasLogin":true}') is True
    assert _has_login_true('{"loginResult":"success"}') is True
    # 这句是「没有结论」，不是「有效」
    assert _has_login_true(PASSPORT_STUB) is False
    assert _has_login_true('{"hasLogin":false}') is False


def test_cookie_flags_spots_missing_long_lived_ticket():
    with_ticket = cookie_flags("unb=1; cookie2=c; sgcookie=s; cookie17=t; _tb_token_=tt; csg=x; _m_h5_tk=k; cna=n")
    assert with_ticket["has_cookie17"] is True
    assert with_ticket["missing"] == []

    without = cookie_flags("unb=1; sgcookie=s")
    assert without["has_cookie17"] is False
    assert "cookie17" in without["missing"]
    assert "names" in without


# ---------------------------------------------------------------- 保活


async def test_keepalive_stub_neither_succeeds_nor_fakes_expiry():
    """有效账号也会拿到这句「登录态已失效」——不能当成功，更不能当过期。"""
    client = make_client(lambda request: httpx.Response(200, text=PASSPORT_STUB))
    assert await client.keepalive() is False  # 没有结论
    assert client.cookie_str.startswith("unb=1")  # 也没被改坏
    await client.close()


async def test_keepalive_merges_cookies_when_server_sends_them():
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            text='{"hasLogin":true}',
            headers={"set-cookie": "cookie17=long-lived; Path=/; Domain=.taobao.com"},
        )

    client = make_client(handler)

    async def persist(cookie: str) -> None:
        seen.append(cookie)

    client.on_cookie_refresh = persist

    assert await client.keepalive() is True
    assert "cookie17=long-lived" in client.cookie_str
    assert seen and "cookie17=long-lived" in seen[0]
    await client.close()


# ---------------------------------------------------------------- 业务自检


async def test_verify_login_passes_on_success():
    client = make_client(lambda request: httpx.Response(200, json={"ret": ["SUCCESS::调用成功"]}))
    assert await client.verify_login() is True
    await client.close()


async def test_verify_login_raises_cookie_expired_on_session_expired():
    client = make_client(
        lambda request: httpx.Response(200, json={"ret": ["FAIL_SYS_SESSION_EXPIRED::Session过期"]})
    )
    with pytest.raises(CookieExpired):
        await client.verify_login()
    await client.close()


# ---------------------------------------------------------------- Cookie 续期


async def test_mtop_merges_set_cookie_from_business_response():
    """业务回包也会下发续期票；老实现只在不相关的两条支路里合并，全扔了。"""
    persisted: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"ret": ["SUCCESS::调用成功"]},
            headers={"set-cookie": "cookie2=rotated; Path=/; Domain=.goofish.com"},
        )

    client = make_client(handler)

    async def persist(cookie: str) -> None:
        persisted.append(cookie)

    client.on_cookie_refresh = persist

    await client.mtop("mtop.demo.api", {})
    assert client.cookies["cookie2"] == "rotated"
    assert "cookie2=rotated" in client.cookie_str
    assert persisted, "非签名票变化必须写回数据库，否则重启又用旧票"
    await client.close()


async def test_token_only_rotation_is_persisted_at_most_once_per_gap():
    """_m_h5_tk 几乎每个回包都会续一次，不能变成 30 秒一次的写库。"""
    persisted: list[str] = []
    counter = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        counter["n"] += 1
        return httpx.Response(
            200,
            json={"ret": ["SUCCESS::调用成功"]},
            headers={"set-cookie": f"_m_h5_tk=tok_{counter['n']}_1793000000000; Path=/"},
        )

    client = make_client(handler)

    async def persist(cookie: str) -> None:
        persisted.append(cookie)

    client.on_cookie_refresh = persist

    await client.mtop("mtop.demo.api", {})
    await client.mtop("mtop.demo.api", {})
    assert "tok_2" in client.cookies["_m_h5_tk"]  # 内存里始终是最新值
    assert len(persisted) == 1  # 但只写了一次库
    await client.close()


# ---------------------------------------------------------------- 扫码登录


async def test_qr_finalize_keeps_taobao_domain_cookies():
    """cookie17（保持登录票据）下发在 .taobao.com，不能因为域名过滤被丢掉。"""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ret": ["SUCCESS::调用成功"]})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client.cookies.set("unb", "4167468834", domain=".goofish.com", path="/")
    client.cookies.set("sgcookie", "sg-value", domain=".goofish.com", path="/")
    client.cookies.set("cookie17", "long-lived", domain=".taobao.com", path="/")
    client.cookies.set("third_party", "x", domain=".example.com", path="/")

    session = QrSession(session_id="s1", qr_url="https://qr", t="1", ck="2", client=client)
    session.status = "CONFIRMED"
    await QrLoginManager()._finalize(session)

    assert "cookie17=long-lived" in session.cookie
    assert "sgcookie=sg-value" in session.cookie
    assert "third_party" not in session.cookie
    assert session.unb == "4167468834"


# ---------------------------------------------------------------- 到期预警


def test_parse_iso_epoch_handles_junk():
    assert _parse_iso_epoch("2026-10-28 12:52:52") > 0
    assert _parse_iso_epoch(None) == 0
    assert _parse_iso_epoch("") == 0
    assert _parse_iso_epoch("不是时间") == 0


async def test_relogin_warning_fires_after_warn_hours(db):
    account_id = await db.upsert_account("主号", "unb=1; sgcookie=s; _m_h5_tk=k")
    account = await db.get_account(account_id)
    worker = AccountWorker(db, account, DeliveryEngine(db))

    worker.login_at = time.time() - 25 * 3600  # 已经超过 24 小时
    worker._warn_relogin_if_due()
    assert worker.relogin_due is True

    report = worker.login_report()
    assert report["relogin_due"] is True
    assert report["has_cookie17"] is False
    assert "cookie17" in report["missing_cookies"]
    assert report["login_age_hours"] >= 24

    worker.login_at = time.time() - 3600  # 刚登录一小时
    worker._warn_relogin_if_due()
    assert worker.relogin_due is False
    assert worker.login_report()["login_age_hours"] < 24


async def test_record_login_is_stored_separately_from_cookie_refresh(db):
    account_id = await db.upsert_account("主号", "unb=1; _m_h5_tk=k")
    assert await db.get_login_at(account_id) in (None, "")

    stamp = await db.record_login(account_id)
    assert stamp[:4].isdigit()
    assert await db.get_login_at(account_id) == stamp

    # 刷新 Cookie 不能把「真实登录时刻」顶掉，否则预警永远不响
    await db.update_account_cookie(account_id, "unb=1; _m_h5_tk=new")
    assert await db.get_login_at(account_id) == stamp
    assert now_iso()[:4].isdigit()

async def test_missing_login_record_starts_clock_now_not_from_created_at(db):
    """老账号没有登录时间记录时，不能拿建号时间当真 —— 否则新登录的号会立刻误报。"""
    account_id = await db.upsert_account("老号", "unb=1; sgcookie=s")
    account = await db.get_account(account_id)
    account["created_at"] = "2026-01-01 00:00:00"  # 建号很久以前

    worker = AccountWorker(db, account, DeliveryEngine(db))
    await worker._load_login_meta()

    assert time.time() - worker.login_at < 60  # 从现在开始计时
    assert worker.relogin_due is False
    assert await db.get_login_at(account_id) is not None  # 基线已落库，重启不重置

# ---------------------------------------------------------------- 后台接口


class StubManager:
    """完全不碰网络的 WorkerManager 替身（只测 Web 层的字段）。"""

    def __init__(self) -> None:
        self.synced = 0

    async def sync(self) -> None:
        self.synced += 1

    async def stop_all(self) -> None:
        pass

    def status(self) -> list[dict]:
        return []


async def test_accounts_api_reports_cookie17_and_login_time(tmp_path):
    db = Database(tmp_path / "login.db")
    app = create_app(db=db, manager=StubManager())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1") as ac:
        async with app.router.lifespan_context(app):
            await ac.post("/api/accounts", json={
                "name": "主号",
                "cookie": "unb=1; sgcookie=s; cookie17=long; _m_h5_tk=k",
            })
            rows = (await ac.get("/api/accounts")).json()
            assert rows[0]["has_cookie17"] is True
            assert "cookie17" not in rows[0]["missing_cookies"]
            assert rows[0]["login_at"]

            # 只改备注不能把「上次真正登录的时间」顶掉，否则预警永远不响
            first_login_at = rows[0]["login_at"]
            await ac.post("/api/accounts", json={"name": "主号", "cookie": "", "remark": "改个备注"})
            again = (await ac.get("/api/accounts")).json()
            assert again[0]["login_at"] == first_login_at


def test_manager_status_surfaces_login_report():
    """WorkerManager.status() 必须把登录态摊平给前端（否则界面无从提示）。"""
    class FakeWorker:
        def __init__(self) -> None:
            self.name = "主号"
            self.status = "online"
            self.last_error = ""
            self.account = {"cookie": "unb=1; sgcookie=s"}

        def login_report(self) -> dict:
            return {
                "login_state": "ok",
                "login_checked_at": "2026-10-28 12:00:00",
                "login_at": "2026-10-27 12:52:52",
                "login_age_hours": 25.0,
                "relogin_due": True,
                "has_cookie17": False,
                "missing_cookies": ["cookie17"],
            }

    db = Database(":memory:")
    manager = WorkerManager(db)
    manager.workers[1] = FakeWorker()  # type: ignore[assignment]

    row = manager.status()[0]
    assert row["login_state"] == "ok"
    assert row["relogin_due"] is True
    assert row["has_cookie17"] is False
    assert row["login_age_hours"] == 25.0
