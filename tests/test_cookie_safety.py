"""Cookie 安全：响应头/粘贴内容不能变成「请求头注入」或刷写磁盘的把手。

背景：这一版把「业务回包里的 Set-Cookie 也合并进 Cookie」打开了，等于让**上游**
往我们手里最敏感的那个字符串里写东西 —— 而这串东西之后会被原样拼进
每一个请求的 `Cookie:` 头、落库、跨重启复用。所以必须逐条校验：
名字用 RFC 6265 的 token 集合、值里不许有控制字符、单个值和整串都有长度上限。
"""

from __future__ import annotations

import httpx
import pytest

from app.client import CookieExpired, RiskControl, XianyuClient
from app.engine import DeliveryEngine
from app.qrlogin import QrLoginManager, QrSession
from app.runner import AccountWorker
from app.sign import (
    MAX_COOKIE_STRING_LEN,
    clean_cookie_pair,
    sanitize_cookie_string,
)

# ---------------------------------------------------------------- 基础清洗


@pytest.mark.parametrize("name,value,ok", [
    ("unb", "1", True),
    ("_m_h5_tk", "abc_1793000000000", True),
    ("tracknick", "普通昵称", True),
    ("ok", "value with space", True),
    ("bad name", "v", False),          # 空格不是 token 字符
    ("bad;name", "v", False),          # 分号会切断 Cookie 串
    ("bad\x01name", "v", False),       # 控制字符
    ("x" * 65, "v", False),            # 名字超长
    ("ok", "v" * 4097, False),         # 值超长
    ("ok", "a\r\nX-Evil: 1", False),   # CRLF：响应头 → 请求头注入
    ("ok", "a\nb", False),
    ("ok", "a\x00b", False),
    ("", "v", False),
])
def test_clean_cookie_pair(name, value, ok):
    got = clean_cookie_pair(name, value)
    assert (got is not None) is ok


def test_sanitize_cookie_string_drops_dangerous_pairs_and_keeps_order():
    """换行只当分隔符（浏览器复制会带），名字非法 / 值超长 / 控制字符一律丢。"""
    raw = "unb=1; good=g; bad\x01=1; nl=a\r\nb; huge=" + "A" * 5000 + "; tail=t"
    clean, dropped = sanitize_cookie_string(raw)

    # 换行被当作分隔符：nl=a 保留（值截到 a），后面的 b 没有 = 被跳过
    assert clean == "unb=1; good=g; nl=a; tail=t"
    assert "\r" not in clean and "\n" not in clean
    assert set(dropped) == {"bad\x01", "huge"}
    assert sanitize_cookie_string(clean) == (clean, [])  # 幂等


def test_sanitize_cookie_string_strips_pasted_header_name():
    """F12 里整行复制会带上 Cookie 头名，不能把它当成一个 cookie 名。"""
    clean, dropped = sanitize_cookie_string("Cookie: unb=1; _m_h5_tk=tok_1")
    assert clean == "unb=1; _m_h5_tk=tok_1"
    assert dropped == []


def test_sanitize_cookie_string_caps_total_length():
    raw = "; ".join(f"k{i}=" + "v" * 2000 for i in range(10))
    clean, dropped = sanitize_cookie_string(raw)
    assert len(clean) <= MAX_COOKIE_STRING_LEN
    assert dropped  # 丢掉的是排在后面的那些


# ---------------------------------------------------------------- 客户端入口


def test_client_refuses_cookie_with_control_characters():
    """手工粘贴常见的「复制到换行 / 粘了 Cookie: 前缀」不能带进请求头。"""
    client = XianyuClient("Cookie: unb=1\r\n; _m_h5_tk=tok_1; evil=v\r\nX-Injected: 1")
    # 换行当分隔符 + 头名被去掉：unb 还在，伪造的请求头名进不来
    assert "\r" not in client.cookie_str and "\n" not in client.cookie_str
    assert client.cookie_str.startswith("unb=1")
    assert "X-Injected" not in client.cookie_str
    # 整串里一个控制字符都不能有（否则就是个能塞进请求头的洞）
    assert all(ord(ch) >= 32 and ord(ch) != 127 for ch in client.cookie_str)


def test_client_rejects_cookie_without_unb():
    with pytest.raises(Exception) as excinfo:
        XianyuClient("just_a_name=1; _m_h5_tk=tok_1")
    assert "unb" in str(excinfo.value)


async def test_merge_set_cookie_ignores_injection_and_oversize():
    persisted: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"ret": ["SUCCESS::调用成功"]},
            headers=[
                ("set-cookie", "safe=1; Path=/"),
                ("set-cookie", "nl=a\r\nX-Evil: 1; Path=/"),
                ("set-cookie", "huge=" + "B" * 5000 + "; Path=/"),
            ],
        )

    client = XianyuClient("unb=1; _m_h5_tk=tok_1", transport=httpx.MockTransport(handler))

    async def persist(cookie: str) -> None:
        persisted.append(cookie)

    client.on_cookie_refresh = persist
    await client.mtop("mtop.demo.api", {})

    assert client.cookies["safe"] == "1"
    assert "nl" not in client.cookies
    assert "huge" not in client.cookies
    assert "\r" not in client.cookie_str
    await client.close()


async def test_cookie_string_has_a_hard_length_cap():
    """上游靠不停下发新键把整条 Cookie（以及库里的那一列）撑爆是不行的。"""
    def handler(request: httpx.Request) -> httpx.Response:
        pairs = [("set-cookie", f"k{i}=" + "v" * 900 + "; Path=/") for i in range(40)]
        return httpx.Response(200, json={"ret": ["SUCCESS::调用成功"]}, headers=pairs)

    client = XianyuClient("unb=1; _m_h5_tk=tok_1", transport=httpx.MockTransport(handler))
    await client.mtop("mtop.demo.api", {})
    assert len(client.cookie_str) <= MAX_COOKIE_STRING_LEN
    await client.close()


# ---------------------------------------------------------------- 写库节流


async def test_critical_ticket_persists_immediately():
    """cookie17 / sgcookie / unb 变了必须马上落库（丢了就得重新扫码）。"""
    requests = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        requests["n"] += 1
        return httpx.Response(
            200,
            json={"ret": ["SUCCESS::调用成功"]},
            headers={"set-cookie": f"cookie17=v{requests['n']}; Path=/"},
        )

    persisted: list[str] = []
    client = XianyuClient("unb=1; _m_h5_tk=tok_1", transport=httpx.MockTransport(handler))

    async def persist(cookie: str) -> None:
        persisted.append(cookie)

    client.on_cookie_refresh = persist

    await client.mtop("mtop.demo.api", {})
    await client.mtop("mtop.demo.api", {})  # 紧接着又来一次，也不能被节流挡住
    assert len(persisted) == 2
    await client.close()


async def test_slow_ticket_change_is_throttled_but_flushed_on_close():
    counter = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        counter["n"] += 1
        return httpx.Response(
            200,
            json={"ret": ["SUCCESS::调用成功"]},
            headers={"set-cookie": f"csg=c{counter['n']}; Path=/"},
        )

    persisted: list[str] = []
    client = XianyuClient("unb=1; _m_h5_tk=tok_1", transport=httpx.MockTransport(handler))

    async def persist(cookie: str) -> None:
        persisted.append(cookie)

    client.on_cookie_refresh = persist

    await client.mtop("mtop.demo.api", {})
    await client.mtop("mtop.demo.api", {})
    assert len(persisted) == 1        # 第二次被节流
    assert client._cookie_dirty is True

    await client.close()              # 退出前补写，不丢东西
    assert len(persisted) == 2
    assert "csg=c2" in persisted[-1]


# ---------------------------------------------------------------- 扫码登录的域名校验


async def _finalize_with(cookies: list[tuple[str, str, str]]) -> QrSession:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ret": ["SUCCESS::调用成功"]})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    for name, value, domain in cookies:
        client.cookies.set(name, value, domain=domain, path="/")
    session = QrSession(session_id="s", qr_url="https://qr", t="1", ck="2", client=client)
    session.status = "CONFIRMED"
    await QrLoginManager()._finalize(session)
    return session


async def test_qr_finalize_rejects_lookalike_domains():
    """evilgoofish.com / goofish.com.attacker.tld 这类近似域不能混进来。"""
    session = await _finalize_with([
        ("unb", "1", ".goofish.com"),
        ("good", "g", ".goofish.com"),
        ("spoof1", "x", "evilgoofish.com"),
        ("spoof2", "y", "goofish.com.attacker.tld"),
        ("third", "z", ".example.com"),
    ])
    assert "good=g" in session.cookie
    assert session.unb == "1"
    for bad in ("spoof1", "spoof2", "third"):
        assert bad not in session.cookie


# ---------------------------------------------------------------- 自检不该掐断连接


class _StubClient:
    def __init__(self, exc: Exception | None = None) -> None:
        self.exc = exc

    async def verify_login(self) -> bool:
        if self.exc is not None:
            raise self.exc
        return True


async def test_self_check_risk_control_keeps_connection(db):
    account_id = await db.upsert_account("主号", "unb=1; _m_h5_tk=k")
    worker = AccountWorker(db, await db.get_account(account_id), DeliveryEngine(db))
    worker.client = _StubClient(RiskControl("FAIL_SYS_USER_VALIDATE"))

    await worker._check_login_state()  # 不抛
    assert worker.login_state == "unknown"


async def test_self_check_cookie_expired_still_stops(db):
    account_id = await db.upsert_account("主号", "unb=1; _m_h5_tk=k")
    worker = AccountWorker(db, await db.get_account(account_id), DeliveryEngine(db))
    worker.client = _StubClient(CookieExpired("FAIL_SYS_SESSION_EXPIRED"))

    with pytest.raises(CookieExpired):
        await worker._check_login_state()


async def test_self_check_success_marks_ok(db):
    account_id = await db.upsert_account("主号", "unb=1; _m_h5_tk=k")
    worker = AccountWorker(db, await db.get_account(account_id), DeliveryEngine(db))
    worker.client = _StubClient()

    await worker._check_login_state()
    assert worker.login_state == "ok"
    assert worker.login_checked_at


# ---------------------------------------------------------------- 后台写入路径


class _NoopManager:
    """不碰网络的 WorkerManager 替身。"""

    def __init__(self) -> None:
        self.synced = 0

    async def sync(self) -> None:
        self.synced += 1

    async def stop_all(self) -> None:
        pass

    def status(self) -> list[dict]:
        return []


async def _post_cookie(tmp_path, cookie: str):
    from app.db import Database
    from app.web import create_app

    db = Database(tmp_path / "save.db")
    manager = _NoopManager()
    app = create_app(db=db, manager=manager)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1") as ac:
        async with app.router.lifespan_context(app):
            res = await ac.post("/api/accounts", json={"name": "主号", "cookie": cookie})
            return res, db


async def test_pasted_multiline_cookie_is_cleaned_before_storage(tmp_path):
    """多行 / 带 Cookie 头名的粘贴：落库前就该洗干净，别把脏串存进去。"""
    res, db = await _post_cookie(tmp_path, "Cookie: unb=1\r\n; _m_h5_tk=abc_1\n")
    assert res.status_code == 200

    row = await db.get_account(res.json()["id"])
    assert "\r" not in row["cookie"] and "\n" not in row["cookie"]
    assert row["cookie"].startswith("unb=1")
    # 粘贴成功也算一次真正登录，预警计时从这一刻开始
    assert await db.get_login_at(res.json()["id"])


async def test_pasted_cookie_without_unb_is_rejected(tmp_path):
    res, _db = await _post_cookie(tmp_path, "_m_h5_tk=abc_1")
    assert res.status_code == 400
    assert "unb" in res.json()["detail"]
