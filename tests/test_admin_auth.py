"""配置容错与管理接口鉴权。

两条都是「默认应该安全」的性质：
* 一个写错的环境变量不该让进程在启动时崩掉；
* 管理接口不该在没配口令时对任何人敞开。
"""

from __future__ import annotations

import httpx

from app.config import Settings


def test_bad_int_env_falls_back_instead_of_crashing(monkeypatch):
    """XY_PORT=abc 以前会让整个进程在 import 期抛 ValueError。"""
    monkeypatch.setenv("XY_PORT", "abc")
    monkeypatch.setenv("XY_ORDER_POLL_INTERVAL", "三十分钟")
    monkeypatch.setenv("XY_DELIVERY_COOLDOWN", "")
    settings = Settings()
    assert settings.port == 8000
    assert settings.order_poll_interval == 30
    assert settings.delivery_cooldown == 600


def test_valid_int_env_is_honoured(monkeypatch):
    monkeypatch.setenv("XY_PORT", "9001")
    monkeypatch.setenv("XY_ORDER_POLL_INTERVAL", "7")
    settings = Settings()
    assert settings.port == 9001
    assert settings.order_poll_interval == 7


# ------------------------------------------------------------ 管理接口鉴权


class StubManager:
    def __init__(self, db):
        self.db = db

    async def sync(self):
        pass

    async def stop_all(self):
        pass

    def status(self):
        return []


def make_app(tmp_path, monkeypatch, *, token="", allow_remote=False, client_host="127.0.0.1"):
    from app.db import Database
    from app.web import create_app

    monkeypatch.setattr("app.config.settings.admin_token", token, raising=False)
    monkeypatch.setattr("app.config.settings.admin_allow_remote", allow_remote, raising=False)
    db = Database(tmp_path / "auth.db")
    app = create_app(db=db, manager=StubManager(db))
    return app, client_host


async def call(app, client_host, method, path, **kwargs):
    # raise_app_exceptions=False：让 500 走我们自己的 handler，而不是在测试里炸开
    transport = httpx.ASGITransport(app=app, client=(client_host, 5555),
                                    raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        async with app.router.lifespan_context(app):
            return await ac.request(method, path, **kwargs)


async def test_remote_is_rejected_when_no_token_configured(tmp_path, monkeypatch):
    """没配口令 + 外网来源 → 一律 401（以前是全开）。"""
    app, _ = make_app(tmp_path, monkeypatch)
    res = await call(app, "203.0.113.9", "GET", "/api/orders")
    assert res.status_code == 401
    assert "XY_ADMIN_TOKEN" in res.json()["detail"]


async def test_loopback_still_works_without_token(tmp_path, monkeypatch):
    """本机访问维持原行为，不影响单机使用。"""
    app, _ = make_app(tmp_path, monkeypatch)
    res = await call(app, "127.0.0.1", "GET", "/api/orders")
    assert res.status_code == 200


async def test_remote_allowed_when_explicitly_enabled(tmp_path, monkeypatch):
    app, _ = make_app(tmp_path, monkeypatch, allow_remote=True)
    res = await call(app, "203.0.113.9", "GET", "/api/orders")
    assert res.status_code == 200


async def test_token_is_enforced_everywhere(tmp_path, monkeypatch):
    app, _ = make_app(tmp_path, monkeypatch, token="s3cret")
    assert (await call(app, "127.0.0.1", "GET", "/api/orders")).status_code == 401
    ok = await call(app, "127.0.0.1", "GET", "/api/orders",
                    headers={"X-Admin-Token": "s3cret"})
    assert ok.status_code == 200
    bearer = await call(app, "127.0.0.1", "GET", "/api/orders",
                        headers={"Authorization": "Bearer s3cret"})
    assert bearer.status_code == 200


async def test_qrcode_endpoint_also_requires_token(tmp_path, monkeypatch):
    """这个接口以前漏了 guard，任何人都能凭 session_id 拉二维码。"""
    app, _ = make_app(tmp_path, monkeypatch, token="s3cret")
    res = await call(app, "127.0.0.1", "GET", "/api/login/whatever/qrcode")
    assert res.status_code == 401


async def test_non_ascii_admin_token_still_answers_401(tmp_path, monkeypatch):
    """口令里带中文时不能 500：hmac.compare_digest 不接受非 ASCII 的 str。

    以前这里会 TypeError —— 于是任何一个请求都变成 500（配了中文口令直接锁死后台）。
    """
    app, _ = make_app(tmp_path, monkeypatch, token="中文口令")
    res = await call(app, "127.0.0.1", "GET", "/api/orders",
                     headers={"X-Admin-Token": "wrong"})
    assert res.status_code == 401
    assert res.json()["detail"] == "管理口令错误"


def test_uvicorn_does_not_trust_forwarded_headers():
    """uvicorn 默认 proxy_headers=True 会让 X-Forwarded-For 冒充本机访问。"""
    from main import uvicorn_run_kwargs

    kwargs = uvicorn_run_kwargs()
    assert kwargs["proxy_headers"] is False


async def test_internal_error_does_not_leak_details(tmp_path, monkeypatch):
    """500 不能把内部异常原文回给前端。"""
    app, _ = make_app(tmp_path, monkeypatch)

    @app.get("/boom")
    async def boom():
        raise RuntimeError("数据库连接串 postgres://user:pass@host/db 打不开")

    res = await call(app, "127.0.0.1", "GET", "/boom")
    assert res.status_code == 500
    text = res.text
    assert "postgres://" not in text
    assert "request_id" in text
