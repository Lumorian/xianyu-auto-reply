"""配置容错与管理接口鉴权。

两条都是「默认应该安全」的性质：
* 一个写错的环境变量不该让进程在启动时崩掉；
* 管理接口不该在没配口令时对任何人敞开。
"""

from __future__ import annotations

import httpx

from app.config import BASE_DIR, Settings


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


def test_db_path_empty_or_relative_is_resolved(monkeypatch):
    """XY_DB_PATH= 不能变成 Path(".")；相对路径按项目根目录解释。"""
    monkeypatch.setenv("XY_DB_PATH", "")
    assert Settings().db_path == BASE_DIR / "data" / "app.db"

    monkeypatch.setenv("XY_DB_PATH", "./data/x.db")
    assert Settings().db_path == BASE_DIR / "data" / "x.db"


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
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1") as ac:
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


async def test_forwarded_header_cannot_fake_loopback_without_token(tmp_path, monkeypatch):
    """同机反代会把 peer 变成 127.0.0.1；带了代理头就不能再走本机兜底。"""
    app, _ = make_app(tmp_path, monkeypatch)
    res = await call(
        app, "127.0.0.1", "GET", "/api/orders",
        headers={"X-Forwarded-For": "203.0.113.9"},
    )
    assert res.status_code == 401
    assert "XY_ADMIN_TOKEN" in res.json()["detail"]


async def test_proxied_public_host_cannot_use_loopback_fallback(tmp_path, monkeypatch):
    """反代保留公网域名 Host、又剥掉所有代理头时，peer 仍是 127.0.0.1。

    这种请求不能让「本机无口令」兜底放行：Host 不是本机名字就按远程处理。
    """
    monkeypatch.setattr("app.config.settings.allowed_hosts", "panel.example.com", raising=False)
    app, _ = make_app(tmp_path, monkeypatch)
    res = await call(
        app, "127.0.0.1", "GET", "/api/orders",
        headers={"Host": "panel.example.com"},
    )
    assert res.status_code == 401
    assert "XY_ADMIN_TOKEN" in res.json()["detail"]


async def test_lookalike_loopback_host_is_not_trusted(tmp_path, monkeypatch):
    """127.evil.com 这种「以 127. 开头」的域名不能被当成回环地址。"""
    monkeypatch.setattr("app.config.settings.allowed_hosts", "127.evil.com", raising=False)
    app, _ = make_app(tmp_path, monkeypatch)
    res = await call(
        app, "127.0.0.1", "GET", "/api/orders",
        headers={"Host": "127.evil.com"},
    )
    assert res.status_code == 401
    assert "XY_ADMIN_TOKEN" in res.json()["detail"]


async def test_localhost_host_header_still_uses_loopback_fallback(tmp_path, monkeypatch):
    """本机用 localhost / 127.0.0.1 访问仍免口令，不影响单机使用。"""
    app, _ = make_app(tmp_path, monkeypatch)
    for host in ("localhost", "127.0.0.1:8000"):
        res = await call(app, "127.0.0.1", "GET", "/api/orders", headers={"Host": host})
        assert res.status_code == 200


async def test_oversized_content_length_is_rejected_before_auth(tmp_path, monkeypatch):
    """鉴权依赖之前就会读 body；超大 Content-Length 必须直接 413。"""
    monkeypatch.setattr("app.config.settings.max_body_mb", 1, raising=False)
    app, _ = make_app(tmp_path, monkeypatch)
    res = await call(
        app, "203.0.113.9", "POST", "/api/accounts",
        headers={"Content-Length": str(2 * 1024 * 1024)},
        content=b"{}",
    )
    assert res.status_code == 413
    assert "请求体太大" in res.json()["detail"]


async def test_oversized_streamed_body_is_rejected(tmp_path, monkeypatch):
    """chunked 请求没有 Content-Length，也要在流式读取时按累计字节拦住。"""
    monkeypatch.setattr("app.config.settings.max_body_mb", 1, raising=False)
    app, _ = make_app(tmp_path, monkeypatch)

    async def body():
        yield b"x" * (600 * 1024)
        yield b"x" * (600 * 1024)

    res = await call(app, "203.0.113.9", "POST", "/api/accounts", content=body())
    assert res.status_code == 413
    assert "请求体太大" in res.json()["detail"]


async def test_allow_remote_without_token_is_refused(tmp_path, monkeypatch):
    """ALLOW_REMOTE 只放宽来源、不校验口令：没口令时它不能生效（否则等于全开）。"""
    app, _ = make_app(tmp_path, monkeypatch, allow_remote=True)
    res = await call(app, "203.0.113.9", "GET", "/api/orders")
    assert res.status_code == 401
    assert "XY_ADMIN_TOKEN" in res.json()["detail"]

    # 配上口令之后，这个开关才有意义：远程带对口令可以进
    app2, _ = make_app(tmp_path, monkeypatch, token="s3cret", allow_remote=True)
    ok = await call(app2, "203.0.113.9", "GET", "/api/orders",
                    headers={"X-Admin-Token": "s3cret"})
    assert ok.status_code == 200


# ------------------------------------------- Host / CSRF / 限流 / 响应头


async def test_foreign_host_header_is_rejected(tmp_path, monkeypatch):
    """DNS rebinding：恶意域名解析到 127.0.0.1 时，Host 不是本机名字 → 拒绝。"""
    app, _ = make_app(tmp_path, monkeypatch)
    res = await call(app, "127.0.0.1", "GET", "/api/orders", headers={"Host": "evil.example"})
    assert res.status_code == 400
    assert "XY_ALLOWED_HOSTS" in res.json()["detail"]


async def test_loopback_and_ip_hosts_are_accepted(tmp_path, monkeypatch):
    """回环名字、配置的 host、以及任何纯 IP 写法都要放行（局域网用 IP 访问很常见）。"""
    app, _ = make_app(tmp_path, monkeypatch)
    for host in ("127.0.0.1:8000", "localhost:8000", "10.0.0.5:8000", "[::1]:8000"):
        res = await call(app, "127.0.0.1", "GET", "/healthz", headers={"Host": host})
        assert res.status_code == 200, host


async def test_allowed_hosts_setting_whitelists_a_domain(tmp_path, monkeypatch):
    """挂反代用域名访问时，把域名写进 XY_ALLOWED_HOSTS 就能过 Host 校验。"""
    monkeypatch.setattr("app.config.settings.allowed_hosts", "panel.example.com", raising=False)
    app, _ = make_app(tmp_path, monkeypatch)
    res = await call(app, "127.0.0.1", "GET", "/healthz", headers={"Host": "panel.example.com"})
    assert res.status_code == 200


async def test_cross_site_write_is_rejected(tmp_path, monkeypatch):
    """CSRF：浏览器标注为 cross-site 的写请求直接拒（表单 POST 不需要预检）。"""
    app, _ = make_app(tmp_path, monkeypatch)
    res = await call(
        app, "127.0.0.1", "POST", "/api/accounts/999/toggle?enabled=false",
        headers={"Sec-Fetch-Site": "cross-site"},
    )
    assert res.status_code == 403


async def test_cross_origin_write_is_rejected(tmp_path, monkeypatch):
    app, _ = make_app(tmp_path, monkeypatch)
    res = await call(
        app, "127.0.0.1", "POST", "/api/accounts/999/toggle?enabled=false",
        headers={"Origin": "https://evil.example"},
    )
    assert res.status_code == 403


async def test_same_origin_write_is_allowed(tmp_path, monkeypatch):
    """本站发起的写请求不能被误伤（这里靠 404 证明已经走到业务逻辑）。"""
    app, _ = make_app(tmp_path, monkeypatch)
    res = await call(
        app, "127.0.0.1", "POST", "/api/accounts/999/toggle?enabled=false",
        headers={"Origin": "http://127.0.0.1", "Sec-Fetch-Site": "same-origin"},
    )
    assert res.status_code == 404


async def test_cross_site_get_with_side_effects_is_rejected(tmp_path, monkeypatch):
    """GET 也可能建账号/同步聊天/打上游：跨站页面不能借浏览器触发。"""
    app, _ = make_app(tmp_path, monkeypatch)
    paths = (
        "/api/config",              # 会按 TTL 请求 new-api /api/status
        "/api/newapi/tokens",       # 会请求 new-api
        "/api/accounts/1/items",    # 会请求闲鱼并写 item_titles
        "/api/login/nope/poll",     # 确认后会建账号并启动监听
    )
    for path in paths:
        res = await call(app, "127.0.0.1", "GET", path,
                         headers={"Sec-Fetch-Site": "cross-site"})
        assert res.status_code == 403, path
        assert res.json()["detail"] == "跨站请求被拒绝"


async def test_top_level_navigation_cannot_trigger_side_effect_get(tmp_path, monkeypatch):
    """地址栏/书签打开是 Sec-Fetch-Site: none；攻击者也能用顶层导航触发副作用。"""
    app, _ = make_app(tmp_path, monkeypatch)
    res = await call(
        app, "127.0.0.1", "GET", "/api/config",
        headers={"Sec-Fetch-Site": "none"},
    )
    assert res.status_code == 403
    assert res.json()["detail"] == "跨站请求被拒绝"


async def test_cross_origin_get_with_side_effects_is_rejected(tmp_path, monkeypatch):
    app, _ = make_app(tmp_path, monkeypatch)
    res = await call(
        app, "127.0.0.1", "GET", "/api/config",
        headers={"Origin": "https://evil.example"},
    )
    assert res.status_code == 403


async def test_referer_is_checked_when_origin_is_absent(tmp_path, monkeypatch):
    """老浏览器不发 Origin 时，Referer 也要能挡住跨站 GET。"""
    app, _ = make_app(tmp_path, monkeypatch)
    bad = await call(
        app, "127.0.0.1", "GET", "/api/accounts/1/items",
        headers={"Referer": "https://evil.example/page"},
    )
    assert bad.status_code == 403
    good = await call(
        app, "127.0.0.1", "GET", "/api/accounts/1/items",
        headers={"Referer": "http://127.0.0.1/some/page"},
    )
    assert good.status_code == 404  # 走到业务逻辑：账号不存在


async def test_same_origin_get_with_side_effects_is_allowed(tmp_path, monkeypatch):
    app, _ = make_app(tmp_path, monkeypatch)
    res = await call(
        app, "127.0.0.1", "GET", "/api/accounts/1/items",
        headers={"Origin": "http://127.0.0.1", "Sec-Fetch-Site": "same-origin"},
    )
    assert res.status_code == 404  # 过了同源检查，账号不存在


async def test_security_headers_are_present(tmp_path, monkeypatch):
    app, _ = make_app(tmp_path, monkeypatch)
    page = await call(app, "127.0.0.1", "GET", "/")
    assert page.headers["X-Frame-Options"] == "DENY"
    assert page.headers["X-Content-Type-Options"] == "nosniff"
    assert page.headers["Referrer-Policy"] == "no-referrer"
    assert "frame-ancestors 'none'" in page.headers["Content-Security-Policy"]

    api = await call(app, "127.0.0.1", "GET", "/api/orders")
    assert api.headers["Cache-Control"] == "no-store", "卡密/订单不能留在浏览器缓存里"


async def test_admin_token_bruteforce_is_rate_limited(tmp_path, monkeypatch):
    """口令错误不能无限猜：10 次/分钟之后直接 429。"""
    app, _ = make_app(tmp_path, monkeypatch, token="s3cret")
    for _ in range(10):
        res = await call(app, "127.0.0.1", "GET", "/api/orders",
                         headers={"X-Admin-Token": "guess"})
        assert res.status_code == 401

    blocked = await call(app, "127.0.0.1", "GET", "/api/orders",
                         headers={"X-Admin-Token": "guess"})
    assert blocked.status_code == 429
    assert int(blocked.headers["Retry-After"]) > 0


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


def test_non_loopback_bind_without_token_is_rejected():
    """监听 0.0.0.0 且没配口令时不能启动：反代会把远程请求变成本机。"""
    from main import validate_admin_binding

    assert validate_admin_binding("127.0.0.1", "") is None
    assert validate_admin_binding("localhost", "") is None
    assert validate_admin_binding("127.0.0.5", "") is None
    bind_all = "0.0.0.0"  # noqa: S104 - 只是字符串，不涉及 bind
    assert validate_admin_binding(bind_all, "") is not None
    assert validate_admin_binding(bind_all, "s3cret") is None
    assert validate_admin_binding("panel.example.com", "") is not None


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
