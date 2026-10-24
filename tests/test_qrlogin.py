"""扫码登录与二维码渲染的单元测试（httpx 打桩，不触网）。"""

from __future__ import annotations

import httpx
import pytest

from app.qrlogin import LoginError, QrLoginManager, QrSession, render_qr_png


def make_manager(handler):
    """用打桩 transport 组一个 QrLoginManager，完全不碰网络。"""
    return QrLoginManager(
        client_factory=lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(handler), follow_redirects=True
        )
    )


def base_handler(extra=None):
    """共用：cna / XSRF / 二维码生成。extra 可覆盖特定路径。"""

    def handler(request):
        path = request.url.path
        if extra and path in extra:
            return extra[path](request)
        if path == "/eg.js":
            return httpx.Response(200, text="ok", headers={"set-cookie": "cna=abc123; Path=/"})
        if path == "/mini_login.htm":
            return httpx.Response(200, text="", headers={"set-cookie": "XSRF-TOKEN=csrf-token; Path=/"})
        if path.endswith("/generate.do"):
            return httpx.Response(200, json={"content": {"data": {
                "codeContent": "https://passport.goofish.com/qrcodeCheck.htm?lgToken=xyz",
                "t": "1234567890123",
                "ck": "abcdef",
            }}})
        return httpx.Response(200, json={})

    return handler


async def test_start_returns_qr_url():
    manager = make_manager(base_handler())
    try:
        session = await manager.start("s1")
    finally:
        await manager.close()

    assert isinstance(session, QrSession)
    assert "qrcodeCheck" in session.qr_url
    assert session.t == "1234567890123"
    assert session.ck == "abcdef"
    assert session.status == "NEW"
    assert session.done is False


async def test_start_raises_when_passport_returns_garbage():
    def handler(request):
        if request.url.path.endswith("/generate.do"):
            return httpx.Response(200, json={"content": {}})
        return httpx.Response(200, json={})

    manager = make_manager(handler)
    with pytest.raises(LoginError):
        await manager.start("s2")
    await manager.close()


async def test_poll_unknown_session_raises():
    manager = QrLoginManager()
    with pytest.raises(LoginError):
        await manager.poll("nope")


async def test_poll_confirmed_builds_cookie():
    """确认扫码后要能拼出完整 Cookie（含 unb 与 _m_h5_tk）。"""

    def query(request):
        return httpx.Response(
            200,
            json={"content": {"data": {"qrCodeStatus": "CONFIRMED", "token": "lg-token-1"}}},
            headers={"set-cookie": "unb=2000000000002; Domain=.goofish.com; Path=/"},
        )

    def nav(request):
        # 真实流程里登录后请求这个接口，服务端会下发新的 _m_h5_tk
        return httpx.Response(
            200,
            json={"ret": ["SUCCESS::调用成功"]},
            headers=[
                ("set-cookie", "_m_h5_tk=abcdef123456_1792737000000; Domain=.goofish.com; Path=/"),
                ("set-cookie", "tracknick=wang; Domain=.goofish.com; Path=/"),
            ],
        )

    manager = make_manager(base_handler({
        "/newlogin/qrcode/query.do": query,
        "/h5/mtop.idle.web.user.page.nav/1.0/": nav,
    }))
    try:
        await manager.start("s3")
        session = await manager.poll("s3")
    finally:
        await manager.close()

    assert session.status == "CONFIRMED"
    assert session.unb == "2000000000002"
    assert session.nick == "wang"
    assert "_m_h5_tk=" in session.cookie
    assert "unb=2000000000002" in session.cookie


async def test_poll_confirmed_without_h5_tk_reports_error():
    """登录成功但拿不到 _m_h5_tk 时必须报错，不能假装成功。"""

    def query(request):
        return httpx.Response(
            200,
            json={"content": {"data": {"qrCodeStatus": "CONFIRMED", "token": "lg"}}},
            headers={"set-cookie": "unb=2000000000002; Domain=.goofish.com; Path=/"},
        )

    manager = make_manager(base_handler({"/newlogin/qrcode/query.do": query}))
    try:
        await manager.start("s6")
        session = await manager.poll("s6")
    finally:
        await manager.close()

    assert session.status == "ERROR"
    assert "_m_h5_tk" in session.error


async def test_poll_reports_expired():
    def query(request):
        return httpx.Response(200, json={"content": {"data": {"qrCodeStatus": "EXPIRED"}}})

    manager = make_manager(base_handler({"/newlogin/qrcode/query.do": query}))
    try:
        await manager.start("s4")
        session = await manager.poll("s4")
    finally:
        await manager.close()

    assert session.status == "EXPIRED"
    assert session.done is True


async def test_poll_scanned_keeps_polling():
    def query(request):
        return httpx.Response(200, json={"content": {"data": {"qrCodeStatus": "SCANNED"}}})

    manager = make_manager(base_handler({"/newlogin/qrcode/query.do": query}))
    try:
        await manager.start("s5")
        session = await manager.poll("s5")
    finally:
        await manager.close()

    assert session.status == "SCANNED"
    assert session.done is False


def test_render_qr_png_is_real_png():
    data = render_qr_png("https://example.com/abc")
    assert data[:8] == b"\x89PNG\r\n\x1a\n"
    assert len(data) > 200


def test_status_text_covers_all_states():
    from app.qrlogin import STATUS_TEXT

    for state in ("NEW", "SCANNED", "CONFIRMED", "EXPIRED"):
        assert state in STATUS_TEXT
