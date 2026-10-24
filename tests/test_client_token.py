"""IM token 获取 / _m_h5_tk 刷新 / 风控分类的单元测试（全部离线）。

背景：线上日志里出现过
    FAIL_SYS_USER_VALIDATE / RGV587_ERROR::SM::哎哟喂,被挤爆啦
旧实现把它当成 Cookie 失效直接停掉 worker，用户只能反复扫码。
这些测试锁住三条行为：
1. token 接口必须带浏览器 Origin/Referer；
2. 风控响应归类为 RiskControl，不能是 CookieExpired；
3. 令牌过期时按三步流程刷新 _m_h5_tk 后重试。
"""

from __future__ import annotations

import httpx
import pytest

from app.client import CookieExpired, RiskControl, TokenExpired, XianyuClient
from app.sign import generate_device_id


def test_device_id_is_stable_per_account():
    first = generate_device_id("2000000000002")
    second = generate_device_id("2000000000002")
    other = generate_device_id("4100000003")

    assert first == second
    assert first != other
    assert first.endswith("-2000000000002")
    assert len(first.rsplit("-", 1)[0]) == 36


async def test_mtop_sends_browser_origin_and_referer():
    seen: dict[str, str | None] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["origin"] = request.headers.get("origin")
        seen["referer"] = request.headers.get("referer")
        seen["cookie"] = request.headers.get("cookie")
        return httpx.Response(200, json={"ret": ["SUCCESS::调用成功"]})

    client = XianyuClient(
        "unb=1; _m_h5_tk=tok_1",
        transport=httpx.MockTransport(handler),
    )
    await client.mtop("mtop.demo.api", {"x": 1})

    assert seen["origin"] == "https://www.goofish.com"
    assert seen["referer"] == "https://www.goofish.com/"
    assert "unb=1" in (seen["cookie"] or "")
    await client.close()


async def test_risk_control_is_not_cookie_expired():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"ret": ["FAIL_SYS_USER_VALIDATE", "RGV587_ERROR::SM::哎哟喂,被挤爆啦,请稍后重试"]},
        )

    client = XianyuClient("unb=1; _m_h5_tk=tok_1", transport=httpx.MockTransport(handler))
    with pytest.raises(RiskControl):
        await client.get_im_token()
    # 关键：不能是 CookieExpired，否则 worker 会永久停止，用户只能反复扫码
    assert not issubclass(RiskControl, CookieExpired)
    await client.close()


async def test_token_expired_refreshes_m_h5_tk_and_retries():
    token_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal token_calls
        path = request.url.path
        if path.endswith("/mtop.taobao.idlemessage.pc.login.token/1.0/"):
            token_calls += 1
            if token_calls == 1:
                return httpx.Response(200, json={"ret": ["FAIL_SYS_TOKEN_EXOIRED::令牌过期"]})
            return httpx.Response(
                200,
                json={"ret": ["SUCCESS::调用成功"], "data": {"accessToken": "im-token-ok"}},
            )
        if path.endswith("/mtop.gaia.nodejs.gaia.idle.data.gw.v2.index.get/1.0/"):
            if request.method == "GET":
                return httpx.Response(200, text="ok")
            # 空 token POST 下发新 _m_h5_tk；真实 token POST 激活
            return httpx.Response(
                200,
                json={"ret": ["FAIL_SYS_TOKEN_EMPTY::令牌为空"]},
                headers={"set-cookie": "_m_h5_tk=new_token_1793000000000; Path=/; Domain=.goofish.com"},
            )
        return httpx.Response(404)

    client = XianyuClient("unb=1; _m_h5_tk=old_token_1", transport=httpx.MockTransport(handler))
    token = await client.get_im_token()

    assert token == "im-token-ok"
    assert token_calls == 2
    assert "_m_h5_tk=new_token_1793000000000" in client.cookie_str
    await client.close()


async def test_refresh_failure_is_token_expired_not_cookie_expired():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/mtop.taobao.idlemessage.pc.login.token/1.0/"):
            return httpx.Response(200, json={"ret": ["FAIL_SYS_TOKEN_EXOIRED::令牌过期"]})
        # 刷新流程不返回新的 _m_h5_tk，模拟真的刷新不了
        return httpx.Response(200, json={"ret": ["FAIL_SYS_TOKEN_EMPTY::令牌为空"]})

    client = XianyuClient("unb=1; _m_h5_tk=old_token_1", transport=httpx.MockTransport(handler))
    with pytest.raises(TokenExpired):
        await client.get_im_token()
    await client.close()

async def test_keepalive_session_expired_raises_cookie_expired():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text='{"hasLogin":false,"error":"SESSION_EXPIRED"}')

    client = XianyuClient("unb=1; _m_h5_tk=tok_1", transport=httpx.MockTransport(handler))
    with pytest.raises(CookieExpired):
        await client.keepalive()
    await client.close()


async def test_keepalive_risk_is_not_session_expired():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="RGV587_ERROR::SM::哎哟喂,被挤爆啦,请稍后重试")

    client = XianyuClient("unb=1; _m_h5_tk=tok_1", transport=httpx.MockTransport(handler))
    with pytest.raises(RiskControl):
        await client.keepalive()
    await client.close()
