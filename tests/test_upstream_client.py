"""NewApiClient / CardShopClient 的 HTTP 契约测试（不打真实网络）。"""

from __future__ import annotations

import json

import httpx
import pytest

from app.upstream import CardShopClient, NewApiClient, UpstreamError


def make_newapi(handler, *, answer_self=True, **kwargs):
    """把 httpx 的 transport 换成 MockTransport，拦住所有出网请求。

    answer_self=True 时 /api/user/self 统一回答「配置的 id 就是 token 的主人」，
    省得每个用例都写一遍；要测探测/报错逻辑的用例传 False，交回自己的 handler。
    """
    client = NewApiClient("https://newapi.test", **kwargs)
    headers = {}
    if client.access_token:
        headers["authorization"] = "Bearer " + client.access_token
    if client.user_id:
        headers["new-api-user"] = client.user_id

    def dispatch(request):
        if answer_self and request.url.path == "/api/user/self":
            return httpx.Response(
                200,
                json={"success": True, "data": {"id": int(client.user_id or 1), "username": "u"}},
            )
        return handler(request)

    client._http = httpx.AsyncClient(
        base_url=client.base_url,
        transport=httpx.MockTransport(dispatch),
        headers=headers,
    )
    return client


def make_cardshop(handler, **kwargs):
    client = CardShopClient("https://cards.test", **kwargs)
    client._http = httpx.AsyncClient(
        base_url=client.base_url, transport=httpx.MockTransport(handler)
    )
    return client


async def test_create_token_rolls_back_when_key_cannot_be_read():
    """令牌建出来了但取不到 Key：必须把它删掉，不能留个白吃额度的孤儿 Key。"""
    seen: list[tuple[str, str]] = []

    def handler(request):
        seen.append((request.method, request.url.path))
        if request.url.path == "/api/status":
            return httpx.Response(200, json={"success": True, "data": {"quota_per_unit": 500000}})
        if request.url.path == "/api/token/" and request.method == "POST":
            return httpx.Response(200, json={"success": True, "data": {"id": 42}})
        if request.url.path == "/api/token/42/key":
            return httpx.Response(200, json={"success": False, "message": "boom"})
        if request.url.path == "/api/token/42" and request.method == "DELETE":
            return httpx.Response(200, json={"success": True})
        raise AssertionError(f"没预料到的请求 {request.method} {request.url.path}")

    client = make_newapi(handler, access_token="jwt", user_id="3")
    with pytest.raises(UpstreamError):
        await client.create_token(10.0, "买家#123")

    assert ("DELETE", "/api/token/42") in seen, f"没有回滚刚建的令牌：{seen}"


async def test_create_token_flow():
    seen = []

    def handler(request):
        seen.append((request.method, request.url.path))
        if request.url.path == "/api/status":
            return httpx.Response(200, json={"success": True, "data": {"quota_per_unit": 500000}})
        if request.url.path == "/api/token/" and request.method == "POST":
            return httpx.Response(200, json={"success": True, "message": ""})
        if request.url.path == "/api/token/" and request.method == "GET":
            items = [{"id": 7, "name": "闲鱼-10.00元 #123"}, {"id": 6, "name": "别的"}]
            return httpx.Response(200, json={"success": True, "data": {"items": items}})
        if request.url.path == "/api/token/7/key":
            return httpx.Response(200, json={"success": True, "data": {"key": "sk-test-0123456789"}})
        return httpx.Response(404, json={"success": False, "message": "unexpected"})

    client = make_newapi(handler, access_token="jwt-abc", user_id="3")
    try:
        token_id, key = await client.create_token(10.0, "闲鱼-10.00元 #123")
    finally:
        await client.close()

    assert token_id == "7"
    assert key == "sk-test-0123456789"
    assert ("POST", "/api/token/") in seen
    assert ("GET", "/api/token/") in seen
    assert ("POST", "/api/token/7/key") in seen


async def test_create_token_converts_usd_to_quota():
    captured = {}

    def handler(request):
        path = request.url.path
        if path == "/api/status":
            return httpx.Response(200, json={"data": {"quota_per_unit": 500000}})
        if path.endswith("/key"):
            return httpx.Response(200, json={"data": {"key": "sk-abcdefgh"}})
        if path == "/api/token/" and request.method == "POST":
            captured.update(json.loads(request.content))
            return httpx.Response(200, json={"success": True})
        return httpx.Response(200, json={"data": {"items": [{"id": 1, "name": "x"}]}})

    client = make_newapi(handler, access_token="jwt", user_id="3")
    try:
        await client.create_token(10.0, "x")
    finally:
        await client.close()

    assert captured["remain_quota"] == 5_000_000  # 10 USD × 500000 quota/USD
    assert captured["unlimited_quota"] is False
    assert captured["name"] == "x"


async def test_create_token_restricts_models_by_default():
    """新建令牌必须带上模型白名单，不能是 model_limits_enabled=False。"""
    captured = {}

    def handler(request):
        path = request.url.path
        if path == "/api/status":
            return httpx.Response(200, json={"data": {"quota_per_unit": 500000}})
        if path.endswith("/key"):
            return httpx.Response(200, json={"data": {"key": "sk-abcdefgh"}})
        if path == "/api/token/" and request.method == "POST":
            captured.update(json.loads(request.content))
            return httpx.Response(200, json={"success": True})
        return httpx.Response(200, json={"data": {"items": [{"id": 1, "name": "x"}]}})

    client = make_newapi(handler, access_token="jwt", user_id="3")
    try:
        await client.create_token(10.0, "x")
    finally:
        await client.close()

    assert captured["model_limits_enabled"] is True
    assert captured["model_limits"] == "deepseek-v4.1-flash"


async def test_create_token_accepts_per_item_model_limits():
    """商品级配置可以覆盖客户端上的全局白名单。"""
    captured = {}

    def handler(request):
        path = request.url.path
        if path == "/api/status":
            return httpx.Response(200, json={"data": {"quota_per_unit": 500000}})
        if path.endswith("/key"):
            return httpx.Response(200, json={"data": {"key": "sk-abcdefgh"}})
        if path == "/api/token/" and request.method == "POST":
            captured.update(json.loads(request.content))
            return httpx.Response(200, json={"success": True})
        return httpx.Response(200, json={"data": {"items": [{"id": 1, "name": "x"}]}})

    client = make_newapi(
        handler, access_token="jwt", user_id="3", model_limits="global-model"
    )
    try:
        await client.create_token(10.0, "x", model_limits=" item-a ， item-b ")
    finally:
        await client.close()

    assert captured["model_limits_enabled"] is True
    assert captured["model_limits"] == "item-a,item-b"


async def test_create_token_with_empty_model_limits_disables_restriction():
    """显式留空时才允许回到「不限制模型」的老行为。"""
    captured = {}

    def handler(request):
        path = request.url.path
        if path == "/api/status":
            return httpx.Response(200, json={"data": {"quota_per_unit": 500000}})
        if path.endswith("/key"):
            return httpx.Response(200, json={"data": {"key": "sk-abcdefgh"}})
        if path == "/api/token/" and request.method == "POST":
            captured.update(json.loads(request.content))
            return httpx.Response(200, json={"success": True})
        return httpx.Response(200, json={"data": {"items": [{"id": 1, "name": "x"}]}})

    client = make_newapi(handler, access_token="jwt", user_id="3", model_limits="")
    try:
        await client.create_token(10.0, "x")
    finally:
        await client.close()

    assert captured["model_limits_enabled"] is False
    assert captured["model_limits"] == ""


def test_normalize_model_limits_accepts_commas_and_spaces():
    from app.upstream import normalize_model_limits

    assert normalize_model_limits(" deepseek-v4.1-flash , deepseek-v4.1 ") == "deepseek-v4.1-flash,deepseek-v4.1"
    assert normalize_model_limits("deepseek-v4.1-flash，deepseek-v4.1-flash") == "deepseek-v4.1-flash"
    assert normalize_model_limits("\n deepseek-v4.1-flash \n") == "deepseek-v4.1-flash"
    assert normalize_model_limits("") == ""


async def test_provisioner_wires_model_limits_from_settings(monkeypatch, tmp_path):
    """Provisioner 必须把配置里的模型白名单传给 NewApiClient。"""
    from app.config import settings
    from app.upstream import Provisioner

    # 指到一个不存在的库：不许读这台机器上真实的 app_config，
    # 否则运营在后台改过「Key 模型限制」之后这个测试就会飘。
    monkeypatch.setattr(settings, "db_path", tmp_path / "missing.db")
    monkeypatch.setattr(settings, "newapi_model_limits", "deepseek-v4.1-flash")
    provisioner = Provisioner()
    try:
        assert provisioner.newapi.model_limits == "deepseek-v4.1-flash"
    finally:
        await provisioner.close()


async def test_login_uses_access_token_from_response():
    def handler(request):
        if request.url.path == "/api/user/login":
            return httpx.Response(200, json={"success": True, "data": {"access_token": "jwt-xyz"}})
        return httpx.Response(404, json={})

    client = make_newapi(handler, username="u", password="p")
    try:
        await client.login()
    finally:
        await client.close()
    assert client.access_token == "jwt-xyz"


async def test_login_failure_raises():
    def handler(request):
        return httpx.Response(200, json={"success": False, "message": "用户名或密码错误"})

    client = make_newapi(handler, username="u", password="p")
    try:
        with pytest.raises(UpstreamError) as exc:
            await client.login()
    finally:
        await client.close()
    assert "用户名或密码错误" in str(exc.value)


async def test_create_token_rejects_zero_quota():
    def handler(request):
        if request.url.path == "/api/status":
            return httpx.Response(200, json={"data": {"quota_per_unit": 500000}})
        return httpx.Response(200, json={"success": True})

    client = make_newapi(handler, access_token="jwt", user_id="3")
    try:
        with pytest.raises(UpstreamError):
            await client.create_token(0, "x")
    finally:
        await client.close()


async def test_cardshop_create_card_returns_code():
    seen = {}

    def handler(request):
        seen["path"] = request.url.path
        seen["auth"] = request.headers.get("authorization")
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True, "created": [{"code": "ABCD-EFGH"}]})

    client = make_cardshop(handler, admin_token="tok")
    try:
        code = await client.create_card("sk-abc", 10.0, "闲鱼 10 元")
    finally:
        await client.close()

    assert code == "ABCD-EFGH"
    assert seen["path"] == "/api/admin/cards"
    assert seen["auth"] == "Bearer tok"
    # 卡密站改版后的契约：一卡一码，只收 code
    assert seen["body"] == {
        "code": "sk-abc", "label": "闲鱼 10 元", "quotaUsd": 10.0, "groups": 5,
    }


async def test_cardshop_error_surfaces():
    def handler(request):
        return httpx.Response(428, json={"ok": False, "error": "首次登录必须先修改密码"})

    client = make_cardshop(handler, admin_token="tok")
    try:
        with pytest.raises(UpstreamError) as exc:
            await client.create_card("sk-abc", 10.0, "x")
    finally:
        await client.close()
    assert "修改密码" in str(exc.value)


async def test_provision_rolls_back_token_when_card_creation_fails():
    """卡密没建成时，必须把刚建的令牌删掉，不能留下一个没人知道的可用 Key。"""
    from app.upstream import Provisioner

    deleted = []

    def newapi_handler(request):
        path = request.url.path
        if path == "/api/status":
            return httpx.Response(200, json={"data": {"quota_per_unit": 500000}})
        if path.endswith("/key"):
            return httpx.Response(200, json={"data": {"key": "sk-abcdefgh"}})
        if request.method == "DELETE":
            deleted.append(path)
            return httpx.Response(200, json={"success": True})
        if path == "/api/token/" and request.method == "POST":
            return httpx.Response(200, json={"success": True})
        return httpx.Response(200, json={"data": {"items": [{"id": 9, "name": "买家#1"}]}})

    def cardshop_handler(request):
        return httpx.Response(500, json={"ok": False, "error": "卡密生成冲突过多，请重试"})

    provisioner = Provisioner.__new__(Provisioner)  # 跳过 __init__ 里读配置
    provisioner.newapi = make_newapi(newapi_handler, access_token="jwt", user_id="3")
    provisioner.cardshop = make_cardshop(cardshop_handler, admin_token="tok")

    try:
        with pytest.raises(UpstreamError):
            await provisioner.provision(10.0, "10.00元 #1", "买家")
    finally:
        await provisioner.close()

    assert deleted == ["/api/token/9"]


async def test_provision_reports_cardshop_key_rejection():
    """卡密站改版后拒收 API Key：这条规则必须失败并回滚令牌，不能留孤儿 Key。"""
    from app.upstream import Provisioner

    posts: list[dict] = []
    deleted: list[str] = []

    def newapi_handler(request):
        path = request.url.path
        if path == "/api/status":
            return httpx.Response(200, json={"data": {"quota_per_unit": 500000}})
        if path.endswith("/key"):
            return httpx.Response(200, json={"data": {"key": "sk-abcdefgh-0123"}})
        if path == "/api/token/" and request.method == "POST":
            return httpx.Response(200, json={"success": True})
        if request.method == "DELETE":
            deleted.append(path)
            return httpx.Response(200, json={"success": True})
        return httpx.Response(200, json={"data": {"items": [{"id": 9, "name": "买家#1"}]}})

    def cardshop_handler(request):
        body = json.loads(request.content)
        posts.append(body)
        return httpx.Response(400, json={
            "ok": False, "error": "额度兑换码不能是 API Key（旧版 Key 不再接受）",
        })

    provisioner = Provisioner.__new__(Provisioner)
    provisioner.newapi = make_newapi(newapi_handler, access_token="jwt", user_id="3")
    provisioner.cardshop = make_cardshop(cardshop_handler, admin_token="tok")

    try:
        with pytest.raises(UpstreamError) as exc:
            await provisioner.provision(10.0, "10.00元 #1", "买家")
    finally:
        await provisioner.close()

    assert "不再接受" in str(exc.value)
    # 卡密站现在只认 code 字段
    assert posts[0]["code"].startswith("sk-")
    assert "key" not in posts[0] and "count" not in posts[0]
    assert deleted == ["/api/token/9"]


async def test_new_api_user_header_is_sent():
    """0.13.x 要求所有后台请求带 New-Api-User，否则 401。"""
    seen = {}

    def handler(request):
        seen["new_api_user"] = request.headers.get("new-api-user")
        return httpx.Response(200, json={"success": True, "data": {"items": []}})

    client = make_newapi(handler, access_token="jwt", user_id="42")
    try:
        await client._find_token_id("x")
    except UpstreamError:
        pass
    finally:
        await client.close()

    assert seen["new_api_user"] == "42"


async def test_user_id_auto_detected_from_self():
    """没显式配 user_id 时，从 /api/user/self 探测一次。"""

    def handler(request):
        if request.url.path == "/api/user/self":
            return httpx.Response(200, json={"success": True, "data": {"id": 7, "username": "u"}})
        return httpx.Response(200, json={"success": True, "data": {"items": []}})

    client = make_newapi(handler, answer_self=False, access_token="jwt")
    try:
        await client.ensure_user_id()
    finally:
        await client.close()

    assert client.user_id == "7"


async def test_user_id_missing_raises_actionable_error():
    """真实 0.13.2 的文案是 "Unauthorized, New-Api-User header not provided"。"""

    def handler(request):
        return httpx.Response(
            200,
            json={"success": False, "message": "Unauthorized, New-Api-User header not provided"},
        )

    client = make_newapi(handler, answer_self=False, access_token="jwt")
    try:
        with pytest.raises(UpstreamError) as exc:
            await client.ensure_user_id()
    finally:
        await client.close()
    assert "XY_NEWAPI_USER_ID" in str(exc.value)


async def test_login_captures_user_id_from_login_response():
    def handler(request):
        if request.url.path == "/api/user/login":
            return httpx.Response(200, json={
                "success": True,
                "data": {"access_token": "jwt-x", "id": 11, "username": "u"},
            })
        return httpx.Response(404, json={})

    client = make_newapi(handler, username="u", password="p")
    try:
        await client.login()
    finally:
        await client.close()
    assert client.access_token == "jwt-x"
    assert client.user_id == "11"


async def test_cardshop_login_with_password_when_no_token():
    """卡密站没配 ADMIN_TOKEN 时，用账号密码换会话 Cookie。"""
    seen = {}

    def handler(request):
        if request.url.path == "/api/admin/login":
            return httpx.Response(
                200,
                json={"ok": True},
                headers={"set-cookie": "admin_session=abc123; Path=/; HttpOnly"},
            )
        seen["cookie"] = request.headers.get("cookie")
        return httpx.Response(200, json={"ok": True, "created": [{"code": "C-1"}]})

    client = make_cardshop(handler, username="admin", password="pw")
    try:
        code = await client.create_card("sk-x", 1.0, "lbl")
    finally:
        await client.close()

    assert code == "C-1"
    assert "admin_session=abc123" in (seen["cookie"] or "")


async def test_cardshop_relogs_in_when_saved_session_expires():
    """旧 Cookie 失效后自动重登并重试，不能把「未登录」留给每一单。"""
    logins = 0
    card_requests = 0

    def handler(request):
        nonlocal logins, card_requests
        if request.url.path == "/api/admin/login":
            logins += 1
            return httpx.Response(
                200, json={"ok": True},
                headers={"set-cookie": "nac_admin=fresh; Path=/; HttpOnly"},
            )
        card_requests += 1
        if request.headers.get("cookie") == "nac_admin=stale":
            return httpx.Response(200, json={"ok": False, "error": "未登录，请先登录管理后台"})
        assert request.headers.get("cookie") == "nac_admin=fresh"
        return httpx.Response(200, json={"ok": True, "created": [{"code": "C-1"}]})

    client = make_cardshop(handler, username="admin", password="pw")
    client._http.cookies.set("nac_admin", "stale")
    client._logged_in = True
    try:
        assert await client.create_card("reward-code", 1.0, "lbl") == "C-1"
    finally:
        await client.close()
    assert logins == 1
    assert card_requests == 2


async def test_cardshop_reuses_card_when_server_says_reward_is_already_bound():
    """建卡已落库但响应失败时，重试要取回原卡，不能困在唯一约束上。"""
    paths = []

    def handler(request):
        paths.append((request.method, request.url.path))
        if request.method == "POST":
            return httpx.Response(409, json={
                "ok": False, "error": "第 1 个额度兑换码已经绑定到卡密 #66，不能重复使用",
            })
        return httpx.Response(200, json={
            "ok": True, "list": {"items": [{"id": 66, "code": "EXISTING-CARD"}]},
        })

    client = make_cardshop(handler, admin_token="tok")
    try:
        assert await client.create_card("reward-code", 1.0, "lbl") == "EXISTING-CARD"
    finally:
        await client.close()
    assert paths == [("POST", "/api/admin/cards"), ("GET", "/api/admin/cards")]


async def test_cardshop_does_not_retry_non_authentication_errors():
    requests = 0

    def handler(request):
        nonlocal requests
        requests += 1
        return httpx.Response(500, json={"ok": False, "error": "temporary failure"})

    client = make_cardshop(handler, admin_token="tok")
    try:
        with pytest.raises(UpstreamError, match="temporary failure"):
            await client.create_card("reward-code", 1.0, "lbl")
    finally:
        await client.close()
    assert requests == 1


async def test_user_id_bootstrap_impossible_with_only_token():
    """光有 access_token 无法自举 user id —— 报错必须说清两条出路。"""

    def handler(request):
        return httpx.Response(
            200,
            json={"success": False, "message": "Unauthorized, New-Api-User header not provided"},
        )

    client = make_newapi(handler, answer_self=False, access_token="jwt")
    try:
        with pytest.raises(UpstreamError) as exc:
            await client.ensure_user_id()
    finally:
        await client.close()

    message = str(exc.value)
    assert "XY_NEWAPI_USER_ID" in message
    assert "XY_NEWAPI_USERNAME" in message


async def test_provisioner_uses_web_setting_over_env(tmp_path, monkeypatch):
    """后台「设置」页存下的模型白名单要压过 .env，且不用重启就能生效。"""
    from app.config import MODEL_LIMITS_KEY, settings
    from app.db import Database
    from app.upstream import Provisioner

    monkeypatch.setattr(settings, "db_path", tmp_path / "app.db")
    monkeypatch.setattr(settings, "newapi_model_limits", "from-env")

    db = Database(tmp_path / "app.db")
    await db.init()
    provisioner = Provisioner()
    try:
        assert provisioner.newapi.model_limits == "from-env"

        await db.set_config(MODEL_LIMITS_KEY, "from-web")

        # 新建 Provisioner 才能拿到新值（WorkerManager 每单都新建一次）
        again = Provisioner()
        try:
            assert again.newapi.model_limits == "from-web"
        finally:
            await again.close()
    finally:
        await provisioner.close()

    # 清掉后台值 → 退回 .env
    await db.delete_config(MODEL_LIMITS_KEY)
    back = Provisioner()
    try:
        assert back.newapi.model_limits == "from-env"
    finally:
        await back.close()


def test_resolved_model_limits_falls_back_to_default(tmp_path, monkeypatch):
    from app.config import DEFAULT_MODEL_LIMITS, resolved_model_limits, settings

    monkeypatch.setattr(settings, "db_path", tmp_path / "missing.db")
    monkeypatch.setattr(settings, "newapi_model_limits", "")
    assert resolved_model_limits() == ""
    # 内置默认值只作为 Settings 的默认，不受 DB 影响
    assert DEFAULT_MODEL_LIMITS == "deepseek-v4.1-flash"


# ---------------------------------------------------- 令牌 scope 受限（profile:read）

SCOPE_DENIED = {
    "code": "ACCESS_TOKEN_SCOPE_DENIED",
    "message": "The access token lacks the required permission: profile:read",
    "message_key": "The access token lacks the required permission: {{scope}}",
    "message_params": {"scope": "profile:read"},
    "success": False,
}


async def test_scope_denied_with_user_id_configured_still_provisions(monkeypatch):
    """令牌缺 profile:read 但配了 user id：不该因此发不出货。

    这是线上真实踩到的问题：/api/user/self 403，而建令牌根本不需要它，
    结果整条发货链路在「核对用户 id」这一步被卡死。
    """
    captured = {}
    warnings = []

    def handler(request):
        path = request.url.path
        if path == "/api/user/self":
            # 这条 token 没有 profile:read
            return httpx.Response(403, json=SCOPE_DENIED)
        if path == "/api/status":
            return httpx.Response(200, json={"data": {"quota_per_unit": 500000}})
        if path.endswith("/key"):
            return httpx.Response(200, json={"data": {"key": "sk-abcdefgh"}})
        if path == "/api/token/" and request.method == "POST":
            captured.update(json.loads(request.content))
            captured["_new_api_user"] = request.headers.get("new-api-user")
            return httpx.Response(200, json={"success": True})
        return httpx.Response(200, json={"data": {"items": [{"id": 1, "name": "x"}]}})

    client = make_newapi(handler, access_token="nap_scoped", user_id="1", answer_self=False)
    monkeypatch.setattr("app.upstream.logger.warning", lambda *a, **k: warnings.append(a))
    try:
        token_id, key = await client.create_token(1.0, "x")
    finally:
        await client.close()

    assert token_id
    assert key
    # 建令牌成功，而且用的是配置里的 id
    assert captured["_new_api_user"] == "1"
    assert captured["model_limits_enabled"] is True
    # 应该留下一条「跳过核对」的告警，方便排查
    assert any("profile:read" in str(args) for args in warnings)


async def test_scope_denied_without_user_id_gives_actionable_error():
    """没配 user id 又没有 profile:read：必须说清楚怎么办，而不是含糊报错。"""

    def handler(request):
        if request.url.path == "/api/user/self":
            return httpx.Response(403, json=SCOPE_DENIED)
        return httpx.Response(200, json={"data": {}})

    client = make_newapi(handler, access_token="nap_scoped", user_id="", answer_self=False)
    try:
        with pytest.raises(UpstreamError) as excinfo:
            await client.create_token(1.0, "x")
    finally:
        await client.close()

    message = str(excinfo.value)
    assert "profile:read" in message
    assert "XY_NEWAPI_USER_ID" in message


async def test_invalid_token_error_is_not_confused_with_scope_denied():
    """token 真的失效时，还是要报「重新生成」，别被 scope 分支吞掉。"""

    def handler(request):
        return httpx.Response(401, json={"success": False, "message": "Invalid access token"})

    client = make_newapi(handler, access_token="bad", user_id="1", answer_self=False)
    try:
        with pytest.raises(UpstreamError) as excinfo:
            await client.ensure_user_id()
    finally:
        await client.close()

    assert "重新生成" in str(excinfo.value) or "不认这串" in str(excinfo.value)


def test_is_scope_denied_matches_known_shapes():
    from app.upstream import _is_scope_denied

    assert _is_scope_denied(SCOPE_DENIED["message"].lower(), SCOPE_DENIED["message"])
    assert _is_scope_denied("access_token_scope_denied", "x")
    assert _is_scope_denied("the access token lacks the required permission: profile:read", "x")
    assert not _is_scope_denied("invalid access token", "Invalid access token")
    assert not _is_scope_denied("new-api-user does not match", "x")


# ---------------------------------------------------- 站点货币为 CNY 时的额度换算


def _make_status_handler(status_data, captured):
    """站点 /api/status 返回指定计价配置，并记录建令牌的 payload。"""

    def handler(request):
        path = request.url.path
        if path == "/api/status":
            return httpx.Response(200, json={"data": status_data})
        if path.endswith("/key"):
            return httpx.Response(200, json={"data": {"key": "sk-abcdefgh"}})
        if path == "/api/token/" and request.method == "POST":
            captured.update(json.loads(request.content))
            return httpx.Response(200, json={"success": True})
        return httpx.Response(200, json={"data": {"items": [{"id": 1, "name": "x"}]}})

    return handler


CNY_STATUS = {
    "quota_per_unit": 500000,
    "quota_display_type": "CNY",
    "usd_exchange_rate": 7.3,
}


async def test_cny_site_does_not_overgrant_by_exchange_rate():
    """站点展示货币是 CNY 时，1 元订单只能拿到 ¥1 的额度。

    回归用例：以前无条件把 quota_per_unit 当成「$1 = 500000」，
    在 CNY 站点上 1 元订单会发出 ¥7.30 的额度（多给 7.3 倍），
    等于每单白送 7 倍额度。
    """
    captured = {}
    client = make_newapi(_make_status_handler(CNY_STATUS, captured), access_token="jwt", user_id="3")
    try:
        await client.create_token(1.0, "x")
    finally:
        await client.close()

    # new-api: 显示金额 = quota / quotaPerUnit * exchangeRate
    shown = captured["remain_quota"] / 500000 * 7.3
    assert abs(shown - 1.0) < 0.01, f"1 元订单给出了 ¥{shown:.2f} 的额度"
    assert captured["remain_quota"] == 68493


async def test_status_failure_is_not_cached():
    """一次 /api/status 抖动不能把错误汇率锁死整个进程。

    旧实现失败时也会缓存「美元 1:1」；CNY 站点之后会一直按这个汇率超发。
    """
    calls = {"n": 0}

    def handler(request):
        if request.url.path == "/api/status":
            calls["n"] += 1
            if calls["n"] == 1:
                return httpx.Response(503, text="temporarily down")
            return httpx.Response(200, json={"data": CNY_STATUS})
        return httpx.Response(200, json={"data": {}})

    client = make_newapi(handler, access_token="jwt", user_id="3")
    try:
        first = await client.site_quota_model()
        assert first.currency == "USD", "读失败时应临时按美元兜底"
        second = await client.site_quota_model()
        assert second.currency == "CNY", "第二次读取成功后必须用真实配置"
        assert calls["n"] == 2, "失败结果不能被缓存"
    finally:
        await client.close()


async def test_usd_site_keeps_legacy_math():
    """美元站点（exchangeRate=1）行为不变：1 元 = 500000 quota。"""
    captured = {}
    status = {"quota_per_unit": 500000, "quota_display_type": "USD", "usd_exchange_rate": 7.3}
    client = make_newapi(_make_status_handler(status, captured), access_token="jwt", user_id="3")
    try:
        await client.create_token(1.0, "x")
    finally:
        await client.close()

    assert captured["remain_quota"] == 500000


async def test_quota_for_amount_roundtrips_for_all_values():
    """任意金额都应当能反算回原值（不因取整漂移太多）。"""
    client = make_newapi(
        _make_status_handler(CNY_STATUS, {}), access_token="jwt", user_id="3"
    )
    try:
        model = await client.site_quota_model()
        for amount in (0.01, 0.5, 1.0, 9.99, 200.0):
            quota = await client.quota_for_amount(amount)
            shown = quota / model.quota_per_unit * model.usd_exchange_rate
            assert abs(shown - amount) < 0.01, f"{amount} -> ¥{shown}"
    finally:
        await client.close()


async def test_cny_without_exchange_rate_falls_back_to_setting(monkeypatch):
    """站点说自己是 CNY 却没给汇率：不能猜成 1（否则又少给 7 倍）。"""
    from app.config import settings

    monkeypatch.setattr(settings, "cny_per_usd", 7.0)
    captured = {}
    status = {"quota_per_unit": 500000, "quota_display_type": "CNY"}
    client = make_newapi(_make_status_handler(status, captured), access_token="jwt", user_id="3")
    try:
        model = await client.site_quota_model()
        assert model.usd_exchange_rate == 7.0
        await client.create_token(1.0, "x")
    finally:
        await client.close()

    shown = captured["remain_quota"] / 500000 * 7.0
    assert abs(shown - 1.0) < 0.01


async def test_custom_currency_uses_its_own_rate():
    """CUSTOM 货币按自定义汇率换算。"""
    captured = {}
    status = {
        "quota_per_unit": 500000,
        "quota_display_type": "CUSTOM",
        "custom_currency_symbol": "¤",
        "custom_currency_exchange_rate": 2.0,
    }
    client = make_newapi(_make_status_handler(status, captured), access_token="jwt", user_id="3")
    try:
        await client.create_token(1.0, "x")
    finally:
        await client.close()

    assert captured["remain_quota"] == 250000  # 1 / 2 * 500000


# ------------------------------------------------------------------ 账号


async def test_find_user_requires_exact_username():
    """搜索是模糊的：搜 abc 会连 abc123 一起返回，绝不能拿第一条当真。"""
    def handler(request):
        assert request.url.path == "/api/user/search"
        return httpx.Response(200, json={
            "success": True,
            "data": {"items": [
                {"id": 1, "username": "abc123"},
                {"id": 2, "username": "abc"},
            ]},
        })

    client = make_newapi(handler, access_token="jwt", user_id="3")
    try:
        user = await client.find_user("abc")
        assert user["id"] == 2
        # 只有 abc123 时，找 abc 必须返回 None，不能糊弄成「就是这个人」
        assert await client.find_user("abcd") is None
    finally:
        await client.close()


async def test_create_user_posts_role_and_resolves_id():
    seen: dict = {}

    def handler(request):
        if request.url.path == "/api/user/" and request.method == "POST":
            seen["body"] = json.loads(request.content)
            return httpx.Response(200, json={"success": True, "message": ""})
        if request.url.path == "/api/user/search":
            return httpx.Response(200, json={
                "success": True,
                "data": {"items": [{"id": 11, "username": seen["body"]["username"]}]},
            })
        return httpx.Response(404, json={"success": False})

    client = make_newapi(handler, access_token="jwt", user_id="3")
    try:
        user = await client.create_user("xinbuyer", "pw12345678")
    finally:
        await client.close()

    assert seen["body"]["role"] == 1
    assert seen["body"]["password"] == "pw12345678"
    assert seen["body"]["display_name"] == "xinbuyer"
    assert user["id"] == 11


async def test_create_user_rejects_bad_name_without_network():
    def handler(request):  # pragma: no cover - 不该被调用
        raise AssertionError("不该发请求")

    client = make_newapi(handler, access_token="jwt", user_id="3")
    try:
        with pytest.raises(UpstreamError):
            await client.create_user("x" * 21, "pw12345678")
        with pytest.raises(UpstreamError):
            await client.create_user("okname", "short")
    finally:
        await client.close()


async def test_add_user_quota_posts_manage_with_converted_quota():
    seen: dict = {}

    def handler(request):
        if request.url.path == "/api/status":
            return httpx.Response(200, json={
                "success": True, "data": {"quota_per_unit": 500000}})
        if request.url.path == "/api/user/manage":
            seen["body"] = json.loads(request.content)
            return httpx.Response(200, json={"success": True})
        if request.url.path == "/api/user/7":
            return httpx.Response(200, json={
                "success": True, "data": {"id": 7, "username": "abc", "quota": 5500000}})
        return httpx.Response(404, json={"success": False})

    client = make_newapi(handler, access_token="jwt", user_id="3")
    try:
        user = await client.add_user_quota("7", 10.0)
        balance = await client.user_balance(user)
    finally:
        await client.close()

    # 10 美元 × 500000 quota/美元
    assert seen["body"] == {"id": 7, "action": "add_quota", "mode": "add", "value": 5000000}
    assert user["quota"] == 5500000
    assert balance == pytest.approx(11.0)


async def test_add_user_quota_survives_balance_read_failure():
    """额度加上了、随后读账号失败，绝不能抛异常——否则调用方会当成没充过再充一次。"""
    def handler(request):
        if request.url.path == "/api/status":
            return httpx.Response(200, json={"success": True, "data": {"quota_per_unit": 500000}})
        if request.url.path == "/api/user/manage":
            return httpx.Response(200, json={"success": True})
        return httpx.Response(500, json={"success": False})

    client = make_newapi(handler, access_token="jwt", user_id="3")
    try:
        user = await client.add_user_quota("7", 10.0)
        balance = await client.user_balance(user)
    finally:
        await client.close()

    assert user == {"id": 7}
    assert balance is None, "读不到余额要显示「—」，不能显示 0.00"


# ------------------------------------------------------------------ 兑换码


async def test_create_redemptions_posts_expected_payload():
    seen: dict = {}

    def handler(request):
        if request.url.path == "/api/redemption/" and request.method == "POST":
            seen["body"] = json.loads(request.content)
            return httpx.Response(200, json={"success": True, "data": ["k" * 32]})
        return httpx.Response(404, json={"success": False})

    client = make_newapi(handler, access_token="jwt", user_id="3")
    try:
        codes = await client.create_redemptions(5_000_000, "买家#00000001")
    finally:
        await client.close()

    assert codes == ["k" * 32]
    assert seen["body"] == {
        "name": "买家#00000001", "quota": 5_000_000, "count": 1, "expired_time": 0,
    }


async def test_create_redemptions_truncates_long_name():
    """new-api 只收 1~20 个字的名称，超了会 400，必须自己先截。"""
    seen: dict = {}

    def handler(request):
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"success": True, "data": ["k" * 32]})

    client = make_newapi(handler, access_token="jwt", user_id="3")
    try:
        await client.create_redemptions(1, "很长的名字" * 10)
    finally:
        await client.close()

    assert len(seen["body"]["name"]) == 20


async def test_create_redemptions_partial_failure_raises():
    """new-api 中途失败会回 200 + success=false + 已经生成的码：不能当成功。"""
    def handler(request):
        return httpx.Response(200, json={
            "success": False, "message": "Failed to create redemption code",
            "data": ["k" * 32],
        })

    client = make_newapi(handler, access_token="jwt", user_id="3")
    try:
        with pytest.raises(UpstreamError):
            await client.create_redemptions(1, "x")
    finally:
        await client.close()


async def test_create_redemptions_explains_compliance_gate():
    """new-api 的合规闸门报错是英文长句，要顺手翻成运营看得懂的提示。"""
    def handler(request):
        return httpx.Response(200, json={
            "success": False,
            "message": (
                "Payment, redemption, subscription, and invitation reward features "
                "are disabled. The administrator must confirm compliance terms before enabling them."
            ),
            "data": [],
        })

    client = make_newapi(handler, access_token="jwt", user_id="3")
    try:
        with pytest.raises(UpstreamError) as exc:
            await client.create_redemptions(1, "x")
    finally:
        await client.close()
    assert "合规条款" in str(exc.value)


async def test_create_redemptions_rejects_missing_codes():
    def handler(request):
        return httpx.Response(200, json={"success": True, "data": []})

    client = make_newapi(handler, access_token="jwt", user_id="3")
    try:
        with pytest.raises(UpstreamError):
            await client.create_redemptions(1, "x")
    finally:
        await client.close()


async def test_add_user_quota_denied_is_definite_error():
    """权限不够（令牌没给「用户 编辑」）是确定的失败：可以安全重试 / 报警。"""
    def handler(request):
        return httpx.Response(403, json={
            "success": False,
            "code": "ACCESS_TOKEN_SCOPE_DENIED",
            "message": "The access token lacks the required permission: user:write",
        })

    from app.upstream import TopupUncertainError

    client = make_newapi(handler, access_token="jwt", user_id="3")
    try:
        with pytest.raises(UpstreamError) as exc:
            await client.add_user_quota("7", 10.0)
    finally:
        await client.close()
    # 不能是「结果未知」：没加就是没加，重试是安全的
    assert not isinstance(exc.value, TopupUncertainError)


async def test_add_user_quota_connection_error_is_uncertain():
    from app.upstream import TopupUncertainError

    def handler(request):
        if request.url.path == "/api/status":
            return httpx.Response(200, json={"success": True, "data": {"quota_per_unit": 500000}})
        raise httpx.ConnectError("boom", request=request)

    client = make_newapi(handler, access_token="jwt", user_id="3")
    try:
        with pytest.raises(TopupUncertainError):
            await client.add_user_quota("7", 10.0)
    finally:
        await client.close()
