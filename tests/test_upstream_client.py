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


async def test_provisioner_wires_model_limits_from_settings(monkeypatch):
    """Provisioner 必须把配置里的模型白名单传给 NewApiClient。"""
    from app.config import settings
    from app.upstream import Provisioner

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
    assert seen["body"]["quotaUsd"] == 10.0
    assert seen["body"]["count"] == 1


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


async def test_provision_happy_path_returns_card_code():
    from app.upstream import Provisioner

    def newapi_handler(request):
        path = request.url.path
        if path == "/api/status":
            return httpx.Response(200, json={"data": {"quota_per_unit": 500000}})
        if path.endswith("/key"):
            return httpx.Response(200, json={"data": {"key": "sk-abcdefgh-0123"}})
        if path == "/api/token/" and request.method == "POST":
            return httpx.Response(200, json={"success": True})
        return httpx.Response(200, json={"data": {"items": [{"id": 9, "name": "买家#1"}]}})

    def cardshop_handler(request):
        return httpx.Response(200, json={"ok": True, "created": [{"code": "CODE-1234-5678"}]})

    provisioner = Provisioner.__new__(Provisioner)
    provisioner.newapi = make_newapi(newapi_handler, access_token="jwt", user_id="3")
    provisioner.cardshop = make_cardshop(cardshop_handler, admin_token="tok")

    try:
        result = await provisioner.provision(10.0, "10.00元 #1", "买家")
    finally:
        await provisioner.close()

    assert result.card_code == "CODE-1234-5678"
    assert result.quota_usd == 10.0
    assert result.token_id == "9"
    assert result.key_mask.startswith("sk-abc")


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
