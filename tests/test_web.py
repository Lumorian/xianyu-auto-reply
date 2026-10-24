"""Web 接口的冒烟测试。"""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from app.db import Database
from app.web import create_app


class StubManager:
    """替身：完全不碰网络，只记录 sync 调用。"""

    def __init__(self, db: Database) -> None:
        self.db = db
        self.synced = 0

    async def sync(self) -> None:
        self.synced += 1

    async def stop_all(self) -> None:
        pass

    def status(self) -> list[dict[str, Any]]:
        return [{"account_id": 1, "name": "主号", "status": "online", "last_error": ""}]


@pytest.fixture()
async def client(tmp_path):
    db = Database(tmp_path / "web.db")
    app = create_app(db=db, manager=StubManager(db))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1") as ac:
        async with app.router.lifespan_context(app):
            yield ac


async def test_index_page(client):
    res = await client.get("/")
    assert res.status_code == 200
    assert "闲鱼自动发货" in res.text


async def test_account_crud_hides_cookie(client):
    res = await client.post("/api/accounts", json={"name": "主号", "cookie": "unb=1; _m_h5_tk=abc_1"})
    assert res.status_code == 200
    accounts = (await client.get("/api/accounts")).json()
    assert accounts[0]["name"] == "主号"
    assert accounts[0]["cookie"] == "***"


async def test_cookie_survives_remark_only_update(client):
    await client.post("/api/accounts", json={"name": "主号", "cookie": "unb=1; _m_h5_tk=abc_1"})
    await client.post("/api/accounts", json={"name": "主号", "cookie": "", "remark": "改个备注"})
    row = await client.get("/api/accounts")
    accounts = row.json()
    assert accounts[0]["remark"] == "改个备注"


async def test_rule_and_card_flow(client):
    res = await client.post("/api/rules", json={
        "name": "月卡", "item_id": "*", "mode": "card", "content": "卡密：{card}",
    })
    assert res.status_code == 200

    res = await client.post("/api/cards", json={"item_id": "*", "raw_text": "A-1\nA-2"})
    assert res.json()["added"] == 2

    stock = (await client.get("/api/cards/stock")).json()
    assert stock[0]["available"] == 2

    rules = (await client.get("/api/rules")).json()
    assert rules[0]["mode"] == "card"


async def test_rejects_empty_cards(client):
    res = await client.post("/api/cards", json={"item_id": "*", "raw_text": "   "})
    assert res.status_code == 400


async def test_reply_rules_crud(client):
    res = await client.post("/api/reply-rules", json={
        "name": "发货时间", "kind": "keyword", "keywords": "发货, 多久",
        "match_type": "contains", "content": "24 小时内发 {buyer}", "cooldown": 30,
    })
    assert res.status_code == 200
    rule_id = res.json()["id"]

    res = await client.post("/api/reply-rules", json={
        "name": "商品默认", "kind": "item", "item_id": "900052644277",
        "content": "在的，拍下自动发货",
    })
    assert res.status_code == 200

    rules = (await client.get("/api/reply-rules")).json()
    assert [r["name"] for r in rules] == ["发货时间", "商品默认"]
    assert rules[0]["keywords"] == "发货, 多久"
    assert rules[0]["once_per_chat"] == 1

    res = await client.put(f"/api/reply-rules/{rule_id}", json={
        "name": "发货时间（改）", "kind": "keyword", "keywords": "发货",
        "content": "改好了", "enabled": False,
    })
    assert res.status_code == 200
    rules = (await client.get("/api/reply-rules")).json()
    assert rules[0]["name"] == "发货时间（改）"
    assert rules[0]["enabled"] == 0

    assert (await client.delete(f"/api/reply-rules/{rule_id}")).status_code == 200
    assert len((await client.get("/api/reply-rules")).json()) == 1


async def test_reply_rule_requires_keywords_for_keyword_kind(client):
    res = await client.post("/api/reply-rules", json={
        "name": "缺关键词", "kind": "keyword", "keywords": "  ", "content": "回复",
    })
    assert res.status_code == 400
    assert "关键词" in res.json()["detail"]


async def test_reply_rule_requires_content(client):
    res = await client.post("/api/reply-rules", json={
        "name": "缺内容", "kind": "item", "content": "   ",
    })
    assert res.status_code == 400


async def test_reply_rule_reset_clears_hits(client):
    rule_id = (await client.post("/api/reply-rules", json={
        "name": "重置", "kind": "item", "content": "在的",
    })).json()["id"]
    db = client._transport.app.state.db
    await db.record_reply_hit(1, "4100000001", "47983389096", rule_id)
    assert await db.fetch_one("SELECT id FROM reply_hits") is not None

    res = await client.post(f"/api/reply-rules/{rule_id}/reset")
    assert res.status_code == 200
    assert await db.fetch_one("SELECT id FROM reply_hits") is None


async def test_reply_rule_toggle(client):
    rule_id = (await client.post("/api/reply-rules", json={
        "name": "开关", "kind": "item", "content": "在的",
    })).json()["id"]
    res = await client.post(f"/api/reply-rules/{rule_id}/toggle?enabled=false")
    assert res.status_code == 200
    rules = (await client.get("/api/reply-rules")).json()
    assert rules[0]["enabled"] == 0
    await client.post(f"/api/reply-rules/{rule_id}/toggle?enabled=true")
    assert (await client.get("/api/reply-rules")).json()[0]["enabled"] == 1

    assert (await client.post("/api/reply-rules/999/toggle")).status_code == 404


async def test_reply_rule_update_unknown_returns_404(client):
    res = await client.put("/api/reply-rules/999", json={
        "name": "x", "kind": "item", "content": "y",
    })
    assert res.status_code == 404


async def test_config_exposes_auto_reply_switch(client):
    cfg = (await client.get("/api/config")).json()
    assert "auto_reply" in cfg
    assert cfg["auto_reply"]["enabled"] is True


async def test_model_limits_settings_override_env(client, monkeypatch):
    """后台设置页改模型白名单：存进 DB，并立刻生效到 /api/config。"""
    from app.config import settings

    monkeypatch.setattr(settings, "newapi_model_limits", "deepseek-v4.1-flash")

    cfg = (await client.get("/api/config")).json()
    assert cfg["newapi"]["model_limits"] == "deepseek-v4.1-flash"
    assert cfg["newapi"]["model_limits_source"] == "env"

    res = await client.put("/api/config/settings", json={
        "model_limits": " deepseek-v4.1-flash ， deepseek-v4.1 , deepseek-v4.1 ",
    })
    assert res.status_code == 200
    # 中文逗号 / 空格被规整，重复项去重
    assert res.json()["model_limits"]["value"] == "deepseek-v4.1-flash,deepseek-v4.1"

    cfg = (await client.get("/api/config")).json()
    assert cfg["newapi"]["model_limits"] == "deepseek-v4.1-flash,deepseek-v4.1"
    assert cfg["newapi"]["model_limits_source"] == "web"
    assert cfg["newapi"]["model_limits_unlimited"] is False

    got = (await client.get("/api/config/settings")).json()
    assert got["model_limits"]["value"] == "deepseek-v4.1-flash,deepseek-v4.1"


async def test_model_limits_setting_survives_restart_and_wins_over_env(client, monkeypatch):
    """设置存在数据库里，重建 app 后依然生效（且优先于 .env）。"""
    from app.config import settings
    from app.web import create_app

    monkeypatch.setattr(settings, "newapi_model_limits", "from-env")
    await client.put("/api/config/settings", json={"model_limits": "from-web"})

    db = client._transport.app.state.db
    app = create_app(db=db, manager=StubManager(db))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1") as ac:
        async with app.router.lifespan_context(app):
            cfg = (await ac.get("/api/config")).json()
    assert cfg["newapi"]["model_limits"] == "from-web"
    assert cfg["newapi"]["model_limits_source"] == "web"


async def test_emptying_model_limits_is_allowed_and_flagged(client):
    """允许主动清空（回到不限制），但要让前端能看出「现在是敞开的」。"""
    res = await client.put("/api/config/settings", json={"model_limits": "   "})
    assert res.status_code == 200
    state = res.json()["model_limits"]
    assert state["value"] == ""
    assert state["unlimited"] is True

    cfg = (await client.get("/api/config")).json()
    assert cfg["newapi"]["model_limits_unlimited"] is True


async def test_site_currency_is_cached_between_refreshes(client, monkeypatch):
    """前端每 5 秒拉一次 /api/config，不能每次都去问 new-api 的 /api/status。"""
    import app.web as web_mod
    from app.upstream import QuotaModel

    web_mod._SITE_CURRENCY_CACHE.clear()
    calls = {"n": 0}

    async def fake_site_quota_model(self):
        calls["n"] += 1
        return QuotaModel(quota_per_unit=500000.0, currency="CNY", usd_exchange_rate=7.3,
                          exchange_rate=7.3)

    monkeypatch.setattr("app.upstream.NewApiClient.site_quota_model", fake_site_quota_model)

    first = (await client.get("/api/config")).json()
    await client.get("/api/config")
    assert first["newapi"]["currency"] == "CNY"
    assert calls["n"] == 1, "60 秒内必须命中缓存"

    web_mod._SITE_CURRENCY_CACHE.clear()  # 模拟 TTL 到期
    await client.get("/api/config")
    assert calls["n"] == 2


async def test_saving_model_limits_updates_the_running_provisioner(tmp_path):
    """后台保存后必须同步给**运行中**的上游客户端，否则「保存即生效」是假的。

    Provisioner 是进程启动时按当时的配置构造的；只写 app_config 不改它，
    得重启才生效 —— 运营以为收紧了模型白名单，实际每单还在建不限模型的 Key。
    """
    from types import SimpleNamespace

    db = Database(tmp_path / "hot.db")
    manager = StubManager(db)
    manager.provisioner = SimpleNamespace(
        newapi=SimpleNamespace(model_limits="deepseek-old"),
    )
    app = create_app(db=db, manager=manager)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1") as ac:
        async with app.router.lifespan_context(app):
            res = await ac.put(
                "/api/config/settings", json={"model_limits": "deepseek-v4.1-flash, deepseek-v4.1"}
            )
            assert res.status_code == 200

    assert manager.provisioner.newapi.model_limits == "deepseek-v4.1-flash,deepseek-v4.1"


async def test_settings_endpoints_require_admin_token(client, monkeypatch):
    """设置能放宽 Key 的限制，必须和别的管理接口一样受口令保护。"""
    from app.config import settings

    monkeypatch.setattr(settings, "admin_token", "s3cret")
    res = await client.put("/api/config/settings", json={"model_limits": "x"})
    assert res.status_code == 401
    res = await client.get("/api/config/settings")
    assert res.status_code == 401


async def test_config_exposes_site_currency(client):
    """/api/config 要带上站点计价货币，前端才好决定额度显示成 ¥ 还是 $。"""
    cfg = (await client.get("/api/config")).json()
    assert cfg["newapi"]["currency"] in ("USD", "CNY", "CUSTOM")
