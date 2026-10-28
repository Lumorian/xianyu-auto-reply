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
    # 自动回复规则弹窗里的「多条回复」编辑器在页面里
    assert "reply-entries" in res.text
    assert "加一条回复" in res.text


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


async def test_reply_rule_can_hold_multiple_entries(client):
    """一条规则 = 商品范围 + 多条「关键词 -> 回复」，而不是一个关键词一条规则。"""
    res = await client.post("/api/reply-rules", json={
        "name": "商品 FAQ", "kind": "keyword", "item_id": "1094740913216",
        "match_type": "contains",
        "entries": [
            {"keywords": "充值, 续费", "content": "充值直接拍下，发新 key"},
            {"keywords": "命中率, 缓存", "content": "实测 96% 以上缓存命中"},
            {"keywords": "人工, 客服", "content": "客服在线 12:00-22:00"},
        ],
    })
    assert res.status_code == 200
    rule_id = res.json()["id"]

    rule = (await client.get("/api/reply-rules")).json()[0]
    assert rule["id"] == rule_id
    # 顶层 keywords 是第一条的兼容镜像
    assert rule["keywords"] == "充值, 续费"
    assert [e["keywords"] for e in rule["entries"]] == [
        "充值, 续费", "命中率, 缓存", "人工, 客服",
    ]
    entry_ids = [e["id"] for e in rule["entries"]]
    assert all(entry_ids)

    # 编辑：改第一条、保留第二条、删掉第三条、再加一条
    res = await client.put(f"/api/reply-rules/{rule_id}", json={
        "name": "商品 FAQ（改）", "kind": "keyword", "item_id": "1094740913216",
        "match_type": "contains",
        "entries": [
            {"id": entry_ids[0], "keywords": "充值", "content": "改好的充值说明"},
            {"id": entry_ids[1], "keywords": "命中率", "content": "实测 96%"},
            {"keywords": "1017", "content": "是 1017 版"},
        ],
        "enabled": True,
    })
    assert res.status_code == 200
    rule = (await client.get("/api/reply-rules")).json()[0]
    assert rule["name"] == "商品 FAQ（改）"
    assert [e["id"] for e in rule["entries"]][:2] == entry_ids[:2]
    assert [e["keywords"] for e in rule["entries"]] == ["充值", "命中率", "1017"]


async def test_reply_rule_auto_names_and_flat_fields_still_work(client):
    """规则名留空自动生成；没传 entries 时，老的 keywords/content 当成唯一一条。"""
    res = await client.post("/api/reply-rules", json={
        "kind": "keyword", "keywords": "发货, 多久", "content": "24 小时内发",
    })
    assert res.status_code == 200
    rule = (await client.get("/api/reply-rules")).json()[0]
    assert rule["name"] == "发货 / 多久"
    assert rule["entries"][0]["keywords"] == "发货, 多久"
    assert rule["entries"][0]["content"] == "24 小时内发"


async def test_reply_rule_entry_validation_points_at_row(client):
    """第 2 条缺关键词：整条规则不落库，错误信息指明是第几条。"""
    res = await client.post("/api/reply-rules", json={
        "name": "坏条目", "kind": "keyword",
        "entries": [
            {"keywords": "发货", "content": "A"},
            {"keywords": " ", "content": "B"},
        ],
    })
    assert res.status_code == 400
    assert "第 2 条" in res.json()["detail"]
    assert (await client.get("/api/reply-rules")).json() == []


async def test_reply_rule_entries_are_capped(client):
    entries = [{"keywords": f"k{i}", "content": "在的"} for i in range(51)]
    res = await client.post("/api/reply-rules", json={"kind": "keyword", "entries": entries})
    assert res.status_code == 400
    assert "50" in res.json()["detail"]


async def test_reply_rule_merge_same_item_keeps_hits(client):
    """老用户一个商品建了好几条规则：一键并成一条，回复记录跟着走。"""

    async def create(name: str, item: str, keywords: str) -> int:
        res = await client.post("/api/reply-rules", json={
            "name": name, "kind": "keyword", "item_id": item, "match_type": "contains",
            "entries": [{"keywords": keywords, "content": name + " 的回复"}],
        })
        return res.json()["id"]

    target = await create("目标", "1094740913216", "发货")
    sibling1 = await create("兄弟一", "1094740913216", "退款")
    await create("兄弟二", "1094740913216", "人工")
    await create("别的商品", "900052644277", "发货")

    db = client._transport.app.state.db
    sibling_rule = await db.get_reply_rule(sibling1)
    sibling_entry = sibling_rule["entries"][0]
    await db.record_reply_hit(1, "buyer", "chat", sibling1, sibling_entry["id"])

    res = await client.post(f"/api/reply-rules/{target}/merge")
    assert res.status_code == 200
    assert res.json() == {"merged": 2, "entries": 3}

    rules = (await client.get("/api/reply-rules")).json()
    assert sorted(r["name"] for r in rules) == ["别的商品", "目标"]
    merged = next(r for r in rules if r["id"] == target)
    assert [e["keywords"] for e in merged["entries"]] == ["发货", "退款", "人工"]
    assert all(entry["id"] for entry in merged["entries"])

    hit = await db.fetch_one("SELECT rule_id, entry_id FROM reply_hits")
    assert hit["rule_id"] == target
    refund = next(e for e in merged["entries"] if e["keywords"] == "退款")
    assert hit["entry_id"] == refund["id"]

    assert (await client.post("/api/reply-rules/999/merge")).status_code == 404


async def test_reply_rule_merge_dedupes_identical_entries(client):
    async def create(name: str) -> int:
        res = await client.post("/api/reply-rules", json={
            "name": name, "kind": "keyword", "item_id": "123", "match_type": "contains",
            "entries": [{"keywords": "发货", "content": "24 小时内发"}],
        })
        return res.json()["id"]

    target = await create("A")
    await create("B")
    res = await client.post(f"/api/reply-rules/{target}/merge")
    assert res.status_code == 200
    assert res.json() == {"merged": 1, "entries": 1}
    rule = (await client.get("/api/reply-rules")).json()[0]
    assert len(rule["entries"]) == 1


async def test_reply_rule_requires_at_least_one_entry(client):
    res = await client.post("/api/reply-rules", json={
        "name": "空规则", "kind": "item", "entries": [],
    })
    assert res.status_code == 400
    assert "至少" in res.json()["detail"]


async def test_old_reply_rule_gets_entry_and_hits_are_migrated(tmp_path):
    """老库升级：规则补一条条目，历史命中记录挂到这条条目上，去重不会失效。"""
    db = Database(tmp_path / "old.db")
    await db.init()
    await db.execute(
        "INSERT INTO reply_rules (name, kind, item_id, keywords, match_type, content,"
        " enabled, once_per_chat, cooldown, priority, remark, created_at, updated_at)"
        " VALUES (?, ?, ?, ?, ?, ?, 1, 1, 60, 0, '', ?, ?)",
        ("老规则", "keyword", "*", "发货", "contains", "24 小时内发",
         "2026-01-01 00:00:00", "2026-01-01 00:00:00"),
    )
    rule_id = (await db.fetch_one("SELECT id FROM reply_rules"))["id"]
    await db.record_reply_hit(1, "b", "c", rule_id)
    assert (await db.fetch_one("SELECT entry_id FROM reply_hits"))["entry_id"] == 0

    await db.init()  # 再跑一次升级

    rule = await db.get_reply_rule(rule_id)
    assert len(rule["entries"]) == 1
    assert rule["entries"][0]["keywords"] == "发货"
    hit = await db.fetch_one("SELECT entry_id FROM reply_hits")
    assert hit["entry_id"] == rule["entries"][0]["id"]


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


async def test_rule_account_mode_roundtrip(client):
    """账号模式的三个文案字段要能存能取，否则后台改完刷新就丢了。"""
    res = await client.post("/api/rules", json={
        "name": "账号发货", "item_id": "*", "mode": "account",
        "account_prompt": "你有账号吗", "account_confirm": "充到 {username}（{balance}）？",
        "account_card": "账号：{username}\n密码：{password}",
    })
    assert res.status_code == 200
    rules = (await client.get("/api/rules")).json()
    assert rules[0]["mode"] == "account"
    assert rules[0]["account_prompt"] == "你有账号吗"
    assert rules[0]["account_confirm"] == "充到 {username}（{balance}）？"
    assert "密码：{password}" in rules[0]["account_card"]


async def test_rule_redeem_mode_roundtrip(client):
    """兑换码模式的有效期 / 卡密内容 / 发货文案都要能存能取。"""
    res = await client.post("/api/rules", json={
        "name": "兑换码 10 元", "item_id": "*", "mode": "redeem",
        "redeem_expiry_days": 30, "content": "卡密：{card} 兑换：{shop_url}",
    })
    assert res.status_code == 200
    rules = (await client.get("/api/rules")).json()
    assert rules[0]["mode"] == "redeem"
    assert rules[0]["redeem_expiry_days"] == 30
    assert rules[0]["content"] == "卡密：{card} 兑换：{shop_url}"


async def test_rule_redeem_expiry_rejects_negative(client):
    res = await client.post("/api/rules", json={
        "name": "负有效期", "item_id": "*", "mode": "redeem", "redeem_expiry_days": -1,
    })
    assert res.status_code == 422


async def test_buyer_account_binding_api(tmp_path, monkeypatch):
    """绑定 / 列表 / 看密码三条接口：列表里绝不能带明文密码。"""
    from types import SimpleNamespace

    from app.config import settings

    monkeypatch.setattr(settings, "newapi_base_url", "https://newapi.test")
    monkeypatch.setattr(settings, "newapi_session", "jwt")

    class FakeNewApi:
        async def get_user(self, user_id):
            return {"id": 7, "username": "abc", "quota": 100}

        async def find_user(self, name):
            return {"id": 7, "username": name, "quota": 100} if name == "abc" else None

        async def user_balance(self, user):
            return 5.0

        async def close(self):
            pass

    db = Database(tmp_path / "web2.db")
    manager = StubManager(db)
    manager.provisioner = SimpleNamespace(newapi=FakeNewApi())
    app = create_app(db=db, manager=manager)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1") as ac:
        async with app.router.lifespan_context(app):
            res = await ac.post("/api/buyer-accounts", json={
                "buyer_id": "b1", "username": "abc", "password": "pw12345678",
            })
            assert res.status_code == 200
            rows = (await ac.get("/api/buyer-accounts")).json()
            assert rows[0]["username"] == "abc"
            assert "password" not in rows[0]
            assert rows[0]["has_password"] is True

            secret = (await ac.get("/api/buyer-accounts/" + str(rows[0]["id"]) + "/secret")).json()
            assert secret["username"] == "abc"
            assert secret["password"] == "pw12345678"

            # 后台表单里没有密码这一栏：编辑一次绑定不能把开号时存的密码抹掉
            res = await ac.put("/api/buyer-accounts/" + str(rows[0]["id"]), json={
                "buyer_id": "b1", "username": "abc", "remark": "改个备注",
            })
            assert res.status_code == 200
            secret = (await ac.get("/api/buyer-accounts/" + str(rows[0]["id"]) + "/secret")).json()
            assert secret["password"] == "pw12345678", "编辑绑定不能清空密码"

            assert (await ac.delete("/api/buyer-accounts/" + str(rows[0]["id"]))).status_code == 200
            assert (await ac.get("/api/buyer-accounts")).json() == []


async def test_index_page_has_buyer_accounts_view(client):
    res = await client.get("/")
    assert res.status_code == 200
    assert "买家账号" in res.text
    assert "view-buyeraccounts" in res.text
    assert "账号模式" in res.text

