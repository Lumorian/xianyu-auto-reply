"""商品级模型白名单：存储、优先级、接口与发货链路。"""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from app.codec import IncomingMessage
from app.config import MODEL_LIMITS_KEY, resolve_model_limits, settings
from app.db import Database
from app.engine import DeliveryEngine
from app.runner import AccountWorker
from app.upstream import Provisioned
from app.web import create_app


class StubManager:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def sync(self) -> None:
        pass

    async def stop_all(self) -> None:
        pass

    def status(self) -> list[dict[str, Any]]:
        return []


@pytest.fixture()
async def client(tmp_path):
    db = Database(tmp_path / "web.db")
    app = create_app(db=db, manager=StubManager(db))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1") as ac:
        async with app.router.lifespan_context(app):
            yield ac


class FakeClient:
    user_id = "2000000000002"
    websocket = object()


class RecordingProvisioner:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def provision(self, quota_usd, label, username="", model_limits=None):
        self.calls.append({
            "quota": quota_usd, "label": label, "username": username,
            "model_limits": model_limits,
        })
        return Provisioned("CARD-ITEM-1", "sk-a…z", quota_usd, "1")


async def build_worker(db: Database) -> AccountWorker:
    account_id = await db.upsert_account("主号", "unb=2000000000002; _m_h5_tk=token_1")
    account = await db.get_account(account_id)
    worker = AccountWorker(db, account, DeliveryEngine(db))
    worker.client = FakeClient()  # type: ignore[assignment]
    worker.provisioner = RecordingProvisioner()  # type: ignore[assignment]
    return worker


def upstream_msg(item_id: str | None = "900052644277") -> IncomingMessage:
    return IncomingMessage(
        chat_id="47983389096",
        sender_id="4100000001",
        sender_name="测试小鱼",
        content="[订单待发货]",
        item_id=item_id,
        order_id="2500000000000000001",
        amount_cny_hint=10.0,
    )


UPSTREAM_RULE = {
    "id": 1,
    "name": "月卡",
    "mode": "upstream",
    "content": "卡密：{card}",
    "usd_multiplier": 1.0,
    "fixed_usd": 0.0,
    "auto_confirm": False,
}


def test_resolve_model_limits_priority():
    models = {"900052644277": "item-model", "*": "wildcard-model"}
    overrides = {MODEL_LIMITS_KEY: "global-model"}
    assert resolve_model_limits("900052644277", models, overrides) == ("item-model", "item:900052644277")
    assert resolve_model_limits("123456", models, overrides) == ("wildcard-model", "item:*")
    assert resolve_model_limits("123456", {}, overrides) == ("global-model", "web")


def test_resolve_model_limits_empty_item_value_means_unlimited():
    """商品级写空串是「这个商品不限模型」，不能被全局值兜回去。"""
    models = {"900052644277": ""}
    overrides = {MODEL_LIMITS_KEY: "global-model"}
    assert resolve_model_limits("900052644277", models, overrides) == ("", "item:900052644277")


def test_resolve_model_limits_source_env_and_default(monkeypatch):
    monkeypatch.setattr(settings, "newapi_model_limits", "env-model")
    assert resolve_model_limits(None, {}, {}) == ("env-model", "env")
    monkeypatch.setattr(settings, "newapi_model_limits", "")
    assert resolve_model_limits(None, {}, {}) == ("", "default")


async def test_item_models_storage_upserts_by_item_id(db):
    first = await db.save_item_model({"item_id": "900052644277", "model_limits": "m-a", "remark": "a"})
    again = await db.save_item_model({"item_id": "900052644277", "model_limits": "m-b", "remark": ""})
    assert first == again, "同一商品重复保存应该更新而不是插入第二行"

    rows = await db.list_item_models()
    assert len(rows) == 1
    assert rows[0]["model_limits"] == "m-b"

    await db.save_item_model({"item_id": "*", "model_limits": "m-all", "remark": ""})
    rows = await db.list_item_models()
    assert [row["item_id"] for row in rows] == ["900052644277", "*"]

    item_models, overrides = await db.model_settings()
    assert item_models == {"900052644277": "m-b", "*": "m-all"}
    assert overrides == {}

    await db.delete_item_model(int(rows[0]["id"]))
    assert len(await db.list_item_models()) == 1


async def test_item_models_reject_duplicate_item_on_update(db):
    first = await db.save_item_model({"item_id": "100", "model_limits": "a", "remark": ""})
    second = await db.save_item_model({"item_id": "200", "model_limits": "b", "remark": ""})
    with pytest.raises(ValueError):
        await db.save_item_model({"item_id": "200", "model_limits": "c", "remark": ""}, first)
    assert int(second) > 0


async def test_item_model_crud_api(client):
    res = await client.post("/api/item-models", json={
        "item_id": "900052644277",
        "model_limits": " deepseek-v4.1-flash， deepseek-v4.1 ",
        "remark": "月卡",
    })
    assert res.status_code == 200
    row_id = res.json()["id"]

    rows = (await client.get("/api/item-models")).json()
    assert len(rows) == 1
    assert rows[0]["item_id"] == "900052644277"
    assert rows[0]["models"] == "deepseek-v4.1-flash,deepseek-v4.1"
    assert rows[0]["unlimited"] is False

    res = await client.put(f"/api/item-models/{row_id}", json={
        "item_id": "900052644277", "model_limits": "", "remark": "放开",
    })
    assert res.status_code == 200
    rows = (await client.get("/api/item-models")).json()
    assert rows[0]["models"] == ""
    assert rows[0]["unlimited"] is True

    assert (await client.delete(f"/api/item-models/{row_id}")).status_code == 200
    assert (await client.get("/api/item-models")).json() == []


async def test_item_model_api_validates_and_404s(client):
    res = await client.post("/api/item-models", json={"item_id": "  ", "model_limits": "m"})
    assert res.status_code == 400  # 全是空白，strip 之后为空
    res = await client.post("/api/item-models", json={"item_id": "x", "model_limits": "m"})
    assert res.status_code == 200
    row_id = res.json()["id"]
    assert (await client.put(f"/api/item-models/{row_id + 999}", json={
        "item_id": "y", "model_limits": "m",
    })).status_code == 404


async def test_provision_uses_item_specific_model_limits(db):
    worker = await build_worker(db)
    await db.save_item_model({"item_id": "900052644277", "model_limits": "item-model", "remark": ""})
    await db.set_config(MODEL_LIMITS_KEY, "global-model")

    result = await worker._provision(upstream_msg(), UPSTREAM_RULE)

    assert result is not None
    provisioner = worker.provisioner
    assert isinstance(provisioner, RecordingProvisioner)
    assert provisioner.calls[-1]["model_limits"] == "item-model"


async def test_provision_falls_back_to_global_model_limits(db):
    worker = await build_worker(db)
    await db.set_config(MODEL_LIMITS_KEY, "global-model")

    await worker._provision(upstream_msg(), UPSTREAM_RULE)

    provisioner = worker.provisioner
    assert isinstance(provisioner, RecordingProvisioner)
    assert provisioner.calls[-1]["model_limits"] == "global-model"


async def test_provision_uses_wildcard_item_model(db):
    worker = await build_worker(db)
    await db.save_item_model({"item_id": "*", "model_limits": "wildcard-model", "remark": ""})

    await worker._provision(upstream_msg(), UPSTREAM_RULE)

    provisioner = worker.provisioner
    assert isinstance(provisioner, RecordingProvisioner)
    assert provisioner.calls[-1]["model_limits"] == "wildcard-model"
