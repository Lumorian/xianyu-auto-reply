"""自动评价：请求识别、mtop 调用、Worker 触发与设置接口。"""

from __future__ import annotations

import base64
import json
from typing import Any

import httpx
import pytest

from app.codec import IncomingMessage, parse_message
from app.config import AUTO_RATE_ENABLED_KEY, AUTO_RATE_FEEDBACK_KEY
from app.db import Database
from app.engine import DeliveryEngine
from app.rate import is_rate_request, rate_request_order, render_feedback
from app.runner import AccountWorker
from app.web import create_app


def test_rate_request_detection_ignores_completion_notice():
    assert is_rate_request("快给ta一个评价吧～\n去评价")
    assert is_rate_request("快给 ta 一个评价吧")
    assert not is_rate_request("[我完成了评价]")
    assert not is_rate_request("查看评价")
    assert not is_rate_request("买家说：去评价一下吧")


def test_captured_rate_card_is_recognized_as_trusted_system_card():
    order_id = "3317156040024036264"
    inner = {
        "contentType": 25,
        "dxCard": {"item": {"main": {
            "targetUrl": "fleamarket://order_detail?id=" + order_id + "&role=Seller",
            "exContent": {
                "title": "快给ta一个评价吧～",
                "subTitle": "说说这次的交易体验，帮助更多人",
                "button": {"text": "去评价"},
            },
        }}},
    }
    payload = {
        "1": {
            "1": {"1": "2216160306798@goofish"},
            "2": "68250276867@goofish",
            "3": "4370287657081.PNM",
            "5": 1792823014544,
            "6": {"1": 101, "3": {
                "1": "", "2": "快给ta一个评价吧～", "3": "", "4": 25,
                "5": json.dumps(inner, ensure_ascii=False),
            }},
        },
        "10": {
            "reminderContent": "快给ta一个评价吧～",
            "senderUserId": "2216160306798",
            "senderNick": "买家小王",
        },
    }

    msg = parse_message(payload, my_id="2000000000002")
    assert msg is not None
    assert is_rate_request(msg.content)
    assert msg.order_id == order_id
    assert msg.is_system_card is True
    assert msg.order_id_trusted is True


def test_pulled_rate_card_marks_order_id_trusted():
    from app.client import XianyuClient

    order_id = "3317156040024036264"
    inner = {
        "contentType": 25,
        "dxCard": {"item": {"main": {
            "targetUrl": "fleamarket://order_detail?id=" + order_id + "&role=Seller",
            "exContent": {"title": "快给ta一个评价吧～", "button": {"text": "去评价"}},
        }}},
    }
    data = base64.b64encode(json.dumps(inner, ensure_ascii=False).encode()).decode()
    message = {
        "cid": "68250276867@goofish",
        "messageId": "m-rate",
        "createAt": 1792823014544,
        "sender": {"uid": "2216160306798"},
        "content": {"contentType": 101, "custom": {"type": 1, "data": data}},
        "extension": {
            "reminderContent": "快给ta一个评价吧～",
            "reminderUrl": "fleamarket://message_chat?itemId=1093125158699&peerUserId=2216160306798",
            "senderUserId": "2216160306798",
        },
    }
    client = XianyuClient("unb=2000000000002; _m_h5_tk=token_1")
    parsed = client._parse_im_message(message)
    assert parsed is not None
    assert parsed["order_id"] == order_id
    assert parsed["order_id_trusted"] is True
    assert parsed["content_type"] == 25
    assert is_rate_request(parsed["content"])


def test_render_feedback_supports_variables_and_limits_length():
    out = render_feedback(
        "  {buyer} 的 {item_id} / {order_id} 交易愉快  ",
        buyer_name="小鱼", item_id="i1", order_id="o1",
    )
    assert out == "小鱼 的 i1 / o1 交易愉快"
    assert len(render_feedback("x" * 500)) <= 200
    assert rate_request_order(" 2500000000000000001 ") == "2500000000000000001"
    assert rate_request_order("not a valid order") == ""


class RateClient:
    user_id = "2000000000002"
    websocket = object()

    def __init__(self, result=(True, "")):
        self.result = result
        self.calls: list[tuple[str, str]] = []

    async def rate_buyer(self, order_id: str, feedback: str = ""):
        self.calls.append((order_id, feedback))
        return self.result

    async def list_sold_orders(self, page_size: int = 20):
        return [{
            "order_id": "2500000000000000002",
            "item_id": "i2",
            "order_status": "交易成功",
            "seller_rate_status": "0",
            "buyer_id": "b2",
            "buyer_name": "买家二号",
        }]


async def build_worker(db: Database, client: RateClient | None = None):
    account_id = await db.upsert_account("主号", "unb=2000000000002; _m_h5_tk=token_1")
    account = await db.get_account(account_id)
    worker = AccountWorker(db, account, DeliveryEngine(db))
    worker.client = client or RateClient()  # type: ignore[assignment]
    return worker, account_id


def rate_card(order_id="2500000000000000001") -> IncomingMessage:
    return IncomingMessage(
        chat_id="c1",
        sender_id="b1",
        sender_name="买家小王",
        content="快给ta一个评价吧～\n去评价",
        item_id="i1",
        order_id=order_id,
        is_system_card=True,
        order_id_trusted=True,
    )


async def test_pulled_rate_card_becomes_system_card_in_worker(db):
    worker, _ = await build_worker(db)
    parsed = {
        "chat_id": "c1", "buyer_id": "b1", "buyer_name": "买家小王",
        "content": "快给ta一个评价吧～", "item_id": "i1",
        "order_id": "3317156040024036264", "order_id_trusted": True,
        "content_type": 25, "custom_type": 1, "message_id": "m-rate",
    }
    msg = worker._to_incoming(parsed)
    assert msg is not None
    assert msg.is_system_card is True
    assert is_rate_request(msg.content)


async def test_message_rating_calls_api_and_records_success(db):
    await db.set_config(AUTO_RATE_ENABLED_KEY, "1")
    await db.set_config(AUTO_RATE_FEEDBACK_KEY, "感谢 {buyer}，交易愉快")
    worker, account_id = await build_worker(db)
    client = worker.client
    assert isinstance(client, RateClient)

    task = await worker._maybe_auto_rate(rate_card())
    assert task is not None
    await task

    assert client.calls == [("2500000000000000001", "感谢 买家小王，交易愉快")]
    row = await db.get_rate_record(account_id, "2500000000000000001")
    assert row is not None
    assert row["status"] == "rated"
    assert row["feedback"] == "感谢 买家小王，交易愉快"

    # 同一条评价请求再次出现，不再打接口
    assert await worker._maybe_auto_rate(rate_card()) is None
    assert len(client.calls) == 1


async def test_disabled_or_untrusted_request_does_not_rate(db):
    worker, _ = await build_worker(db)
    client = worker.client
    assert isinstance(client, RateClient)

    await db.set_config(AUTO_RATE_ENABLED_KEY, "0")
    assert await worker._maybe_auto_rate(rate_card()) is None
    assert client.calls == []

    await db.set_config(AUTO_RATE_ENABLED_KEY, "1")
    untrusted = rate_card()
    untrusted.order_id_trusted = False
    assert await worker._maybe_auto_rate(untrusted) is None
    assert client.calls == []


async def test_failed_rating_is_recorded_and_recently_retried_is_blocked(db):
    await db.set_config(AUTO_RATE_ENABLED_KEY, "1")
    worker, account_id = await build_worker(db, RateClient(result=(False, "评价接口拒绝")))
    client = worker.client
    assert isinstance(client, RateClient)

    task = await worker._maybe_auto_rate(rate_card())
    assert task is not None
    await task
    row = await db.get_rate_record(account_id, "2500000000000000001")
    assert row is not None
    assert row["status"] == "failed"
    assert "拒绝" in row["detail"]
    assert await worker._maybe_auto_rate(rate_card()) is None


async def test_rate_poll_finds_transaction_success_order(db):
    await db.set_config(AUTO_RATE_ENABLED_KEY, "1")
    worker, account_id = await build_worker(db)
    client = worker.client
    assert isinstance(client, RateClient)

    done = await worker._poll_rate_candidates()
    assert done == 1
    assert client.calls[0][0] == "2500000000000000002"
    row = await db.get_rate_record(account_id, "2500000000000000002")
    assert row is not None and row["status"] == "rated"


class RateManager:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def sync(self) -> None:
        pass

    async def stop_all(self) -> None:
        pass

    def status(self) -> list[dict[str, Any]]:
        return []


@pytest.fixture()
async def admin_client(tmp_path):
    db = Database(tmp_path / "web.db")
    app = create_app(db=db, manager=RateManager(db))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1") as ac:
        async with app.router.lifespan_context(app):
            yield ac, db


async def test_auto_rate_settings_endpoint(admin_client):
    client, _db = admin_client
    got = (await client.get("/api/config/settings")).json()
    assert got["auto_rate"]["enabled"] is False

    res = await client.put("/api/config/settings", json={
        "auto_rate_enabled": True,
        "auto_rate_feedback": "  谢谢支持  ",
    })
    assert res.status_code == 200
    assert res.json()["auto_rate"]["enabled"] is True
    assert res.json()["auto_rate"]["feedback"] == "谢谢支持"
    cfg = (await client.get("/api/config")).json()
    assert cfg["auto_rate"]["enabled"] is True

    # 只改自动评价时不能把全局模型白名单清空
    await client.put("/api/config/settings", json={"model_limits": "deepseek-v4.1-flash"})
    await client.put("/api/config/settings", json={"auto_rate_enabled": False})
    assert (await client.get("/api/config")).json()["newapi"]["model_limits"] == "deepseek-v4.1-flash"


async def test_rate_records_endpoint(admin_client):
    client, db = admin_client
    await db.record_rate(
        account_id=1, order_id="2500000000000000009", status="rated",
        feedback="不错的买家", buyer_name="买家小王",
    )
    rows = (await client.get("/api/rate-records")).json()
    assert len(rows) == 1
    assert rows[0]["order_id"] == "2500000000000000009"
    assert rows[0]["status"] == "rated"


async def test_client_rate_buyer_calls_mtop():
    from app.client import XianyuClient

    client = XianyuClient("unb=2000000000002; _m_h5_tk=token_1")
    seen: list[tuple[str, dict[str, Any], str]] = []

    async def fake_mtop(api, data, version="1.0"):
        seen.append((api, data, version))
        return {"ret": ["SUCCESS::调用成功"]}

    client.mtop = fake_mtop  # type: ignore[method-assign]
    ok, detail = await client.rate_buyer("2500000000000000001", " 不错的 买家 ")
    assert ok is True and detail == ""
    assert seen[0][0] == "mtop.taobao.idle.rate.create"
    assert seen[0][1]["tradeId"] == "2500000000000000001"
    assert seen[0][1]["rate"] == 1
    assert seen[0][1]["feedback"] == "不错的 买家"
    assert seen[0][2] == "4.0"


async def test_client_list_sold_orders_parses_rate_status():
    from app.client import XianyuClient

    client = XianyuClient("unb=2000000000002; _m_h5_tk=token_1")

    async def fake_mtop(api, data, version="1.0"):
        return {
            "ret": ["SUCCESS::调用成功"],
            "data": {"module": {"items": [{
                "commonData": {"orderId": "2500000000000000001", "itemId": "i1",
                               "orderStatus": "交易成功", "sellerRateStatus": "0"},
                "buyerInfoVO": {"buyerId": "b1", "userNick": "买家小王"},
                "priceVO": {"totalPrice": "10.00"},
            }]}},
        }

    client.mtop = fake_mtop  # type: ignore[method-assign]
    orders = await client.list_sold_orders()
    assert orders[0]["order_id"] == "2500000000000000001"
    assert orders[0]["order_status"] == "交易成功"
    assert orders[0]["seller_rate_status"] == "0"
    assert orders[0]["buyer_name"] == "买家小王"
