"""用线上真实抓到的 flat 负载回放整条链路（不打网络）。"""

from __future__ import annotations

import base64
import json

import pytest

from app.db import Database
from app.engine import DeliveryEngine
from app.runner import AccountWorker

MY = "4100000003"
BUYER = "4100000001"
ITEM_ID = "2000000000001"
ORDER_ID = "2500000000000000001"
SESSION = "61051610079"


class FakeClient:
    user_id = MY

    def __init__(self) -> None:
        self.sent: list[tuple[str, str, str]] = []
        self.confirmed: list[str] = []
        self.acked: list[dict] = []
        self.websocket = self
        self.order = {"priceVO": {"actualPayFee": "1.00"}}

    async def ack(self, ws, frame):
        self.acked.append(frame)

    async def send_text_safe(self, chat_id, to_id, text):
        self.sent.append((chat_id, to_id, text))

    async def confirm_shipping(self, order_id):
        self.confirmed.append(order_id)
        return True, "SUCCESS"

    async def find_order(self, order_id):
        return self.order

    async def close(self):
        pass


class FakeProvisioner:
    def __init__(self) -> None:
        self.calls = []

    async def provision(self, quota_usd, label, username=""):
        from app.upstream import Provisioned

        self.calls.append((quota_usd, label))
        return Provisioned("CARD-FLAT-0001", "sk-abc…wxyz", quota_usd, "1")


def frame_with(payload: dict) -> str:
    """把 flat 负载包成真实 WebSocket 帧（base64 后放进 syncPushPackage）。"""
    inner = base64.b64encode(json.dumps(payload, ensure_ascii=False).encode()).decode()
    return json.dumps({
        "headers": {"mid": "m1", "sid": "s1"},
        "body": {"syncPushPackage": {"data": [{
            "bizType": 370, "objectType": 370000, "streamId": 370, "data": inner,
        }]}},
    })


def flat(content, *, owner=MY, ext=BUYER):
    return {
        "chatType": 1, "incrementType": 1, "sessionId": SESSION,
        "operation": {
            "content": content,
            "receiverIds": [MY],
            "sessionInfo": {
                "sessionId": SESSION, "ownerUserId": owner, "createTime": 1777287861000,
                "extensions": {"itemId": ITEM_ID, "itemTitle": "dsv4.1f1017",
                               "extUserId": ext, "itemSellerId": owner},
            },
        },
    }


def paid_card():
    card = {"contentType": 26, "dxCard": {"item": {"main": {
        "exContent": {"title": "我已付款，等待你发货"},
        "targetUrl": "fleamarket://order_detail?id=" + ORDER_ID + "&role=seller",
    }}}}
    return flat({"contentType": 26,
                 "custom": {"type": 26, "data": base64.b64encode(json.dumps(card).encode()).decode()}})


async def build(db: Database):
    account_id = await db.upsert_account("主号", "unb=" + MY + "; _m_h5_tk=tok_1")
    account = await db.get_account(account_id)
    provisioner = FakeProvisioner()
    worker = AccountWorker(db, account, DeliveryEngine(db), provisioner)
    worker.client = FakeClient()
    return worker, provisioner, account_id


async def test_real_flat_paid_card_triggers_delivery(db):
    """线上真实格式的付款卡片，回放后必须走完发货。"""
    await db.save_rule({
        "name": "按金额", "item_id": ITEM_ID, "enabled": True, "mode": "upstream",
        "content": "你好 {buyer}，卡密：{card}\n额度 {quota} USD",
        "auto_confirm": True, "priority": 0, "remark": "",
        "usd_multiplier": 1.0, "fixed_usd": 0.0,
    })
    worker, provisioner, _ = await build(db)

    await worker._on_frame(frame_with(paid_card()))

    assert len(provisioner.calls) == 1
    assert provisioner.calls[0][0] == pytest.approx(1.0)
    assert len(worker.client.sent) == 1
    chat_id, to_id, text = worker.client.sent[0]
    assert chat_id == SESSION
    assert to_id == BUYER
    assert "CARD-FLAT-0001" in text
    assert worker.client.confirmed == [ORDER_ID]

    orders = await db.list_orders()
    assert orders[0]["status"] == "delivered"
    assert orders[0]["order_id"] == ORDER_ID
    assert orders[0]["amount_cny"] == pytest.approx(1.0)


async def test_real_flat_plain_text_is_logged_not_delivered(db):
    await db.save_rule({
        "name": "按金额", "item_id": "*", "enabled": True, "mode": "upstream",
        "content": "{card}", "auto_confirm": False, "priority": 0, "remark": "",
        "usd_multiplier": 1.0, "fixed_usd": 0.0,
    })
    worker, provisioner, _ = await build(db)

    await worker._on_frame(frame_with(flat({"contentType": 1, "text": {"text": "在吗？"}})))

    assert provisioner.calls == []
    assert worker.client.sent == []
    messages = await db.list_messages()
    assert messages[0]["content"] == "在吗？"


async def test_session_arouse_does_not_pollute_message_log(db):
    """会话引导消息不该进消息表，否则日志会被 133 条噪声刷满。"""
    worker, _, _ = await build(db)
    await worker._on_frame(frame_with(flat({"contentType": 8, "sessionArouse": {"sessionArouseInfo": {}}})))
    assert await db.list_messages() == []
