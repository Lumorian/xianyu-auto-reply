"""付款卡片只带 orderId 时，从订单补 itemId 再决策。"""

from __future__ import annotations

import base64
import json

from app.engine import DeliveryEngine
from app.runner import AccountWorker

MY = "4100000003"
BUYER = "4100000001"
ITEM_ID = "2000000000001"
ORDER_ID = "2500000000000000001"
SESSION = "61051610079"


class FakeClient:
    user_id = MY

    def __init__(self, order):
        self.sent = []
        self.confirmed = []
        self.acked = []
        self.websocket = self
        self.order = order
        self.order_lookups = []

    async def ack(self, ws, frame):
        self.acked.append(frame)

    async def send_text_safe(self, chat_id, to_id, text):
        self.sent.append((chat_id, to_id, text))

    async def confirm_shipping(self, order_id):
        self.confirmed.append(order_id)
        return True, "SUCCESS"

    async def find_order(self, order_id):
        self.order_lookups.append(order_id)
        return self.order

    async def close(self):
        pass


class FakeProvisioner:
    def __init__(self):
        self.calls = []

    async def provision(self, quota_usd, label, username="", model_limits=None):
        from app.upstream import Provisioned

        self.calls.append((quota_usd, label))
        return Provisioned("CARD-ORDER-1", "sk-a…z", quota_usd, "1")


def paid_card_via_order_only():
    """真实付款卡片：只有 orderId，没有 itemId（没有 reminderUrl）。"""
    card = json.dumps({
        "contentType": 26,
        "dxCard": {"item": {"main": {
            "exContent": {"title": "我已付款，等待你发货"},
            "targetUrl": "fleamarket://order_detail?id=" + ORDER_ID + "&role=seller",
        }}},
    }, ensure_ascii=False)
    return {"1": {
        "1": {"1": BUYER + "@goofish"},
        "2": SESSION + "@goofish",
        "3": "abc.PNM",
        "5": 1777287861000,
        "6": {"1": 101, "3": {"1": "", "2": "[我已付款，等待你发货]", "4": 26, "5": card}},
    }}


def frame_with(payload):
    inner = base64.b64encode(json.dumps(payload, ensure_ascii=False).encode()).decode()
    return json.dumps({"headers": {"mid": "m1"}, "body": {"syncPushPackage": {"data": [{"data": inner}]}}})


async def build(db, order):
    account_id = await db.upsert_account("主号", "unb=" + MY + "; _m_h5_tk=t")
    account = await db.get_account(account_id)
    provisioner = FakeProvisioner()
    worker = AccountWorker(db, account, DeliveryEngine(db), provisioner)
    worker.client = FakeClient(order)
    return worker, provisioner


async def test_item_id_backfilled_from_order(db):
    """商品专属规则 + 卡片不带 itemId → 必须先查订单补 itemId，才能命中规则。"""
    await db.save_rule({
        "name": "按金额", "item_id": ITEM_ID, "enabled": True, "mode": "upstream",
        "content": "卡密：{card}", "auto_confirm": True, "priority": 0, "remark": "",
        "usd_multiplier": 1.0, "fixed_usd": 0.0,
    })
    order = {"commonData": {"orderId": ORDER_ID, "itemId": ITEM_ID},
             "priceVO": {"actualPayFee": "1.00"}}
    worker, provisioner = await build(db, order)

    await worker._on_frame(frame_with(paid_card_via_order_only()))

    # 只查一次：补 itemId 拿到的订单详情被缓存，取金额时直接复用
    assert worker.client.order_lookups == [ORDER_ID]
    assert len(provisioner.calls) == 1
    assert len(worker.client.sent) == 1
    assert "CARD-ORDER-1" in worker.client.sent[0][2]
    orders = await db.list_orders()
    assert orders[0]["status"] == "delivered"
    assert orders[0]["item_id"] == ITEM_ID


async def test_no_backfill_when_rule_is_wildcard(db):
    """通配规则不需要补 itemId，就不该多打一次订单接口。"""
    await db.save_rule({
        "name": "通配", "item_id": "*", "enabled": True, "mode": "upstream",
        "content": "{card}", "auto_confirm": False, "priority": 0, "remark": "",
        "usd_multiplier": 1.0, "fixed_usd": 0.0,
    })
    order = {"commonData": {"orderId": ORDER_ID, "itemId": ITEM_ID},
             "priceVO": {"actualPayFee": "1.00"}}
    worker, provisioner = await build(db, order)

    await worker._on_frame(frame_with(paid_card_via_order_only()))

    assert len(provisioner.calls) == 1
    # 决策阶段已命中通配规则，不该为了补 itemId 再查一次
    assert worker.client.order_lookups == [ORDER_ID]  # 只来自 _provision 的金额查询
