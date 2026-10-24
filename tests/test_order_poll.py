"""订单轮询兜底：IM 推送丢了也能按订单发货。

订单结构用的是线上真实回包（mtop.taobao.idle.trade.merchant.sold.get）。
"""

from __future__ import annotations

import pytest

from app.engine import DeliveryEngine
from app.runner import AccountWorker

MY = "4100000003"
BUYER = "2000000000003"
ITEM_ID = "2000000000001"
ORDER_ID = "2500000000000000006"
CID = "68100583197"


def real_order(order_id=ORDER_ID, item_id=ITEM_ID, buyer=BUYER, status="待发货", price="1.00"):
    """线上真实结构（节选关键字段）。"""
    return {
        "commonData": {
            "orderId": order_id,
            "itemId": item_id,
            "orderStatus": status,
            "createTime": "2026-10-23 15:20:00",
            "paySuccessTime": "2026-10-23 15:20:15",
            "inRefund": False,
        },
        "buyerInfoVO": {"buyerId": buyer, "userNick": "示例酸酸"},
        "priceVO": {"auctionPrice": price, "totalPrice": price, "buyNum": 1},
    }


class FakeClient:
    user_id = MY

    def __init__(self, orders, cid=CID):
        self.sent = []
        self.confirmed = []
        self.acked = []
        self.websocket = self
        self.orders = orders
        self.cid = cid
        self.created_chats = []
        self.pending_calls = 0

    async def ack(self, ws, frame):
        self.acked.append(frame)

    async def send_text_safe(self, chat_id, to_id, text):
        self.sent.append((chat_id, to_id, text))

    async def confirm_shipping(self, order_id):
        self.confirmed.append(order_id)
        return True, "SUCCESS"

    async def find_order(self, order_id):
        for o in self.orders:
            if str(o["commonData"]["orderId"]) == str(order_id):
                return o
        return None

    async def list_pending_orders(self):
        self.pending_calls += 1
        out = []
        for o in self.orders:
            common = o["commonData"]
            if common.get("orderStatus") != "待发货":
                continue
            buyer = o.get("buyerInfoVO") or {}
            price = o.get("priceVO") or {}
            out.append({
                "order_id": str(common["orderId"]),
                "item_id": str(common.get("itemId") or ""),
                "buyer_id": str(buyer.get("buyerId") or ""),
                "buyer_name": str(buyer.get("userNick") or ""),
                "amount": price.get("totalPrice"),
                "raw": o,
            })
        return out

    async def create_chat(self, to_id, item_id):
        self.created_chats.append((to_id, item_id))
        return self.cid

    async def close(self):
        pass


class FakeProvisioner:
    def __init__(self):
        self.calls = []

    async def provision(self, quota_usd, label, username=""):
        from app.upstream import Provisioned

        self.calls.append((quota_usd, label))
        return Provisioned("CARD-POLL-1", "sk-a…z", quota_usd, "1")


async def build(db, orders):
    account_id = await db.upsert_account("主号", "unb=" + MY + "; _m_h5_tk=t")
    account = await db.get_account(account_id)
    provisioner = FakeProvisioner()
    worker = AccountWorker(db, account, DeliveryEngine(db), provisioner)
    worker.client = FakeClient(orders)
    return worker, provisioner


UPSTREAM_RULE = {
    "name": "按金额", "item_id": ITEM_ID, "enabled": True, "mode": "upstream",
    "content": "你好 {buyer}，卡密：{card}\n额度 {quota} USD",
    "auto_confirm": True, "priority": 0, "remark": "",
    "usd_multiplier": 1.0, "fixed_usd": 0.0,
}


async def test_polling_delivers_paid_order_without_any_push(db):
    """完全没有 IM 推送，只靠订单轮询也要能发货。"""
    await db.save_rule(dict(UPSTREAM_RULE))
    worker, provisioner = await build(db, [real_order()])

    await worker._poll_pending_orders()

    assert len(provisioner.calls) == 1
    assert provisioner.calls[0][0] == pytest.approx(1.0)
    assert worker.client.created_chats == [(BUYER, ITEM_ID)]
    assert len(worker.client.sent) == 1
    chat_id, to_id, text = worker.client.sent[0]
    assert chat_id == CID
    assert to_id == BUYER
    assert "CARD-POLL-1" in text
    assert worker.client.confirmed == [ORDER_ID]

    orders = await db.list_orders()
    assert orders[0]["status"] == "delivered"
    assert orders[0]["order_id"] == ORDER_ID


async def test_polling_skips_unpaid_and_closed(db):
    await db.save_rule(dict(UPSTREAM_RULE))
    orders = [
        real_order(order_id="1", status="待付款"),
        real_order(order_id="2", status="交易关闭"),
        real_order(order_id="3", status="已发货"),
    ]
    worker, provisioner = await build(db, orders)

    await worker._poll_pending_orders()

    assert provisioner.calls == []
    assert worker.client.sent == []


async def test_polling_does_not_repeat_delivered_order(db):
    await db.save_rule(dict(UPSTREAM_RULE))
    await db.record_order(
        account_id=1, order_id=ORDER_ID, item_id=ITEM_ID, buyer_id=BUYER,
        buyer_name="示例酸酸", rule_id=1, card_id=None, content="x",
        status="delivered", detail="",
    )
    worker, provisioner = await build(db, [real_order()])

    await worker._poll_pending_orders()

    assert provisioner.calls == []


async def test_polling_without_matching_rule_does_nothing(db):
    await db.save_rule({**UPSTREAM_RULE, "item_id": "999999"})
    worker, provisioner = await build(db, [real_order()])

    await worker._poll_pending_orders()

    assert provisioner.calls == []
    assert worker.client.sent == []


async def test_polling_reports_create_chat_failure(db):
    await db.save_rule(dict(UPSTREAM_RULE))

    class BrokenChatClient(FakeClient):
        async def create_chat(self, to_id, item_id):
            from app.client import XianyuError

            raise XianyuError("创建会话失败（code=400）")

    account_id = await db.upsert_account("主号", "unb=" + MY + "; _m_h5_tk=t")
    account = await db.get_account(account_id)
    worker = AccountWorker(db, account, DeliveryEngine(db), FakeProvisioner())
    worker.client = BrokenChatClient([real_order()])

    await worker._poll_pending_orders()

    orders = await db.list_orders()
    assert orders[0]["status"] == "upstream_failed"
    assert "创建会话失败" in orders[0]["detail"]
