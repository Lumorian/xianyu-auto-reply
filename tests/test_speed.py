"""响应速度相关的行为：会话复用、金额走轮询结果、推送提前唤醒轮询。

这三条都是为了把「买家付款 → 收到卡密」的等待从「等一轮轮询（20~60 秒）」
压到「一两秒」，同时不增加任何额外的接口调用。
"""

from __future__ import annotations

import asyncio

import pytest

from app.config import settings
from app.engine import DeliveryEngine
from app.runner import AccountWorker

MY = "4100000003"
BUYER = "2000000000003"
ITEM_ID = "2000000000001"
ORDER_ID = "2500000000000000006"
CID = "68100583197"


def order_item(order_id=ORDER_ID, item_id=ITEM_ID, buyer=BUYER, status="待发货", price="1.00"):
    return {
        "commonData": {"orderId": order_id, "itemId": item_id, "orderStatus": status},
        "buyerInfoVO": {"buyerId": buyer, "userNick": "买家"},
        "priceVO": {"auctionPrice": price, "totalPrice": price, "buyNum": 1},
    }


class FakeClient:
    user_id = MY

    def __init__(self, orders):
        self.sent = []
        self.confirmed = []
        self.websocket = self
        self.orders = orders
        self.created_chats = []
        self.order_lookups = []
        self.list_calls = 0

    async def ack(self, ws, frame):
        pass

    async def send_text_safe(self, chat_id, to_id, text):
        self.sent.append((chat_id, to_id, text))

    async def confirm_shipping(self, order_id):
        self.confirmed.append(order_id)
        return True, "SUCCESS"

    async def find_order(self, order_id):
        self.order_lookups.append(order_id)
        for o in self.orders:
            if str(o["commonData"]["orderId"]) == str(order_id):
                return o
        return None

    async def create_chat(self, to_id, item_id):
        self.created_chats.append((to_id, item_id))
        return CID

    async def list_pending_orders(self):
        self.list_calls += 1
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

    async def close(self):
        pass


class FakeProvisioner:
    def __init__(self):
        self.calls = []

    async def provision(self, quota_usd, label, username=""):
        from app.upstream import Provisioned

        self.calls.append((quota_usd, label))
        return Provisioned("CARD-FAST-1", "sk-a…z", quota_usd, "1")


RULE = {
    "name": "按金额", "item_id": ITEM_ID, "enabled": True, "mode": "upstream",
    "content": "卡密：{card}", "auto_confirm": True, "priority": 0, "remark": "",
    "usd_multiplier": 1.0, "fixed_usd": 0.0,
}


async def build(db, orders):
    account_id = await db.upsert_account("主号", "unb=" + MY + "; _m_h5_tk=t")
    account = await db.get_account(account_id)
    provisioner = FakeProvisioner()
    worker = AccountWorker(db, account, DeliveryEngine(db), provisioner)
    worker.client = FakeClient(orders)
    return worker, provisioner


async def test_amount_from_poll_avoids_order_lookup(db):
    """轮询那条订单里已经带金额了，不该再查一次订单详情。"""
    await db.save_rule(dict(RULE))
    worker, provisioner = await build(db, [order_item()])

    await worker._poll_pending_orders()

    assert provisioner.calls == [(1.0, pytest.approx(provisioner.calls[0][1]))]
    assert worker.client.order_lookups == []  # 一次详情都没查
    assert len(worker.client.sent) == 1


async def test_chat_reused_from_history(db):
    """这个买家为这个商品发过消息 → 直接复用会话，不再 create_chat。"""
    await db.save_rule(dict(RULE))
    await db.log_message(
        account_id=1, chat_id=CID, sender_id=BUYER, sender_name="买家",
        item_id=ITEM_ID, order_id=None, content="在吗？", kind="incoming",
    )
    worker, _ = await build(db, [order_item()])

    await worker._poll_pending_orders()

    assert worker.client.created_chats == []
    assert worker.client.sent[0][0] == CID


async def test_chat_not_reused_across_items(db):
    """会话是按「买家 × 商品」分开的，别的商品的会话不能拿来用。"""
    await db.save_rule({**RULE, "item_id": "*"})
    await db.log_message(
        account_id=1, chat_id="999888", sender_id=BUYER, sender_name="买家",
        item_id="55555", order_id=None, content="在吗？", kind="incoming",
    )
    worker, _ = await build(db, [order_item()])

    await worker._poll_pending_orders()

    assert worker.client.created_chats == [(BUYER, ITEM_ID)]
    assert worker.client.sent[0][0] == CID


async def test_push_hint_wakes_poll_loop_immediately(db, monkeypatch):
    """收到推送就把轮询叫醒，不用等满一个间隔。"""
    monkeypatch.setattr(settings, "order_poll_interval", 3600, raising=False)
    # 节流窗口很小（真实配置是 3 秒）：推送触发的那次补查几乎立刻发生
    monkeypatch.setattr(settings, "order_hint_poll_gap", 0.05, raising=False)
    worker, _ = await build(db, [])
    await db.save_rule(dict(RULE))

    task = asyncio.create_task(worker._order_poll_loop())
    try:
        await asyncio.sleep(0.05)
        first = worker.client.list_calls
        assert first >= 1  # 连上就先查一次

        worker._hint_poll()  # 模拟「收到一帧推送」
        for _ in range(50):
            if worker.client.list_calls > first:
                break
            await asyncio.sleep(0.02)
        assert worker.client.list_calls > first
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_same_order_concurrent_triggers_deliver_once(db):
    """推送和轮询同时命中同一单时，只能发一次。"""
    await db.save_rule(dict(RULE))
    worker, provisioner = await build(db, [order_item()])
    order = (await worker.client.list_pending_orders())[0]

    await asyncio.gather(
        worker._fulfil_order(dict(order)),
        worker._fulfil_order(dict(order)),
    )

    assert len(provisioner.calls) == 1
    assert len(worker.client.sent) == 1


async def test_hint_gap_backs_off_when_nothing_found(db):
    """推送叫醒却查不到订单 → 拉长触发间隔，别把接口打成风控。"""
    worker, _ = await build(db, [])
    worker._hint_gap = 5.0

    for expected in (10.0, 20.0, 30.0, 30.0):  # 翻倍，封顶 30 秒
        worker._tune_hint_gap(found=0, by_hint=True, base_gap=5.0, interval=60)
        assert worker._hint_gap == pytest.approx(expected)


async def test_hint_gap_resets_after_a_delivery(db):
    """一旦真有单，触发间隔立刻恢复成配置值，下一单又快起来。"""
    worker, _ = await build(db, [])
    worker._hint_gap = 30.0

    worker._tune_hint_gap(found=1, by_hint=True, base_gap=5.0, interval=60)

    assert worker._hint_gap == pytest.approx(5.0)


async def test_hint_gap_never_exceeds_poll_interval(db):
    """轮询间隔配得很小时，节流窗口也不能超过它（否则等于没在轮询）。"""
    worker, _ = await build(db, [])
    worker._hint_gap = 10.0

    worker._tune_hint_gap(found=0, by_hint=True, base_gap=5.0, interval=8)

    assert worker._hint_gap <= 8.0


async def test_concurrent_duplicate_does_not_eat_card_stock(db):
    """两条路同时命中同一单：只发一次，多锁的卡密要还回池子。"""
    await db.save_rule({
        "name": "卡密池", "item_id": ITEM_ID, "enabled": True, "mode": "card",
        "content": "卡密：{card}", "auto_confirm": False, "priority": 0, "remark": "",
    })
    await db.add_cards(["CARD-A"], ITEM_ID)
    await db.add_cards(["CARD-B"], ITEM_ID)
    worker, _ = await build(db, [order_item()])
    order = (await worker.client.list_pending_orders())[0]

    await asyncio.gather(
        worker._fulfil_order(dict(order)),
        worker._fulfil_order(dict(order)),
    )

    assert len(worker.client.sent) == 1
    stock = await db.card_stock()
    remaining = sum(s["available"] for s in stock)
    assert remaining == 1, "多发一次就该少一张；被跳过的那次必须把卡密还回来"
