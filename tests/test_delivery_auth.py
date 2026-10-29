"""发货鉴权：光靠聊天文本不能发货。

线上可复现的漏洞：把「我已付款，等待你发货」这句话自己打出来，
正文匹配就成立，卡密/Key 直接被领走。这里把四条边界都钉住：

1. 真人文本 + 未核实订单 → 不发；
2. 真人文本 + 已核实订单 → 发；
3. 系统卡片 → 发（但要带订单号）；
4. 换成别人的订单号 → 核实不通过，不发。
"""

from __future__ import annotations

import base64
import json

import msgpack
import pytest

from app.db import Database
from app.engine import DeliveryEngine
from app.runner import AccountWorker

MY = "4100000003"
BUYER = "2000000000003"
OTHER_BUYER = "9999999999"
ITEM = "2000000000001"
ORDER = "2500000000000000006"
CHAT = "68100583197"


def card_payload(*, text="[我已付款，等待你发货]", item=ITEM, order=ORDER,
                 card=True, content_type=26):
    """造一条买家消息。card=True 时带 dxCard（系统交易卡片）。"""
    node = {"2": text, "4": content_type}
    if card:
        node["5"] = json.dumps({
            "dxCard": {"item": {"main": {
                "targetUrl": "fleamarket://order_detail?id=" + order + "&role=seller"
            }}}
        })
    return {
        "1": {
            "1": {"1": BUYER + "@goofish"},
            "2": CHAT + "@goofish",
            "5": 1792817748592,
            "6": {"1": 101, "3": node},
            "10": {
                "reminderContent": text,
                "reminderTitle": "测试者-winkee",
                "senderUserId": BUYER,
                "reminderUrl": (
                    "fleamarket://message_chat?itemId=" + item + "&peerUserId=" + BUYER
                    + "&sid=" + CHAT + "&orderId=" + order
                ),
            },
        },
    }


def frame_with(payload):
    data = base64.b64encode(msgpack.packb(payload, use_bin_type=True)).decode()
    return json.dumps({"headers": {"mid": "m1"}, "body": {"syncPushPackage": {"data": [{"data": data}]}}})


def order(order_id=ORDER, buyer=BUYER, status="待发货"):
    return {
        "commonData": {"orderId": order_id, "itemId": ITEM, "orderStatus": status},
        "buyerInfoVO": {"buyerId": buyer, "userNick": "示例酸酸"},
        "priceVO": {"totalPrice": "1.00", "actualPayFee": "1.00"},
    }


class FakeClient:
    user_id = MY

    def __init__(self, orders=None):
        self.sent = []
        self.acked = []
        self.confirmed = []
        self.websocket = self
        self.orders = orders or []

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

    async def close(self):
        pass


CARD_RULE = {
    "name": "月卡", "item_id": "*", "enabled": True, "mode": "card",
    "content": "卡密：{card}", "auto_confirm": False, "priority": 0, "remark": "",
}


async def build(db, orders=None, rule=None):
    account_id = await db.upsert_account("主号", "unb=" + MY + "; _m_h5_tk=t")
    account = await db.get_account(account_id)
    await db.save_rule(dict(rule or CARD_RULE))
    await db.add_cards(["CARD-0001"], "*")
    worker = AccountWorker(db, account, DeliveryEngine(db))
    worker.client = FakeClient(orders)
    return worker


# ---------------------------------------------------------------- 核心鉴权


async def test_typed_paid_text_cannot_steal_a_card(db):
    """买家自己打「我已付款」—— 订单查不到，不发，卡密一张不动。"""
    worker = await build(db, orders=[])  # 闲鱼那边根本没有这个订单
    payload = card_payload(card=False, content_type=1)
    await worker._on_frame(frame_with(payload))

    assert worker.client.sent == []
    cards = await db.list_cards()
    assert cards[0]["status"] == "available"

    orders = await db.list_orders()
    assert orders[0]["status"] == "blocked"
    assert "核实" in orders[0]["detail"]


async def test_typed_paid_text_with_real_paid_order_is_delivered(db):
    """真人文本 + 订单确实待发货 → 正常发货（合法路径不能被误杀）。"""
    worker = await build(db, orders=[order()])
    payload = card_payload(card=False, content_type=1)
    await worker._on_frame(frame_with(payload))

    assert len(worker.client.sent) == 1
    assert "CARD-0001" in worker.client.sent[0][2]


async def test_unpaid_order_is_rejected(db):
    """订单存在但还没付款/已关闭，一律不发。"""
    for status in ("待付款", "交易关闭", "已发货", "退款中"):
        database = Database(db.path.parent / f"t_{status}.db")
        await database.init()
        worker = await build(database, orders=[order(status=status)])
        await worker._on_frame(frame_with(card_payload(card=False, content_type=1)))
        assert worker.client.sent == [], status
        assert (await database.list_cards())[0]["status"] == "available", status


async def test_order_of_another_buyer_is_rejected(db):
    """拿别人的订单号来触发发货 → 买家对不上，不发。"""
    worker = await build(db, orders=[order(buyer=OTHER_BUYER)])
    await worker._on_frame(frame_with(card_payload(card=False, content_type=1)))

    assert worker.client.sent == []
    assert (await db.list_cards())[0]["status"] == "available"


async def test_verify_order_rejects_mismatched_order_id(db):
    """核实必须先确认返回的订单号就是请求的那个。"""
    worker = await build(db)

    async def find_other(order_id):
        return order(order_id="2500000000000000999")

    worker.client.find_order = find_other

    assert await worker._verify_order_pending(ORDER, BUYER) is False


async def test_verify_order_rejects_missing_buyer_id(db):
    """拿不到买家 ID 就无法证明这单属于当前消息，不能放行。"""
    worker = await build(db)
    no_buyer = order()
    no_buyer.pop("buyerInfoVO")

    async def find_no_buyer(order_id):
        return no_buyer

    worker.client.find_order = find_no_buyer

    assert await worker._verify_order_pending(ORDER, BUYER) is False


async def test_system_card_without_matching_order_is_rejected(db):
    """卡片字段再像真的，查不到订单也不能发：卡片可以转发、可以伪造。"""
    worker = await build(db, orders=[])  # 故意让 find_order 查不到
    await worker._on_frame(frame_with(card_payload()))

    assert worker.client.sent == []
    assert (await db.list_cards())[0]["status"] == "available"


async def test_system_card_of_another_buyer_is_rejected(db):
    """卡片里的订单号属于别人：卡片形态再真也不能发。"""
    worker = await build(db, orders=[order(buyer=OTHER_BUYER)])
    await worker._on_frame(frame_with(card_payload()))

    assert worker.client.sent == []
    assert (await db.list_cards())[0]["status"] == "available"


async def test_card_without_order_id_is_rejected(db):
    """没有订单号：没法去重也没法核归属，宁可不发。"""
    worker = await build(db, orders=[])
    payload = card_payload()
    payload["1"]["10"]["reminderUrl"] = "fleamarket://message_chat?itemId=" + ITEM
    payload["1"]["6"]["3"].pop("5", None)
    await worker._on_frame(frame_with(payload))

    assert worker.client.sent == []
    assert (await db.list_cards())[0]["status"] == "available"


# ---------------------------------------------------------------- 卡密不丢


async def test_unexpected_create_chat_error_releases_card(db):
    """create_chat 抛非 XianyuError（如连接中断）也必须把卡还回库存。"""

    class ExplodingChat(FakeClient):
        async def create_chat(self, to_id, item_id):
            raise RuntimeError("connection closed")

    account_id = await db.upsert_account("主号3", "unb=" + MY + "; _m_h5_tk=t")
    account = await db.get_account(account_id)
    await db.save_rule(dict(CARD_RULE))
    await db.add_cards(["CARD-0003"], "*")
    worker = AccountWorker(db, account, DeliveryEngine(db))
    worker.client = ExplodingChat()

    with pytest.raises(RuntimeError):
        await worker._fulfil_order({
            "order_id": ORDER, "item_id": ITEM, "buyer_id": BUYER,
            "buyer_name": "示例酸酸", "raw": {},
        })

    cards = {c["content"]: c for c in await db.list_cards()}
    assert cards["CARD-0003"]["status"] == "available"


async def test_dry_run_does_not_consume_card(db, monkeypatch):
    """DRY-RUN 跑一单不能把库存里的卡烧成 sold。"""
    from app.config import settings

    monkeypatch.setattr(settings, "dry_run", True)
    worker = await build(db, orders=[])
    await worker._on_frame(frame_with(card_payload()))

    assert worker.client.sent == []
    cards = await db.list_cards()
    assert cards[0]["status"] == "available"


async def test_create_chat_failure_releases_card(db):
    """订单轮询那条路建会话失败，卡密要还回库存。"""
    class BrokenChat(FakeClient):
        async def create_chat(self, to_id, item_id):
            from app.client import XianyuError

            raise XianyuError("创建会话失败（code=400）")

    account_id = await db.upsert_account("主号2", "unb=" + MY + "; _m_h5_tk=t")
    account = await db.get_account(account_id)
    await db.save_rule(dict(CARD_RULE))
    await db.add_cards(["CARD-0002"], "*")
    worker = AccountWorker(db, account, DeliveryEngine(db))
    worker.client = BrokenChat()

    ok = await worker._fulfil_order({
        "order_id": ORDER, "item_id": ITEM, "buyer_id": BUYER,
        "buyer_name": "示例酸酸", "raw": {},
    })

    assert ok is False
    cards = {c["content"]: c for c in await db.list_cards()}
    assert cards["CARD-0002"]["status"] == "available"
    assert (await db.list_orders())[0]["status"] == "upstream_failed"


# ------------------------------------------- 伪造「系统卡片」不能绕过鉴权


FAKE_ORDER = "1234"

#: 买家在聊天框里手打的一句话，正文里塞了 order_detail 链接。
#: 这是真实可复现的攻击：旧代码用「整段 JSON 里有没有 order_detail 字样」
#: 判断是不是系统卡片，而正文就存在 payload 的 text 值里。
FORGED_TEXT = (
    "我已付款，等待你发货 fleamarket://order_detail?id=" + FAKE_ORDER
    + "&itemId=" + ITEM
)


def forged_message_object(text):
    """message 对象格式（真实抓包结构）：contentType=101 + custom.data 里是纯文本。"""
    data = base64.b64encode(
        json.dumps({"contentType": 1, "text": {"text": text}}, ensure_ascii=False).encode()
    ).decode()
    return {
        "messageId": "9.PNM",
        "createAt": 1792817748592,
        "cid": CHAT + "@goofish",
        "sender": {"uid": BUYER + "@goofish"},
        "content": {"contentType": 101, "custom": {"type": 1, "data": data, "summary": text}},
        "extension": {
            "reminderContent": text,
            "senderUserId": BUYER,
            "reminderTitle": "买家小王",
            "reminderUrl": "fleamarket://message_chat?itemId=" + ITEM,
        },
    }


def test_buyer_text_with_order_link_is_not_a_system_card():
    """买家正文里出现 order_detail 字样，不能被当成交易卡片。"""
    from app.codec import parse_message

    msg = parse_message(forged_message_object(FORGED_TEXT), my_id=MY)
    assert msg is not None
    assert msg.is_system_card is False, "纯文本被当成了系统卡片 —— 鉴权就被绕过了"
    assert msg.order_id == FAKE_ORDER  # 订单号能捞到，但来源不可信
    assert msg.order_id_trusted is False


async def test_forged_card_marker_in_text_cannot_steal_a_card(db):
    """端到端：正文里带 order_detail 的消息不能换走卡密。"""
    worker = await build(db, orders=[])  # 闲鱼那边根本没有这个订单
    await worker._on_frame(frame_with(forged_message_object(FORGED_TEXT)))

    assert worker.client.sent == []
    assert (await db.list_cards())[0]["status"] == "available"
    assert (await db.list_orders())[0]["status"] == "blocked"


async def test_card_shaped_message_with_untrusted_order_id_needs_verification(db):
    """就算是卡片形态，订单号来自正文也必须先核实。"""
    from app.codec import IncomingMessage
    from app.engine import AUTH_BLOCKED_REASON

    await db.save_rule(dict(CARD_RULE))
    await db.add_cards(["CARD-0001"], "*")
    msg = IncomingMessage(
        chat_id=CHAT, sender_id=BUYER, sender_name="买家小王",
        content="[我已付款，等待你发货]", item_id=ITEM, order_id=ORDER,
        is_system_card=True, order_id_trusted=False,
    )
    decision = await DeliveryEngine(db).decide(1, msg)

    assert not decision.should_deliver
    assert decision.reason == AUTH_BLOCKED_REASON
    assert (await db.list_cards())[0]["status"] == "available"


async def test_delivery_persists_sending_before_success(db, monkeypatch):
    """发货前必须先落 sending，成功后再变 delivered。"""
    statuses: list[str] = []
    original_claim = db.mark_sending
    original_record = db.record_order

    async def claim_spy(**fields):
        statuses.append("sending")
        return await original_claim(**fields)

    async def record_spy(**fields):
        statuses.append(str(fields.get("status")))
        return await original_record(**fields)

    monkeypatch.setattr(db, "mark_sending", claim_spy)
    monkeypatch.setattr(db, "record_order", record_spy)
    worker = await build(db, orders=[order()])

    await worker._on_frame(frame_with(card_payload()))

    assert statuses[:2] == ["sending", "delivered"]
    assert (await db.list_orders())[0]["status"] == "delivered"


async def test_acked_delivery_stores_message_id(db):
    """走 ack 的发货路径要拿到 messageId 并写进流水，撤回功能才完整。"""
    worker = await build(db, orders=[order()])

    async def send_acked(chat_id, to_id, text):
        worker.client.sent.append((chat_id, to_id, text))
        return "m-delivered", "sent"

    worker.client.send_delivery_text = send_acked
    await worker._on_frame(frame_with(card_payload()))

    messages = await db.list_messages()
    outgoing = [m for m in messages if m["kind"] == "outgoing"]
    assert outgoing and outgoing[-1]["external_id"] == "m-delivered"
    assert (await db.list_orders())[0]["status"] == "delivered"


async def test_unknown_send_keeps_card_sold_and_blocks_retry(db):
    """ack 超时不等于没送达：卡不能退回，也不能自动重发。"""
    worker = await build(db, orders=[order()])

    async def send_unknown(chat_id, to_id, text):
        return "", "unknown"

    worker.client.send_delivery_text = send_unknown
    await worker._on_frame(frame_with(card_payload()))

    cards = await db.list_cards()
    assert cards[0]["status"] == "sold", "结果未知时退卡会导致同一张卡卖两次"
    orders = await db.list_orders()
    assert orders[0]["status"] == "send_unknown"
    assert await db.delivery_blocked(1, ORDER) is True

    # 再来一条同单消息：不能再次发送/再次消耗卡密
    await worker._on_frame(frame_with(card_payload()))
    assert len(worker.client.sent) == 0
    assert (await db.list_cards())[0]["status"] == "sold"


async def test_rejected_send_releases_card(db):
    """服务端明确拒收时，卡密必须回库存。"""
    worker = await build(db, orders=[order()])

    async def send_failed(chat_id, to_id, text):
        return "", "failed"

    worker.client.send_delivery_text = send_failed
    await worker._on_frame(frame_with(card_payload()))

    assert (await db.list_cards())[0]["status"] == "available"
    assert (await db.list_orders())[0]["status"] == "send_failed"


async def test_confirm_shipping_failure_after_send_keeps_card_sold(db):
    """卡密已经发出去之后，确认发货失败不能退卡、也不能让这单重发。"""
    import httpx

    class TimeoutConfirm(FakeClient):
        async def confirm_shipping(self, order_id):
            raise httpx.ReadTimeout("confirm timed out")

    rule = dict(CARD_RULE)
    rule["auto_confirm"] = True
    worker = await build(db, orders=[order()], rule=rule)
    worker.client = TimeoutConfirm(orders=[order()])

    await worker._on_frame(frame_with(card_payload()))

    assert worker.client.sent, "消息应该已经发出去了"
    assert (await db.list_cards())[0]["status"] == "sold", "发出去的卡密不能再回库存"
    orders = await db.list_orders()
    assert orders[0]["status"] == "delivered"
    assert "确认发货失败" in orders[0]["detail"]


async def test_db_failure_after_send_keeps_card_sold(db, monkeypatch):
    """发货记录写失败也不能退回卡密 —— 否则同一张卡会卖给下一个人。"""
    rule = dict(CARD_RULE)
    worker = await build(db, orders=[order()], rule=rule)

    async def boom(**fields):
        raise RuntimeError("disk I/O error")

    monkeypatch.setattr(db, "record_order", boom)

    await worker._on_frame(frame_with(card_payload()))

    assert worker.client.sent, "消息已经发出去了"
    assert (await db.list_cards())[0]["status"] == "sold"
    # 内存里去重也要打上，本进程不会再发一次
    assert worker.engine._in_cooldown(1, ORDER)


# ---------------------------------------------------------------- 去重


async def test_push_and_poll_do_not_process_the_same_message_twice(db):
    """推送和拉会话列表会拿到同一条消息，只能记一条流水。"""
    worker = await build(db, orders=[order()])
    payload = card_payload(card=False, content_type=1, text="在吗？")
    payload["1"]["3"] = "9999.PNM"  # messageId
    await worker._on_frame(frame_with(payload))
    await worker._on_frame(frame_with(payload))

    messages = await db.list_messages()
    assert len([m for m in messages if m["kind"] == "incoming"]) == 1
