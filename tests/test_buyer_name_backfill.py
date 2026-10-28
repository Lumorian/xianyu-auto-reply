"""买家昵称：新买家第一次下单时也不能叫「买家」。

线上实测的现象：买家没在聊天里说过话就下单，付款卡片自带的 reminderTitle
是卡片标题（「等待你发货」），聊天记录里也翻不到昵称，于是订单和令牌名
一路记成「买家」，后台根本认不出是谁买的。

真名其实一直在订单接口里（buyerInfoVO.userNick），只是以前没人去取。
这组测试钉住三件事：

1. 订单里的买家昵称只从 buyerInfoVO 取，不能顺手扫到卖家名/商品名；
2. 付款链路上先补名字再建令牌，令牌名与订单记录都得是真名；
3. 已经记成「买家」的历史流水，拿到真名后要回填。
"""

from __future__ import annotations

import base64
import json

import msgpack

from app.client import XianyuClient
from app.codec import buyer_nick_from_order, buyer_nick_from_session, is_placeholder_name
from app.engine import DeliveryEngine
from app.runner import AccountWorker

MY = "4100000003"
BUYER = "2000000000005"
ITEM = "2000000000001"
ORDER = "2500000000000000002"
CHAT = "68451018701"
NICK = "唐狙不辱屎命"


# ---------------------------------------------------------------- 纯函数


def test_buyer_nick_comes_from_buyer_info_vo():
    order = {
        "commonData": {"orderId": ORDER, "itemId": ITEM, "orderStatus": "待发货"},
        "buyerInfoVO": {"buyerId": BUYER, "userNick": NICK},
        # 卖家自己的昵称和商品标题也在这棵树里，绝不能被抓来当买家名
        "sellerInfoVO": {"userId": MY, "userNick": "卖家的名字"},
        "itemVO": {"title": "dsv4.1f1017 0.0002/次"},
    }
    assert buyer_nick_from_order(order) == NICK


def test_buyer_nick_ignores_card_titles_and_empty_orders():
    assert buyer_nick_from_order(None) == ""
    assert buyer_nick_from_order({}) == ""
    assert buyer_nick_from_order({"buyerInfoVO": {"buyerId": BUYER}}) == ""
    # 订单里如果只有标题式的「昵称」，等于没拿到
    assert buyer_nick_from_order({"buyerInfoVO": {"userNick": "等待你发货"}}) == ""


# ---------------------------------------------------------------- 落库回填


async def test_backfill_replaces_placeholder_rows_only(db):
    await db.log_message(account_id=1, chat_id=CHAT, sender_id=BUYER,
                         sender_name="买家", item_id=ITEM, order_id=ORDER,
                         content="[我已付款，等待你发货]", kind="incoming")
    await db.log_message(account_id=1, chat_id=CHAT, sender_id=BUYER,
                         sender_name="等待你发货", item_id=ITEM, order_id=None,
                         content="[你已发货]", kind="incoming")
    await db.log_message(account_id=1, chat_id=CHAT, sender_id=BUYER,
                         sender_name=NICK, item_id=ITEM, order_id=None,
                         content="好的", kind="incoming")
    await db.record_order(account_id=1, order_id=ORDER, item_id=ITEM, buyer_id=BUYER,
                          buyer_name="买家", rule_id=1, card_id=None, content="x",
                          status="delivered", detail="")

    changed = await db.backfill_buyer_name(1, BUYER, NICK)
    assert changed == 3  # 两条消息 + 一条订单；真名那条不动

    messages = await db.list_messages(limit=10)
    assert {m["sender_name"] for m in messages} == {NICK}
    orders = await db.list_orders()
    assert orders[0]["buyer_name"] == NICK


async def test_backfill_never_overwrites_with_a_placeholder(db):
    await db.log_message(account_id=1, chat_id=CHAT, sender_id=BUYER,
                         sender_name=NICK, item_id=ITEM, order_id=None,
                         content="在吗", kind="incoming")
    assert await db.backfill_buyer_name(1, BUYER, "买家") == 0
    assert (await db.list_messages(limit=1))[0]["sender_name"] == NICK


# ---------------------------------------------------------------- 付款链路


def paid_card_payload():
    """真实「已付款」卡片：reminderTitle 是标题，payload 里没有任何昵称。"""
    card = {"dxCard": {"item": {"main": {
        "targetUrl": "flearmarket://order_detail?id=" + ORDER + "&role=seller"
    }}}}
    return {
        "1": {
            "1": {"1": BUYER + "@goofish"},
            "2": CHAT + "@goofish",
            "5": 1792830992480,
            "6": {"1": 101, "3": {"2": "[我已付款，等待你发货]",
                                  "4": 26, "5": json.dumps(card)}},
            "10": {
                "reminderContent": "[我已付款，等待你发货]",
                "reminderTitle": "等待你发货",
                "senderUserId": BUYER,
                "reminderUrl": (
                    "fleamarket://message_chat?itemId=" + ITEM
                    + "&peerUserId=" + BUYER + "&sid=" + CHAT
                ),
            },
        },
    }


def frame_with(payload):
    data = base64.b64encode(msgpack.packb(payload, use_bin_type=True)).decode()
    return json.dumps({"headers": {"mid": "m1"},
                       "body": {"syncPushPackage": {"data": [{"data": data}]}}})


def order_item(nick=NICK):
    return {
        "commonData": {"orderId": ORDER, "itemId": ITEM, "orderStatus": "待发货"},
        "buyerInfoVO": {"buyerId": BUYER, "userNick": nick},
        "priceVO": {"auctionPrice": "1.00", "totalPrice": "1.00", "actualPayFee": "1.00"},
    }


class FakeClient:
    user_id = MY

    def __init__(self, order):
        self.sent = []
        self.acked = []
        self.confirmed = []
        self.websocket = self
        self.order = order
        self.order_lookups = 0

    async def ack(self, ws, frame):
        self.acked.append(frame)

    async def send_text_safe(self, chat_id, to_id, text):
        self.sent.append((chat_id, to_id, text))

    async def confirm_shipping(self, order_id):
        self.confirmed.append(order_id)
        return True, "SUCCESS"

    async def find_order(self, order_id):
        self.order_lookups += 1
        return self.order if str(order_id) == ORDER else None

    async def close(self):
        pass


class FakeProvisioner:
    def __init__(self):
        self.usernames = []
        self.labels = []

    async def provision(self, quota_usd, label, username="", model_limits=None):
        from app.upstream import Provisioned

        self.usernames.append(username)
        self.labels.append(label)
        return Provisioned("CARD-1", "sk-a…z", quota_usd, "1")


async def build_worker(db, order):
    account_id = await db.upsert_account("主号", "unb=" + MY + "; _m_h5_tk=t")
    account = await db.get_account(account_id)
    provisioner = FakeProvisioner()
    worker = AccountWorker(db, account, DeliveryEngine(db), provisioner)
    worker.client = FakeClient(order)
    return worker, provisioner


async def save_rule(db):
    await db.save_rule({
        "name": "按金额", "item_id": "*", "enabled": True, "mode": "upstream",
        "content": "你好 {buyer}，卡密：{card}", "auto_confirm": False, "priority": 0,
        "remark": "", "usd_multiplier": 1.0, "fixed_usd": 0.0,
    })


async def test_first_time_buyer_gets_real_name_on_paid_card(db):
    """新买家（聊天记录里一条都没有）付款：名字必须来自订单，不能是「买家」。"""
    await save_rule(db)
    worker, provisioner = await build_worker(db, order_item())

    await worker._on_frame(frame_with(paid_card_payload()))

    assert provisioner.usernames == [NICK], "令牌名不能是「买家」"
    assert provisioner.labels[0].startswith(NICK + "#")
    orders = await db.list_orders()
    assert orders[0]["status"] == "delivered"
    assert orders[0]["buyer_name"] == NICK
    text = worker.client.sent[0][2]
    assert NICK in text
    assert not is_placeholder_name(text.split("，")[0].removeprefix("你好 "))


async def test_earlier_placeholder_rows_are_backfilled_on_delivery(db):
    """付款前那几条流水记成了「买家」，拿到真名后要一起纠正。"""
    await save_rule(db)
    await db.log_message(account_id=1, chat_id=CHAT, sender_id=BUYER,
                         sender_name="买家", item_id=ITEM, order_id=ORDER,
                         content="[我已拍下，待付款]", kind="incoming")
    worker, _ = await build_worker(db, order_item())

    await worker._on_frame(frame_with(paid_card_payload()))

    messages = await db.list_messages(limit=20)
    assert messages  # 有流水
    assert all(m["sender_name"] == NICK for m in messages), \
        [m["sender_name"] for m in messages]


async def test_order_without_nick_still_falls_back_gracefully(db):
    """订单里也没有昵称（老接口 / 脏数据）：退化成「买家」，不能空着或报错。"""
    await save_rule(db)
    worker, provisioner = await build_worker(db, order_item(nick=""))

    await worker._on_frame(frame_with(paid_card_payload()))

    assert provisioner.usernames == ["买家"]
    assert (await db.list_orders())[0]["buyer_name"] == "买家"


# ---------------------------------------------------------------- 会话里的昵称


def test_session_nick_is_preferred_over_card_title():
    """会话 extension 里有对方资料 —— 那是真名，卡片标题只是标题。"""
    session_ext = {"extUserId": BUYER, "extUserName": "西北野泰迪", "itemId": ITEM}
    extension = {"reminderTitle": "等待你发货"}
    assert buyer_nick_from_session(session_ext) == "西北野泰迪"
    assert buyer_nick_from_session(extension) == ""


def test_session_titles_are_not_nicknames():
    """「交易消息」「工作台通知」是会话标题，不是人。"""
    for title in ("交易消息", "工作台通知", "我完成了评价", "去评价"):
        assert buyer_nick_from_session({"extUserName": title}) == "", title


def test_conversation_parse_uses_session_nick_for_card_messages():
    """会话列表里的付款卡片：reminderTitle 是标题，昵称来自会话 extension。"""
    conv = {
        "singleChatUserConversation": {
            "singleChatConversation": {
                "cid": CHAT + "@goofish",
                "extension": {"itemId": ITEM, "extUserId": BUYER, "extUserName": NICK},
            },
            "lastMessage": {
                "message": {
                    "extension": {
                        "reminderContent": "[我已付款，等待你发货]",
                        "reminderTitle": "等待你发货",
                        "senderUserId": BUYER,
                        "reminderUrl": (
                            "fleamarket://message_chat?itemId=" + ITEM
                            + "&peerUserId=" + BUYER + "&sid=" + CHAT
                        ),
                    },
                    "messageId": "4358042467840.PNM",
                    "createAt": 1792813119548,
                    "cid": CHAT + "@goofish",
                    "content": {
                        "contentType": 101,
                        "custom": {
                            "type": 26,
                            "data": base64.b64encode(json.dumps({
                                "contentType": 26,
                                "dxCard": {"item": {"main": {
                                    "targetUrl": "fleamarket://order_detail?id="
                                                  + ORDER + "&role=seller"
                                }}},
                            }).encode()).decode(),
                        },
                    },
                },
            },
        },
    }
    parsed = XianyuClient("unb=" + MY + "; _m_h5_tk=tok_1")._parse_conversation(conv)
    assert parsed is not None
    assert parsed["buyer_name"] == NICK
    assert parsed["buyer_name"] != "等待你发货"


# ---------------------------------------------------------------- 固定文案规则


async def test_fixed_text_rule_resolves_name_before_rendering(db):
    """卡密池/固定文案规则不查订单金额，也得在渲染文案前把名字找回来。"""
    await db.save_rule({
        "name": "固定文案", "item_id": "*", "enabled": True, "mode": "card",
        "content": "你好 {buyer}，这是你的卡密：{card}", "auto_confirm": False,
        "priority": 0, "remark": "", "usd_multiplier": 1.0, "fixed_usd": 0.0,
    })
    await db.add_cards(["AAAA-BBBB"], item_id=ITEM)
    worker, _ = await build_worker(db, order_item())

    await worker._on_frame(frame_with(paid_card_payload()))

    text = worker.client.sent[0][2]
    assert NICK in text, text
    assert "你好 买家" not in text
    assert (await db.list_orders())[0]["buyer_name"] == NICK
