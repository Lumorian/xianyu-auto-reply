"""买家昵称解析：卡片标题不能当昵称用。

线上真实事故：新买家付款时，交易卡片的 reminderTitle 是「等待你发货」，
被当成买家名一路传到 new-api，令牌名变成「等待你发货#2500000000000000007」，
后台根本认不出这是谁。同一个 reminderTitle 字段在真人消息里却是真昵称，
所以必须按内容过滤 + 回查聊天记录。
"""

from __future__ import annotations

import base64
import json

import msgpack

from app.codec import is_placeholder_name, parse_message, pick_display_name
from app.engine import DeliveryEngine
from app.runner import AccountWorker
from app.upstream import build_token_name

MY = "4100000003"
BUYER = "4100000002"
ITEM = "2000000000001"
ORDER = "2500000000000000007"
CHAT = "68169574308"
REAL_NICK = "测试者-winkee"


# ---------------------------------------------------------------- 纯函数


def test_card_titles_are_not_names():
    for title in ("等待你发货", "买家已拍下，待付款", "我发起了退款申请",
                  "等待买家收货", "交易关闭", "你已发货", "买家", "", "  "):
        assert is_placeholder_name(title), title


def test_real_nicknames_pass_through():
    for nick in (REAL_NICK, "_示例_", "测试小鱼", "示例酸酸", "demo001", "示例买家昵称八"):
        assert not is_placeholder_name(nick), nick
        assert pick_display_name(nick) == nick


def test_overlong_text_is_not_a_name():
    """昵称不会长成一整段提示文案。"""
    assert is_placeholder_name("这是一段特别长的系统提示文案" * 3)


def test_pick_display_name_falls_back():
    assert pick_display_name("等待你发货", None, "") == "买家"
    assert pick_display_name(None, "瓜瓜瓜瓜") == "瓜瓜瓜瓜"


def test_token_name_uses_real_nickname_not_card_title():
    resolved = pick_display_name("等待你发货", REAL_NICK)
    assert build_token_name(resolved, ORDER) == REAL_NICK + "#" + ORDER


# ---------------------------------------------------------------- 解析链路


def paid_card_payload(nick="等待你发货"):
    """真实「已付款」卡片：reminderTitle 是标题，不是昵称。"""
    card = {
        "dxCard": {"item": {"main": {
            "targetUrl": "fleamarket://order_detail?id=" + ORDER + "&role=seller"
        }}}
    }
    return {
        "1": {
            "1": {"1": BUYER + "@goofish"},
            "2": CHAT + "@goofish",
            "5": 1792817748592,
            "6": {"1": 101, "3": {"2": "[我已付款，等待你发货]",
                                  "4": 26, "5": json.dumps(card)}},
            "10": {
                "reminderContent": "[我已付款，等待你发货]",
                "reminderTitle": nick,
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
    return json.dumps({"headers": {"mid": "m1"}, "body": {"syncPushPackage": {"data": [{"data": data}]}}})


def test_parsed_card_does_not_carry_card_title_as_name():
    msg = parse_message(paid_card_payload(), my_id=MY)
    assert msg is not None
    assert msg.sender_name == "买家"
    assert msg.is_system_card is True
    assert msg.order_id == ORDER


def test_parsed_text_message_keeps_real_nickname():
    payload = paid_card_payload(nick=REAL_NICK)
    payload["1"]["6"]["3"] = {"2": "怎么充值", "4": 1}
    payload["1"]["10"]["reminderContent"] = "怎么充值"
    msg = parse_message(payload, my_id=MY)
    assert msg is not None
    assert msg.sender_name == REAL_NICK
    assert msg.is_system_card is False


# ---------------------------------------------------------------- 落库回查


class FakeClient:
    user_id = MY

    def __init__(self):
        self.sent = []
        self.acked = []
        self.websocket = self
        self.confirmed = []

    async def ack(self, ws, frame):
        self.acked.append(frame)

    async def send_text_safe(self, chat_id, to_id, text):
        self.sent.append((chat_id, to_id, text))

    async def confirm_shipping(self, order_id):
        self.confirmed.append(order_id)
        return True, "SUCCESS"

    async def find_order(self, order_id):
        # 规则用固定额度，不需要真去查金额；返回 None 走 fixed_usd 分支
        return None

    async def close(self):
        pass


class FakeProvisioner:
    def __init__(self):
        self.usernames = []

    async def provision(self, quota_usd, label, username=""):
        from app.upstream import Provisioned

        self.usernames.append(username)
        return Provisioned("CARD-1", "sk-a…z", quota_usd, "1")


async def build_worker(db):
    account_id = await db.upsert_account("主号", "unb=" + MY + "; _m_h5_tk=t")
    account = await db.get_account(account_id)
    provisioner = FakeProvisioner()
    worker = AccountWorker(db, account, DeliveryEngine(db), provisioner)
    worker.client = FakeClient()
    return worker, provisioner


async def test_known_buyer_name_lookup_skips_card_titles(db):
    await db.log_message(account_id=1, chat_id=CHAT, sender_id=BUYER,
                         sender_name="等待你发货", item_id=ITEM, order_id=None,
                         content="[你已发货]", kind="incoming")
    await db.log_message(account_id=1, chat_id=CHAT, sender_id=BUYER,
                         sender_name=REAL_NICK, item_id=ITEM, order_id=None,
                         content="怎么充值", kind="incoming")
    assert await db.known_buyer_name(1, BUYER) == REAL_NICK
    assert await db.known_buyer_name(1, "不存在的人") is None


async def test_provision_uses_real_nickname_from_history(db):
    """卡片没带昵称时，用聊天记录里的真昵称去建令牌。"""
    await db.log_message(account_id=1, chat_id=CHAT, sender_id=BUYER,
                         sender_name=REAL_NICK, item_id=ITEM, order_id=None,
                         content="在吗", kind="incoming")
    await db.save_rule({
        "name": "按金额", "item_id": "*", "enabled": True, "mode": "upstream",
        "content": "卡密：{card}", "auto_confirm": False, "priority": 0, "remark": "",
        "usd_multiplier": 1.0, "fixed_usd": 1.0,
    })
    worker, provisioner = await build_worker(db)
    await worker._on_frame(frame_with(paid_card_payload()))

    assert provisioner.usernames == [REAL_NICK]
    orders = await db.list_orders()
    assert orders[0]["buyer_name"] == REAL_NICK


async def test_unknown_buyer_still_gets_a_sane_token_name(db):
    """查不到历史时退化成「买家」，绝不能是「等待你发货」。"""
    await db.save_rule({
        "name": "按金额", "item_id": "*", "enabled": True, "mode": "upstream",
        "content": "卡密：{card}", "auto_confirm": False, "priority": 0, "remark": "",
        "usd_multiplier": 1.0, "fixed_usd": 1.0,
    })
    worker, provisioner = await build_worker(db)
    await worker._on_frame(frame_with(paid_card_payload()))

    assert provisioner.usernames == ["买家"]
    assert "等待你发货" not in build_token_name(provisioner.usernames[0], ORDER)
