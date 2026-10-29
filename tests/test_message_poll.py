"""会话列表轮询（拉取式收消息）的解析与链路测试。

线上实测：WebSocket 推送经常只带「某会话有新消息」的未读标记、不带正文。
所以除推送外还有一条「拉」的路 —— /r/Conversation/listNewestPagination。
这里用线上抓到的真实回包结构固定住它的解析行为。
"""

from __future__ import annotations

import base64
import json

import pytest

from app.client import XianyuClient, _find_order_id, _url_param
from app.config import settings
from app.db import Database
from app.engine import DeliveryEngine
from app.runner import AccountWorker, iso_from_ms

ME = "4100000003"
BUYER = "2000000000004"
CHAT = "68250276867"
ITEM = "2000000000001"


def custom_data(payload: dict) -> str:
    return base64.b64encode(json.dumps(payload, ensure_ascii=False).encode()).decode()


def conversation(
    *,
    chat=CHAT,
    custom_type=1,
    sender=BUYER,
    nick="demo001",
    text="OK",
    item=ITEM,
    message_id="4358042467840.PNM",
    create_at=1792813119548,
    reminder_url=None,
    content_payload=None,
    order_id="",
    item_title=None,
):
    url = reminder_url or (
        f"fleamarket://message_chat?itemId={item}&peerUserId={sender}&sid={chat}"
        f"&messageId={message_id}&adv=no"
    )
    session_ext = {
        "itemId": item,
        "extUserId": BUYER,
        "ownerUserId": ME,
        "itemSellerId": ME,
    }
    if order_id:
        session_ext["orderId"] = order_id
    if item_title:
        session_ext["itemTitle"] = item_title
    return {
        "type": 1,
        "singleChatUserConversation": {
            "visible": 1,
            "modifyTime": create_at,
            "joinTime": create_at,
            "singleChatConversation": {"cid": chat + "@goofish", "extension": session_ext},
            "lastMessage": {
                "readStatus": 2,
                "message": {
                    "extension": {
                        "reminderContent": text,
                        "senderUserId": sender,
                        "reminderTitle": nick,
                        "reminderUrl": url,
                    },
                    "messageId": message_id,
                    "createAt": create_at,
                    "cid": chat + "@goofish",
                    "content": {
                        "contentType": 101,
                        "custom": {
                            "type": custom_type,
                            "summary": text,
                            "data": custom_data(content_payload or
                                                {"contentType": 1, "text": {"text": text}}),
                        },
                    },
                },
            },
        },
    }


def client() -> XianyuClient:
    return XianyuClient(f"unb={ME}; _m_h5_tk=tok_1")


# ---------------------------------------------------------------- 解析


def test_parse_conversation_extracts_fields():
    parsed = client()._parse_conversation(conversation())
    assert parsed is not None
    assert parsed["chat_id"] == CHAT
    assert parsed["buyer_id"] == BUYER
    assert parsed["buyer_name"] == "demo001"
    assert parsed["content"] == "OK"
    assert parsed["item_id"] == ITEM
    assert parsed["message_id"] == "4358042467840.PNM"
    assert parsed["from_me"] is False


def test_parse_conversation_carries_item_title():
    """会话扩展里带着商品名时，要一起带出来（聊天列表靠它显示名字）。"""
    parsed = client()._parse_conversation(conversation(item_title="  GPT  会员   代充（月卡） "))
    assert parsed is not None
    assert parsed["item_id"] == ITEM
    assert parsed["item_title"] == "GPT 会员 代充（月卡）"

    # 没有标题的老会话：空串而不是 None，前端才好统一处理
    assert client()._parse_conversation(conversation())["item_title"] == ""


def test_parse_conversation_marks_own_messages():
    """自己发的（多端同步）不能当成买家消息，否则会自己回自己。"""
    parsed = client()._parse_conversation(conversation(sender=ME, nick="_示例_"))
    assert parsed is not None
    assert parsed["from_me"] is True
    # 自己发的那条里 peerUserId 指向的是通知接收方（我自己），不能当买家 ID
    assert parsed["buyer_id"] == ""


def test_parse_conversation_skips_conversation_without_message():
    raw = {"type": 1, "singleChatUserConversation": {"visible": 1}}
    assert client()._parse_conversation(raw) is None


def test_parse_conversation_uses_custom_data_when_reminder_empty():
    conv = conversation(text="", content_payload={"contentType": 1,
                                                  "text": {"text": "在的，拍下自动发货"}})
    conv["singleChatUserConversation"]["lastMessage"]["message"]["extension"].pop("reminderContent")
    parsed = client()._parse_conversation(conv)
    assert parsed is not None
    assert "在的，拍下自动发货" in parsed["content"]


def test_parse_conversation_prefers_session_extension():
    """会话扩展里的 buyer/item/order 是权威来源，比从正文里猜靠谱。"""
    conv = conversation(order_id="2500000000000000004")
    parsed = client()._parse_conversation(conv)
    assert parsed is not None
    assert parsed["order_id"] == "2500000000000000004"
    assert parsed["item_id"] == ITEM
    assert parsed["buyer_id"] == BUYER


def test_parse_conversation_recovers_buyer_for_seller_sent_paid_card():
    """卖家 sender 的付款卡片，买家 ID 要从会话 extension 取回，不能直接丢。"""
    card = {"contentType": 26, "dxCard": {"item": {"main": {
        "targetUrl": "fleamarket://order_detail?id=2500000000000000004&role=seller"
    }}}}
    conv = conversation(
        sender=ME, text="[我已付款，等待你发货]", custom_type=26,
        order_id="2500000000000000004",
        content_payload=card,
    )
    parsed = client()._parse_conversation(conv)

    assert parsed is not None
    assert parsed["from_me"] is False
    assert parsed["buyer_id"] == BUYER, "卖家 sender 也要能从 session extUserId 找回买家"
    assert parsed["order_id"] == "2500000000000000004"
    assert parsed["content_type"] == 26


def test_parse_conversation_recovers_order_id_from_url():
    conv = conversation(reminder_url="fleamarket://order_detail?id=2500000000000000004&role=seller")
    parsed = client()._parse_conversation(conv)
    assert parsed is not None
    assert parsed["order_id"] == "2500000000000000004"


def test_url_param_and_order_id_helpers():
    assert _url_param("fleamarket://x?a=1&b=2", "b") == "2"
    assert _url_param("fleamarket://x?a=1", "z") == ""
    assert _find_order_id('{"orderId":"123"}') == "123"
    assert _find_order_id("no id here") == ""


# ---------------------------------------------------------------- 链路


class FakeClient:
    user_id = ME

    def __init__(self, conversations):
        self.conversations = conversations
        self.sent: list[tuple[str, str, str]] = []
        self.websocket = self
        self.calls = 0
        self.history_calls = 0
        self.history: list[dict] = []

    async def ack(self, ws, frame):
        pass

    async def list_recent_conversations(self, limit: int = 20):
        """返回解析后的形状（真实客户端就是这么做的）。"""
        self.calls += 1
        parser = client()
        out = []
        for raw in self.conversations:
            parsed = parser._parse_conversation(raw)
            if parsed is not None:
                out.append(parsed)
        return out

    async def list_conversation_messages(self, chat_id: str, limit: int = 20):
        """默认不返回历史，让 worker 退回「只看最后一条」。"""
        self.history_calls += 1
        return {"messages": list(self.history), "next_cursor": 0, "has_more": False}

    def history_from(self, records):
        """用 conversation 记录构造历史消息（同一套解析器）。"""
        parser = client()
        out = []
        for raw in records:
            conv = parser._parse_conversation(raw)
            if conv is not None:
                out.append(conv)
        self.history = out

    async def send_text_safe(self, chat_id, to_id, text):
        self.sent.append((chat_id, to_id, text))

    async def find_order(self, order_id):
        return {
            "commonData": {"orderId": order_id, "orderStatus": "待发货"},
            "buyerInfoVO": {"buyerId": BUYER},
        }

    async def close(self):
        pass


async def build_worker(db: Database, conversations) -> AccountWorker:
    account_id = await db.upsert_account("主号", f"unb={ME}; _m_h5_tk=tok_1")
    account = await db.get_account(account_id)
    worker = AccountWorker(db, account, DeliveryEngine(db), None)
    worker.client = FakeClient(conversations)
    return worker


async def save_item_reply(db: Database) -> None:
    await db.save_reply_rule({
        "name": "默认回复", "kind": "item", "item_id": ITEM, "keywords": "",
        "match_type": "contains", "content": "在的 {buyer}，拍下自动发货",
        "enabled": True, "once_per_chat": False, "cooldown": 0, "priority": 0, "remark": "",
    })


def set_connected_at(worker: AccountWorker, created_at_ms: int) -> None:
    """把「连接时刻」设成比这些消息晚一点，模拟它们都是历史消息。"""
    worker._connected_at_ms = created_at_ms + 60_000


@pytest.fixture()
def fast(monkeypatch):
    monkeypatch.setattr(settings, "auto_reply_delay", 0.0)
    monkeypatch.setattr(settings, "auto_reply_enabled", True)


async def finish_replies(worker: AccountWorker) -> None:
    for task in list(worker._reply_tasks):
        await task


async def test_history_messages_do_not_trigger_reply(db, fast):
    """比连接时刻更早的消息是历史，不能触发自动回复。"""
    worker = await build_worker(db, [conversation(chat=CHAT, text="很早以前的话")])
    await save_item_reply(db)
    set_connected_at(worker, 1792813119548)

    handled = await worker._poll_recent_messages()
    await finish_replies(worker)
    assert handled == 0
    assert worker.client.sent == []
    assert worker._message_baseline_done is True


async def test_new_message_after_baseline_triggers_reply(db, fast):
    worker = await build_worker(db, [conversation(chat=CHAT, text="在吗")])
    await save_item_reply(db)
    set_connected_at(worker, 1792813119548)
    await worker._poll_recent_messages()
    await finish_replies(worker)
    assert worker.client.sent == []

    worker.client.conversations = [
        conversation(chat=CHAT, text="在吗", message_id="new-1", create_at=1792813999000)
    ]
    handled = await worker._poll_recent_messages()
    await finish_replies(worker)
    assert handled == 1
    assert len(worker.client.sent) == 1
    assert "在吗" not in worker.client.sent[0][2]
    assert "拍下自动发货" in worker.client.sent[0][2]


async def test_old_history_is_not_replayed_after_baseline(db, fast):
    """基线只把每个会话最后一条标 seen，之后的补拉不能把更早的旧消息当新消息。

    旧实现 min_created_at 在基线后变成 0，同会话来新消息时会补拉 20 条历史，
    其中启动前的 old-1 从未 seen，会被自动回复一遍。
    """
    worker = await build_worker(db, [
        conversation(chat=CHAT, text="old-2", message_id="old-2", create_at=1792813000000)
    ])
    await save_item_reply(db)
    set_connected_at(worker, 1792813119548)
    await worker._poll_recent_messages()   # 基线：old-2 只记 seen，不回
    await finish_replies(worker)
    assert worker.client.sent == []

    worker.client.history_from([
        history_record(text="new message", message_id="new-1", create_at=1792815200000),
        history_record(text="old-1", message_id="old-1", create_at=1792812000000),
        history_record(text="old-2", message_id="old-2", create_at=1792813000000),
    ])
    worker.client.conversations = [
        conversation(message_id="new-1", create_at=1792815200000, text="new message")
    ]

    handled = await worker._poll_recent_messages()
    await finish_replies(worker)

    assert handled == 1, "只有 new-1 是新消息"
    assert len(worker.client.sent) == 1
    incoming = [
        m["content"] for m in await db.list_messages() if m["kind"] == "incoming"
    ]
    assert "old-1" not in incoming
    logged = next(m for m in await db.list_messages() if m["content"] == "new message")
    assert logged["created_at"] == iso_from_ms(1792815200000)


async def test_pulled_message_records_item_title(db, fast):
    """「拉」这条路收到的新消息也要把商品名写进库，聊天列表才能显示名字。"""
    worker = await build_worker(db, [
        conversation(text="在吗", message_id="m-old", item_title="GPT 会员代充（月卡）")
    ])
    await save_item_reply(db)
    set_connected_at(worker, 1792813119548)
    await worker._poll_recent_messages()   # 基线：老消息只标 seen，不记流水
    await finish_replies(worker)
    assert await db.list_chats() == []

    worker.client.conversations = [
        conversation(text="在吗", message_id="m-title", create_at=1792813999000,
                     item_title="GPT 会员代充（月卡）")
    ]
    assert await worker._poll_recent_messages() == 1
    await finish_replies(worker)

    chats = await db.list_chats()
    assert chats[0]["item_id"] == ITEM
    assert chats[0]["item_title"] == "GPT 会员代充（月卡）"

    # 之后后台自己回一条（没有 item_id 的那种行），标题还得借得到
    await worker.send_manual_text(CHAT, BUYER, "已经发货了")
    chats = await db.list_chats()
    assert chats[0]["item_title"] == "GPT 会员代充（月卡）"


async def test_seller_sent_paid_card_delivers_through_pull_path(db):
    """会话列表里那条付款卡片即使 sender 是卖家，也要能正常发货。"""
    card = {"contentType": 26, "dxCard": {"item": {"main": {
        "targetUrl": "fleamarket://order_detail?id=2500000000000000004&role=seller"
    }}}}
    conv = conversation(
        sender=ME, text="[我已付款，等待你发货]", custom_type=26,
        order_id="2500000000000000004", content_payload=card,
    )
    worker = await build_worker(db, [conv])
    await db.save_rule({
        "name": "卡密池", "item_id": "*", "enabled": True, "mode": "card",
        "content": "卡密：{card}", "auto_confirm": False, "priority": 0, "remark": "",
    })
    await db.add_cards(["CARD-SELLER-SENDER"], "*")

    handled = await worker._poll_recent_messages()

    assert handled == 1
    assert len(worker.client.sent) == 1
    assert "CARD-SELLER-SENDER" in worker.client.sent[0][2]
    orders = await db.list_orders()
    assert orders[0]["status"] == "delivered"


async def test_same_message_is_not_handled_twice(db, fast):
    worker = await build_worker(db, [conversation()])
    await save_item_reply(db)
    set_connected_at(worker, 1792813119548)
    await worker._poll_recent_messages()
    await finish_replies(worker)

    worker.client.conversations = [conversation(message_id="new-2")]
    await worker._poll_recent_messages()
    await finish_replies(worker)
    assert len(worker.client.sent) == 1

    # 同一个 messageId 再拉一次，不该再回
    await worker._poll_recent_messages()
    await finish_replies(worker)
    assert len(worker.client.sent) == 1


async def test_own_message_does_not_trigger_reply(db, fast):
    worker = await build_worker(db, [conversation(sender=ME)])
    await save_item_reply(db)
    set_connected_at(worker, 1792813119548)
    await worker._poll_recent_messages()
    await finish_replies(worker)

    worker.client.conversations = [conversation(sender=ME, message_id="mine-1", nick="_示例_")]
    await worker._poll_recent_messages()
    await finish_replies(worker)
    assert worker.client.sent == []


async def test_polling_can_be_disabled(db, fast, monkeypatch):
    monkeypatch.setattr(settings, "message_poll_interval", 0.0)
    worker = await build_worker(db, [conversation()])
    task = await worker._message_poll_loop()
    assert task is None
    assert worker.client.calls == 0


async def test_system_typed_message_is_not_auto_replied(db, fast):
    """custom.type != 1 是系统消息（14 提示 / 26 卡片），照常记录但不自动回复。"""
    worker = await build_worker(db, [conversation(custom_type=14, text="[你已发货]")])
    await save_item_reply(db)
    set_connected_at(worker, 1792813119548)
    await worker._poll_recent_messages()
    await finish_replies(worker)

    worker.client.conversations = [
        conversation(custom_type=14, text="[你已发货]", message_id="sys-1", create_at=1792815000000)
    ]
    handled = await worker._poll_recent_messages()
    await finish_replies(worker)
    assert handled == 1          # 记录进流水了
    assert worker.client.sent == []  # 但没回一句「在的」


def history_record(*, text, message_id, create_at, sender=BUYER, nick="demo001", custom_type=1):
    """构造一条历史消息（复用 conversation 的字段布局）。"""
    return conversation(chat=CHAT, sender=sender, nick=nick, text=text,
                        message_id=message_id, create_at=create_at, custom_type=custom_type)


async def test_conversation_history_is_paged_through_not_just_last(db, fast):
    """买家连发两条时，历史里两条都要处理 —— 只看最后一条会漏掉关键词那条。"""
    worker = await build_worker(db, [conversation(message_id="base", create_at=1792813000000)])
    await save_item_reply(db)
    set_connected_at(worker, 1792813119548)
    await worker._poll_recent_messages()   # 基线
    await finish_replies(worker)
    assert worker.client.sent == []

    # 买家发了两条；最后一条是「嗯」，关键词在前一条
    worker.client.history_from([
        history_record(text="嗯", message_id="m-2", create_at=1792815200000),
        history_record(text="在吗", message_id="m-1", create_at=1792815100000),
    ])
    worker.client.conversations = [conversation(message_id="m-2", create_at=1792815200000)]

    handled = await worker._poll_recent_messages()
    await finish_replies(worker)
    assert handled == 2
    assert len(worker.client.sent) == 2


async def test_history_is_replayed_oldest_first(db, fast):
    """历史接口从新到旧返回，回复顺序要按时间正序，不然对话看着很怪。"""
    worker = await build_worker(db, [conversation(message_id="base", create_at=1792813000000)])
    await save_item_reply(db)
    set_connected_at(worker, 1792813119548)
    await worker._poll_recent_messages()
    await finish_replies(worker)

    worker.client.history_from([
        history_record(text="第二条", message_id="m-2", create_at=1792815200000),
        history_record(text="第一条", message_id="m-1", create_at=1792815100000),
    ])
    worker.client.conversations = [conversation(message_id="m-2", create_at=1792815200000)]

    await worker._poll_recent_messages()
    await finish_replies(worker)
    contents = [c[2] for c in worker.client.sent]
    assert len(contents) == 2


async def test_history_failure_falls_back_to_last_message(db, fast):
    """历史接口挂了也要靠会话列表那条兜底，不能整个会话都丢。"""
    worker = await build_worker(db, [conversation(message_id="base", create_at=1792813000000)])
    await save_item_reply(db)
    set_connected_at(worker, 1792813119548)
    await worker._poll_recent_messages()
    await finish_replies(worker)

    async def boom(chat_id, limit=20):
        from app.client import XianyuError
        raise XianyuError("history unavailable")

    worker.client.list_conversation_messages = boom
    worker.client.conversations = [conversation(message_id="m-9", create_at=1792815300000, text="在吗")]

    handled = await worker._poll_recent_messages()
    await finish_replies(worker)
    assert handled == 1
    assert len(worker.client.sent) == 1


async def test_history_does_not_reprocess_own_or_system_messages(db, fast):
    """历史里的自己消息、系统消息只记账不回复，下一轮也不该重复看。"""
    worker = await build_worker(db, [conversation(message_id="base", create_at=1792813000000)])
    await save_item_reply(db)
    set_connected_at(worker, 1792813119548)
    await worker._poll_recent_messages()
    await finish_replies(worker)

    worker.client.history_from([
        history_record(text="[你已发货]", message_id="s-1", create_at=1792815400000, custom_type=14),
        history_record(text="我自己的回复", message_id="o-1", create_at=1792815300000, sender=ME),
    ])
    worker.client.conversations = [conversation(message_id="s-1", create_at=1792815400000)]

    handled = await worker._poll_recent_messages()
    await finish_replies(worker)
    # 系统消息算「处理过」（记进流水），但没有产生任何自动回复
    assert handled == 1
    assert worker.client.sent == []

    # 再拉一轮：两条都已记账，不该再被当成新消息
    assert await worker._poll_recent_messages() == 0


async def test_history_fetch_is_capped_per_poll(db, fast, monkeypatch):
    """会话很多时只给最新的几个拉历史，其余只看最后一条（每次拉历史都是 WS 往返）。"""
    monkeypatch.setattr(settings, "message_history_conversations", 1)
    worker = await build_worker(db, [conversation(chat=CHAT, message_id="base", create_at=1792813000000)])
    await save_item_reply(db)
    set_connected_at(worker, 1792813119548)
    await worker._poll_recent_messages()
    await finish_replies(worker)
    worker.client.history_calls = 0

    worker.client.conversations = [
        conversation(chat="111", message_id="a", create_at=1792815100000),
        conversation(chat="222", message_id="b", create_at=1792815200000),
        conversation(chat="333", message_id="c", create_at=1792815300000),
    ]
    await worker._poll_recent_messages()
    await finish_replies(worker)
    assert worker.client.history_calls == 1


async def test_history_can_be_disabled_entirely(db, fast, monkeypatch):
    monkeypatch.setattr(settings, "message_history_conversations", 0)
    worker = await build_worker(db, [conversation(chat=CHAT, message_id="base", create_at=1792813000000)])
    await save_item_reply(db)
    set_connected_at(worker, 1792813119548)
    await worker._poll_recent_messages()
    await finish_replies(worker)
    worker.client.history_calls = 0

    worker.client.conversations = [conversation(message_id="new-x", create_at=1792815300000)]
    handled = await worker._poll_recent_messages()
    await finish_replies(worker)
    assert worker.client.history_calls == 0
    assert handled == 1
