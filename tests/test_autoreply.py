"""自动回复（商品 / 关键词）的单元与链路测试。"""

from __future__ import annotations

import asyncio

import pytest

from app.autoreply import (
    AutoReplyEngine,
    chat_key_of,
    match_keyword,
    render_reply,
    split_keywords,
)
from app.codec import IncomingMessage
from app.config import settings
from app.db import Database
from app.engine import DeliveryEngine
from app.runner import AccountWorker


def make_message(**overrides) -> IncomingMessage:
    data = {
        "chat_id": "47983389096",
        "sender_id": "4100000001",
        "sender_name": "买家小王",
        "content": "在吗，什么时候发货",
        "item_id": "900052644277",
        "order_id": None,
    }
    data.update(overrides)
    return IncomingMessage(**data)


async def save_reply_rule(db: Database, **fields) -> int:
    payload = {
        "name": "打招呼",
        "kind": "item",
        "item_id": "*",
        "keywords": "",
        "match_type": "contains",
        "content": "在的 {buyer}，拍下自动发货。",
        "enabled": True,
        "once_per_chat": True,
        "cooldown": 0,
        "priority": 0,
        "remark": "",
    }
    payload.update(fields)
    return await db.save_reply_rule(payload)


# ---------------------------------------------------------------- 纯函数


def test_split_keywords_handles_mixed_separators():
    assert split_keywords("发货, 多久;能便宜吗｜怎么用\n发货") == ["发货", "多久", "能便宜吗", "怎么用"]
    assert split_keywords("") == []
    assert split_keywords(None) == []


def test_match_keyword_modes():
    assert match_keyword("什么时候发货？", ["发货"], "contains") == "发货"
    assert match_keyword("发货", ["发货"], "exact") == "发货"
    assert match_keyword("什么时候发货", ["发货"], "exact") is None
    assert match_keyword("ORDER-1234", ["order-\\d+"], "regex") == "order-\\d+"
    assert match_keyword("在吗", ["发货"], "contains") is None
    # 正则写错了不能抛异常，只是不命中
    assert match_keyword("x", ["("], "regex") is None


def test_render_reply_substitutes_variables():
    out = render_reply("你好{buyer}，关于「{keyword}」→ {item_id}",
                       buyer_name="小王", item_id="900", keyword="发货")
    assert "小王" in out and "发货" in out and "900" in out


def test_chat_key_prefers_chat_id():
    assert chat_key_of(make_message()) == "47983389096"
    assert chat_key_of(make_message(chat_id="", item_id="900")) == "4100000001@900"


# ---------------------------------------------------------------- 引擎


async def test_item_reply_matches_any_message(db):
    await save_reply_rule(db, kind="item")
    engine = AutoReplyEngine(db)
    decision = await engine.decide(1, make_message(content="这个还有货吗"))
    assert decision.should_reply
    assert "买家小王" in decision.content
    assert decision.chat_key == "47983389096"


async def test_keyword_reply_requires_keyword(db):
    await save_reply_rule(db, kind="keyword", keywords="发货, 什么时候", content="马上就发")
    engine = AutoReplyEngine(db)
    hit = await engine.decide(1, make_message(content="今天能发货吗"))
    assert hit.should_reply and hit.keyword == "发货"

    miss = await engine.decide(1, make_message(content="这个多少钱"))
    assert not miss.should_reply


async def test_keyword_rule_wins_over_item_rule(db):
    await save_reply_rule(db, name="商品默认", kind="item", content="默认回复")
    await save_reply_rule(db, name="关键词", kind="keyword", keywords="发货", content="关键词回复")
    engine = AutoReplyEngine(db)
    decision = await engine.decide(1, make_message(content="什么时候发货"))
    assert decision.rule["name"] == "关键词"


async def test_item_specific_beats_wildcard(db):
    await save_reply_rule(db, name="通配", kind="item", item_id="*", content="通配回复")
    await save_reply_rule(db, name="专属", kind="item", item_id="900052644277", content="专属回复")
    engine = AutoReplyEngine(db)
    decision = await engine.decide(1, make_message(content="在吗"))
    assert decision.rule["name"] == "专属"

    other = await engine.decide(1, make_message(content="在吗", item_id="111"))
    assert other.rule["name"] == "通配"


async def test_rule_for_other_item_is_not_used(db):
    await save_reply_rule(db, kind="item", item_id="123456")
    engine = AutoReplyEngine(db)
    decision = await engine.decide(1, make_message(item_id="900052644277"))
    assert not decision.should_reply


async def test_system_and_paid_messages_are_not_auto_replied(db):
    await save_reply_rule(db, kind="item")
    engine = AutoReplyEngine(db)
    paid = await engine.decide(1, make_message(content="[我已付款，等待你发货]"))
    assert not paid.should_reply
    ignored = await engine.decide(1, make_message(content="[我已拍下，待付款]"))
    assert not ignored.should_reply


async def test_disabled_rule_is_skipped(db):
    await save_reply_rule(db, kind="item", enabled=False)
    engine = AutoReplyEngine(db)
    decision = await engine.decide(1, make_message())
    assert not decision.should_reply


async def test_once_per_chat_blocks_second_hit(db):
    rule_id = await save_reply_rule(db, kind="item", once_per_chat=True)
    engine = AutoReplyEngine(db)
    first = await engine.decide(1, make_message())
    assert first.should_reply
    await db.record_reply_hit(1, "4100000001", first.chat_key, rule_id)

    again = await engine.decide(1, make_message())
    assert not again.should_reply

    # 换个会话（另一个商品）应该还能回
    other_chat = await engine.decide(1, make_message(item_id="555", chat_id="47983389999"))
    assert other_chat.should_reply

    # 清空记录后又能回
    await db.clear_reply_hits(rule_id)
    assert (await engine.decide(1, make_message())).should_reply


async def test_repeatable_rule_allows_multiple_replies(db):
    rule_id = await save_reply_rule(db, kind="item", once_per_chat=False, cooldown=0)
    engine = AutoReplyEngine(db)
    first = await engine.decide(1, make_message())
    assert first.should_reply
    await db.record_reply_hit(1, "4100000001", first.chat_key, rule_id)
    assert (await engine.decide(1, make_message())).should_reply


async def test_cooldown_blocks_recent_hit(db):
    rule_id = await save_reply_rule(db, kind="item", once_per_chat=False, cooldown=600)
    engine = AutoReplyEngine(db)
    first = await engine.decide(1, make_message())
    assert first.should_reply
    await db.record_reply_hit(1, "4100000001", first.chat_key, rule_id)
    assert not (await engine.decide(1, make_message())).should_reply

    # 冷却为 0 的规则可以立刻再回
    await save_reply_rule(db, name="不限", kind="item", once_per_chat=False, cooldown=0)
    await db.clear_reply_hits()
    assert (await engine.decide(1, make_message())).should_reply


async def test_empty_content_rule_is_not_sent(db):
    await save_reply_rule(db, kind="item", content="   ")
    engine = AutoReplyEngine(db)
    decision = await engine.decide(1, make_message())
    assert not decision.should_reply
    assert "为空" in decision.reason


# ---------------------------------------------------------------- 运行链路


class FakeClient:
    user_id = "2000000000002"

    def __init__(self) -> None:
        self.sent: list[tuple[str, str, str]] = []
        self.websocket = self
        self.fail = False

    async def ack(self, ws, frame):
        pass

    async def send_text_safe(self, chat_id, to_id, text):
        if self.fail:
            from app.client import XianyuError

            raise XianyuError("发送失败")
        self.sent.append((chat_id, to_id, text))

    async def close(self):
        pass


async def build_worker(db: Database) -> AccountWorker:
    account_id = await db.upsert_account("主号", "unb=2000000000002; _m_h5_tk=token_1")
    account = await db.get_account(account_id)
    worker = AccountWorker(db, account, DeliveryEngine(db), None, AutoReplyEngine(db))
    worker.client = FakeClient()
    return worker


@pytest.fixture()
def fast_reply(monkeypatch):
    """测试里别真等「像人工」的那一秒。"""
    monkeypatch.setattr(settings, "auto_reply_delay", 0.0)
    monkeypatch.setattr(settings, "auto_reply_enabled", True)
    return settings


async def await_reply(worker: AccountWorker, msg) -> None:
    """_maybe_auto_reply 把发送排进后台任务，测试里直接等它跑完。"""
    task = await worker._maybe_auto_reply(msg)
    if task is not None:
        await task


async def test_worker_auto_reply_does_not_block_frame_loop(db, fast_reply):
    """自动回复是后台任务：收帧循环不能因为那个「像人工」的延迟而卡住。"""
    worker = await build_worker(db)
    await save_reply_rule(db, kind="item")
    task = await worker._maybe_auto_reply(make_message())
    assert isinstance(task, asyncio.Task)
    assert not task.done()
    await task
    assert len(worker.client.sent) == 1


async def test_worker_auto_reply_deduplicates_concurrent_triggers(db, fast_reply, monkeypatch):
    """同一会话同一规则并发触发时只发一条。"""
    monkeypatch.setattr(settings, "auto_reply_delay", 0.05)
    worker = await build_worker(db)
    await save_reply_rule(db, kind="item", once_per_chat=False, cooldown=0)
    msg = make_message()
    first = await worker._maybe_auto_reply(msg)
    second = await worker._maybe_auto_reply(msg)
    assert first is not None and second is None
    await first
    assert len(worker.client.sent) == 1


async def test_worker_sends_auto_reply_and_records_it(db, fast_reply):
    worker = await build_worker(db)
    await save_reply_rule(db, kind="keyword", keywords="发货", content="今天发货 {buyer}")
    msg = make_message(content="什么时候发货")
    await await_reply(worker, msg)

    assert len(worker.client.sent) == 1
    chat_id, to_id, text = worker.client.sent[0]
    assert chat_id == "47983389096" and to_id == "4100000001"
    assert "今天发货 买家小王" in text

    messages = await db.list_messages()
    assert messages[0]["kind"] == "outgoing"
    # 记了 hit：同一会话不会再回第二次
    await await_reply(worker, msg)
    assert len(worker.client.sent) == 1


async def test_worker_skips_reply_when_disabled(db, fast_reply, monkeypatch):
    monkeypatch.setattr(settings, "auto_reply_enabled", False)
    worker = await build_worker(db)
    await save_reply_rule(db, kind="item")
    await await_reply(worker, make_message())
    assert worker.client.sent == []


async def test_worker_does_not_record_hit_when_send_fails(db, fast_reply):
    worker = await build_worker(db)
    await save_reply_rule(db, kind="item", id=None, once_per_chat=True)
    worker.client.fail = True
    await await_reply(worker, make_message())
    assert worker.client.sent == []
    assert await db.fetch_one("SELECT id FROM reply_hits") is None


def frame_with(payload) -> str:
    import base64
    import json

    import msgpack

    data = base64.b64encode(msgpack.packb(payload, use_bin_type=True)).decode()
    return json.dumps({"headers": {"mid": "m1", "sid": "s1"},
                       "body": {"syncPushPackage": {"data": [{"data": data}]}}})


def chat_payload(content: str = "在吗？", item_id: str = "900052644277") -> dict:
    return {
        "1": {
            "2": "47983389096@goofish",
            "5": 1742458817389,
            "10": {
                "reminderContent": content,
                "senderUserId": "4100000001",
                "senderNick": "买家小王",
                "reminderUrl": f"fleamarket://message_chat?itemId={item_id}&peerUserId=4100000001",
            },
        },
    }


async def test_frame_triggers_auto_reply(db, fast_reply):
    """走完整收帧链路：买家问「在吗」→ 自动回一条。"""
    worker = await build_worker(db)
    await save_reply_rule(db, kind="item", item_id="900052644277", content="在的 {buyer}")
    await worker._on_frame(frame_with(chat_payload("在吗？")))
    for task in list(worker._reply_tasks):
        await task
    assert len(worker.client.sent) == 1
    assert "在的 买家小王" in worker.client.sent[0][2]


async def test_frame_paid_card_does_not_auto_reply(db, fast_reply):
    """付款卡片不自动回复（那单该走发货，不该掺一句「在的」）。"""
    worker = await build_worker(db)
    await save_reply_rule(db, kind="item")
    await worker._on_frame(frame_with(chat_payload("[我已付款，等待你发货]")))
    for task in list(worker._reply_tasks):
        await task
    assert worker.client.sent == []


async def test_worker_dry_run_does_not_send(db, fast_reply, monkeypatch):
    monkeypatch.setattr(settings, "dry_run", True)
    worker = await build_worker(db)
    await save_reply_rule(db, kind="item")
    await worker._maybe_auto_reply(make_message())
    assert worker.client.sent == []
