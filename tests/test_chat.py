"""后台聊天：会话查询、已读、手动发送与按需补拉历史。"""

from __future__ import annotations

import asyncio
import sqlite3
from typing import Any

import httpx
import pytest

from app.client import XianyuError
from app.db import Database
from app.engine import DeliveryEngine
from app.runner import AccountWorker
from app.web import create_app


class FakeClient:
    user_id = "2000000000002"

    def __init__(self, history: list[dict[str, Any]] | None = None) -> None:
        self.sent: list[tuple[str, str, str]] = []
        self.history = history or []
        self.created: tuple[str, str] | None = None
        self.created_chat = "chat-new"
        self.websocket = self

    async def send_text_safe(self, chat_id, to_id, text):
        self.sent.append((chat_id, to_id, text))

    async def list_conversation_messages(self, chat_id, limit=20, cursor=0):
        return {"messages": list(self.history), "next_cursor": 0, "has_more": False}

    async def create_chat(self, to_id, item_id):
        self.created = (to_id, item_id)
        return self.created_chat


async def build_worker(db: Database, client: FakeClient | None = None) -> tuple[AccountWorker, int]:
    account_id = await db.upsert_account("主号", "unb=2000000000002; _m_h5_tk=token_1")
    account = await db.get_account(account_id)
    worker = AccountWorker(db, account, DeliveryEngine(db))
    worker.client = client or FakeClient()  # type: ignore[assignment]
    return worker, account_id


async def seed_message(db: Database, account_id: int, **overrides: Any) -> int:
    fields = {
        "account_id": account_id,
        "chat_id": "c1",
        "sender_id": "b1",
        "sender_name": "买家小王",
        "item_id": "i1",
        "content": "在吗",
        "kind": "incoming",
    }
    fields.update(overrides)
    return await db.log_message(**fields)


# ------------------------------------------------------------------ 存储层


async def test_chat_list_groups_by_conversation_and_counts_unread(db):
    account_id = await db.upsert_account("主号", "cookie")
    await seed_message(db, account_id, content="在吗")
    await seed_message(db, account_id, content="在的", kind="outgoing")
    await seed_message(db, account_id, chat_id="c2", sender_id="b2", sender_name="另一个买家",
                       item_id="i2", content="发货快吗")

    chats = await db.list_chats()
    assert [row["chat_id"] for row in chats] == ["c2", "c1"]
    first = chats[1]
    assert first["account_name"] == "主号"
    assert first["content"] == "在的"
    assert first["unread"] == 1
    assert first["buyer_id"] == "b1"
    assert chats[0]["unread"] == 1

    only = await db.list_chats(account_id=account_id, limit=1)
    assert len(only) == 1
    assert only[0]["chat_id"] == "c2"


async def test_chat_messages_mark_read_and_peer_lookup(db):
    account_id = await db.upsert_account("主号", "cookie")
    await seed_message(db, account_id, content="第一句")
    await seed_message(db, account_id, content="第二句")

    rows = await db.list_chat_messages(account_id, "c1")
    assert [row["content"] for row in rows] == ["第一句", "第二句"]
    assert await db.chat_peer_id(account_id, "c1") == "b1"
    assert await db.last_chat_id_any(account_id, "b1") == "c1"

    chat = await db.get_chat(account_id, "c1")
    assert chat is not None
    assert chat["unread"] == 2
    assert await db.mark_chat_read(account_id, "c1") == 2
    assert (await db.get_chat(account_id, "c1"))["unread"] == 0
    assert await db.mark_chat_read(account_id, "c1") == 0


def test_old_database_backfills_read_state(tmp_path):
    """老库升级时，历史消息一次性标成已读，别让聊天界面挂满未读红点。"""
    path = tmp_path / "old.db"
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE messages (id INTEGER PRIMARY KEY AUTOINCREMENT, account_id INTEGER,"
        " chat_id TEXT, sender_id TEXT, sender_name TEXT, item_id TEXT, order_id TEXT,"
        " content TEXT, kind TEXT NOT NULL DEFAULT 'incoming', created_at TEXT NOT NULL)"
    )
    conn.execute(
        "INSERT INTO messages (account_id, chat_id, sender_id, sender_name, content, kind, created_at)"
        " VALUES (1, 'c1', 'b1', '老买家', '旧消息', 'incoming', '2026-01-01 10:00:00')"
    )
    conn.commit()
    conn.close()

    database = Database(path)
    asyncio.run(database.init())
    row = asyncio.run(database.fetch_one("SELECT * FROM messages WHERE chat_id = 'c1'"))
    assert row is not None
    assert row["read_at"] == "2026-01-01 10:00:00"
    assert row["external_id"] is None
    assert asyncio.run(database.list_item_models()) == []


async def test_synced_history_dedupes_against_polled_messages(db):
    account_id = await db.upsert_account("主号", "cookie")
    await seed_message(db, account_id, content="老消息", created_at="2026-01-01 10:00:00")

    await db.log_synced_message(
        account_id=account_id, chat_id="c1", sender_id="b1", sender_name="买家小王",
        content="老消息", kind="incoming", external_id="m-old", read_at="2026-01-01 10:01:00",
        created_at="2026-01-01 10:00:05",
    )
    rows = await db.list_chat_messages(account_id, "c1")
    assert len(rows) == 1, "老库里没有 messageId 时，按内容 + 时间窗兜底去重"
    assert rows[0]["external_id"] == "m-old"

    await db.log_synced_message(
        account_id=account_id, chat_id="c1", sender_id="b1", sender_name="买家小王",
        content="新消息", kind="incoming", external_id="m-new", read_at="2026-01-01 10:02:00",
        created_at="2026-01-01 10:00:10",
    )
    await db.log_synced_message(
        account_id=account_id, chat_id="c1", sender_id="b1", sender_name="买家小王",
        content="新消息", kind="incoming", external_id="m-new",
        created_at="2026-01-01 10:00:10",
    )
    rows = await db.list_chat_messages(account_id, "c1")
    assert [row["content"] for row in rows] == ["老消息", "新消息"]


# ------------------------------------------------------------------ Worker


async def test_manual_send_sends_and_logs_outgoing(db):
    worker, account_id = await build_worker(db)
    await seed_message(db, account_id, content="你好")

    message = await worker.send_manual_text(
        "c1", "b1", "  已经发货了  ", item_id="i1", order_id="o1", buyer_name="买家小王"
    )

    assert worker.client is not None
    assert worker.client.sent == [("c1", "b1", "已经发货了")]  # type: ignore[attr-defined]
    assert message["kind"] == "outgoing"
    assert message["content"] == "已经发货了"
    rows = await db.list_chat_messages(account_id, "c1")
    assert rows[-1]["content"] == "已经发货了"
    assert rows[-1]["kind"] == "outgoing"
    assert (await db.get_chat(account_id, "c1"))["unread"] == 0


async def test_manual_send_survives_logging_failure(db, monkeypatch):
    """消息已经发出去之后，写流水失败不能再让调用方以为「没发成功」。"""
    worker, _ = await build_worker(db)

    async def boom(**kwargs):
        raise RuntimeError("数据库写失败")

    monkeypatch.setattr(worker.db, "log_message", boom)
    message = await worker.send_manual_text("c1", "b1", "已经发出去了")

    assert worker.client.sent == [("c1", "b1", "已经发出去了")]  # type: ignore[attr-defined]
    assert message["id"] == 0
    assert message["content"] == "已经发出去了"


async def test_manual_send_resolves_peer_from_history(db):
    worker, account_id = await build_worker(db)
    await seed_message(db, account_id, content="在吗")

    await worker.send_manual_text("c1", "", "收到")

    assert worker.client.sent == [("c1", "b1", "收到")]  # type: ignore[attr-defined]


async def test_manual_send_requires_connection_and_text(db):
    worker, _ = await build_worker(db)
    worker.client = None
    with pytest.raises(XianyuError):
        await worker.send_manual_text("c1", "b1", "你好")

    worker.client = FakeClient()  # type: ignore[assignment]
    with pytest.raises(ValueError):
        await worker.send_manual_text("c1", "b1", "   ")
    with pytest.raises(ValueError):
        await worker.send_manual_text("c1", "b1", "x" * 2001)


async def test_sync_history_only_writes_incoming_and_dedupes(db):
    history = [
        {"chat_id": "c1", "buyer_id": "b1", "buyer_name": "买家小王", "content": "第一句",
         "created_at": 1767225600000, "message_id": "m1", "from_me": False},
        {"chat_id": "c1", "buyer_id": "", "buyer_name": "", "content": "我回的",
         "created_at": 1767225601000, "message_id": "m2", "from_me": True},
        {"chat_id": "c1", "buyer_id": "b1", "buyer_name": "买家小王", "content": "第二句",
         "created_at": 1767225602000, "message_id": "m3", "from_me": False},
    ]
    worker, account_id = await build_worker(db, FakeClient(history))

    handled = await worker.sync_chat_history("c1", limit=50)
    assert handled == 2
    rows = await db.list_chat_messages(account_id, "c1")
    assert [row["content"] for row in rows] == ["第一句", "第二句"]
    assert all(row["read_at"] for row in rows)
    assert await db.chat_peer_id(account_id, "c1") == "b1"

    await worker.sync_chat_history("c1", limit=50)
    rows = await db.list_chat_messages(account_id, "c1")
    assert len(rows) == 2, "同一条历史消息反复同步不能变成多条"


async def test_ensure_chat_reuses_history_or_creates_one(db):
    worker, account_id = await build_worker(db)
    await seed_message(db, account_id, content="在吗")

    assert await worker.ensure_chat("b1", item_id="i1") == "c1"
    assert worker.client.created is None  # type: ignore[attr-defined]

    assert await worker.ensure_chat("b2", item_id="i2") == "chat-new"
    assert worker.client.created == ("b2", "i2")  # type: ignore[attr-defined]

    with pytest.raises(ValueError):
        await worker.ensure_chat("b3")


# ------------------------------------------------------------------ Web API


class ChatManager:
    def __init__(self, db: Database) -> None:
        self.db = db
        self.sent: list[dict[str, Any]] = []
        self.synced: list[tuple[int, str]] = []

    async def sync(self) -> None:
        pass

    async def stop_all(self) -> None:
        pass

    def status(self) -> list[dict[str, Any]]:
        return []

    async def send_chat_text(self, account_id, chat_id, to_id, text, **kwargs):
        self.sent.append({
            "account_id": account_id, "chat_id": chat_id, "to_id": to_id,
            "text": text, **kwargs,
        })
        message_id = await self.db.log_message(
            account_id=account_id, chat_id=chat_id, sender_id=to_id,
            sender_name=kwargs.get("buyer_name") or "买家", item_id=kwargs.get("item_id"),
            order_id=kwargs.get("order_id"), content=text, kind="outgoing",
            created_at="2026-01-01 10:00:00",
        )
        return {
            "id": message_id, "account_id": account_id, "chat_id": chat_id,
            "sender_id": to_id, "content": text, "kind": "outgoing",
            "created_at": "2026-01-01 10:00:00",
        }

    async def sync_chat(self, account_id, chat_id, limit=50):
        self.synced.append((account_id, chat_id))
        return 0

    async def open_chat(self, account_id, buyer_id, item_id="", order_id=""):
        self.sent.append({"opened": (account_id, buyer_id, item_id, order_id)})
        return "chat-opened"


@pytest.fixture()
async def chat_client(tmp_path):
    db = Database(tmp_path / "chat.db")
    manager = ChatManager(db)
    app = create_app(db=db, manager=manager)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1") as ac:
        async with app.router.lifespan_context(app):
            yield ac, manager, db


async def test_chat_api_lists_reads_and_sends(chat_client):
    client, manager, db = chat_client
    account_id = await db.upsert_account("主号", "cookie")
    await seed_message(db, account_id, content="在吗？")

    chats = (await client.get("/api/chats")).json()
    assert len(chats) == 1
    assert chats[0]["unread"] == 1
    assert chats[0]["account_name"] == "主号"

    detail = (await client.get(
        f"/api/chats/c1/messages?account_id={account_id}&sync=true"
    )).json()
    assert detail["sync_error"] == ""
    assert manager.synced == [(account_id, "c1")]
    assert [m["content"] for m in detail["messages"]] == ["在吗？"]

    assert (await client.post(f"/api/chats/c1/read?account_id={account_id}")).json()["read"] == 1
    assert (await client.get("/api/chats")).json()[0]["unread"] == 0

    res = await client.post("/api/chats/c1/send", json={
        "account_id": account_id, "to_id": "b1", "text": " 在的 ",
        "item_id": "i1", "order_id": "o1", "buyer_name": "买家小王",
    })
    assert res.status_code == 200
    assert res.json()["message"]["kind"] == "outgoing"
    assert manager.sent[-1]["text"] == "在的"
    assert manager.sent[-1]["buyer_name"] == "买家小王"

    res = await client.post("/api/chats/open", json={
        "account_id": account_id, "buyer_id": "b1", "item_id": "i1", "order_id": "o1",
    })
    assert res.status_code == 200
    assert res.json()["chat_id"] == "chat-opened"
    assert manager.sent[-1]["opened"] == (account_id, "b1", "i1", "o1")


async def test_chat_api_reports_offline_send_and_keeps_local_on_sync_error(chat_client, monkeypatch):
    client, manager, db = chat_client
    account_id = await db.upsert_account("主号", "cookie")
    await seed_message(db, account_id, content="在吗？")

    async def offline(*args, **kwargs):
        raise LookupError("账号不在运行中")

    async def sync_fail(*args, **kwargs):
        raise XianyuError("连接断了")

    monkeypatch.setattr(manager, "send_chat_text", offline)
    res = await client.post("/api/chats/c1/send", json={
        "account_id": account_id, "to_id": "b1", "text": "在的",
    })
    assert res.status_code == 409
    assert "不在运行" in res.json()["detail"]

    monkeypatch.setattr(manager, "sync_chat", sync_fail)
    detail = (await client.get(
        f"/api/chats/c1/messages?account_id={account_id}&sync=true"
    )).json()
    assert "连接断了" in detail["sync_error"]
    assert [m["content"] for m in detail["messages"]] == ["在吗？"]


async def test_chat_send_validates_empty_text(chat_client):
    client, _, db = chat_client
    account_id = await db.upsert_account("主号", "cookie")
    res = await client.post("/api/chats/c1/send", json={
        "account_id": account_id, "to_id": "b1", "text": "   ",
    })
    assert res.status_code == 400
