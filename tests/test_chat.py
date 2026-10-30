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


async def test_synced_same_text_with_distinct_message_ids_is_not_merged(db):
    """两句话一样但 messageId 不同，是两条消息，不能按内容兜底吃掉一条。"""
    account_id = await db.upsert_account("主号", "cookie")
    for external_id in ("m-1", "m-2"):
        await db.log_synced_message(
            account_id=account_id, chat_id="c1", sender_id="b1", sender_name="买家小王",
            content="在吗？", kind="outgoing", external_id=external_id,
            created_at="2026-01-01 10:00:00",
        )

    rows = await db.list_chat_messages(account_id, "c1")
    assert len(rows) == 2
    assert {row["external_id"] for row in rows} == {"m-1", "m-2"}


# --------------------------------------------------- 商品名（聊天列表显示用）


async def test_chat_list_shows_item_title(db):
    """会话列表显示商品名：推来的消息里带着 itemTitle，要记下来给列表用。"""
    account_id = await db.upsert_account("主号", "cookie")
    await seed_message(db, account_id, item_id="i1", item_title="GPT 会员代充（月卡）")

    chat = (await db.list_chats())[0]
    assert chat["item_id"] == "i1"
    assert chat["item_title"] == "GPT 会员代充（月卡）"

    # 之后这个会话再来消息、哪怕没带标题，也能按商品 ID 借到同一个名字
    await seed_message(db, account_id, content="在的", kind="outgoing", item_id="i1")
    assert (await db.list_chats())[0]["item_title"] == "GPT 会员代充（月卡）"


async def test_chat_title_falls_back_to_same_chat_item(db):
    """后台自己发出去的消息行常常没有 item_id，标题得靠同一个会话里带 ID 的那条。"""
    account_id = await db.upsert_account("主号", "cookie")
    await seed_message(db, account_id, item_id="i9", item_title="Kimi 会员")
    await seed_message(db, account_id, content="我发出的回复", kind="outgoing",
                       item_id=None, item_title="")

    chat = (await db.list_chats())[0]
    assert chat["item_id"] is None
    assert chat["item_title"] == "Kimi 会员"


async def test_remember_item_titles_is_cheap_and_safe(db):
    """拉商品列表时批量记标题：空 ID / 空标题跳过，标题没变就不写库。"""
    changed = await db.remember_item_titles([("i1", "月卡"), ("", "没商品 ID"), ("i2", "   ")])
    assert changed == 1
    assert await db.item_titles(["i1", "i2", "nope"]) == {"i1": "月卡"}

    # 每轮消息都会调一次，标题没变必须什么也不做
    assert await db.remember_item_title("i1", "月卡") is False
    assert await db.remember_item_title("i1", "月卡 Pro") is True
    assert await db.item_titles(["i1"]) == {"i1": "月卡 Pro"}

    # 空标题不能把已有标题抹掉（有些回调里 title 字段就是空的）
    assert await db.remember_item_title("i1", "   ") is False
    assert await db.item_titles(["i1"]) == {"i1": "月卡 Pro"}
    assert await db.item_titles([]) == {}


# ------------------------------------------------------------------ 删除会话


async def test_delete_chat_clears_local_records_only(db):
    """删会话 = 纯本地清理：消息流水 + 这个会话的自动回复命中记录。"""
    account_id = await db.upsert_account("主号", "cookie")
    other_account = await db.upsert_account("小号", "cookie2")
    await seed_message(db, account_id, content="在吗")
    await seed_message(db, account_id, content="在的", kind="outgoing")
    await seed_message(db, account_id, chat_id="c2", sender_id="b2", content="别的会话")
    await seed_message(db, other_account, content="同一个 chat_id，另一个账号")

    await db.record_reply_hit(account_id, "b1", "c1", 1)
    await db.record_reply_hit(account_id, "b2", "c2", 1)
    await db.mark_messages_seen(account_id, ["m-1"])

    removed = await db.delete_chat(account_id, " c1 ")
    assert removed == {"messages": 2, "reply_hits": 1}

    assert await db.list_chat_messages(account_id, "c1") == []
    assert await db.get_chat(account_id, "c1") is None
    assert [c["chat_id"] for c in await db.list_chats(account_id=account_id)] == ["c2"]

    # 命中记录跟着删：不然会话重新出现时自动回复会以为「早回过」而不吭声
    assert await db.reply_hit_exists(account_id, "b1", "c1", 1) is False
    assert await db.reply_hit_exists(account_id, "b2", "c2", 1) is True

    # 已处理标记故意留着：删掉的话下一轮轮询会把最后一条消息重新导进来，会话原地复活
    assert await db.messages_seen(account_id, ["m-1"]) == {"m-1"}

    # 别的账号碰巧用同一个 chat_id，不能被误删
    assert len(await db.list_chat_messages(other_account, "c1")) == 1

    with pytest.raises(ValueError):
        await db.delete_chat(account_id, "   ")


async def test_delete_chat_twice_is_harmless(db):
    """删过的会话再删一次只是 0 条，不能报错（用户会连点两下）。"""
    account_id = await db.upsert_account("主号", "cookie")
    await seed_message(db, account_id, content="在吗")

    assert (await db.delete_chat(account_id, "c1"))["messages"] == 1
    assert await db.delete_chat(account_id, "c1") == {"messages": 0, "reply_hits": 0}


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


async def test_sync_history_writes_incoming_and_outgoing_and_dedupes(db):
    history = [
        {"chat_id": "c1", "buyer_id": "b1", "buyer_name": "买家小王", "content": "第一句",
         "created_at": 1767225600000, "message_id": "m1", "from_me": False},
        {"chat_id": "c1", "buyer_id": "", "peer_id": "b1", "buyer_name": "", "content": "我回的",
         "created_at": 1767225601000, "message_id": "m2", "from_me": True},
        {"chat_id": "c1", "buyer_id": "b1", "buyer_name": "买家小王", "content": "第二句",
         "created_at": 1767225602000, "message_id": "m3", "from_me": False},
    ]
    worker, account_id = await build_worker(db, FakeClient(history))

    handled = await worker.sync_chat_history("c1", limit=50)
    assert handled == 3
    rows = await db.list_chat_messages(account_id, "c1")
    assert [row["content"] for row in rows] == ["第一句", "我回的", "第二句"]
    assert [row["kind"] for row in rows] == ["incoming", "outgoing", "incoming"]
    assert all(row["read_at"] for row in rows)
    assert await db.chat_peer_id(account_id, "c1") == "b1"

    await worker.sync_chat_history("c1", limit=50)
    rows = await db.list_chat_messages(account_id, "c1")
    assert len(rows) == 3, "同一条历史消息反复同步不能变成多条"


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


async def test_chat_delete_api_clears_one_conversation(chat_client):
    client, _, db = chat_client
    account_id = await db.upsert_account("主号", "cookie")
    await seed_message(db, account_id, content="在吗？")

    res = await client.delete(f"/api/chats/c1?account_id={account_id}")
    assert res.status_code == 200
    body = res.json()
    assert body["ok"] is True
    assert body["messages"] == 1
    assert (await client.get("/api/chats")).json() == []

    # 再删一次：0 条，不报错
    again = await client.delete(f"/api/chats/c1?account_id={account_id}")
    assert again.status_code == 200
    assert again.json()["messages"] == 0

    # 会话 ID 是空白 → 400，别真去删别的会话
    assert (await client.delete(f"/api/chats/%20%20?account_id={account_id}")).status_code == 400
    # 账号必填：少了就 422，不能瞎猜删到哪个账号头上
    assert (await client.delete("/api/chats/c1")).status_code == 422
