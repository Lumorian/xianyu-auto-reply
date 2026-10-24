"""orders / 消息去重表的并发与保留策略测试。"""

from __future__ import annotations

import asyncio

import pytest

from app.db import Database


def order_fields(**overrides):
    data = {
        "account_id": 1,
        "order_id": "2500000000000000001",
        "item_id": "900052644277",
        "buyer_id": "4100000001",
        "buyer_name": "买家小王",
        "rule_id": 1,
        "card_id": None,
        "content": "卡密：CARD-1",
        "status": "delivered",
        "detail": "",
    }
    data.update(overrides)
    return data


async def test_concurrent_record_order_for_the_same_order_does_not_raise(tmp_path):
    """推送和轮询同时写同一单：只能有一行，且不能抛 IntegrityError。

    旧实现是「先 SELECT 再 INSERT/UPDATE」，两边都查不到就都 INSERT，
    后一个撞唯一索引 —— 而那一步在发货之后，会被上层当成「没发出去」。
    """
    db = Database(tmp_path / "orders.db")
    await db.init()

    async def write(i: int) -> int:
        return await db.record_order(**order_fields(content=f"第 {i} 次"))

    ids = await asyncio.gather(*(write(i) for i in range(8)))

    assert len(set(ids)) == 1, "同一单必须落到同一行"
    rows = await db.list_orders()
    assert len(rows) == 1


async def test_blocked_record_keeps_its_reason_after_delivery(db):
    """blocked（有人在试别人的订单）被 delivered 覆盖时，审计原因不能丢。"""
    await db.record_order(**order_fields(
        rule_id=None, content="", status="blocked",
        detail="真人文本消息，订单未经核实，不自动发货",
    ))
    await db.record_order(**order_fields(status="delivered", detail="确认发货成功：SUCCESS"))

    rows = await db.list_orders()
    assert len(rows) == 1
    assert rows[0]["status"] == "delivered"
    assert "确认发货成功" in rows[0]["detail"]
    assert "此前曾拦截" in rows[0]["detail"]
    assert "未经核实" in rows[0]["detail"]


async def test_record_order_without_order_id_always_inserts(db):
    """没有订单号（占位/调试）不参与唯一索引，写几条就是几条。"""
    a = await db.record_order(**order_fields(order_id=None))
    b = await db.record_order(**order_fields(order_id=None))
    assert a != b
    assert len(await db.list_orders()) == 2


async def test_unmark_messages_seen_allows_retry(db):
    """撤销 seen 标记后，同一个 key 还能被重新处理。"""
    await db.mark_messages_seen(1, ["c1:m1", "c1:m2"])
    assert await db.messages_seen(1, ["c1:m1", "c1:m2"]) == {"c1:m1", "c1:m2"}

    assert await db.unmark_messages_seen(1, ["c1:m1"]) == 1
    assert await db.messages_seen(1, ["c1:m1", "c1:m2"]) == {"c1:m2"}
    assert await db.unmark_messages_seen(1, ["不存在"]) == 0


async def test_prune_history_only_removes_old_messages(db):
    """只清 messages；seen_messages / reply_hits 一动，就会重复回复。"""
    await db.log_message(account_id=1, chat_id="c1", sender_id="b1", sender_name="买家",
                         item_id="i1", order_id=None, content="很久以前的消息")
    await db.execute(
        "UPDATE messages SET created_at = datetime('now', 'localtime', '-90 days')"
    )
    await db.mark_messages_seen(1, ["c1:old"])
    await db.record_reply_hit(1, "b1", "c1", 1)
    await db.log_message(account_id=1, chat_id="c1", sender_id="b1", sender_name="买家",
                         item_id="i1", order_id=None, content="刚发的消息")

    deleted = await db.prune_history(30)

    assert deleted == 1
    assert [m["content"] for m in await db.list_messages()] == ["刚发的消息"]
    assert await db.messages_seen(1, ["c1:old"]) == {"c1:old"}
    assert await db.reply_hit_exists(1, "b1", "c1", 1)


@pytest.mark.parametrize("days", [0, -1])
async def test_prune_history_is_off_when_disabled(db, days):
    await db.log_message(account_id=1, chat_id="c1", sender_id="b1", sender_name="买家",
                         item_id="i1", order_id=None, content="旧消息")
    await db.execute("UPDATE messages SET created_at = datetime('now', 'localtime', '-900 days')")
    assert await db.prune_history(days) == 0
    assert len(await db.list_messages()) == 1
