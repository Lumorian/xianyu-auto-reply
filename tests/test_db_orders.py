"""orders / 消息去重表的并发与保留策略测试。"""

from __future__ import annotations

import asyncio
import stat

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


async def test_database_file_is_private(tmp_path):
    """DB 里有 Cookie/卡密/Key，不能是 644（同机其他用户可读）。"""
    path = tmp_path / "private.db"
    db = Database(path)
    await db.init()
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


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


async def test_delivered_record_cannot_be_downgraded_to_blocked(db):
    """已发货记录是终态：后续核实失败不能把它改成 blocked。

    旧实现会直接覆盖 status，导致 delivered_order_exists() 变 False；
    如果订单仍是待发货，轮询就会再发一次。
    """
    await db.record_order(**order_fields(
        content="卡密：CARD-1", card_id=1, card_code="CARD-1", status="delivered",
        detail="确认发货成功",
    ))
    await db.record_order(**order_fields(
        rule_id=None, card_id=None, content="", status="blocked",
        detail="真人文本消息，订单未经核实，不自动发货",
    ))

    rows = await db.list_orders()
    assert len(rows) == 1
    assert rows[0]["status"] == "delivered"
    assert rows[0]["content"] == "卡密：CARD-1"
    assert rows[0]["card_id"] == 1
    assert rows[0]["card_code"] == "CARD-1"
    assert "已发货后收到 blocked" in rows[0]["detail"]
    assert await db.delivered_order_exists(1, "2500000000000000001") is True


async def test_record_order_without_order_id_always_inserts(db):
    """没有订单号（占位/调试）不参与唯一索引，写几条就是几条。"""
    a = await db.record_order(**order_fields(order_id=None))
    b = await db.record_order(**order_fields(order_id=None))
    assert a != b
    assert len(await db.list_orders()) == 2


async def test_stale_sending_is_recovered_as_unknown(db):
    """进程中断留下的 sending 不能当成未发货重发。"""
    await db.record_order(**order_fields(status="sending", detail="正在发送", card_id=3))
    assert await db.delivery_blocked(1, "2500000000000000001") is True

    assert await db.recover_stale_sending() == 1
    row = (await db.list_orders())[0]
    assert row["status"] == "send_unknown"
    assert "进程在发送过程中中断" in row["detail"]
    assert row["card_id"] == 3

    assert await db.recover_stale_sending() == 0


async def test_add_cards_deduplicates_within_and_across_pools(db):
    """同一张卡不能因为粘两次/换商品池就卖两次。"""
    assert await db.add_cards(["CARD-1", "CARD-1", "  "], "*") == 1
    assert await db.add_cards(["CARD-1"], "item-a") == 0, "跨池也不能重复"
    assert await db.add_cards(["CARD-2"], "item-a") == 1

    cards = await db.list_cards()
    assert sorted(row["content"] for row in cards) == ["CARD-1", "CARD-2"]


async def test_log_message_dedups_by_external_id(db):
    """同一条服务端消息并发写入只能留一行；第二次返回同一 id。"""
    fields = {
        "account_id": 1, "chat_id": "c1", "sender_id": "b1", "sender_name": "买家",
        "content": "hi", "kind": "incoming", "external_id": "m-1",
    }
    first = await db.log_message(**fields)
    second = await db.log_message(**fields)
    assert first == second
    assert len(await db.list_messages()) == 1


async def test_claim_message_seen_is_atomic(db):
    """并发 claim 只能有一个成功，不能两边都拿到 False 后各处理一遍。"""
    results = await asyncio.gather(
        *(db.claim_message_seen(1, "c1:m-1") for _ in range(5))
    )
    assert sum(1 for ok in results if ok) == 1


async def test_message_sender_indexes_exist(db):
    """昵称回填/复用会话按 sender 查询，消息表需要对应索引。"""
    rows = await db.fetch_all(
        "SELECT name FROM sqlite_master WHERE type = 'index'"
        " AND name IN ('idx_messages_sender', 'idx_messages_sender_item')"
    )
    assert {row["name"] for row in rows} == {
        "idx_messages_sender", "idx_messages_sender_item"
    }


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



async def test_mark_sending_claims_a_fresh_order(db):
    """新订单第一次发送：直接以 sending 落库，拿到发送权。"""
    assert await db.mark_sending(**order_fields()) is True
    rows = await db.list_orders()
    assert len(rows) == 1
    assert rows[0]["status"] == "sending"
    assert await db.delivery_blocked(1, "2500000000000000001") is True


async def test_mark_sending_allows_retry_after_failure(db):
    """上次发送失败可以重试；认领成功后状态必须变成 sending。"""
    await db.record_order(**order_fields(status="send_failed", detail="网络错误"))
    assert await db.mark_sending(**order_fields(status="sending")) is True
    rows = await db.list_orders()
    assert len(rows) == 1
    assert rows[0]["status"] == "sending"


@pytest.mark.parametrize(
    "blocked_status", ["delivered", "sending", "send_unknown", "topup_unknown"]
)
async def test_mark_sending_refuses_orders_that_already_left(db, blocked_status):
    """已发出 / 结果未知 / 正在发送的单，不能被第二条路径再次认领。"""
    await db.record_order(**order_fields(status=blocked_status, card_id=None))
    assert await db.mark_sending(**order_fields()) is False
    rows = await db.list_orders()
    assert len(rows) == 1
    assert rows[0]["status"] == blocked_status


async def test_mark_sending_concurrent_claim_only_one_wins(db):
    """两条触发路径同时认领同一单：只能有一个 True。"""
    await db.record_order(**order_fields(status="send_failed", detail="上次失败"))

    results = await asyncio.gather(*(db.mark_sending(**order_fields()) for _ in range(8)))

    assert sum(1 for ok in results if ok) == 1
    rows = await db.list_orders()
    assert len(rows) == 1
    assert rows[0]["status"] == "sending"
