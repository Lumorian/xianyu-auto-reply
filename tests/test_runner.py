"""Worker 处理链路的集成测试（用假客户端替代真实闲鱼连接）。"""

from __future__ import annotations

import asyncio
import base64
import json

import msgpack
import pytest

from app.client import CookieExpired, RiskControl, XianyuClient
from app.db import Database
from app.engine import DeliveryEngine
from app.runner import AccountWorker, WorkerManager


class FakeClient:
    """记录发出的消息与确认发货调用，不产生任何网络请求。"""

    user_id = "2000000000002"

    def __init__(self) -> None:
        self.sent: list[tuple[str, str, str]] = []
        self.confirmed: list[str] = []
        self.acked: list[dict] = []
        self.websocket = self  # 仅用于内部判空
        self.fail_first = False

    async def ack(self, ws, frame):
        self.acked.append(frame)

    async def send_text_safe(self, chat_id, to_id, text):
        self.sent.append((chat_id, to_id, text))

    async def confirm_shipping(self, order_id):
        self.confirmed.append(order_id)
        return True, "SUCCESS"

    async def close(self):
        pass


def card_payload(content="[我已付款，等待你发货]", item_id="900052644277", order_id="2500000000000000001"):
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
            "6": {
                "3": {
                    "5": json.dumps({
                        "dxCard": {
                            "item": {
                                "main": {"targetUrl": f"fleamarket://order_detail?id={order_id}&role=seller"}
                            }
                        }
                    })
                }
            },
        },
    }


def frame_with(payload):
    data = base64.b64encode(msgpack.packb(payload, use_bin_type=True)).decode()
    return json.dumps({
        "headers": {"mid": "m1", "sid": "s1"},
        "body": {"syncPushPackage": {"data": [{"data": data}]}},
    })


async def build_worker(db: Database):
    account_id = await db.upsert_account("主号", "unb=2000000000002; _m_h5_tk=token_1")
    account = await db.get_account(account_id)
    engine = DeliveryEngine(db)
    worker = AccountWorker(db, account, engine)
    worker.client = FakeClient()
    return worker, account_id


async def test_paid_message_triggers_delivery(db):
    worker, _ = await build_worker(db)
    await db.save_rule({
        "name": "月卡", "item_id": "*", "enabled": True, "mode": "card",
        "content": "你好{buyer}，卡密：{card}", "auto_confirm": True, "priority": 0, "remark": "",
    })
    await db.add_cards(["CARD-0001"], "*")

    await worker._on_frame(frame_with(card_payload()))

    client = worker.client
    assert len(client.acked) == 1
    assert len(client.sent) == 1
    chat_id, to_id, text = client.sent[0]
    assert chat_id == "47983389096"
    assert to_id == "4100000001"
    assert "CARD-0001" in text
    assert client.confirmed == ["2500000000000000001"]

    orders = await db.list_orders()
    assert orders[0]["status"] == "delivered"
    assert orders[0]["card_id"] is not None


async def test_ack_delivery_does_not_block_the_receive_loop(db):
    """真实客户端等 ack 时，不能把收帧主循环自己卡死。

    ack 回包只能由 _on_frame 的主循环 dispatch；如果在 _on_frame 里直接 await
    send_delivery_text，回包永远没人处理，只能等 15 秒超时。
    """
    class SlowAckClient(XianyuClient):
        def __init__(self) -> None:
            super().__init__("unb=2000000000002; _m_h5_tk=tok_1")
            self.websocket = self
            self.started = asyncio.Event()
            self.gate = asyncio.Event()
            self.sent_text: list[str] = []

        async def ack(self, ws, frame):
            pass

        async def send_delivery_text(self, chat_id, to_id, text):
            self.started.set()
            await self.gate.wait()
            self.sent_text.append(text)
            return "m-ack", "sent"

        async def confirm_shipping(self, order_id):
            return True, "SUCCESS"

    await db.save_rule({
        "name": "月卡", "item_id": "*", "enabled": True, "mode": "card",
        "content": "卡密：{card}", "auto_confirm": False, "priority": 0, "remark": "",
    })
    await db.add_cards(["CARD-ACK"], "*")
    account_id = await db.upsert_account("主号", "unb=2000000000002; _m_h5_tk=token_1")
    account = await db.get_account(account_id)
    worker = AccountWorker(db, account, DeliveryEngine(db))
    worker.client = SlowAckClient()

    await asyncio.wait_for(worker._on_frame(frame_with(card_payload())), timeout=1.0)
    await asyncio.wait_for(worker.client.started.wait(), timeout=1.0)
    assert worker.client.sent_text == []  # gate 还没开，说明它在等 ack 而不是主循环

    worker.client.gate.set()
    while worker._delivery_tasks:
        await asyncio.gather(*list(worker._delivery_tasks))

    assert len(worker.client.sent_text) == 1
    assert (await db.list_orders())[0]["status"] == "delivered"


async def test_same_order_not_delivered_twice(db):
    worker, _ = await build_worker(db)
    await db.save_rule({
        "name": "文案", "item_id": "*", "enabled": True, "mode": "text",
        "content": "已发货", "auto_confirm": False, "priority": 0, "remark": "",
    })
    frame = frame_with(card_payload())
    await worker._on_frame(frame)
    await worker._on_frame(frame)
    assert len(worker.client.sent) == 1


async def test_unrelated_message_is_logged_only(db):
    worker, _ = await build_worker(db)
    await db.save_rule({
        "name": "文案", "item_id": "*", "enabled": True, "mode": "text",
        "content": "已发货", "auto_confirm": False, "priority": 0, "remark": "",
    })
    await worker._on_frame(frame_with(card_payload(content="在吗？")))
    assert worker.client.sent == []
    messages = await db.list_messages()
    assert messages[0]["content"] == "在吗？"


async def test_self_message_is_ignored(db):
    worker, _ = await build_worker(db)
    payload = card_payload()
    payload["1"]["10"]["senderUserId"] = "2000000000002"
    await worker._on_frame(frame_with(payload))
    assert worker.client.sent == []


async def test_invalid_cookie_is_cookie_expired_not_endless_retry(db):
    """Cookie 缺 unb 时立刻归为需要重新扫码，而不是每 5 秒重连一次。"""
    account_id = await db.upsert_account("坏 Cookie", "foo=bar")
    account = await db.get_account(account_id)
    worker = AccountWorker(db, account, DeliveryEngine(db))

    with pytest.raises(CookieExpired):
        await worker._once()
    assert worker.status == "cookie_expired"
    assert worker.client is None


async def test_broken_frame_does_not_raise(db):
    worker, _ = await build_worker(db)
    await worker._on_frame("not json")
    await worker._on_frame(json.dumps({"body": {"syncPushPackage": {"data": [{"data": "!!!"}]}}}))
    assert worker.client.sent == []

# ------------------------------------- 处理失败不能把消息永久吞掉


async def test_failed_message_is_retried_instead_of_dropped(db, monkeypatch):
    """处理时抛异常必须撤销 seen 标记，否则这条消息就永远丢了。"""
    from app.codec import IncomingMessage

    worker, account_id = await build_worker(db)
    msg = IncomingMessage(
        chat_id="47983389096", sender_id="4100000001",
        sender_name="买家小王", content="在吗？", message_id="m-1",
    )
    original = worker._handle_incoming
    calls = {"n": 0}

    async def flaky(message, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("模拟下游异常")
        return await original(message, **kwargs)

    monkeypatch.setattr(worker, "_handle_incoming", flaky)

    with pytest.raises(RuntimeError):
        await worker._process_incoming(msg, allow_auto_reply=False)
    assert await db.messages_seen(account_id, ["47983389096:m-1"]) == set(),         "失败后必须撤销去重标记，下一轮才有机会重来"

    await worker._process_incoming(msg, allow_auto_reply=False)
    assert [m["content"] for m in await db.list_messages()] == ["在吗？"]


async def test_message_that_keeps_failing_is_given_up(db, monkeypatch):
    """同一条消息连续失败超限就放弃，免得每轮都重试、把日志刷满。"""
    from app.codec import IncomingMessage
    from app.runner import SEEN_RETRY_LIMIT

    worker, account_id = await build_worker(db)
    msg = IncomingMessage(
        chat_id="c9", sender_id="b9", sender_name="买家", content="坏消息", message_id="m-9",
    )

    async def always_boom(message, **kwargs):
        raise RuntimeError("总是失败")

    monkeypatch.setattr(worker, "_handle_incoming", always_boom)

    for _ in range(SEEN_RETRY_LIMIT):
        with pytest.raises(RuntimeError):
            await worker._process_incoming(msg, allow_auto_reply=False)
        assert await db.messages_seen(account_id, ["c9:m-9"]) == set()

    with pytest.raises(RuntimeError):
        await worker._process_incoming(msg, allow_auto_reply=False)
    assert await db.messages_seen(account_id, ["c9:m-9"]) == {"c9:m-9"},         "超过重试上限后要保留标记，别再重试"


async def test_persist_cookie_writes_back_to_db(db):
    """刷新后的 Cookie（含新 _m_h5_tk）必须写回，否则重启又拿旧 token 撞风控。"""
    worker, account_id = await build_worker(db)
    new_cookie = "unb=2000000000002; _m_h5_tk=new_token_1793000000000"
    await worker._persist_cookie(new_cookie)

    account = await db.get_account(account_id)
    assert account["cookie"] == new_cookie
    assert worker.account["cookie"] == new_cookie


async def test_sync_restarts_worker_that_already_exited(db, monkeypatch):
    """扫码重新登录后，旧 worker 如果已经退出，sync 必须把它重新拉起来。"""
    account_id = await db.upsert_account("重启测试", "unb=2000000000002; _m_h5_tk=token_1")
    manager = WorkerManager(db, provisioner=object())
    started: list[int] = []
    monkeypatch.setattr(AccountWorker, "start", lambda self: started.append(self.account_id))

    await manager.sync()
    worker = manager.workers[account_id]
    assert started == [account_id]

    # 模拟 worker 因 cookie_expired 退出（任务结束）
    worker._task = None
    await manager.sync()
    assert started == [account_id, account_id]

async def test_risk_control_keeps_worker_alive(db):
    """风控/滑块不能停 worker，必须进入 risk_control 后继续重试。"""
    worker, _ = await build_worker(db)

    async def fake_once():
        raise RiskControl("FAIL_SYS_USER_VALIDATE / RGV587_ERROR")

    worker._once = fake_once
    worker.start()
    try:
        for _ in range(50):
            await asyncio.sleep(0.01)
            if worker.status == "risk_control":
                break
        assert worker.status == "risk_control"
        assert worker._task is not None and not worker._task.done()
        assert "RGV587" in worker.last_error
    finally:
        await worker.stop()
