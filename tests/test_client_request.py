"""请求-响应关联（dispatch / _request / create_chat）。

背景：websockets 不允许两个协程同时 recv。
订单触发的发货需要 create_chat 拿会话 ID，而回包和普通推送走同一条流，
所以必须由收帧主循环 dispatch 回包，不能在业务代码里另起 recv。
之前正是这里报了 cannot call recv while another coroutine is already running recv。
"""

from __future__ import annotations

import asyncio
import json

import pytest

from app.client import XianyuClient, XianyuError


class FakeWS:
    """假的 WebSocket：记录发出的帧，可手工注入回包。"""

    def __init__(self):
        self.sent: list[dict] = []

    async def send(self, payload: str):
        self.sent.append(json.loads(payload))

    async def close(self):
        pass


def make_client() -> XianyuClient:
    client = XianyuClient("unb=1; _m_h5_tk=tok_1")
    client.websocket = FakeWS()
    return client


async def test_dispatch_resolves_pending_future():
    client = make_client()
    future = asyncio.get_running_loop().create_future()
    client._pending["mid-1"] = future

    consumed = client.dispatch({"headers": {"mid": "mid-1"}, "code": 200, "body": {}})
    assert consumed is True
    assert future.result()["code"] == 200


async def test_dispatch_ignores_unknown_mid():
    client = make_client()
    assert client.dispatch({"headers": {"mid": "nope"}}) is False


async def test_dispatch_ignores_frame_without_mid():
    client = make_client()
    assert client.dispatch({"body": {}}) is False


async def test_note_sid_only_records_first():
    client = make_client()
    client.note_sid({"headers": {"sid": "abc"}})
    client.note_sid({"headers": {"sid": "def"}})
    assert client.sid == "abc"


async def test_request_times_out():
    client = make_client()
    frame = {"headers": {"mid": "m-timeout"}, "lwp": "/x"}
    with pytest.raises(XianyuError):
        await client._request(frame, timeout=0.05)


async def test_request_requires_connection():
    client = XianyuClient("unb=1; _m_h5_tk=t")
    with pytest.raises(XianyuError):
        await client._request({"headers": {"mid": "m"}}, timeout=0.1)


async def _responder(client: XianyuClient, body: dict, code: int = 200):
    """等请求发出去，再注入回包（模拟主循环 dispatch）。"""
    for _ in range(100):
        await asyncio.sleep(0.01)
        if client.websocket.sent:
            break
    mid = client.websocket.sent[0]["headers"]["mid"]
    client.dispatch({"headers": {"mid": mid}, "code": code, "body": body})


async def test_create_chat_returns_cid_via_dispatch():
    client = make_client()
    task = asyncio.create_task(
        _responder(client, {"singleChatConversation": {"cid": "68100583197@goofish"}})
    )
    try:
        cid = await client.create_chat("2000000000003", "2000000000001")
    finally:
        task.cancel()

    assert cid == "68100583197"
    sent = client.websocket.sent[0]
    assert sent["lwp"] == "/r/SingleChatConversation/create"
    assert sent["body"][0]["pairFirst"] == "2000000000003@goofish"


async def test_create_chat_raises_on_error_code():
    client = make_client()
    task = asyncio.create_task(_responder(client, {}, code=400))
    try:
        with pytest.raises(XianyuError):
            await client.create_chat("2000000000003", "2000000000001")
    finally:
        task.cancel()


async def test_send_text_carries_sid():
    client = make_client()
    client.sid = "sid-xyz"
    await client.send_text(client.websocket, "68100583197", "2000000000003", "hi")
    assert client.websocket.sent[0]["headers"]["sid"] == "sid-xyz"
