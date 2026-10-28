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

from app.client import RiskControl, XianyuClient, XianyuError, _pick_order


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


async def test_order_listing_propagates_risk_control():
    """风控不能被当成「查不到订单」吞掉，否则 worker 不会退避。"""
    client = make_client()

    async def boom(api, data, version="1.0"):
        raise RiskControl("RGV587")

    client.mtop = boom  # type: ignore[method-assign]
    with pytest.raises(RiskControl):
        await client.find_order("2500000000000000001")
    with pytest.raises(RiskControl):
        await client.list_pending_orders()


# ------------------------------------------- 订单精确匹配（鉴权前置条件）


def test_pick_order_rejects_mismatched_single_item():
    """回包只有一条、但订单号不是请求的那个：不能认成目标订单。

    旧实现只要「只有一条 + 有 priceVO」就返回，买家编一个假单号就能拿
    卖家唯一的那一单通过核实，进而在假单号下领走卡密。
    """
    response = {"data": {"module": {"items": [{
        "commonData": {"orderId": "111", "orderStatus": "待发货"},
        "priceVO": {"totalPrice": "10.00"},
    }]}}}
    assert _pick_order(response, "999") is None


def test_pick_order_allows_legacy_single_item_without_order_id():
    """旧接口回包不带 orderId 时，单条兜底仍然保留。"""
    response = {"data": {"module": {"items": [{
        "priceVO": {"totalPrice": "10.00"},
    }]}}}
    assert _pick_order(response, "999") is not None
