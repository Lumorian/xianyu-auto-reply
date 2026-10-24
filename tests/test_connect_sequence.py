"""注册流程的顺序要求（线上验证结论）。

现象是「连接看着在线，但永远收不到买家私信正文」——日志里只有
/s/sync、/s/vulcan 这种框架帧和「未读通知」。两个原因都出在注册阶段：

1. 心跳必须在 /reg 之前就动起来。注册期间长时间没有 /! 时，服务端会把
   连接判成不活跃，之后只推框架帧、不再推消息正文。
2. /reg 与 ackDiff 之间要留一口气（线上验证是 1 秒）。紧凑连发会让注册
   在服务端不生效，症状和上面完全一样。
"""

from __future__ import annotations

import asyncio
import json

import pytest

from app.client import XianyuClient
from app.config import settings


class RecordingWS:
    """记录所有发出的帧，并按顺序打时间戳。"""

    def __init__(self) -> None:
        self.frames: list[dict] = []

    async def send(self, payload: str) -> None:
        self.frames.append(json.loads(payload))

    async def close(self) -> None:
        pass


def make_client() -> XianyuClient:
    client = XianyuClient("unb=1; _m_h5_tk=tok_1")
    client._device_id = "dev"
    return client


@pytest.fixture()
def fast_register(monkeypatch):
    """1 秒间隔在测试里没必要等，但必须存在（用 0.05 证明它被 await 了）。"""
    monkeypatch.setattr(settings, "register_gap", 0.05)
    monkeypatch.setattr(settings, "heartbeat_interval", 10.0)


async def test_heartbeat_starts_before_register(fast_register, monkeypatch):
    client = make_client()
    ws = RecordingWS()

    async def fake_connect(*args, **kwargs):
        return ws

    async def fake_token():
        return "im-token"

    monkeypatch.setattr("app.client.websockets.connect", fake_connect)
    monkeypatch.setattr(client, "get_im_token", fake_token)

    try:
        await client.connect()
        # 心跳要真的先跑起来：等到它发出第一帧
        for _ in range(50):
            await asyncio.sleep(0.01)
            if any(f.get("lwp") == "/!" for f in ws.frames):
                break
    finally:
        await client.stop_heartbeat()

    lwps = [f.get("lwp") for f in ws.frames]
    assert lwps[0] == "/!"  # 首跳在 /reg 之前
    assert "/reg" in lwps
    assert "/r/SyncStatus/ackDiff" in lwps
    assert lwps.index("/!") < lwps.index("/reg") < lwps.index("/r/SyncStatus/ackDiff")


async def test_register_gap_is_awaited(fast_register, monkeypatch):
    client = make_client()
    ws = RecordingWS()

    async def fake_connect(*args, **kwargs):
        return ws

    async def fake_token():
        return "im-token"

    monkeypatch.setattr("app.client.websockets.connect", fake_connect)
    monkeypatch.setattr(client, "get_im_token", fake_token)

    seen: list[float] = []
    real_send = ws.send

    async def timed_send(payload: str) -> None:
        await real_send(payload)
        seen.append(asyncio.get_running_loop().time())

    ws.send = timed_send  # type: ignore[method-assign]
    try:
        await client.connect()
    finally:
        await client.stop_heartbeat()

    lwps = [f.get("lwp") for f in ws.frames]
    reg_at = seen[lwps.index("/reg")]
    diff_at = seen[lwps.index("/r/SyncStatus/ackDiff")]
    assert diff_at - reg_at >= 0.04


async def test_connect_returns_working_websocket(fast_register, monkeypatch):
    client = make_client()
    ws = RecordingWS()

    async def fake_connect(*args, **kwargs):
        return ws

    async def fake_token():
        return "im-token"

    monkeypatch.setattr("app.client.websockets.connect", fake_connect)
    monkeypatch.setattr(client, "get_im_token", fake_token)
    try:
        returned = await client.connect()
        assert returned is ws
        assert client.websocket is ws
        reg = next(f for f in ws.frames if f.get("lwp") == "/reg")
        assert reg["headers"]["app-key"] == settings.im_app_key
        assert reg["headers"]["token"] == "im-token"
    finally:
        await client.stop_heartbeat()


async def test_stop_heartbeat_is_idempotent(fast_register, monkeypatch):
    client = make_client()
    ws = RecordingWS()

    async def fake_connect(*args, **kwargs):
        return ws

    async def fake_token():
        return "im-token"

    monkeypatch.setattr("app.client.websockets.connect", fake_connect)
    monkeypatch.setattr(client, "get_im_token", fake_token)
    await client.connect()
    await client.stop_heartbeat()
    await client.stop_heartbeat()
    assert client._heartbeat_task is None


async def test_heartbeat_sends_first_frame_immediately(monkeypatch):
    """心跳的首跳不能等一个间隔（注册期间的空窗就是问题本身）。"""
    monkeypatch.setattr(settings, "heartbeat_interval", 30.0)
    client = XianyuClient("unb=1; _m_h5_tk=tok_1")
    ws = RecordingWS()
    task = asyncio.create_task(client.heartbeat(ws))
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    assert ws.frames and ws.frames[0]["lwp"] == "/!"
