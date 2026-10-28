"""XianyuClient 的媒体发送 / 撤回 / 分片上传协议。"""

from __future__ import annotations

import base64
import json
from typing import Any

import pytest

from app.client import XianyuClient, XianyuError


def make_client() -> XianyuClient:
    return XianyuClient("unb=2000000000002; _m_h5_tk=token_1")


def custom_payload(frame: dict[str, Any]) -> dict[str, Any]:
    data = frame["body"][0]["content"]["custom"]["data"]
    return json.loads(base64.b64decode(data))


async def test_send_text_with_id_returns_message_id_and_captures_frame():
    client = make_client()
    seen: list[dict[str, Any]] = []

    async def fake(frame, timeout=15.0):
        seen.append(frame)
        return {"code": 200, "body": {"messageId": "m-text"}}

    client._request = fake  # type: ignore[method-assign]
    assert await client.send_text_with_id("c1", "b1", "hello") == "m-text"
    frame = seen[0]
    assert frame["lwp"] == "/r/MessageSend/sendByReceiverScope"
    assert custom_payload(frame) == {"contentType": 1, "text": {"text": "hello"}}
    assert frame["body"][1]["actualReceivers"] == ["b1@goofish", "2000000000002@goofish"]


async def test_send_delivery_text_classifies_ack_outcomes():
    """发货需要区分 sent / failed / unknown，不能把超时当失败。"""
    client = make_client()

    async def ok(frame, timeout=15.0):
        return {"code": 200, "body": {"messageId": "m-1"}}

    client._request = ok  # type: ignore[method-assign]
    assert await client.send_delivery_text("c1", "b1", "hi") == ("m-1", "sent")

    async def rejected(frame, timeout=15.0):
        return {"code": 200, "body": {"reason": "内容存在不当信息"}}

    client._request = rejected  # type: ignore[method-assign]
    assert await client.send_delivery_text("c1", "b1", "hi") == ("", "failed")

    async def timeout(frame, timeout=15.0):
        raise XianyuError("等待服务端回包超时")

    client._request = timeout  # type: ignore[method-assign]
    assert await client.send_delivery_text("c1", "b1", "hi") == ("", "unknown")


async def test_string_ret_is_handled_for_confirm_and_order_list():
    """mtop 的 ret 可能是字符串；逐字符 join 会把 SUCCESS 判成失败。"""
    client = make_client()

    async def fake(api, data, version="1.0"):
        if api == "mtop.taobao.idle.logistic.consign.dummy":
            return {"ret": "SUCCESS::调用成功"}
        return {
            "ret": "SUCCESS::调用成功",
            "data": {"module": {"items": [{
                "commonData": {"orderId": "1", "itemId": "i", "orderStatus": "待发货"},
                "buyerInfoVO": {"buyerId": "b", "userNick": "买家"},
                "priceVO": {"totalPrice": "1.00"},
            }]}},
        }

    client.mtop = fake  # type: ignore[method-assign]
    assert await client.confirm_shipping("1") == (True, "SUCCESS::调用成功")
    pending = await client.list_pending_orders()
    assert len(pending) == 1
    assert pending[0]["order_id"] == "1"


async def test_send_text_with_id_rejects_server_reason():
    client = make_client()

    async def fake(frame, timeout=15.0):
        return {"code": 200, "body": {"reason": "内容存在不当信息"}}

    client._request = fake  # type: ignore[method-assign]
    with pytest.raises(XianyuError, match="不当信息"):
        await client.send_text_with_id("c1", "b1", "hello")


async def test_send_image_and_video_payloads():
    client = make_client()
    seen: list[dict[str, Any]] = []

    async def fake(frame, timeout=15.0):
        seen.append(frame)
        return {"code": 200, "body": {"messageId": "m-1"}}

    client._request = fake  # type: ignore[method-assign]
    assert await client.send_image_with_id(
        "c1", "b1", "https://img.alicdn.com/a.jpg", 10, 20
    ) == "m-1"
    image = custom_payload(seen[0])
    assert image["contentType"] == 2
    assert image["image"]["pics"][0] == {
        "height": 20, "type": 0, "url": "https://img.alicdn.com/a.jpg", "width": 10,
    }

    assert await client.send_video_with_id(
        "c1", "b1", video_media_id="v1", file_name="a.mp4", file_size=1234,
        duration_ms=1500, width=640, height=360, pic_media_id="p1",
    ) == "m-1"
    video = custom_payload(seen[1])
    assert video["contentType"] == 4
    assert video["video"] == {
        "videoMediaId": "v1", "fileName": "a.mp4", "fileType": 6, "fileSize": 1234,
        "duration": 1500, "width": 640, "height": 360, "picMediaId": "p1",
    }


async def test_upload_media_lwp_sends_pre_frag_and_ci():
    client = make_client()
    seen: list[dict[str, Any]] = []

    async def fake(frame, timeout=15.0):
        seen.append(frame)
        lwp = frame["lwp"]
        if lwp == "/r/FileUpload/pre":
            return {"code": 200, "body": {"uploadInfo": "u1", "mediaId": "m1", "fragLen": 32768}}
        if lwp == "/r/FileUpload/frag":
            return {"code": 200, "body": {}}
        if lwp == "/r/FileUpload/ci":
            return {"code": 200, "body": {"authMediaId": "auth-1"}}
        return {"code": 200, "body": {}}

    client._request = fake  # type: ignore[method-assign]
    data = b"x" * (32768 * 2 + 10)
    assert await client.upload_media_lwp(
        "c1", data, file_name="a.mp4", file_type="mp4", width=64, height=48
    ) == "auth-1"

    lwps = [frame["lwp"] for frame in seen]
    assert lwps == [
        "/r/FileUpload/pre", "/r/FileUpload/frag", "/r/FileUpload/frag", "/r/FileUpload/ci",
    ]
    pre = seen[0]["body"][0]
    assert pre["conversationId"] == "c1@goofish"
    assert pre["fileLen"] == len(data)
    assert pre["width"] == 64
    ci = seen[3]["body"][0]
    assert ci["totalPartNumber"] == 3
    assert ci["partNumber"] == 2
    assert base64.b64decode(ci["body"])


async def test_upload_media_lwp_requires_auth_media_id():
    client = make_client()

    async def fake(frame, timeout=15.0):
        if frame["lwp"] == "/r/FileUpload/pre":
            return {"code": 200, "body": {"uploadInfo": "u1", "mediaId": "m1", "fragLen": 999999}}
        return {"code": 200, "body": {}}

    client._request = fake  # type: ignore[method-assign]
    with pytest.raises(XianyuError, match="authMediaId"):
        await client.upload_media_lwp(
            "c1", b"small", file_name="a.mp4", file_type="mp4", width=1, height=1
        )


async def test_recall_message_frame_and_error():
    client = make_client()
    seen: list[dict[str, Any]] = []

    async def ok(frame, timeout=15.0):
        seen.append(frame)
        return {"code": 200, "body": {}}

    client._request = ok  # type: ignore[method-assign]
    await client.recall_message("m-1")
    assert seen[0]["lwp"] == "/r/MessageManager/recallMessage"
    assert seen[0]["body"] == ["m-1"]

    async def fail(frame, timeout=15.0):
        return {"code": 400, "body": {"reason": "超过两分钟"}}

    client._request = fail  # type: ignore[method-assign]
    with pytest.raises(XianyuError, match="超过两分钟"):
        await client.recall_message("m-2")
