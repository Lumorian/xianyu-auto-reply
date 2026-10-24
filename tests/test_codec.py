"""消息解包与字段提取的单元测试。"""

from __future__ import annotations

import base64
import json

import msgpack
import pytest

from app.codec import (
    DecodeError,
    decode_payload,
    extract_item_id,
    extract_order_id,
    is_ignored,
    is_paid_trigger,
    parse_message,
    sync_payloads,
)


def make_card_payload(content="[我已付款，等待你发货]", item_id="900052644277", order_id="2500000000000000001"):
    """构造一条结构真实的付款卡片消息。"""
    card = {
        "contentType": 26,
        "dxCard": {
            "item": {
                "main": {
                    "exContent": {"title": "我已付款，等待你发货"},
                    "targetUrl": f"fleamarket://order_detail?id={order_id}&role=seller",
                }
            }
        },
    }
    return {
        "1": {
            "1": {"1": "4100000001@goofish"},
            "2": "47983389096@goofish",
            "5": 1742458817389,
            "6": {"1": 101, "3": {"2": content, "5": json.dumps(card)}},
            "10": {
                "reminderContent": content,
                "reminderTitle": "买家已付款",
                "senderUserId": "4100000001",
                "senderNick": "买家小王",
                "reminderUrl": f"fleamarket://message_chat?itemId={item_id}&peerUserId=4100000001",
            },
        },
        "3": {"needPush": "true"},
    }


def encode(payload):
    return base64.b64encode(msgpack.packb(payload, use_bin_type=True)).decode()


def test_decode_payload_msgpack():
    payload = make_card_payload()
    assert decode_payload(encode(payload)) == payload


def test_decode_payload_plain_json():
    payload = {"1": {"10": {"reminderContent": "hi"}}}
    encoded = base64.b64encode(json.dumps(payload).encode()).decode()
    assert decode_payload(encoded) == payload


def test_decode_payload_rejects_garbage():
    with pytest.raises(DecodeError):
        decode_payload(base64.b64encode(b"\x00not-a-map").decode())


def test_extract_ids():
    payload = make_card_payload()
    assert extract_order_id(payload) == "2500000000000000001"
    assert extract_item_id(payload) == "900052644277"


def test_parse_message():
    msg = parse_message(make_card_payload())
    assert msg is not None
    assert msg.chat_id == "47983389096"
    assert msg.sender_id == "4100000001"
    assert msg.sender_name == "买家小王"
    assert msg.content == "[我已付款，等待你发货]"
    assert msg.order_id == "2500000000000000001"


def test_parse_message_ignores_non_chat():
    assert parse_message({"1": {"2": "x@goofish"}}) is None


def test_trigger_keywords():
    assert is_paid_trigger("[我已付款，等待你发货]")
    assert is_paid_trigger("我已付款，等待你发货")
    assert not is_paid_trigger("[我已拍下，待付款]")
    assert is_ignored("[我已拍下，待付款]")


def test_sync_payloads_skips_bad_entries():
    frame = {
        "body": {
            "syncPushPackage": {
                "data": [
                    {"data": encode(make_card_payload())},
                    {"data": "!!!"},
                    {"no_data": True},
                ]
            }
        }
    }
    payloads = sync_payloads(frame)
    assert len(payloads) == 1
    assert payloads[0]["1"]["10"]["senderUserId"] == "4100000001"


def test_sync_payloads_without_package():
    assert sync_payloads({"headers": {}}) == []
