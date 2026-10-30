"""嵌套数字键格式（闲鱼真实聊天消息）的解析测试。

结构依据：
1. 线上抓包（flat 格式）；
2. XianYuPilo 的 ws_protocol.py 对 `"6"` → `"3"` (`"2"` 正文 / `"4"` contentType / `"5"` 卡片 JSON) 的解析；
3. cv-cat/XianYuApis 的 goofish_live.py 对 `"1"` → `"10"` 提醒字段的解析。
"""

from __future__ import annotations

import base64
import json

import pytest

from app.codec import (
    DecodeError,
    _find_msg_info,
    decode_payloads,
    describe_payload,
    frame_shape,
    is_paid_trigger,
    parse_message,
    sync_payloads,
    sync_payloads_detailed,
    unread_sync_chat_id,
)

MY = "4100000003"
BUYER = "4100000001"
ITEM_ID = "2000000000001"
ORDER_ID = "2500000000000000001"
SESSION = "61051610079"


def paid_card_json():
    return json.dumps({
        "contentType": 26,
        "dxCard": {"item": {"main": {
            "exContent": {"title": "我已付款，等待你发货"},
            "targetUrl": "fleamarket://order_detail?id=" + ORDER_ID + "&role=seller",
        }}},
    }, ensure_ascii=False)


def nested(*, content_type=1, text="", card=None, sender=BUYER, nick="小王",
           reminder_content=None, chat=SESSION, ts=1777287861000):
    inner = {"1": "", "2": text, "4": content_type, "5": card or ""}
    return {
        "1": {
            "1": {"1": sender + "@goofish"},
            "2": chat + "@goofish",
            "3": "abc.PNM",
            "5": ts,
            "6": {"1": 101, "3": inner},
            "10": {
                "reminderContent": reminder_content if reminder_content is not None else text,
                "senderUserId": sender,
                "senderNick": nick,
                "reminderUrl": "fleamarket://message_chat?itemId=" + ITEM_ID,
            },
        },
        "3": {"needPush": "true"},
    }


def test_plain_text_message():
    msg = parse_message(nested(text="在吗？这个能用多久"), my_id=MY)
    assert msg is not None
    assert msg.chat_id == SESSION
    assert msg.sender_id == BUYER
    assert msg.sender_name == "小王"
    assert msg.content == "在吗？这个能用多久"
    assert msg.item_id == ITEM_ID


def test_paid_card_message():
    msg = parse_message(
        nested(content_type=26, text="[我已付款，等待你发货]",
               card=paid_card_json(), reminder_content="[我已付款，等待你发货]"),
        my_id=MY,
    )
    assert msg is not None
    assert msg.order_id == ORDER_ID
    assert msg.item_id == ITEM_ID
    assert is_paid_trigger(msg.content)
    # 正文不该重复三遍，也不该混进 targetUrl
    assert msg.content.count("我已付款") == 1
    assert "fleamarket://" not in msg.content


def test_card_url_is_not_treated_as_text():
    """卡片 targetUrl 是跳转地址，不能当正文 —— 否则关键词匹配失效。"""
    card = json.dumps({"dxCard": {"item": {"main": {
        "targetUrl": "fleamarket://order_detail?id=" + ORDER_ID + "&role=seller"}}}})
    msg = parse_message(nested(content_type=26, text="", card=card,
                               reminder_content="[我已付款，等待你发货]"), my_id=MY)
    assert msg is not None
    assert "fleamarket://" not in msg.content
    assert is_paid_trigger(msg.content)
    assert msg.order_id == ORDER_ID


def test_session_arouse_is_skipped():
    msg = parse_message(nested(content_type=8, text="现在买可以做吗？"), my_id=MY)
    assert msg is None


def test_own_message_is_recorded():
    """多端同步回来的自己消息要能落库，但不能当买家消息处理。"""
    msg = parse_message(nested(text="你好", sender=MY), my_id=MY)
    assert msg is not None
    assert msg.from_me is True
    assert msg.content == "你好"


def test_own_plain_text_with_paid_keyword_is_not_buyer():
    """自己在聊天框里打「已付款」不能伪装成买家付款卡片。"""
    msg = parse_message(nested(text="[我已付款，等待你发货]", sender=MY), my_id=MY)
    assert msg is not None
    assert msg.from_me is True
    assert msg.sender_id == MY


def test_read_receipt_is_skipped():
    assert parse_message({"2": 2, "1": {"2": SESSION + "@goofish"}}, my_id=MY) is None


def test_control_packet_is_skipped():
    assert parse_message({"1": [{"1": 1, "2": 2, "3": 3}]}, my_id=MY) is None


def test_chat_id_with_chinese_is_rejected():
    """会话 ID 位置出现中文说明字段错位，宁可丢掉也不能错发。"""
    payload = nested(text="hi")
    payload["1"]["2"] = "你好啊"
    assert parse_message(payload, my_id=MY) is None


def test_find_msg_info_recognizes_pnmid_shape():
    """只有 "1" → "3"(pnmId) 的结构也要能认出来（XianYuPilo 的判据）。"""
    assert _find_msg_info({"1": {"2": SESSION + "@goofish", "3": "x.PNM"}})


def test_legacy_reminder_only_format():
    """最老的一代：只有 "1" → "10" → reminderContent。"""
    payload = {"1": {
        "2": SESSION + "@goofish",
        "10": {"reminderContent": "[我已付款，等待你发货]", "senderUserId": BUYER,
                "reminderUrl": "fleamarket://message_chat?itemId=" + ITEM_ID},
    }}
    msg = parse_message(payload, my_id=MY)
    assert msg is not None
    assert msg.content == "[我已付款，等待你发货]"
    assert msg.item_id == ITEM_ID


# ---------------------------------------------------------------- 解包

def test_decode_payloads_handles_concatenated_objects():
    """base64 里拼了多个 msgpack 对象时，必须全部取出来，否则会漏消息。"""
    import msgpack

    first = {"1": {"2": "a@goofish", "5": 1}}
    second = {"1": {"2": "b@goofish", "5": 2}}
    raw = msgpack.packb(first) + msgpack.packb(second)
    encoded = base64.b64encode(raw).decode()

    objects = decode_payloads(encoded)
    assert len(objects) == 2
    assert objects[0]["1"]["2"] == "a@goofish"
    assert objects[1]["1"]["2"] == "b@goofish"


def test_decode_payloads_handles_base64_json():
    payload = {"chatType": 1, "sessionId": SESSION}
    encoded = base64.b64encode(json.dumps(payload).encode()).decode()
    assert decode_payloads(encoded) == [payload]


def test_decode_payloads_rejects_garbage():
    with pytest.raises(DecodeError):
        decode_payloads(base64.b64encode(b"\x00not-a-map").decode())


def test_sync_payloads_expands_multiple_messages():
    import msgpack

    nested_a = nested(text="第一条")
    nested_b = nested(text="第二条")
    raw = msgpack.packb(nested_a) + msgpack.packb(nested_b)
    frame = {"body": {"syncPushPackage": {"data": [
        {"data": base64.b64encode(raw).decode()},
    ]}}}
    payloads = sync_payloads(frame)
    assert len(payloads) == 2


def test_unknown_shape_includes_content_type():
    """形状指纹要能区分「控制包」和「有正文但没解析出来的消息」。"""
    control = {"1": [{"1": 1, "2": 2}]}
    message = nested(text="hi")
    assert describe_payload(control) == ("1",)
    assert describe_payload(message)[0] == "nested"


# ---------------------------------------------------------------- 未读通知

def unread_packet(chat=SESSION, unread=1):
    """线上抓到的未读同步包：只有会话 ID 和未读标记，没有正文。"""
    return {"1": [{"1": chat + "@goofish", "2": 1, "3": unread, "4": MY + "@goofish"}]}


def test_unread_packet_is_recognized_but_yields_no_message():
    packet = unread_packet()
    assert unread_sync_chat_id(packet) == SESSION
    assert describe_payload(packet) == ("unread",)
    # 它不是买家消息，但也不能当成「认不出的负载」——否则日志会误导排查
    assert parse_message(packet, my_id=MY) is None


def test_control_packet_is_not_mistaken_for_unread():
    """成员状态包的第一项是纯数字，别当成会话 ID。"""
    assert unread_sync_chat_id({"1": [{"1": 1, "2": 2, "3": 3}]}) is None


def test_unread_packet_needs_allowed_keys():
    assert unread_sync_chat_id({"1": [{"1": SESSION + "@goofish", "9": 1}]}) is None
    assert unread_sync_chat_id({"1": []}) is None


# ---------------------------------------------------------------- 帧级诊断

def test_sync_payloads_detailed_reports_undecodable_entries():
    """解不开的 data 必须被报告出来，不能静默跳过。"""
    frame = {"body": {"syncPushPackage": {"data": [
        {"data": base64.b64encode(b"\x00not-a-map").decode()},
    ]}}}
    payloads, failures = sync_payloads_detailed(frame)
    assert payloads == []
    assert len(failures) == 1
    index, reason = failures[0]
    assert index == 0 and reason


def test_sync_payloads_detailed_keeps_good_entries():
    import msgpack

    good = base64.b64encode(msgpack.packb(nested(text="在吗"), use_bin_type=True)).decode()
    frame = {"body": {"syncPushPackage": {"data": [
        {"data": "!!!"},
        {"data": good},
    ]}}}
    payloads, failures = sync_payloads_detailed(frame)
    assert len(payloads) == 1
    assert len(failures) == 1


def test_body_candidates_finds_message_outside_sync_push_package():
    """正文走别的容器时也不能整条丢掉。"""
    frame = {"lwp": "/s/other", "body": {"payload": nested(text="在吗")}}
    payloads, _ = sync_payloads_detailed(frame)
    assert len(payloads) == 1
    assert parse_message(payloads[0], my_id=MY).content == "在吗"


def test_frame_shape_is_loggable():
    assert "lwp=/s/vulcan" in frame_shape({"lwp": "/s/vulcan", "body": {"a": 1}})
    assert "body=[2 项]" in frame_shape({"lwp": "/s/x", "body": [{}, {}]})
