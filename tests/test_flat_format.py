"""flat 消息格式（闲鱼当前实际推送格式）的解析测试。

这些用例的结构全部来自线上真实抓包，不是臆造的：
一个真实账号连上去，收到的负载就是 chatType + operation 这一套。
"""

from __future__ import annotations

import base64
import json

from app.codec import (
    collect_texts,
    decode_custom,
    is_ignored,
    is_paid_trigger,
    note_unparsed,
    parse_message,
)

MY = "4100000003"      # 卖家（我）
BUYER = "4100000001"
ITEM_ID = "2000000000001"
ORDER_ID = "2500000000000000001"
SESSION = "61051610079"


def flat(content, *, owner=MY, ext=BUYER, item_id=ITEM_ID, create_time=1777287861000):
    return {
        "chatType": 1,
        "incrementType": 1,
        "sessionId": SESSION,
        "operation": {
            "content": content,
            "receiverIds": [MY],
            "sessionInfo": {
                "sessionId": SESSION,
                "ownerUserId": owner,
                "createTime": create_time,
                "extensions": {
                    "itemId": item_id,
                    "itemTitle": "dsv4.1f1017 0.0002/次",
                    "extUserId": ext,
                    "itemSellerId": owner,
                },
            },
        },
    }


def paid_card():
    card = {
        "contentType": 26,
        "dxCard": {
            "item": {
                "main": {
                    "exContent": {"title": "我已付款，等待你发货"},
                    "targetUrl": "fleamarket://order_detail?id=" + ORDER_ID + "&role=seller",
                }
            }
        },
    }
    data = base64.b64encode(json.dumps(card).encode()).decode()
    return flat({"contentType": 26, "custom": {"type": 26, "data": data}})


def test_session_arouse_is_not_a_message():
    """contentType=8 是「会话引导」（现在买可以做吗？），不能当消息处理。"""
    payload = flat({"contentType": 8, "sessionArouse": {"sessionArouseInfo": {}}})
    assert parse_message(payload, my_id=MY) is None


def test_flat_paid_card_is_parsed():
    msg = parse_message(paid_card(), my_id=MY)
    assert msg is not None
    assert msg.content == "我已付款，等待你发货"
    assert msg.order_id == ORDER_ID
    assert msg.item_id == ITEM_ID
    assert msg.chat_id == SESSION
    assert msg.sender_id == BUYER
    assert is_paid_trigger(msg.content)


def test_flat_plain_text():
    payload = flat({"contentType": 1, "text": {"text": "在吗？这个能用多久"}})
    msg = parse_message(payload, my_id=MY)
    assert msg is not None
    assert msg.content == "在吗？这个能用多久"
    assert msg.item_id == ITEM_ID


def test_own_message_is_dropped():
    """自己发的消息（含多端同步）不能被当成买家消息。"""
    payload = flat({"contentType": 1, "text": {"text": "你好"}}, owner=MY, ext=MY)
    assert parse_message(payload, my_id=MY) is None


def test_buyer_side_view_uses_owner_as_sender():
    """如果会话 owner 不是自己（买家视角），发送方就是 owner。"""
    payload = flat({"contentType": 1, "text": {"text": "你好"}}, owner="999", ext=MY)
    msg = parse_message(payload, my_id=MY)
    assert msg is not None
    assert msg.sender_id == "999"


def test_flat_without_session_id_is_ignored():
    payload = flat({"contentType": 1, "text": {"text": "x"}})
    payload["sessionId"] = ""
    payload["operation"]["sessionInfo"]["sessionId"] = ""
    assert parse_message(payload, my_id=MY) is None


def test_decode_custom_handles_garbage():
    assert decode_custom({"custom": {"data": "!!!"}}) == {}
    assert decode_custom({}) == {}
    assert decode_custom({"custom": {"type": 1}}) == {}


def test_decode_custom_real_shape():
    inner = {"contentType": 26, "dxCard": {"item": {}}}
    data = base64.b64encode(json.dumps(inner).encode()).decode()
    assert decode_custom({"custom": {"type": 26, "data": data}}) == inner


def test_collect_texts_prefers_body_over_noise():
    texts = collect_texts(
        {"text": {"text": "正文在这里"}},
        {"contentType": 1, "dxCard": {"item": {"main": {"exContent": {"title": "卡片标题"}}}}},
    )
    assert texts[0] == "正文在这里"
    assert "卡片标题" in texts


def test_paid_keywords_not_swallowed_by_ignore_list():
    """已付款关键词不能被「只记录不处理」的规则吞掉。"""
    assert is_paid_trigger("我已付款，等待你发货")
    assert not is_ignored("我已付款，等待你发货")


def test_unknown_shape_is_logged_once():
    """不认识的形状要留痕，而不是静默消失。"""
    from app.codec import _UNKNOWN_SHAPES

    _UNKNOWN_SHAPES.clear()
    note_unparsed({"weird": 1})
    note_unparsed({"weird": 1})
    assert sum(_UNKNOWN_SHAPES.values()) == 2
    assert len(_UNKNOWN_SHAPES) == 1


def test_legacy_nested_format_still_works():
    card = {
        "contentType": 26,
        "dxCard": {"item": {"main": {
            "exContent": {"title": "我已付款，等待你发货"},
            "targetUrl": "fleamarket://order_detail?id=" + ORDER_ID + "&role=seller",
        }}},
    }
    legacy = {
        "1": {
            "2": SESSION + "@goofish",
            "5": 1742458817389,
            "10": {
                "reminderContent": "[我已付款，等待你发货]",
                "senderUserId": BUYER,
                "senderNick": "小王",
                "reminderUrl": "fleamarket://message_chat?itemId=" + ITEM_ID,
            },
            "6": {"3": {"5": json.dumps(card)}},
        }
    }
    msg = parse_message(legacy, my_id=MY)
    assert msg is not None
    assert msg.order_id == ORDER_ID
    assert msg.item_id == ITEM_ID
