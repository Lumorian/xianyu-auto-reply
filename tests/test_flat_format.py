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


def flat(content, *, owner=MY, ext=BUYER, item_id=ITEM_ID, create_time=1777287861000,
         receivers=None):
    return {
        "chatType": 1,
        "incrementType": 1,
        "sessionId": SESSION,
        "operation": {
            "content": content,
            "receiverIds": [MY] if receivers is None else receivers,
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


def paid_card(**kwargs):
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
    return flat({"contentType": 26, "custom": {"type": 26, "data": data}}, **kwargs)


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


def test_flat_message_carries_item_title():
    """商品标题跟着消息一起带进来：聊天列表要显示商品名，不是一串商品 ID。"""
    msg = parse_message(flat({"contentType": 1, "text": {"text": "在吗"}}), my_id=MY)
    assert msg is not None
    assert msg.item_id == ITEM_ID
    assert msg.item_title == "dsv4.1f1017 0.0002/次"


def test_flat_item_title_is_normalized_and_optional():
    """标题里的换行 / 多空格要压成单空格；没有标题时是空串，不能是 None。"""
    payload = flat({"contentType": 1, "text": {"text": "在吗"}})
    payload["operation"]["sessionInfo"]["extensions"]["itemTitle"] = "  GPT  会员\n代充  "
    msg = parse_message(payload, my_id=MY)
    assert msg is not None
    assert msg.item_title == "GPT 会员 代充"

    payload["operation"]["sessionInfo"]["extensions"].pop("itemTitle")
    msg = parse_message(payload, my_id=MY)
    assert msg is not None
    assert msg.item_title == ""
    assert msg.item_id == ITEM_ID


def test_flat_plain_text():
    payload = flat({"contentType": 1, "text": {"text": "在吗？这个能用多久"}})
    msg = parse_message(payload, my_id=MY)
    assert msg is not None
    assert msg.content == "在吗？这个能用多久"
    assert msg.item_id == ITEM_ID


def test_own_message_is_recorded():
    """自己发的消息（含多端同步）要落库，但不能当成买家消息触发自动回复。"""
    payload = flat({"contentType": 1, "text": {"text": "你好"}}, owner=MY, ext=MY)
    msg = parse_message(payload, my_id=MY)
    assert msg is not None
    assert msg.from_me is True
    assert msg.content == "你好"


def test_own_plain_text_with_paid_keyword_is_not_buyer():
    payload = flat({"contentType": 1, "text": {"text": "[我已付款，等待你发货]"}},
                   owner=MY, ext=MY)
    msg = parse_message(payload, my_id=MY)
    assert msg is not None
    assert msg.from_me is True
    assert msg.sender_id == MY


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


def test_own_message_from_another_client_uses_receiver_ids():
    """receiverIds 不含我：这是我从闲鱼 App 发出的同步消息，不是买家消息。"""
    payload = flat(
        {"contentType": 1, "text": {"text": "你好"}},
        owner=MY,
        ext=BUYER,
        receivers=[BUYER],
    )
    msg = parse_message(payload, my_id=MY)
    assert msg is not None
    assert msg.from_me is True
    assert msg.sender_id == MY
    assert msg.peer_id == BUYER


def test_own_paid_card_from_another_client_is_not_buyer():
    """自己在别的客户端发的付款卡片，也不能触发自动发货。"""
    msg = parse_message(
        paid_card(owner=MY, ext=BUYER, receivers=[BUYER]),
        my_id=MY,
    )
    assert msg is not None
    assert msg.from_me is True
    assert msg.sender_id == MY
    assert msg.peer_id == BUYER


def test_incoming_message_with_self_in_receiver_ids_stays_incoming():
    msg = parse_message(
        flat({"contentType": 1, "text": {"text": "在吗"}}, receivers=[MY]),
        my_id=MY,
    )
    assert msg is not None
    assert msg.from_me is False
    assert msg.sender_id == BUYER


def test_flat_chat_id_strips_domain_suffix():
    payload = flat({"contentType": 1, "text": {"text": "x"}})
    payload["sessionId"] = SESSION + "@goofish"
    payload["operation"]["sessionInfo"]["sessionId"] = SESSION + "@goofish"
    msg = parse_message(payload, my_id=MY)
    assert msg is not None
    assert msg.chat_id == SESSION


def test_string_content_type_8_is_still_ignored():
    payload = flat({"contentType": "8", "sessionArouse": {}})
    assert parse_message(payload, my_id=MY) is None
