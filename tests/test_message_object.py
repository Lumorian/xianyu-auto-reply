"""message 对象格式（SingleChatConversation/create 回包实测结构）。

这一份样本是从真实接口回包拿到的，不是臆造：
调用 `/r/SingleChatConversation/create` 返回的 `lastMessage.message` 就是一条真实消息。

关键特征：
- `cid` 直接给会话 ID
- `content.contentType` 恒为 101（表示「自定义消息」），**真实类型在 `content.custom.type`**
- 正文在 `content.custom.data`（base64 JSON）
- 元信息在 `extension`（reminderContent / senderUserId / reminderUrl）
- 发送方在 `sender.uid`
"""

from __future__ import annotations

import base64
import json

from app.codec import describe_payload, is_paid_trigger, parse_message

MY = "4100000003"
BUYER = "2000000000003"
ITEM_ID = "2000000000001"
PAID_ORDER = "2500000000000000006"
UNPAID_ORDER = "2500000000000000005"
CID = "68100583197"


def card_payload(title, order_id):
    main = {"exContent": {"title": title}}
    if order_id:
        main["targetUrl"] = "fleamarket://order_detail?id=" + order_id + "&role=seller"
    return base64.b64encode(json.dumps(
        {"contentType": 26, "dxCard": {"item": {"main": main}}}, ensure_ascii=False
    ).encode()).decode()


def message_object(*, title, order_id, sender_uid, content_type=26, text=None, cid=CID):
    if text is not None:
        data = base64.b64encode(json.dumps(
            {"contentType": 1, "text": {"text": text}}, ensure_ascii=False
        ).encode()).decode()
    else:
        data = card_payload(title, order_id)
    return {
        "messageId": "1.PNM",
        "createAt": 1792740873447,
        "cid": cid + "@goofish",
        "sender": {"uid": sender_uid + "@goofish"},
        "content": {
            "contentType": 101,  # 「自定义消息」，不是业务类型
            "custom": {"type": content_type, "data": data, "summary": title},
        },
        "extension": {
            "reminderContent": title,
            "senderUserId": sender_uid,
            "reminderTitle": "交易消息",
            "reminderUrl": "fleamarket://message_chat?itemId=" + ITEM_ID,
        },
    }


def test_paid_card_from_buyer_is_delivered():
    payload = message_object(
        title="[我已付款，等待你发货]", order_id=PAID_ORDER, sender_uid=BUYER,
    )
    msg = parse_message(payload, my_id=MY)
    assert msg is not None
    assert msg.chat_id == CID
    assert msg.sender_id == BUYER
    assert msg.order_id == PAID_ORDER
    assert msg.item_id == ITEM_ID
    assert is_paid_trigger(msg.content)


def test_paid_card_kept_even_if_sender_is_seller():
    """实测交易卡片可能把 senderUserId 记成卖家自己，不能因此丢掉订单。"""
    payload = message_object(
        title="[我已付款，等待你发货]", order_id=PAID_ORDER, sender_uid=MY,
    )
    msg = parse_message(payload, my_id=MY)
    assert msg is not None
    assert msg.order_id == PAID_ORDER


def test_unpaid_card_is_not_a_paid_trigger():
    """拍下未付款卡片 sender 是卖家自己 → 当自己发的丢掉。
    即使保留，也绝不能触发发货。"""
    payload = message_object(
        title="[你当前宝贝拍下未付款]", order_id=UNPAID_ORDER, sender_uid=MY,
    )
    msg = parse_message(payload, my_id=MY)
    if msg is not None:
        assert not is_paid_trigger(msg.content)


def test_unpaid_card_from_buyer_not_trigger():
    """买家视角的「拍下未付款」：入库但不发货。"""
    payload = message_object(
        title="[我已拍下，待付款]", order_id=UNPAID_ORDER, sender_uid=BUYER,
    )
    msg = parse_message(payload, my_id=MY)
    assert msg is not None
    assert not is_paid_trigger(msg.content)


def test_own_plain_message_is_dropped():
    payload = message_object(
        title="你好", order_id=None, sender_uid=MY, content_type=1, text="你好",
    )
    assert parse_message(payload, my_id=MY) is None


def test_text_message_normalizes_sender():
    payload = message_object(
        title="在吗？", order_id=None, sender_uid=BUYER, content_type=1, text="在吗？",
    )
    msg = parse_message(payload, my_id=MY)
    assert msg is not None
    assert msg.sender_id == BUYER
    assert "@goofish" not in msg.sender_id
    assert msg.content == "在吗？"


def test_guide_message_is_skipped():
    payload = message_object(
        title="现在买可以做吗？", order_id=None, sender_uid=BUYER, content_type=8,
    )
    assert parse_message(payload, my_id=MY) is None


def test_shape_is_recognizable():
    payload = message_object(
        title="[我已付款，等待你发货]", order_id=PAID_ORDER, sender_uid=BUYER,
    )
    shape = describe_payload(payload)
    assert shape[0] == "message" or "cid" in shape
