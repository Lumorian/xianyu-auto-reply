"""令牌名规则：买家用户名 + 订单号。

历史坑：new-api 用 Go 的 len() 校验，单位是**字节**不是字符。
「闲鱼-闲鱼自动发货 1.00元 #2500000000000000006」= 54 字节 > 50，
报错「令牌名称过长」，导致整单 upstream_failed。

现在名字里不再放价格 —— 令牌名和卡密名称都是「哪个买家、哪一单」。
"""

from __future__ import annotations

import pytest

from app.upstream import (
    CARD_LABEL_MAX_CHARS,
    TOKEN_NAME_MAX_BYTES,
    build_card_label,
    build_token_name,
    fit_bytes,
    order_id_from_label,
)

ORDER = "2500000000000000006"
USERNAME = "测试小鱼"


def size(text: str) -> int:
    return len(text.encode("utf-8"))


def test_fit_bytes_never_breaks_multibyte():
    text = "闲鱼自动发货" * 10
    out = fit_bytes(text, 20)
    assert size(out) <= 20
    # 不产生半个汉字：能正常 decode
    out.encode("utf-8").decode("utf-8")
    assert len(out) < len(text)


def test_fit_bytes_leaves_short_text_alone():
    assert fit_bytes("abc", 50) == "abc"


def test_token_name_is_username_plus_order():
    name = build_token_name(USERNAME, ORDER)
    assert name == USERNAME + "#" + ORDER


def test_token_name_has_no_price_or_platform_prefix():
    name = build_token_name(USERNAME, ORDER)
    assert "元" not in name
    assert "闲鱼" not in name
    assert "1.00" not in name


def test_token_name_without_order_id():
    assert build_token_name(USERNAME, "") == USERNAME
    assert build_token_name(USERNAME, "   ") == USERNAME


def test_token_name_falls_back_when_username_missing():
    assert build_token_name("", ORDER) == "买家#" + ORDER
    assert build_token_name("   ", ORDER) == "买家#" + ORDER


def test_token_name_sanitizes_separator_and_whitespace():
    name = build_token_name("  小\n王 #1  ", " 123 ")
    assert name == "小 王 ＃1#123"


@pytest.mark.parametrize("username", [
    USERNAME,
    "示例买家昵称八",
    "一个特别特别特别特别特别特别长的买家用户名",
    "buyer_name_with_english",
])
@pytest.mark.parametrize("prefix", ["", "闲鱼-", "一个很长很长很长很长的前缀-"])
def test_token_name_always_fits(username, prefix):
    name = build_token_name(username, ORDER, prefix=prefix)
    assert size(name) <= TOKEN_NAME_MAX_BYTES


def test_token_name_keeps_full_order_id_when_username_is_long():
    username = "一个特别特别特别特别特别特别长的买家用户名"
    name = build_token_name(username, ORDER)
    assert ORDER in name
    assert size(name) <= TOKEN_NAME_MAX_BYTES
    assert username not in name  # 用户名被截短了


def test_token_name_keeps_full_order_id_when_possible():
    name = build_token_name(USERNAME, ORDER)
    assert ORDER in name
    assert size(name) == len(USERNAME.encode()) + 1 + len(ORDER)


def test_token_name_optional_prefix():
    name = build_token_name(USERNAME, ORDER, prefix="闲鱼-")
    assert name == "闲鱼-" + USERNAME + "#" + ORDER


def test_token_name_shrinks_absurdly_long_order_id():
    order = "9" * 80
    name = build_token_name(USERNAME, order)
    assert size(name) <= TOKEN_NAME_MAX_BYTES
    assert order[-10:] in name


def test_order_id_from_label():
    assert order_id_from_label(USERNAME + "#" + ORDER) == ORDER
    assert order_id_from_label("闲鱼自动发货 1.00元 #" + ORDER) == ORDER  # 老数据兼容
    assert order_id_from_label("闲鱼自动发货 10.00元") == ""
    assert order_id_from_label("") == ""


# ---------------------------------------------------------------- 卡密名称
def test_card_label_is_username_plus_order():
    assert build_card_label(USERNAME, ORDER) == USERNAME + "#" + ORDER


def test_card_label_has_no_price_or_prefix():
    label = build_card_label(USERNAME, ORDER)
    assert "元" not in label
    assert "闲鱼" not in label
    assert "1.00" not in label


def test_card_label_without_order_id():
    assert build_card_label(USERNAME, "") == USERNAME
    assert build_card_label(USERNAME, "   ") == USERNAME


def test_card_label_falls_back_when_username_missing():
    assert build_card_label("", ORDER) == "买家#" + ORDER
    assert build_card_label("   ", ORDER) == "买家#" + ORDER


def test_card_label_sanitizes_separator_and_whitespace():
    assert build_card_label("  小\n王 #1  ", " 123 ") == "小 王 ＃1#123"


def test_card_label_keeps_full_order_id_when_username_is_long():
    # 卡密名称上限 100 字符，用户名要真的超长才会被截
    username = "一个特别特别特别特别特别特别长特别特别特别特别特别特别长特别特别特别特别特别特别长" * 2
    label = build_card_label(username, ORDER)
    assert ORDER in label
    assert len(label) <= CARD_LABEL_MAX_CHARS
    assert username not in label


def test_card_label_shrinks_absurdly_long_order_id():
    order = "9" * 200
    label = build_card_label(USERNAME, order)
    assert len(label) <= CARD_LABEL_MAX_CHARS
    assert order[-10:] in label


def test_old_double_prefix_would_have_failed():
    """记录事故现场：这个 54 字节的名字就是当初报错的那个。"""
    bad = "闲鱼-闲鱼自动发货 1.00元 #" + ORDER
    assert size(bad) == 54
    assert size(bad) > TOKEN_NAME_MAX_BYTES
