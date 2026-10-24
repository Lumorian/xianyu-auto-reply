"""订单金额解析与额度换算的单元测试。"""

from __future__ import annotations

import pytest

from app.engine import quota_from_amount
from app.money import AmountError, parse_amount, pick_amount


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("10.00", 10.0),
        ("¥10.50", 10.5),
        ("1,099.00", 1099.0),
        (10, 10.0),
        (10.5, 10.5),
        ("0.01", 0.01),
    ],
)
def test_parse_amount_yuan(raw, expected):
    amount = parse_amount(raw)
    assert amount is not None
    assert amount.cny == pytest.approx(expected)


def test_parse_amount_auto_treats_big_int_as_fen():
    # 1000 分 = 10 元；纯整数 1000 在闲鱼订单接口里就是「分」
    assert parse_amount(1000).cny == pytest.approx(10.0)
    assert parse_amount("1000").cny == pytest.approx(10.0)
    # 带小数点就不按分算
    assert parse_amount("1000.00").cny == pytest.approx(1000.0)
    # 小于 1000 的整数仍按元
    assert parse_amount(10).cny == pytest.approx(10.0)


def test_parse_amount_forced_units():
    assert parse_amount(1000, unit="yuan").cny == pytest.approx(1000.0)
    assert parse_amount("10.00", unit="fen").cny == pytest.approx(0.1)


def test_parse_amount_rejects_junk():
    assert parse_amount(None) is None
    assert parse_amount("") is None
    assert parse_amount("abc") is None
    assert parse_amount(-5) is None


@pytest.mark.parametrize("raw", ["-5", "-5.00", "¥-10.00", "-0.01", "  -3.5  "])
def test_parse_amount_rejects_negative_text(raw):
    """负号不能被当成噪声剥掉。

    以前 _CLEAN 会把 "-" 一起删掉，"-5" 于是变成 5 元，
    退款/优惠单会被当成正常正价去建 Key。
    """
    assert parse_amount(raw) is None


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1,099.00", 1099.0),
        ("¥1,099.00", 1099.0),
        ("12,345.67", 12345.67),
    ],
)
def test_parse_amount_strips_thousands_separator(raw, expected):
    """千分位逗号要删掉，不能当成「分」的分隔符。"""
    assert parse_amount(raw).cny == pytest.approx(expected)


def test_pick_amount_prefers_actual_pay():
    order = {
        "commonData": {"orderId": "1"},
        "priceVO": {"totalPrice": "99.00", "auctionPrice": "199.00", "confirmFee": "88.00"},
    }
    # totalPrice 优先级高于 auctionPrice
    assert pick_amount(order).cny == pytest.approx(99.0)


def test_pick_amount_reads_actual_pay_fee_first():
    order = {"priceVO": {"actualPayFee": "12.34", "totalPrice": "99.00"}}
    assert pick_amount(order).cny == pytest.approx(12.34)


def test_pick_amount_without_amount_field():
    with pytest.raises(AmountError):
        pick_amount({"commonData": {"orderId": "1"}})


@pytest.mark.parametrize(
    ("cny", "multiplier", "fixed", "rate", "expected"),
    [
        (10, 1, 0, 1, 10.0),
        (10, 2, 0, 1, 20.0),
        (10, 1, 5, 1, 5.0),
        (10, 1, 0, 7.2, 1.39),
        (66, 1, 0, 7.2, 9.17),
    ],
)
def test_quota_from_amount(cny, multiplier, fixed, rate, expected):
    assert quota_from_amount(cny, multiplier, fixed, rate) == pytest.approx(expected)
