"""订单金额解析与换算。

闲鱼订单接口里的金额格式不统一：可能是 "10.00"、"¥10.00"，
也可能是整数分（1000 表示 10 元）。这里集中处理，避免各处重复猜。
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

logger = logging.getLogger(__name__)

#: 从「¥1,099.00」这类展示串里剥出纯数字。注意：
#:   1) 先把负号记下来再剥，否则 "-5" 会被剥成 "5"；
#:   2) 千分位逗号按「后面正好跟 3 位数字」来删，避免把 "1,5" 这种
#:      当成的千分位（真实场景里几乎不会出现，但删错就是 100 倍误差）。
_CLEAN = re.compile(r"[^0-9.]")
_THOUSANDS = re.compile(r"(?<=\d),(?=\d{3}(?:\D|$))")

#: 订单接口里可能的金额字段，按可信度排序（实付 > 确认金额 > 挂价）
AMOUNT_FIELDS: tuple[str, ...] = (
    "actualPayFee",
    "actualPay",
    "payAmount",
    "realPay",
    "paidAmount",
    "tradeAmount",
    "buyerPayAmount",
    "totalPrice",
    "confirmFee",
    "auctionPrice",
    "orderAmount",
    "itemPrice",
)


class AmountError(ValueError):
    """无法从订单数据里解析出可信的金额。"""


@dataclass(slots=True)
class Amount:
    """规整后的订单金额（元）。"""

    cny: float
    source: str = ""


def parse_amount(value: object, unit: str = "auto") -> Amount | None:
    """把单个金额字段解析成元。

    unit 取 yuan 时强制按元处理；取 fen 时强制按分处理；
    取 auto 时，纯整数且数值 >= 1000 视为分（否则 1000 分 = 10 元会被当成 1000 元）。
    """
    if value is None or isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric < 0:
            return None
        text = str(value)
        is_int = numeric.is_integer()
    else:
        raw_text = str(value).strip()
        # 负号的判断必须发生在 _CLEAN 之前：它会把 "-" 一起删掉，
        # 「-5」于是变成「5」，退款/优惠单会被当成正常正价发额度。
        # 位置不限（「¥-10.00」里的负号在货币符号后面）：金额串里出现 "-"
        # 本来就不正常，一律拒掉最省事也最安全。
        if "-" in raw_text or "−" in raw_text:
            return None
        text = _CLEAN.sub("", _THOUSANDS.sub("", raw_text))
        if not text or text == ".":
            return None
        try:
            numeric = float(text)
        except ValueError:
            return None
        is_int = "." not in text

    if numeric < 0:
        return None

    mode = (unit or "auto").lower()
    # 「整数且 >= 1000 就当成分」只是猜：真遇到一笔 1000 元的订单时，
    # 这里会把 ¥1000 算成 ¥10，额度少发 100 倍。猜中猜错都留一条日志，
    # 让「金额看着不对」时有据可查；能确定单位就设 XY_ORDER_AMOUNT_UNIT。
    guessed_fen = mode == "auto" and is_int and numeric >= 1000
    if guessed_fen:
        logger.warning(
            "订单金额 %s 被当成「分」处理，换算结果 %s 元；"
            "如果这其实是元，请把 XY_ORDER_AMOUNT_UNIT 设成 yuan",
            text, round(numeric / 100, 2),
        )
    if mode == "fen" or guessed_fen:
        return Amount(cny=round(numeric / 100, 2), source=text)
    return Amount(cny=round(numeric, 2), source=text)


def amount_from_order_item(payload: dict, unit: str = "auto") -> float | None:
    """从「订单列表里的一项」直接取实付金额，取不到返回 None。

    轮询拿到的订单项和 find_order 查出来的结构是同一份数据，
    所以有它就不用再多打一次 mtop 接口 —— 每单省一次往返。
    """
    if not isinstance(payload, dict) or not payload:
        return None
    try:
        return pick_amount(payload, unit).cny
    except AmountError:
        return None


def pick_amount(payload: dict, unit: str = "auto") -> Amount:
    """从订单 JSON 里挑一个可信金额。找不到就抛 AmountError。"""
    from .codec import walk_pairs

    if not isinstance(payload, dict):
        raise AmountError("订单数据不是对象")

    pairs = list(walk_pairs(payload))
    for field in AMOUNT_FIELDS:
        for key, value in pairs:
            if key.lower() != field.lower():
                continue
            amount = parse_amount(value, unit)
            if amount is not None and amount.cny > 0:
                return Amount(cny=amount.cny, source=field + "=" + str(value))

    raise AmountError("订单数据里没有可识别的金额字段")
