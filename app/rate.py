"""自动评价的纯逻辑：识别评价请求、渲染评价文案。

闲鱼在买家确认收货后会推一张 contentType=25 的系统卡片，正文形如：
    「快给ta一个评价吧～」
    「去评价」
卡片里带 orderId。这一层只做判断和文案，不碰网络。
"""

from __future__ import annotations

import re
import time
from typing import Any

from .template import render_placeholders

#: 淘宝/闲鱼评价最多允许的字数（保守值，超长服务端会拒绝）
MAX_RATE_FEEDBACK = 200

#: 评价请求卡片的特征词。不要用宽泛的「评价」：买家评价完还会推
#: 「[我完成了评价]」「查看评价」，那些是通知，不是请求。
_RATE_REQUEST_MARKERS = (
    "快给ta一个评价",
    "快给 ta 一个评价",
    "快给他一个评价",
    "快给她一个评价",
    "给ta一个评价",
    "说说这次的交易体验",
)


def is_rate_request(content: str) -> bool:
    """这条系统消息是不是「请卖家评价买家」的请求。"""
    text = " ".join(str(content or "").split())
    if not text:
        return False
    lowered = text.lower()
    if "完成" in text or "已评价" in text or "查看评价" in text:
        return False
    return any(marker in lowered for marker in _RATE_REQUEST_MARKERS)


def render_feedback(
    template: str,
    *,
    buyer_name: str = "",
    item_id: str | None = None,
    order_id: str | None = None,
    date: str | None = None,
) -> str:
    """渲染评价文案，支持 {buyer} {item_id} {order_id} {date}。"""
    values = {
        "buyer": buyer_name or "买家",
        "item_id": item_id or "",
        "order_id": order_id or "",
        "date": date or time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    out = render_placeholders(template, values)
    out = " ".join(out.split())
    if len(out) > MAX_RATE_FEEDBACK:
        out = out[:MAX_RATE_FEEDBACK]
    return out


def rate_request_order(order_id: str | None) -> str:
    """从卡片解析出的订单号里取一个干净的值。"""
    value = str(order_id or "").strip()
    if not value or not re.fullmatch(r"[0-9A-Za-z_-]{6,64}", value):
        return ""
    return value


def rate_status_ok(ret: str) -> bool:
    """mtop.taobao.idle.rate.create 的成功判断。"""
    upper = str(ret or "").upper()
    return "SUCCESS" in upper


def rate_failure_text(result: Any) -> str:
    if isinstance(result, dict):
        ret = result.get("ret")
        if isinstance(ret, list):
            return " ".join(str(item) for item in ret)
        if ret:
            return str(ret)
        return str(result.get("message") or result.get("error") or "")
    return str(result or "")
