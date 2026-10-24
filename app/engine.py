"""发货决策引擎：规则匹配 → （卡密出库 | 上游建 Key 出卡密）→ 渲染发货文案。

这一层不碰网络，纯函数 + 数据库读写，方便单元测试。
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

from .codec import IncomingMessage, is_ignored, is_paid_trigger
from .db import Database

logger = logging.getLogger(__name__)

#: 发货被鉴权拦下时的理由（runner 靠它决定要不要留一条审计记录）
AUTH_BLOCKED_REASON = "真人文本消息，订单未经核实，不自动发货"


@dataclass(slots=True)
class Decision:
    """一条消息的处理结论。"""

    action: str  # "deliver" | "skip"
    reason: str = ""
    rule: dict[str, Any] | None = None
    content: str = ""
    card_id: int | None = None
    card_code: str | None = None
    amount_cny: float | None = None
    quota_usd: float | None = None
    auto_confirm: bool = False

    @property
    def should_deliver(self) -> bool:
        return self.action in ("deliver", "provision")

    @property
    def needs_provision(self) -> bool:
        return self.action == "provision"


def pick_rule(rules: list[dict[str, Any]], item_id: str | None) -> dict[str, Any] | None:
    """挑出适用于该商品的规则。

    优先级：商品专属规则 > 通配规则；同优先级下按 priority 降序、id 升序。
    rules 已按 priority DESC, id 排序。
    """
    if not rules:
        return None
    if item_id:
        for rule in rules:
            if str(rule.get("item_id")) == str(item_id):
                return rule
    for rule in rules:
        if str(rule.get("item_id")) in ("*", "", "all"):
            return rule
    return None


def render(template: str, *, buyer_name: str = "", item_id: str | None = None,
           order_id: str | None = None, card: str = "",
           amount_cny: float | None = None, quota_usd: float | None = None) -> str:
    """渲染发货文案，支持 {buyer} {item_id} {order_id} {card} {date} {amount} {quota}。"""
    values = {
        "buyer": buyer_name or "买家",
        "item_id": item_id or "",
        "order_id": order_id or "",
        "card": card,
        "date": time.strftime("%Y-%m-%d %H:%M:%S"),
        "amount": "" if amount_cny is None else f"{amount_cny:.2f}",
        "quota": "" if quota_usd is None else f"{quota_usd:.2f}",
    }
    out = template or ""
    for key, value in values.items():
        out = out.replace("{" + key + "}", str(value))
    return out


def quota_from_amount(amount_cny: float, multiplier: float = 1.0, fixed_usd: float = 0.0,
                      cny_per_usd: float = 1.0) -> float:
    """订单金额（元）→ 上游额度（USD）。

    fixed_usd > 0 时直接用它（一口价场景）；否则按汇率换算后再乘倍率。
    结果保留两位小数，避免拿到 9.999999 这种额度。
    """
    if fixed_usd and fixed_usd > 0:
        return round(fixed_usd, 2)
    rate = cny_per_usd if cny_per_usd > 0 else 1.0
    value = (amount_cny / rate) * (multiplier if multiplier > 0 else 1.0)
    return round(value, 2)


class DeliveryEngine:
    """根据消息决定是否发货、发什么。"""

    def __init__(self, db: Database, cooldown: int = 600) -> None:
        self.db = db
        self.cooldown = cooldown
        #: (account_id, order_id) -> 上次发货时间戳
        self._last_delivery: dict[tuple[int, str], float] = {}

    def _in_cooldown(self, account_id: int, order_id: str | None) -> bool:
        if not order_id:
            return False
        last = self._last_delivery.get((account_id, order_id))
        return last is not None and (time.time() - last) < self.cooldown

    def mark_delivered(self, account_id: int, order_id: str | None) -> None:
        if order_id:
            self._last_delivery[(account_id, order_id)] = time.time()

    async def decide_order(self, account_id: int, msg: IncomingMessage) -> Decision:
        """订单触发的决策：跳过「是否发货触发语」这一步。

        订单轮询拿到的「待发货」本身就是触发条件，不需要聊天里再出现一句话。
        """
        if not msg.sender_id:
            return Decision("skip", "缺少买家 ID")

        if msg.order_id and await self.db.delivered_order_exists(account_id, msg.order_id):
            return Decision("skip", "订单 " + str(msg.order_id) + " 已发过货")

        if self._in_cooldown(account_id, msg.order_id):
            return Decision("skip", "订单 " + str(msg.order_id) + " 处于冷却期")

        rules = await self.db.list_rules(only_enabled=True)
        rule = pick_rule(rules, msg.item_id)
        if rule is None:
            return Decision("skip", "没有命中发货规则")

        if rule["mode"] == "upstream":
            return Decision("provision", reason="订单触发，命中上游规则 " + str(rule["name"]), rule=rule,
                            auto_confirm=bool(rule["auto_confirm"]))

        return await self._materialize(rule, msg)

    async def decide(self, account_id: int, msg: IncomingMessage) -> Decision:
        """核心判断：要不要给这条消息发货。"""
        if not msg.sender_id:
            return Decision("skip", "缺少买家 ID")
        if is_ignored(msg.content) and not is_paid_trigger(msg.content):
            return Decision("skip", "系统提示消息")

        if not is_paid_trigger(msg.content):
            return Decision("skip", "非发货触发消息")

        # 关键鉴权：不能只看「正文里有没有那几句话」。
        # 「我已付款，等待你发货」买家自己也能在聊天框里打出来，
        # 只按文本判定等于把卡密池和上游 Key 交给任何人。
        #
        # 两条可信来源：
        #   1. 系统交易卡片（contentType=26 + dxCard/order_detail，买家造不出来）；
        #   2. 真人文本，但 runner 已经拿订单号去闲鱼核实过（msg.order_verified）。
        # 两种都要有订单号：没有订单号既不能去重，也不能核对归属。
        if not msg.order_id:
            return Decision("skip", "缺少订单号，不自动发货")
        if not msg.is_system_card and not msg.order_verified:
            return Decision("skip", AUTH_BLOCKED_REASON)

        if await self.db.delivered_order_exists(account_id, msg.order_id):
            return Decision("skip", f"订单 {msg.order_id} 已发过货")

        if self._in_cooldown(account_id, msg.order_id):
            return Decision("skip", f"订单 {msg.order_id} 处于冷却期")

        rules = await self.db.list_rules(only_enabled=True)
        rule = pick_rule(rules, msg.item_id)
        if rule is None:
            logger.info("商品 %s（订单 %s）没有命中发货规则", msg.item_id, msg.order_id)
            return Decision("skip", "没有命中发货规则")

        if rule["mode"] == "upstream":
            # 上游模式要查订单金额，网络动作交给 runner，这里只判定「可以走了」
            return Decision("provision", reason="命中上游规则 " + str(rule["name"]), rule=rule,
                            auto_confirm=bool(rule["auto_confirm"]))

        return await self._materialize(rule, msg)

    async def _materialize(self, rule: dict[str, Any], msg: IncomingMessage) -> Decision:
        """按规则出内容（卡密池 / 固定文案），不含上游建 Key。"""
        card_id: int | None = None
        card_text = ""
        if rule["mode"] == "card":
            item_for_pool = msg.item_id or rule["item_id"]
            allocation = await self.db.take_card(item_for_pool or "*", msg.order_id, msg.sender_id)
            if allocation.content is None:
                return Decision("skip", "卡密库存不足", rule=rule)
            card_id = allocation.card_id
            card_text = allocation.content

        content = render(
            rule["content"],
            buyer_name=msg.sender_name,
            item_id=msg.item_id,
            order_id=msg.order_id,
            card=card_text,
        )
        if not content.strip():
            if card_id is not None:
                await self.db.release_card(card_id)
            return Decision("skip", "发货内容为空", rule=rule)
        return Decision(
            "deliver",
            reason="命中规则 " + str(rule["name"]),
            rule=rule,
            content=content,
            card_id=card_id,
            auto_confirm=bool(rule["auto_confirm"]),
        )
