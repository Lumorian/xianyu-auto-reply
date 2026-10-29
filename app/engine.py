"""发货决策引擎：规则匹配 → （卡密出库 | 建新 Key 出卡密 | 充值到已有 Key | 账号开号/充值）→ 渲染发货文案。

这一层不碰网络，纯函数 + 数据库读写，方便单元测试。
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from typing import Any

from .codec import IncomingMessage, is_ignored, is_paid_trigger, walk_pairs
from .db import Database
from .template import render_placeholders

logger = logging.getLogger(__name__)

#: 发货被鉴权拦下时的理由（runner 靠它决定要不要留一条审计记录）
AUTH_BLOCKED_REASON = "真人文本消息，订单未经核实，不自动发货"

#: 充值模式没写文案时的默认模板。{key} / {balance} 只有充值模式才有值。
DEFAULT_TOPUP_CONTENT = (
    "你好 {buyer}，已为你充值 {quota} 额度到原来的 Key：\n\n{key}\n\n"
    "直接继续用这个 Key 就行，不用换。有问题直接回复本消息。"
)

#: 「买家提供旧订单号」模式下，收到充值订单后先发的提示。
DEFAULT_TOPUP_PROMPT = (
    "你好 {buyer}，收到你的充值订单（{order_id}）。\n\n"
    "请把你之前购买 Key 的那笔订单号发给我（只要订单号），"
    "我会把额度充到那个订单对应的 Key 上。"
)

#: 买家发来的订单号找不到对应 Key 时的回复。
DEFAULT_TOPUP_NOT_FOUND = (
    "没有找到 {key_order_id} 对应的 Key。\n\n"
    "请核对订单号后重新发一次（只发数字订单号即可）。"
)

#: 兑换码模式：发给买家的那条消息。
#: 卡密站改版后「一个兑换码一张卡」，卡密里只有兑换码本身、放不下说明，
#: 所以兑换步骤必须写在这条消息里；聊天里也依旧只有卡密串。
DEFAULT_REDEEM_CONTENT = (
    "你好 {buyer}，这是你的卡密：\n\n{card}\n\n"
    "请打开 {shop_url} 兑换，拿到「额度兑换码」后：\n"
    "登录 {login_url} → 钱包 → 在「有兑换码？」里粘贴 → 点「兑换额度」，"
    "本单 {quota} 额度就会到账。"
)

#: 账号模式：收到订单后先问「有没有账号」。
DEFAULT_ACCOUNT_PROMPT = (
    "你好 {buyer}，收到你的订单（{order_id}）。\n\n"
    "请问你之前在本站注册过账号吗？\n"
    "· 有：把账号名直接发给我，我把额度充到你的账号里\n"
    "· 没有：回复「没有」，我给你开一个新账号（账号名可以由你指定）"
)

#: 买家说「没有账号」之后，问他想用什么账号名。
DEFAULT_ACCOUNT_ASK_NAME = (
    "好的，那就开个新账号。\n\n"
    "请回复你想用的账号名（3~20 位，只能用字母、数字、下划线、减号），"
    "密码由系统生成，开通后会一起发给你。"
)

#: 买家给的账号名格式不对。
DEFAULT_ACCOUNT_NAME_INVALID = (
    "这个账号名不能用：只能用字母、数字、下划线或减号，长度 3~20 位，且以字母或数字开头。\n\n"
    "请再发一个。"
)

#: 买家说「有账号」，但还没把账号名发过来。
DEFAULT_ACCOUNT_ASK_EXISTING = (
    "好的，把你之前注册的账号名发给我，我先核对一下余额，确认后就把额度充进去。"
)

#: 账号名已经有人用了。
DEFAULT_ACCOUNT_TAKEN = "账号名「{username}」已经有人注册了，换一个再发给我。"

#: 老用户给的账号名查不到。
DEFAULT_ACCOUNT_NOT_FOUND = (
    "没有找到账号「{username}」。\n\n"
    "请核对后重新发一次账号名；如果之前没注册过，回复「没有」，我给你开新号。"
)

#: 老用户：充值前回显账号和余额，等一句「确认」。
DEFAULT_ACCOUNT_CONFIRM = (
    "找到你的账号了：{username}（当前余额 {balance}）。\n\n"
    "确认把这笔订单的 {quota} 额度充到这个账号上吗？回复「确认」即可。"
)

#: 老用户充值成功的发货文案。
DEFAULT_ACCOUNT_CONTENT = (
    "你好 {buyer}，已给账号 {username} 充值 {quota} 额度，当前余额 {balance}。\n\n"
    "登录地址：{login_url}\n用你的账号密码登录即可，有问题直接回复本消息。"
)

#: 新用户：卡密里放什么（买家在卡密站兑换后看到的就是这段）。
DEFAULT_ACCOUNT_CARD = (
    "账号：{username}\n密码：{password}\n登录地址：{login_url}"
)

#: 新用户：发货文案（{card} 是卡密串）。
DEFAULT_ACCOUNT_NEW_CONTENT = (
    "你好 {buyer}，账号已经开好了。\n\n"
    "卡密：{card}\n\n"
    "请打开 {login_url} 用这张卡密兑换账号和密码。"
)

#: 新用户：卡密站没建成（卡密站挂了）时的兜底文案，直接发账号密码。
#: 账号和额度都已经开出去了，这时候绝不能「记失败」—— 那会让下一轮再充一次。
DEFAULT_ACCOUNT_NEW_CONTENT_NO_CARD = (
    "你好 {buyer}，账号已经开好了（卡密站暂时不可用，直接把账号密码发你）。\n\n"
    "账号：{username}\n密码：{password}\n登录地址：{login_url}\n\n"
    "请尽快登录并修改密码。"
)

#: 买家发来的是「另一个商品」的订单号时的回复（规则开了只认同商品）。
#: 和「查无此单」分开说，否则买家会一直重发同一笔别的商品的订单。
DEFAULT_TOPUP_WRONG_ITEM = (
    "订单 {key_order_id} 不是这个商品的订单，额度不能充到当前商品上。\n\n"
    "请发你购买这个商品时的订单号（只发数字订单号即可）。"
)

#: 「买家直接发 Key」模式下，收到充值订单后先发的提示。
DEFAULT_TOPUP_KEY_PROMPT = (
    "你好 {buyer}，收到你的充值订单（{order_id}）。\n\n"
    "请把要充值的 Key 发给我（sk- 开头那一整串），我会把额度充到这个 Key 上。"
)

#: 买家发的 Key 不在本站中转站里。
DEFAULT_TOPUP_KEY_FOREIGN = (
    "这个 Key 不在本站的中转站里，没法充值。\n\n"
    "请发你在本店购买时收到的 Key（sk- 开头），或者直接发订单号。"
)

#: 买家发的 Key 是本站的，但不是他买的 —— 绝不往别人的 Key 上充钱。
DEFAULT_TOPUP_KEY_NOT_YOURS = (
    "这个 Key 不是你在本店购买的，不能充值。\n\n"
    "请发你自己的 Key，或者直接发订单号。"
)

#: 发来的是打码 / 不完整的 Key（中间带省略号）。
DEFAULT_TOPUP_KEY_MASKED = (
    "你发的是打码后的 Key（中间有省略号），没法用来充值。\n\n"
    "请长按复制完整的 Key（sk- 开头那一整串）再发一次。"
)

#: Key 认到了令牌，但令牌读不到（被删了 / 被换过）。
DEFAULT_TOPUP_KEY_GONE = (
    "这个 Key 对应的令牌读不到（可能已经被删除或更换）。\n\n"
    "请在聊天里找客服核对一下，或者直接发订单号。"
)

#: 站点令牌列表不返回明文 Key，没法按 Key 核对归属。
DEFAULT_TOPUP_KEY_UNAVAILABLE = (
    "暂时没法按 Key 充值（后台读不到明文 Key）。\n\n"
    "请把你之前购买 Key 的订单号发给我，我充到那一单的 Key 上。"
)

#: 规格关键词分隔符：和自动回复的关键词保持一致（中英文逗号 / 分号 / 竖线 / 换行）
_SPEC_SPLIT_RE = re.compile(r"[,，;；|｜\n]+")

#: 从订单 / 卡片里认「规格」的键名线索。闲鱼不同接口对 SKU 字段的命名不统一，
#: 所以按名字找，而不是写死某一条路径。
_SPEC_KEY_HINT = re.compile(r"sku|spec|attr|variant|property", re.IGNORECASE)
_SPEC_EXACT_KEYS = frozenset({"itemtitle", "itemname", "item_title", "titletext"})

#: 规格文本最长保留多少字符：订单里可能塞整段 JSON，没必要全拼进来。
MAX_SPEC_TEXT = 4000


@dataclass(slots=True)
class Decision:
    """一条消息的处理结论。"""

    action: str  # "deliver" | "provision" | "topup" | "skip"
    reason: str = ""
    rule: dict[str, Any] | None = None
    content: str = ""
    card_id: int | None = None
    card_code: str | None = None
    amount_cny: float | None = None
    quota_usd: float | None = None
    auto_confirm: bool = False
    #: 充值模式下买家提供的「旧订单号」（指定要充值到哪个 Key）。
    #: 为空表示还没提供，需要先提示买家。
    key_order_id: str = ""
    #: 充值模式下买家直接发了 Key、已经核对完归属的令牌 id。
    #: 和 key_order_id 二选一：这条路径是「Key → 令牌」，不用再查旧订单。
    key_token_id: str = ""
    #: 账号模式：买家要充值的 / 新建的账号名。只有 name 没有 user_id = 开新号；
    #: 两个都有 = 充到已有账号（买家回复账号名后由 runner 补上 user_id）。
    account_username: str = ""
    account_user_id: str = ""
    #: 发货消息是否**已经真的发到买家手上**。
    #: 这是不可逆点：一旦为 True，后续任何失败都不许再把卡密退回库存
    #: （否则同一张卡会卖给下一个人），也不许让这一单被重新发一次。
    sent: bool = False
    #: 发送结果未知（ack 超时 / 连接中途断开）：消息可能已送达。
    #: 既不能退卡，也不能自动重发，落 send_unknown 交给人工核对。
    sent_unknown: bool = False

    @property
    def should_deliver(self) -> bool:
        return self.action in ("deliver", "provision", "topup", "account", "redeem")

    @property
    def needs_provision(self) -> bool:
        return self.action == "provision"

    @property
    def needs_topup(self) -> bool:
        """充值模式：先把额度充到买家已有的 Key 上，再发消息。"""
        return self.action == "topup"

    @property
    def needs_account(self) -> bool:
        """账号模式：开新号 / 充到已有账号，再发消息。"""
        return self.action == "account"

    @property
    def needs_redeem(self) -> bool:
        """兑换码模式：先生成 new-api 兑换码，再发消息。"""
        return self.action == "redeem"


def split_spec_keywords(raw: str | None) -> list[str]:
    """把规则里的「规格关键词」拆成列表（去空、去重、保序）。"""
    if not raw:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for part in _SPEC_SPLIT_RE.split(str(raw)):
        word = part.strip()
        if not word or word in seen:
            continue
        seen.add(word)
        out.append(word)
    return out


def _append_spec_text(parts: list[str], value: Any) -> None:
    """收集一段规格值：标量直接用，字典 / 列表把里面的字符串都摊平。"""
    stack = [value]
    while stack:
        cur = stack.pop()
        if isinstance(cur, str):
            text = " ".join(cur.split())
            if text:
                parts.append(text[:200])
        elif isinstance(cur, (int, float)) and not isinstance(cur, bool):
            parts.append(str(cur))
        elif isinstance(cur, dict):
            stack.extend(cur.values())
        elif isinstance(cur, (list, tuple)):
            stack.extend(cur)


def spec_text(payload: Any) -> str:
    """从订单 / 付款卡片负载里尽量抽出「规格」相关文本。

    闲鱼对多规格商品的字段命名不统一（skuInfo / skuName / attributeMap…），
    这里按**键名**去找，再拼成一段文本供规则做包含匹配；
    商品标题也算进去 —— 很多卖家把「月卡 / 季卡」直接写在标题里。
    找不到就返回空串：此时带规格条件的规则不参与匹配，自动退回商品级规则。
    """
    if not isinstance(payload, dict) or not payload:
        return ""
    parts: list[str] = []
    for key, value in walk_pairs(payload):
        name = str(key).strip().lower()
        if not (_SPEC_KEY_HINT.search(name) or name in _SPEC_EXACT_KEYS):
            continue
        _append_spec_text(parts, value)
        if sum(len(part) for part in parts) >= MAX_SPEC_TEXT:
            break
    if not parts:
        return ""
    seen: set[str] = set()
    unique: list[str] = []
    for part in parts:
        if part in seen:
            continue
        seen.add(part)
        unique.append(part)
    return " ".join(unique)[:MAX_SPEC_TEXT]


def spec_matches(rule_spec: str | None, text: str) -> bool | None:
    """规则规格是否命中。

    返回 None 表示这条规则没有规格条件（任何规格都算命中），
    True / False 表示有条件且命中 / 没命中。
    """
    keywords = split_spec_keywords(rule_spec)
    if not keywords:
        return None
    if not text:
        return False
    lowered = text.lower()
    return any(word.lower() in lowered for word in keywords)


def amount_bounds(rule: dict[str, Any]) -> tuple[float, float] | None:
    """规则里的金额区间（元）；None 表示不限金额。

    min/max 都是 0 = 不限；只填一边就只卡一边。填反了自动纠正，避免运营手滑
    后规则永远不命中却看不出原因。
    """
    try:
        low = float(rule.get("min_amount") or 0)
        high = float(rule.get("max_amount") or 0)
    except (TypeError, ValueError):
        return None
    if low <= 0 and high <= 0:
        return None
    if low > 0 and high > 0 and high < low:
        low, high = high, low
    return low, high


def amount_matches(rule: dict[str, Any], amount_cny: float | None) -> bool | None:
    """金额条件是否命中；None 表示这条规则没有金额条件。"""
    bounds = amount_bounds(rule)
    if bounds is None:
        return None
    if amount_cny is None:
        return False
    low, high = bounds
    if low > 0 and amount_cny < low - 1e-9:
        return False
    return not (high > 0 and amount_cny > high + 1e-9)


def _rule_scope(rule: dict[str, Any], item_id: str | None, text: str) -> tuple[int, int] | None:
    """只看商品 + 规格的匹配程度；None 表示不适用。"""
    target = str(rule.get("item_id") or "*").strip()
    if item_id and target == str(item_id):
        scope = 2
    elif target in ("*", "", "all"):
        scope = 1
    else:
        return None
    matched = spec_matches(rule.get("spec"), text)
    if matched is False:
        return None
    return (scope, 1 if matched else 0)


def _rule_score(rule: dict[str, Any], item_id: str | None, text: str,
                amount_cny: float | None) -> tuple[int, int, int] | None:
    """规则的匹配优先级；None 表示这条规则不适用。

    先比商品范围（专属 > 通配），再比规格，最后比金额条件：
    有金额条件且命中的规则，优先于不限金额的通用规则。
    """
    scope = _rule_scope(rule, item_id, text)
    if scope is None:
        return None
    matched = amount_matches(rule, amount_cny)
    if matched is False:
        return None
    return (*scope, 1 if matched else 0)


def pick_rule(rules: list[dict[str, Any]], item_id: str | None,
              text: str = "", amount_cny: float | None = None) -> dict[str, Any] | None:
    """挑出适用于该商品 / 规格 / 金额的规则。

    优先级：商品专属 > 通配；同范围内，命中规格 / 金额条件的规则优先于通用规则；
    再按 priority 降序、id 升序（调用方传进来的 rules 已经排好）。

    如果这个商品配了金额规则、但当前拿不到订单金额，宁可**先不选规则**，
    也不要把价格档规则跳过、拿通用规则发错额度 —— 订单轮询拿到金额后会重试。
    """
    if amount_cny is None:
        for rule in rules:
            if amount_bounds(rule) is None:
                continue
            if _rule_scope(rule, item_id, text) is not None:
                logger.info("商品 %s 配了金额区间规则，但订单金额未知，先不匹配", item_id)
                return None

    best: dict[str, Any] | None = None
    best_score: tuple[int, int, int] | None = None
    for rule in rules:
        score = _rule_score(rule, item_id, text, amount_cny)
        if score is None:
            continue
        if best_score is None or score > best_score:
            best = rule
            best_score = score
    return best


def render(template: str, *, buyer_name: str = "", item_id: str | None = None,
           order_id: str | None = None, card: str = "",
           amount_cny: float | None = None, quota_usd: float | None = None,
           key: str = "", balance: str = "", key_order_id: str = "",
           username: str = "", password: str = "", user_id: str = "",
           login_url: str = "", shop_url: str = "") -> str:
    """渲染发货文案。

    支持 {buyer} {item_id} {order_id} {card} {date} {amount} {quota}；
    充值模式另有 {key}（买家原来的 Key）与 {balance}（充值后余额）；
    账号模式另有 {username} / {password} / {user_id} / {login_url}；
    兑换码模式另有 {shop_url}（卡密站地址）—— 兑换码本身只进卡密，不进聊天。
    """
    values = {
        "buyer": buyer_name or "买家",
        "item_id": item_id or "",
        "order_id": order_id or "",
        "card": card,
        "date": time.strftime("%Y-%m-%d %H:%M:%S"),
        "amount": "" if amount_cny is None else f"{amount_cny:.2f}",
        "quota": "" if quota_usd is None else f"{quota_usd:.2f}",
        "key": key,
        "balance": balance,
        "key_order_id": key_order_id,
        "username": username,
        "password": password,
        "user_id": user_id,
        "login_url": login_url,
        "shop_url": shop_url,
    }
    return render_placeholders(template, values)


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
            now = time.time()
            self._last_delivery[(account_id, order_id)] = now
            # 这个字典按订单号增长，长年运行会慢慢吃掉内存；冷却期只有十分钟，
            # 过期的条目没有任何保留价值。
            if len(self._last_delivery) > 2000:
                cutoff = now - self.cooldown
                self._last_delivery = {
                    key: stamp for key, stamp in self._last_delivery.items() if stamp >= cutoff
                }

    async def decide_order(self, account_id: int, msg: IncomingMessage) -> Decision:
        """订单触发的决策：跳过「是否发货触发语」这一步。

        订单轮询拿到的「待发货」本身就是触发条件，不需要聊天里再出现一句话。
        """
        if not msg.sender_id:
            return Decision("skip", "缺少买家 ID")

        if msg.order_id and await self.db.delivery_blocked(account_id, msg.order_id):
            return Decision("skip", "订单 " + str(msg.order_id) + " 已发过货或发送结果待人工核对")

        if self._in_cooldown(account_id, msg.order_id):
            return Decision("skip", "订单 " + str(msg.order_id) + " 处于冷却期")

        rules = await self.db.list_rules(only_enabled=True)
        rule = pick_rule(rules, msg.item_id, spec_text(msg.raw), msg.amount_cny_hint)
        if rule is None:
            return Decision("skip", "没有命中发货规则（或订单金额未知，等待重试）")

        mode = str(rule.get("mode") or "text")
        if mode == "upstream":
            return Decision("provision", reason="订单触发，命中上游规则 " + str(rule["name"]), rule=rule,
                            auto_confirm=bool(rule["auto_confirm"]))
        if mode == "topup":
            return Decision("topup", reason="订单触发，命中充值规则 " + str(rule["name"]), rule=rule,
                            auto_confirm=bool(rule["auto_confirm"]))
        if mode == "account":
            return Decision("account", reason="订单触发，命中账号规则 " + str(rule["name"]), rule=rule,
                            auto_confirm=bool(rule["auto_confirm"]))
        if mode == "redeem":
            return Decision("redeem", reason="订单触发，命中兑换码规则 " + str(rule["name"]), rule=rule,
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
        # 唯一可信来源是 runner 拿订单号去闲鱼核实过（msg.order_verified）：
        # 订单状态必须是「待发货」，且订单里的 buyerId 必须等于消息发送者。
        #
        # 这里不再给「系统卡片」开快速通道。卡片形态（contentType=26 / dxCard）
        # 只能证明这条消息长得像交易卡片，不能证明订单属于当前买家：
        # 买家可以转发别人的付款卡片，或者伪造一个带 dxCard 键的负载。
        # 少一次核实，就是把卡密池和上游 Key 交给任何能构造卡片的人。
        if not msg.order_id:
            return Decision("skip", "缺少订单号，不自动发货")
        if not msg.order_verified:
            return Decision("skip", AUTH_BLOCKED_REASON)

        if await self.db.delivery_blocked(account_id, msg.order_id):
            return Decision("skip", f"订单 {msg.order_id} 已发过货或发送结果待人工核对")

        if self._in_cooldown(account_id, msg.order_id):
            return Decision("skip", f"订单 {msg.order_id} 处于冷却期")

        rules = await self.db.list_rules(only_enabled=True)
        spec = spec_text(msg.raw)
        rule = pick_rule(rules, msg.item_id, spec, msg.amount_cny_hint)
        if rule is None:
            logger.info("商品 %s（订单 %s）没有命中发货规则", msg.item_id, msg.order_id)
            return Decision("skip", "没有命中发货规则（或订单金额未知，等待重试）")

        mode = str(rule.get("mode") or "text")
        if mode == "upstream":
            # 上游模式要查订单金额，网络动作交给 runner，这里只判定「可以走了」
            return Decision("provision", reason="命中上游规则 " + str(rule["name"]), rule=rule,
                            auto_confirm=bool(rule["auto_confirm"]))
        if mode == "topup":
            # 充值模式同样要查金额；绑定 / 开新 Key / 充值都在 runner 里做
            return Decision("topup", reason="命中充值规则 " + str(rule["name"]), rule=rule,
                            auto_confirm=bool(rule["auto_confirm"]))
        if mode == "account":
            # 账号模式：先问买家有没有账号，再开新号 / 充值；问答都在 runner 里
            return Decision("account", reason="命中账号规则 " + str(rule["name"]), rule=rule,
                            auto_confirm=bool(rule["auto_confirm"]))
        if mode == "redeem":
            # 兑换码模式：金额 → new-api 兑换码，直接发；网络动作在 runner 里
            return Decision("redeem", reason="命中兑换码规则 " + str(rule["name"]), rule=rule,
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
            amount_cny=msg.amount_cny_hint,
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
