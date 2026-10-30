"""账号级工作循环：连接 → 收消息 → 决策 → 发货 → 记录。"""

from __future__ import annotations

import asyncio
import logging
import random
import re
import secrets
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from . import media as media_tools
from .autoreply import AutoReplyEngine, ReplyDecision
from .client import CookieExpired, RiskControl, TokenExpired, XianyuClient, XianyuError, cookie_flags
from .codec import (
    IncomingMessage,
    buyer_nick_from_order,
    frame_shape,
    is_guide_payload,
    is_ignored,
    is_paid_trigger,
    is_placeholder_name,
    note_unparsed,
    parse_message,
    pick_display_name,
    sync_payloads_detailed,
    unread_sync_chat_id,
)
from .config import newapi_ready, resolve_model_limits, resolved_auto_rate, settings
from .db import Database, now_iso
from .engine import (
    AUTH_BLOCKED_REASON,
    DEFAULT_ACCOUNT_ASK_EXISTING,
    DEFAULT_ACCOUNT_ASK_NAME,
    DEFAULT_ACCOUNT_CARD,
    DEFAULT_ACCOUNT_CONFIRM,
    DEFAULT_ACCOUNT_CONTENT,
    DEFAULT_ACCOUNT_NAME_INVALID,
    DEFAULT_ACCOUNT_NEW_CONTENT,
    DEFAULT_ACCOUNT_NEW_CONTENT_NO_CARD,
    DEFAULT_ACCOUNT_NOT_FOUND,
    DEFAULT_ACCOUNT_PROMPT,
    DEFAULT_ACCOUNT_TAKEN,
    DEFAULT_REDEEM_CONTENT,
    DEFAULT_TOPUP_CONTENT,
    DEFAULT_TOPUP_KEY_FOREIGN,
    DEFAULT_TOPUP_KEY_GONE,
    DEFAULT_TOPUP_KEY_MASKED,
    DEFAULT_TOPUP_KEY_NOT_YOURS,
    DEFAULT_TOPUP_KEY_PROMPT,
    DEFAULT_TOPUP_KEY_UNAVAILABLE,
    DEFAULT_TOPUP_NOT_FOUND,
    DEFAULT_TOPUP_PROMPT,
    DEFAULT_TOPUP_WRONG_ITEM,
    Decision,
    DeliveryEngine,
    amount_bounds,
    quota_from_amount,
    render,
)
from .memory import cap_mapping, cap_set
from .money import AmountError, amount_from_order_item, pick_amount
from .rate import is_rate_request, rate_request_order, render_feedback
from .upstream import (
    Provisioner,
    TopupUncertainError,
    UpstreamError,
    build_card_label,
    build_redeem_label,
    mask_key,
    normalize_key,
    order_id_from_label,
)

logger = logging.getLogger(__name__)

#: 断线重连的退避序列（秒），循环使用
BACKOFF = (5, 10, 30, 60, 120)
#: 风控/滑块不是 Cookie 失效，退避要更长，避免把账号越撞越黑
RISK_BACKOFF = (60, 180, 300, 600, 900)

#: 同一条消息处理失败最多重试几次（撤销 seen 标记后等下一轮）。
#: 超过就放弃，免得一条永远解析不了的消息每轮都重试、把日志刷满。
SEEN_RETRY_LIMIT = 3

#: 后台手动回复的长度上限。闲鱼本身对单条文本有长度限制，
#: 这里先拦住明显异常的超长粘贴（也避免把前端撑爆）。
MAX_CHAT_TEXT = 2000

#: 闲鱼订单列表里「还没成交 / 已关闭」的状态：历史同步不能当销售额导入。
NON_SALE_ORDER_STATUSES = frozenset({
    "待付款",
    "等待买家付款",
    "未付款",
    "交易关闭",
    "交易取消",
    "已关闭",
})

#: 闲鱼只允许撤回两分钟内发出的消息（服务端也会校验，这里先给个明确提示）
RECALL_WINDOW_SECONDS = 120

#: 充值模式的三种「用哪个 Key」策略
TOPUP_KEY_SOURCE_ORDER = "order_id"
TOPUP_KEY_SOURCE_AUTO = "auto"
#: 买家直接发 Key（sk- 开头）：只认本店发给他本人的令牌
TOPUP_KEY_SOURCE_API_KEY = "api_key"

#: 充值模式允不允许跨商品（同一个站点卖不同商品时，Key 的模型白名单不一样）：
#: any = 买家任意已发货订单都能当充值目标（老行为）；
#: item = 只认同商品的旧订单 / 同商品发过的 Key。
TOPUP_ITEM_SCOPE_ANY = "any"
TOPUP_ITEM_SCOPE_ITEM = "item"

#: 旧订单号 / Key 不能用来充值时，日志和待办记录里的中文原因。
_KEY_ORDER_FAILURE_TEXT = {
    "not_found": "查无此单（不是本人已发货的订单）",
    "other_item": "属于另一个商品",
    "no_token": "这一单没有对应的令牌",
    "unreadable": "对应的令牌读不到（可能已在 new-api 删除）",
    "key_not_yours": "不是本人购买",
    "key_foreign": "不在本站",
    "key_gone": "对应的令牌读不到（可能已被删除）",
    "key_unavailable": "站点读不到明文 Key，没法核对",
    "key_invalid": "格式不对",
}

#: 「买家发的东西不能用来充值」时按原因挑一句默认回复。
#: 运营在规则里改过 topup_not_found 就按运营的来，这里只是兜底。
_TOPUP_REJECT_TEXT = {
    "other_item": DEFAULT_TOPUP_WRONG_ITEM,
    "key_foreign": DEFAULT_TOPUP_KEY_FOREIGN,
    "key_gone": DEFAULT_TOPUP_KEY_GONE,
    "key_not_yours": DEFAULT_TOPUP_KEY_NOT_YOURS,
    "key_masked": DEFAULT_TOPUP_KEY_MASKED,
    "key_unavailable": DEFAULT_TOPUP_KEY_UNAVAILABLE,
}

#: 按 Key 认令牌时最多读多少个令牌的明文（每个一次请求）。
#: 列表接口把 Key 打码的站点只能这样认；站上令牌多的时候要设上限。
KEY_SCAN_LIMIT = 300

#: 按 Key 逐个读明文最多花多少秒。到点还没认出来就按「暂时核对不了」回，
#: 不能让买家的一条消息卡在几十个上游请求上。
KEY_SCAN_SECONDS = 8.0

#: 令牌 id -> (明文 Key, 读取时间)：一次充值流程里会反复用到，缓存 5 分钟。
TOKEN_KEY_TTL = 300

#: 买家提前发来旧订单号时，在内存里留多久（秒）。
#: 只用于覆盖「消息比订单轮询先到」的几秒窗口，故意设短，避免下一次充值
#: 自动沿用上一单的旧订单号（要求是每笔充值都由买家重新指定）。
KEY_ORDER_HINT_TTL = 120

#: 同一个充值订单两次提示之间至少隔多久（秒），避免失败重试时每 30 秒催一次。
TOPUP_PROMPT_GAP = 10 * 60

#: ---- 账号模式（卡密换账号 / 直接充值到已有账号）----
ACCOUNT_STATUS_AWAITING = "awaiting_account"
ACCOUNT_STATUS_USERNAME = "awaiting_username"
ACCOUNT_STATUS_CONFIRM = "awaiting_confirm"

#: 账号名允许的字符。new-api 只限制「≤20 字节」，但中文名在登录页、日志、
#: 卡密备注里都不好处理，也对不上买家手打时的写法，所以这里只放行常见字符。
_ACCOUNT_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{2,19}$")

#: 买家回答「没有账号」的几种说法（先归一化，见 _normalize_reply）。
_ACCOUNT_NO_WORDS = frozenset({
    "没有", "没", "无", "没有账号", "无账号", "没账号", "还没有", "没有有账号",
    "新", "新号", "新的", "新账号", "开新号", "新开", "no", "none", "0",
})

#: 买家回答「我有账号」但还没发账号名。
_ACCOUNT_HAVE_WORDS = frozenset({"有", "有的", "有账号", "有啊", "有吧", "嗯有", "有的哦"})

#: 回答「要账号名」这一步时，这些词不是账号名（避免把「thanks」注册成账号）。
_ACCOUNT_STOP_WORDS = frozenset({
    "thanks", "thank", "thx", "ok", "okay", "yes", "no", "hi", "hello", "hey",
    "好的", "谢谢", "收到", "可以", "行", "嗯",
})

#: 买家回答「确认充值」的几种说法。
_ACCOUNT_YES_WORDS = frozenset({
    "确认", "确定", "对", "对的", "是", "是的", "好", "好的", "可以", "嗯", "恩",
    "ok", "okay", "yes", "y", "1", "充值", "充吧", "没问题",
})

#: 建号时的密码字符集：去掉 0/O/1/l/I 这些抄错之后再也找不回来的字符。
#: 买家会把它从卡密页手抄到登录页，少一次「密码错误」就少一次售后。
_PASSWORD_ALPHABET = "abcdefghjkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"

#: 闲鱼订单号一般是 19 位数字；放宽到 15~20 位，兼容少量老单号。
_ORDER_ID_RE = re.compile(r"(?<!\d)\d{15,20}(?!\d)")

#: 完整 Key：sk- 开头 20+ 位，或 40~64 位纯字母数字（new-api 列表里是裸串）。
#: 订单号是 15~20 位纯数字且长度不够，两边不会误判。
_API_KEY_RE = re.compile(
    r"(?<![A-Za-z0-9])(?:sk-[A-Za-z0-9]{20,64}|[A-Za-z0-9]{40,64})(?![A-Za-z0-9])"
)
#: 打码后的 Key（sk-ab12…wxyz / sk-***）：要能认出来，才能回一句准确的话。
_MASKED_KEY_RE = re.compile(r"sk-[A-Za-z0-9]{0,20}(?:…|\.{3}|\*{2,})", re.IGNORECASE)


def redact_keys(text: str) -> str:
    """把文本里的 Key 打成 sk-ab12…wxyz 再进日志。

    日志会被贴进工单、截图、发群；买家发的 Key 是凭证，不该原样留在日志里。
    数据库里的消息流水不动 —— 后台聊天页要看原文。
    """
    return _API_KEY_RE.sub(lambda match: mask_key(match.group(0)), str(text or ""))


def within_recall_window(created_at: str, seconds: int = RECALL_WINDOW_SECONDS) -> bool:
    try:
        created = datetime.strptime(str(created_at), "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return False
    age = (datetime.now() - created).total_seconds()
    return -10 <= age <= seconds


def _preview(text: str, limit: int = 120) -> str:
    """日志里只放前一小段，长话术文案打全会把日志刷爆。"""
    one_line = " ".join(redact_keys(str(text or "")).split())
    if len(one_line) <= limit:
        return one_line
    return one_line[:limit] + f"…（共 {len(one_line)} 字）"


def _clean_user_id(value: Any) -> str:
    """统一用户 ID：去掉 @goofish 之类的域后缀和首尾空白。"""
    return str(value or "").split("@", 1)[0].strip()


#: debug dump 里需要打码的字段：买家昵称/头像/联系方式，以及各种可定位 ID。
#: 日志经常被截图、贴群、进工单，完整负载等于把买家信息永久留档。
_PII_FIELD_RE = re.compile(
    r"(?:nick|name|avatar|phone|mobile|email|senderid|buyerid|userid|chatid|orderid|itemid|url)$",
    re.IGNORECASE,
)


def redact_payload(value: Any, depth: int = 0) -> Any:
    """递归给 debug 负载打码；保留结构用于排查，不保留买家 PII。"""
    if depth > 6:
        return "…"
    if isinstance(value, dict):
        return {
            str(key): ("***" if _PII_FIELD_RE.search(str(key)) else redact_payload(item, depth + 1))
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_payload(item, depth + 1) for item in value]
    if isinstance(value, str):
        return redact_keys(value[:300])
    return value


@dataclass(slots=True)
class PreparedDelivery:
    """已经准备好、只差发送的一份发货内容。

    上游建卡密 / 充值都会先落一条 provisioned 记录，发送失败时下一轮直接复用，
    绝不重新建 Key 或重复充值。
    """

    card_code: str
    amount_cny: float | None
    quota_usd: float
    content: str
    #: 上游令牌 id：充值模式靠它对账、也写进 orders.api_key_id 供历史回溯。
    api_key_id: str = ""
    #: 充值模式下买家指定的旧订单号（要充到哪一笔订单对应的 Key）。
    key_order_id: str = ""


@dataclass(slots=True)
class KeyOrderLookup:
    """买家提供的旧订单号 → 令牌 的解析结果。

    失败也带原因：跨商品和「查无此单」给买家的回复不一样，
    「等买家发旧订单号」那条记录的 detail 也要能看懂为什么没充。
    """

    resolved: dict[str, Any] | None = None
    #: "" 成功 / not_found 查无此单 / other_item 属于别的商品 /
    #: no_token 这一单没有令牌 / unreadable 令牌读不到（多半被删了）
    reason: str = ""
    #: 旧订单上记的商品 ID（对账用，早期订单可能是空）
    item_id: str = ""


def iso_from_ms(value: Any) -> str:
    """服务端毫秒时间戳 → 数据库里统一的本地时间字符串。"""
    try:
        ms = int(value or 0)
    except (TypeError, ValueError):
        ms = 0
    if ms <= 0:
        return now_iso()
    try:
        return datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
    except (OverflowError, OSError, ValueError):
        return now_iso()


def _parse_iso_epoch(value: str | None) -> float:
    """数据库里的 "YYYY-MM-DD HH:MM:SS" -> epoch 秒；解析不了返回 0。"""
    text = str(value or "").strip()
    if not text:
        return 0.0
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text[:19], fmt).timestamp()
        except ValueError:
            continue
    return 0.0


class AccountWorker:
    """把数据库里的一个账号跑成一条常驻任务。"""

    def __init__(self, db: Database, account: dict[str, Any], engine: DeliveryEngine,
                 provisioner: Provisioner | None = None,
                 reply_engine: AutoReplyEngine | None = None) -> None:
        self.db = db
        self.account = account
        self.account_id = int(account["id"])
        self.name = account["name"]
        self.engine = engine
        self.provisioner = provisioner
        #: 商品 / 关键词自动回复。和发货互不干扰，两条路各自独立判断。
        self.reply_engine = reply_engine or AutoReplyEngine(db)
        self.client: XianyuClient | None = None
        self.status = "idle"
        self.last_error = ""
        #: 登录态：ok / unknown / expired。
        #: 只由「业务接口」给出（连上 IM 或 verify_login 成功），
        #: 绝不由 passport 的 hasLogin 给出 —— 那个接口对任何 Cookie 都回同一句话。
        self.login_state = "unknown"
        self.login_checked_at = ""
        #: 上次真正重新登录的时刻（epoch 秒）。闲鱼网页登录态按它算 TTL。
        self.login_at = 0.0
        #: 是否已经到了「该重新扫码了」的预警线
        self.relogin_due = False
        self._relogin_warned_at = 0.0
        self._task: asyncio.Task[None] | None = None
        #: 订单轮询兜底任务（IM 推送丢了也能发货）
        self._poll_task: asyncio.Task[None] | None = None
        #: 买家 -> 上次「历史订单里也找不到 Key」的时间（monotonic）。
        #: 没有它时，一个 topup_missing=fail 的订单会每 30 秒去 new-api 扫一遍令牌列表。
        self._buyer_key_miss: dict[str, float] = {}
        #: 买家 -> (旧订单号, 记录时间)：覆盖「买家消息比订单轮询先到」的窗口
        self._key_order_hints: dict[str, tuple[str, float]] = {}
        #: 买家 -> (已核对归属的令牌 id, 记录时间)：买家先发 Key、订单后到
        self._key_hints: dict[str, tuple[str, float]] = {}
        #: 买家 -> 试 Key 的次数 / 首次时间：防止有人刷屏（每次都可能有上游调用）
        self._key_attempts: dict[str, tuple[int, float]] = {}
        #: 买家 -> 账号名查询次数 / 首次时间：防止拿机器人当用户名 oracle
        self._account_lookups: dict[str, tuple[int, float]] = {}
        #: 令牌 id -> (明文 Key, 读取时间)：按 Key 认令牌时避免重复读
        self._token_key_cache: dict[str, tuple[str, float]] = {}
        #: (读取时间, 本站令牌列表)：按 Key 认令牌时先拿列表缩小范围
        self._site_tokens_cache: tuple[float, list[dict[str, Any]]] | None = None
        #: 充值订单 -> 上次提示买家发旧订单号的时间，避免失败重试时反复催
        self._prompted_orders: dict[str, float] = {}
        #: 正在处理「买家提供旧订单号」的充值订单：同一条回复被重复投递时只做一次
        self._key_claims: set[str] = set()
        #: 正在处理「买家提供了账号」的账号订单：买家连点两次「确认」也只充一次
        self._account_claims: set[str] = set()
        #: 会话列表轮询兜底任务（推送里拿不到正文时靠它收消息）
        self._msg_poll_task: asyncio.Task[None] | None = None
        #: 自动评价兜底轮询（推送漏了评价请求卡片时补一次）
        self._rate_poll_task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        #: 「该去查一次待发货订单了」的信号。推送本身解析不出正文（实测大多如此），
        #: 但「有推送」是「可能刚有人拍下」最便宜的线索，用它提前叫醒轮询。
        self._poll_wakeup = asyncio.Event()
        self._last_poll_at = 0.0
        #: 正在发货中的订单号，防止「推送 + 轮询」同时命中同一单发两次
        self._inflight: set[str] = set()
        #: 会话列表的第一轮只做基线（比连接时刻更早的消息不回）
        self._message_baseline_done = False
        #: chat_id -> 这个会话已经处理到的时间水位（毫秒）。
        #: 补拉历史时以它为下限，避免把启动前的旧消息当成新消息回一遍。
        self._chat_watermarks: dict[str, int] = {}
        #: 本连接建立的时刻（毫秒），用来区分「历史消息」和「新消息」
        self._connected_at_ms = 0
        #: 已经提示过「收到未读通知但没正文」的会话，避免刷屏
        self._unread_notified: set[str] = set()
        #: (chat_key, rule_id)：正在发的那条自动回复，防止同一会话被连发两次
        self._replying: set[tuple[str, int, int]] = set()
        #: 在飞的自动回复任务，stop() 时一并取消
        self._reply_tasks: set[asyncio.Task[None]] = set()
        #: 在飞的发货任务。真实发送要等 ack，而 ack 必须由收帧主循环 dispatch；
        #: 如果在 _on_frame 里直接 await，会把主循环自己锁死到超时。
        self._delivery_tasks: set[asyncio.Task[None]] = set()
        #: 后台发货的并发上限，避免一次推送风暴同时打爆上游。
        self._delivery_sem = asyncio.Semaphore(max(1, settings.order_poll_concurrency))
        #: 在飞的自动评价任务 / 正在评价的订单号（消息和轮询可能同时命中）
        self._rate_tasks: set[asyncio.Task[None]] = set()
        self._rate_inflight: set[str] = set()
        #: order_id -> (取回时刻, 订单详情)，避免同一单查两遍
        self._order_cache: dict[str, tuple[float, dict[str, Any] | None]] = {}
        self._hint_task: asyncio.Task[None] | None = None
        #: 推送触发轮询的当前间隔。一直查不到东西就往后翻倍，
        #: 免得闲鱼一直推控制包时把请求量顶到风控线；查到东西就恢复。
        self._hint_gap = settings.order_hint_poll_gap
        #: 这一轮轮询是不是推送叫醒的（决定要不要调整 _hint_gap）
        self._poll_by_hint = False
        #: (买家 id, 昵称)：已经回填过历史记录的，别每条消息都扫一次表
        self._name_backfilled: set[str] = set()
        #: 本次进程是否已经扫过「历史里叫买家」的旧记录
        self._names_repaired = False
        #: (会话:消息 id) -> 处理失败次数，用于「撤销 seen 标记」限流
        self._seen_failures: dict[str, int] = {}

    # ------------------------------------------------------------ 生命周期

    @property
    def is_running(self) -> bool:
        """后台任务是否还活着（扫码重登后要靠它判断该不该重启）。"""
        return self._task is not None and not self._task.done()

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self.run(), name=f"worker-{self.name}")

    async def stop(self) -> None:
        self._stop.set()
        for task in list(self._reply_tasks):
            task.cancel()
        self._reply_tasks.clear()
        delivery_tasks = list(self._delivery_tasks)
        for task in delivery_tasks:
            task.cancel()
        self._delivery_tasks.clear()
        for task in list(self._rate_tasks):
            task.cancel()
        self._rate_tasks.clear()
        self._rate_inflight.clear()
        self._replying.clear()
        if self._hint_task is not None:
            self._hint_task.cancel()
            self._hint_task = None
        if self.client is not None and self.client.websocket is not None:
            await self.client.websocket.close()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: S110 - 见下方说明
                # 停止时任务怎么结束都行（正常退出/异常/被取消），
                # 这里只关心「它已经不再跑了」；真要排查异常，
                # worker 自己已经在 run() 里逐次记过日志了。
                pass
        if self.client is not None:
            await self.client.close()
        if delivery_tasks:
            # 等它们把 send_unknown/退卡这些收尾动作做完；不 await 的话
            # 事件循环关闭时任务可能还卡在 DB/WS 上。
            await asyncio.gather(*delivery_tasks, return_exceptions=True)
        self.status = "stopped"

    async def run(self) -> None:
        """主循环：永不为单条消息的异常而退出。

        最容易误判的是 FAIL_SYS_USER_VALIDATE / RGV587：它们是风控/滑块，
        不是 Cookie 失效。老代码把这类响应当成 CookieExpired 直接退出，
        用户只能重新扫码；新 Cookie 又触发同一套风控，于是「总是掉登录」。
        现在单独归类为 RiskControl，长退避后继续重试。
        """
        attempt = 0
        risk_attempt = 0
        await self._load_login_meta()
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                await self._once()
                # 连接活过一分钟才认为这次重连是稳定的，重置退避。
                # 如果刚连上就被服务端断开，也要退避，不能立刻重连狂打接口。
                if time.monotonic() - started > 60:
                    attempt = 0
                    risk_attempt = 0
                else:
                    delay = BACKOFF[min(attempt, len(BACKOFF) - 1)]
                    attempt += 1
                    logger.warning("[%s] 连接已断开，%ss 后重连", self.name, delay)
                    try:
                        await asyncio.wait_for(self._stop.wait(), timeout=delay)
                    except asyncio.TimeoutError:
                        continue
            except CookieExpired as exc:
                self.status = "cookie_expired"
                self.login_state = "expired"
                self.last_error = str(exc)
                logger.error(
                    "[%s] Cookie 真的失效（%s），停止重连；请到后台重新扫码。"
                    "顺带说明：闲鱼网页登录态是按登录时刻算 TTL 的（实测 24 小时整），"
                    "刷新 _m_h5_tk、调 hasLogin 都延长不了它；"
                    "Cookie 里带 cookie17（保持登录票据）才可能挂更久",
                    self.name, exc,
                )
                return
            except RiskControl as exc:
                self.status = "risk_control"
                self.last_error = str(exc)
                delay = RISK_BACKOFF[min(risk_attempt, len(RISK_BACKOFF) - 1)]
                risk_attempt += 1
                logger.warning(
                    "[%s] 触发闲鱼风控/人机验证（不是 Cookie 失效），%ss 后重试：%s",
                    self.name, delay, exc,
                )
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    continue
            except TokenExpired as exc:
                self.status = "token_expired"
                self.last_error = str(exc)
                delay = BACKOFF[min(attempt, len(BACKOFF) - 1)]
                attempt += 1
                logger.warning("[%s] IM token 刷新失败，%ss 后重试：%s", self.name, delay, exc)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.status = "error"
                self.last_error = str(exc)
                delay = BACKOFF[min(attempt, len(BACKOFF) - 1)]
                attempt += 1
                logger.warning("[%s] 连接异常：%s，%ss 后重连", self.name, exc, delay)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    continue

    async def _run_delivery(self, msg: IncomingMessage, decision: Any) -> None:
        """后台发货入口：限制并发，不阻塞收帧主循环。"""
        async with self._delivery_sem:
            try:
                await self._deliver(msg, decision)
            except (RiskControl, CookieExpired, TokenExpired) as exc:
                # 后台任务里的风控/登录失效也要让账号进入退避，
                # 不能只记一条「后台发货任务异常」就继续按原节奏打接口。
                await self._pause_for_account_error(exc)
                raise

    def _delivery_done(self, task: asyncio.Task[None]) -> None:
        """后台发货任务结束：取走异常，别让 asyncio 只打一条未取回警告。"""
        self._delivery_tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error("[%s] 后台发货任务异常：%s", self.name, exc, exc_info=exc)

    async def _persist_cookie(self, cookie: str) -> None:
        """客户端刷新 Cookie 后写回数据库，进程重启也能接着用。"""
        if not cookie or cookie == self.account.get("cookie"):
            return
        self.account["cookie"] = cookie
        await self.db.update_account_cookie(self.account_id, cookie)
        logger.info("[%s] Cookie 已刷新并写回数据库", self.name)

    async def _once(self) -> None:
        started = time.monotonic()
        # 连上之前先把历史里叫「买家」的记录按聊天记录修一遍。
        # 只读本地库、每个账号每次启动跑一次，纯粹是清旧账 —— 新数据
        # 在发货链路上就会补名，不依赖这一步。
        await self._repair_placeholder_names()
        self._connected_at_ms = int(time.time() * 1000)
        try:
            self.client = XianyuClient(self.account["cookie"])
        except XianyuError as exc:
            # Cookie 里连 unb 都没有：再用同一个 Cookie 重试多少次都没用，
            # 直接归为「需要重新扫码」，不要每 5 秒重连一次刷日志。
            self.status = "cookie_expired"
            self.last_error = str(exc)
            raise CookieExpired(str(exc)) from exc
        self.client.on_cookie_refresh = self._persist_cookie
        client = self.client
        try:
            ws = await client.connect()
            self.status = "online"
            self.last_error = ""
            # 能换到 accessToken 并完成 /reg，就说明登录态此刻是有效的
            self.login_state = "ok"
            self.login_checked_at = now_iso()
            self.frames_seen = 0
            self.messages_seen = 0
            logger.info("[%s] 已连接闲鱼 IM（user_id=%s）", self.name, client.user_id)
            # 心跳已经由 client.connect() 在注册前启动，这里只做保活/刷 token
            maintenance = asyncio.create_task(self._maintenance_loop())
            self._poll_task = asyncio.create_task(self._order_poll_loop())
            self._msg_poll_task = asyncio.create_task(self._message_poll_loop())
            self._rate_poll_task = asyncio.create_task(self._rate_poll_loop())
            try:
                async for raw in ws:
                    await self._on_frame(raw)
            finally:
                maintenance.cancel()
                if self._poll_task is not None:
                    self._poll_task.cancel()
                    self._poll_task = None
                if self._msg_poll_task is not None:
                    self._msg_poll_task.cancel()
                    self._msg_poll_task = None
                if self._rate_poll_task is not None:
                    self._rate_poll_task.cancel()
                    self._rate_poll_task = None
        finally:
            # 连上不到 20 秒就断、且一帧都没收到：大概率是 token/设备不匹配，
            # 清掉缓存，别让下一次重连继续用同一个坏 token。
            if time.monotonic() - started < 20 and not getattr(self, "frames_seen", 0):
                client.invalidate_im_token()
            await client.close()
            self.client = None
            if self.status == "online":
                self.status = "disconnected"

        # 保活/轮询发现 Cookie 已真正过期或触发风控：把异常抛给 run()，
        # 让它停止重连（Cookie 失效）或长退避（风控），不要在这里吞掉。
        if self.status == "cookie_expired":
            raise CookieExpired(self.last_error or "Cookie 会话已过期")
        if self.status == "risk_control":
            raise RiskControl(self.last_error or "触发闲鱼风控")
        if self.status == "token_expired":
            raise TokenExpired(self.last_error or "IM token 失效")

    async def _pause_for_account_error(self, exc: Exception) -> None:
        """把账号级错误（风控/登录失效/token 失效）交给 run() 统一退避。

        轮询任务是独立 task，自己 catch 掉异常就会让 worker 一直显示 online、
        继续按原节奏打接口。这里设置状态并关掉长连接，_once 退出后 run()
        会按 RiskControl/CookieExpired/TokenExpired 各自的分支处理。
        """
        if isinstance(exc, CookieExpired):
            self.status = "cookie_expired"
        elif isinstance(exc, TokenExpired):
            self.status = "token_expired"
        else:
            self.status = "risk_control"
        self.last_error = str(exc)
        logger.warning("[%s] 账号级错误（%s），暂停当前连接：%s",
                       self.name, self.status, exc)
        if self.client is not None and self.client.websocket is not None:
            try:
                await self.client.websocket.close()
            except Exception as close_exc:
                logger.debug("[%s] 关闭长连接失败：%s", self.name, close_exc)

    async def _maintenance_loop(self) -> None:
        """登录态维护：定期 hasLogin（合并 Cookie）+ 业务接口自检 + 到期预警。

        三条节奏（参考 xianyu-pilot）：
          - 30 分钟一次 passport hasLogin：**只合并 Cookie**，它判断不了登录态
            （对有效 Cookie 和垃圾 Cookie 回的是同一句话，见 client.keepalive）；
          - 同样 30 分钟一次业务接口自检（verify_login）：这才是「还活着吗」；
          - 1.5~2.5 小时随机刷新 _m_h5_tk：签名令牌只有 90 分钟寿命，不能等它过期。

        另外按「上次真正登录的时刻」算年龄：闲鱼网页登录态实测 24 小时整到期，
        到了预警线（XY_RELOGIN_WARN_HOURS）就在日志/后台提示重扫，
        而不是等某一单发货时才发现发不出去。
        """
        if self.client is None:
            return
        keepalive_interval = settings.session_keepalive_interval
        refresh_min = settings.m_h5_tk_refresh_min
        refresh_max = settings.m_h5_tk_refresh_max
        next_keepalive = time.monotonic() + keepalive_interval if keepalive_interval > 0 else float("inf")
        next_refresh = (
            time.monotonic() + random.uniform(refresh_min, refresh_max)
            if refresh_max > 0 and refresh_min >= 0
            else float("inf")
        )
        if next_keepalive == float("inf") and next_refresh == float("inf"):
            return

        while True:
            delay = min(next_keepalive, next_refresh) - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)

            if time.monotonic() >= next_refresh:
                try:
                    if self.client is not None and await self.client.refresh_m_h5_tk():
                        logger.info("[%s] _m_h5_tk 主动刷新成功", self.name)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("[%s] _m_h5_tk 主动刷新失败：%s", self.name, exc)
                next_refresh = time.monotonic() + random.uniform(refresh_min, refresh_max)

            if time.monotonic() >= next_keepalive:
                try:
                    if self.client is not None:
                        # 先合并一次 Cookie（万一哪天 passport 真的开始续期），
                        # 再用业务接口确认登录态 —— 顺序不能反：自检抛错就不再合并了。
                        await self.client.keepalive()
                        await self._check_login_state()
                except (CookieExpired, RiskControl, TokenExpired) as exc:
                    await self._pause_for_account_error(exc)
                    return
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("[%s] Cookie 保活失败（不影响当前连接）：%s", self.name, exc)
                next_keepalive = time.monotonic() + keepalive_interval

    # ------------------------------------------------------------ 登录态

    async def _load_login_meta(self) -> None:
        """读出「上次真正登录的时刻」，用来算登录态还剩多久。"""
        try:
            raw = await self.db.get_login_at(self.account_id)
        except Exception as exc:  # 读不到不影响跑单
            logger.debug("[%s] 读取登录时间失败：%s", self.name, exc)
            raw = None
        self.login_at = _parse_iso_epoch(raw)
        if self.login_at <= 0:
            # 老账号（这个功能上线前登录的）没有记录。**不能**拿建号时间兜底：
            # 那会让一个刚登录没多久的号立刻被标成「该重扫了」，预警就没人信了。
            # 从现在开始计时，并把基线落库，重启后不会重置。
            self.login_at = time.time()
            try:
                await self.db.record_login(self.account_id)
            except Exception as exc:  # 写不进去也只是预警偏晚，不影响接单
                logger.debug("[%s] 写入登录时间基线失败：%s", self.name, exc)
            logger.info(
                "[%s] 没有登录时间记录（老账号），从现在开始计时；"
                "真实登录可能更早，到期预警会相应偏晚",
                self.name,
            )
        self._warn_relogin_if_due()

    async def _check_login_state(self) -> None:
        """业务接口自检 + 到期预警。

        只有 CookieExpired（登录态真的没了）才往上抛，让 worker 停下来等人重扫。
        风控 / 令牌问题**不**在这里掐断长连接：这是每 30 分钟一次的主动探测，
        为它把一条正常在收消息的连接踢掉不划算 —— 订单轮询本来就会用同一套
        规则处理同类错误，让它去触发退避即可。
        """
        if self.client is None:
            return
        try:
            await self.client.verify_login()
        except CookieExpired:
            raise
        except (RiskControl, TokenExpired) as exc:
            self.login_state = "unknown"
            logger.warning(
                "[%s] 登录态自检遇到风控/令牌问题（保持当前连接，交给订单轮询按同样规则退避）：%s",
                self.name, exc,
            )
            return
        self.login_state = "ok"
        self.login_checked_at = now_iso()
        self._warn_relogin_if_due()

    def _warn_relogin_if_due(self) -> None:
        """到了预警线就提醒一次（每小时最多一条，别把日志刷爆）。"""
        warn_hours = settings.relogin_warn_hours
        if warn_hours <= 0 or self.login_at <= 0:
            return
        age = time.time() - self.login_at
        self.relogin_due = age >= warn_hours * 3600
        if not self.relogin_due:
            return
        if time.monotonic() - self._relogin_warned_at < 3600:
            return
        self._relogin_warned_at = time.monotonic()
        logger.warning(
            "[%s] 登录已 %.1f 小时（闲鱼网页登录态实测 24 小时整到期），建议现在到后台重新扫码。"
            "想少扫几次：让 Cookie 里带上 cookie17（浏览器里勾「保持登录」再复制整条 Cookie）",
            self.name, age / 3600,
        )

    def login_report(self) -> dict[str, Any]:
        """给后台看的一小块状态：登录态、登录多久了、该不该重扫、有没有长效票据。"""
        flags = cookie_flags(self.account.get("cookie", "") or "")
        age_hours = None
        if self.login_at > 0:
            age_hours = round((time.time() - self.login_at) / 3600, 1)
        return {
            "login_state": self.login_state,
            "login_checked_at": self.login_checked_at,
            "login_at": (
                datetime.fromtimestamp(self.login_at).strftime("%Y-%m-%d %H:%M:%S")
                if self.login_at > 0 else ""
            ),
            "login_age_hours": age_hours,
            "relogin_due": self.relogin_due,
            "has_cookie17": flags["has_cookie17"],
            "missing_cookies": flags["missing"],
        }

    # ------------------------------------------------------------ 消息处理

    async def _on_frame(self, raw: str | bytes) -> None:
        import json

        if self.client is None or self.client.websocket is None:
            return
        # 收到任何帧都说明连接还活着（心跳循环用它判断是否静默断开）
        mark_inbound = getattr(self.client, "mark_inbound", None)
        if mark_inbound is not None:
            mark_inbound()
        try:
            frame = json.loads(raw)
        except (TypeError, ValueError):
            return
        if not isinstance(frame, dict):
            return

        # 任何帧都顺手记一下 sid（/reg 回包里带的，create_chat 要用）
        note_sid = getattr(self.client, "note_sid", None)
        if note_sid is not None:
            note_sid(frame)

        # 先看这是不是我们正在等回包的请求响应（create_chat 之类）
        dispatch = getattr(self.client, "dispatch", None)
        if dispatch is not None and dispatch(frame):
            return

        # 任何推送都要回执，否则会被反复重推。
        # 注意只回显 mid/sid/app-key/ua/dt 五个头（见 client.ack）：
        # 参考实现曾因回显全部 headers 被服务端 11 秒后 1006 断连。
        try:
            await self.client.ack(self.client.websocket, frame)
        except Exception as exc:  # 回执失败不影响后续处理
            logger.debug("[%s] 回执失败（不影响后续处理）：%s", self.name, exc)

        self.frames_seen = getattr(self, "frames_seen", 0) + 1
        # 收到推送 = 可能刚有人下单。正文解析不出来也不要紧，让轮询立刻补一次。
        self._hint_poll()
        payloads, failures = sync_payloads_detailed(frame)
        if payloads and self.frames_seen <= 3:
            logger.info("[%s] 收到推送帧 #%s，内含 %s 条负载", self.name, self.frames_seen, len(payloads))
        if failures:
            # 以前这类 data 会被静默跳过，于是「闲鱼改版了」和「没人发消息」
            # 在日志里长得一模一样。现在至少能看见「有东西来了但解不开」。
            logger.warning("[%s] 这一帧里有 %s 条负载解不开（%s）：%s",
                           self.name, len(failures), frame_shape(frame),
                           "；".join(f"#{i} {err}" for i, err in failures[:3]))
        if not payloads:
            if self.frames_seen <= 3:
                logger.info("[%s] 收到不含业务负载的帧 #%s：%s",
                            self.name, self.frames_seen, frame_shape(frame))
            else:
                logger.debug("[%s] 收到不含业务负载的帧：%s", self.name, frame_shape(frame))
        for payload in payloads:
            try:
                await self._handle_payload(payload)
            except (RiskControl, CookieExpired, TokenExpired):
                # 这类不是「坏消息」，是账号级状态：必须让 _once 退出，
                # 交给 run() 做长退避或停止重连，不能在这里被吞掉。
                raise
            except Exception as exc:  # 单条坏消息不该拖垮连接
                logger.exception("[%s] 处理消息出错：%s", self.name, exc)

    async def _handle_payload(self, payload: dict[str, Any]) -> None:
        import json as _json

        msg = parse_message(
            payload, my_id=_clean_user_id(self.client.user_id if self.client else "")
        )
        if msg is None:
            chat_id = unread_sync_chat_id(payload)
            if chat_id:
                # 只有会话 ID、没有正文的未读通知：买家确实发了消息，
                # 只是正文不在这次推送里。这条日志是排查「自动回复不生效」的关键 ——
                # 有它就说明推送通了、是我们的解析或注册位点有问题，而不是没人在说话。
                if chat_id not in self._unread_notified:
                    self._unread_notified.add(chat_id)
                    cap_set(self._unread_notified, 2000)
                    logger.warning(
                        "[%s] 会话 %s 有新消息，但这一帧只有未读标记、没有正文 —— "
                        "推送通得到，正文没拿到（注册位点或推送格式问题）",
                        self.name, chat_id,
                    )
                else:
                    logger.debug("[%s] 会话 %s 未读通知（无正文）", self.name, chat_id)
                return
            # 会话引导（contentType=8）是已知噪声，不 dump 也不留痕
            if not is_guide_payload(payload):
                if settings.debug_payload:
                    # 买家可能把 Key 贴在消息里：dump 也要打码，日志是会被转发的
                    logger.info("[%s][DUMP-未解析] %s", self.name,
                                _json.dumps(redact_payload(payload), ensure_ascii=False)[:2000])
                note_unparsed(payload)
            return
        # 卖家自己在闲鱼 App / 网页里发的消息也会推过来。以前直接 return，
        # 后台就永远看不到自己发出去的那半段对话。现在只记聊天记录，
        # 绝不能进入 _process_incoming（否则会自己回自己）。
        if msg.from_me:
            await self._record_outgoing_message(msg)
            return

        if settings.debug_payload:
            # 同上：debug dump 里也不能留明文 Key
            logger.info("[%s][DUMP-已解析] content=%r item=%s order=%s 原文=%s",
                        self.name, redact_keys(msg.content[:60]), msg.item_id,
                        msg.order_id,
                        _json.dumps(redact_payload(payload), ensure_ascii=False)[:1200])

        await self._process_incoming(msg)

    async def _process_incoming(self, msg: IncomingMessage, *,
                                allow_auto_reply: bool = True) -> asyncio.Task[None] | None:
        """一条买家消息的完整处理：记流水 → 自动回复 → 评估发货。

        推送和「拉会话列表」两条路都走这里，保证行为一致。
        allow_auto_reply=False 用于系统消息（系统提示卡片、评价提醒之类）：
        照常记录、照常判断要不要发货，但不回那句「在的」。
        """
        # 推送和「拉会话列表」会拿到同一条消息（服务端 push 一份、列表里还有一份）。
        # 以前只有拉取路径记 seen，于是同一条消息会被完整处理两遍：
        # 消息流水重复、decide 白跑、once_per_chat=False 时还会真的回两遍。
        # 两边用同一个键（会话:messageId），在这里统一挡住。
        seen_key = f"{msg.chat_id}:{msg.message_id}" if msg.message_id else ""
        if seen_key and not await self.db.claim_message_seen(self.account_id, seen_key):
            logger.debug("[%s] 消息 %s 已处理过，跳过", self.name, seen_key)
            return None
        try:
            return await self._handle_incoming(msg, allow_auto_reply=allow_auto_reply)
        except BaseException:
            # 处理失败就把「已处理」撤掉，否则这条消息永远丢了（自动回复不会补、
            # 发货不会重试）。失败次数太多的就不再重试，免得一条坏消息刷屏。
            await self._forget_seen(seen_key)
            raise

    async def _record_outgoing_message(self, msg: IncomingMessage) -> None:
        """把卖家自己发的消息记进聊天流水（只记录，不触发任何自动动作）。

        自己发出去的消息没有买家 sender_id，落库时统一挂到会话对端买家名下，
        和后台手动发送、自动回复的流水保持一致；聊天界面据此渲染右侧气泡。
        """
        chat_id = str(msg.chat_id or "").strip()
        if not chat_id:
            logger.warning("[%s] 忽略没有会话 ID 的己方消息：%r", self.name, msg.message_id)
            return

        own_id = _clean_user_id(self.client.user_id if self.client else "")
        peer = _clean_user_id(msg.peer_id)
        if peer and peer == own_id:
            # 异常/历史脏数据可能把卖家自己当成对端，绝不能写进 sender_id，
            # 否则会话列表和后续回消息都会指向自己。
            logger.warning("[%s] 己方消息的对端是自己，已忽略：%s", self.name, peer)
            peer = ""
        if not peer:
            peer = _clean_user_id(await self.db.chat_peer_id(self.account_id, chat_id))
            if peer == own_id:
                peer = ""
        name = ""
        if peer:
            try:
                name = await self.db.known_buyer_name(self.account_id, peer) or ""
            except Exception:
                name = ""
        if not name:
            name = "买家" if peer else ""

        await self.db.log_synced_message(
            account_id=self.account_id,
            chat_id=chat_id,
            sender_id=peer,
            sender_name=name,
            item_id=msg.item_id,
            item_title=msg.item_title,
            order_id=msg.order_id,
            content=msg.content,
            kind="outgoing",
            message_type=msg.message_type,
            media_url=msg.media_url or None,
            media_thumb_url=msg.media_thumb_url or None,
            media_width=msg.media_width,
            media_height=msg.media_height,
            media_duration=msg.media_duration,
            external_id=msg.message_id or None,
            read_at=now_iso(),
            created_at=iso_from_ms(msg.created_at),
        )
        # 人都在闲鱼里回消息了，这个会话显然已经看过；顺手清掉后台的红点。
        await self.db.mark_chat_read(self.account_id, chat_id)
        logger.debug("[%s] 记录自己发出的消息（会话 %s，买家 %s）",
                     self.name, chat_id, peer or "-")

    async def _forget_seen(self, seen_key: str) -> None:
        """异常路径专用：撤销 seen 标记，让这条消息下一轮还能重来。

        同一条消息连续失败超过 SEEN_RETRY_LIMIT 次就放弃（否则一条解析不了
        的消息会被每 20 秒重试一次，日志刷屏、也没人修）。
        """
        if not seen_key:
            return
        attempts = self._seen_failures.get(seen_key, 0) + 1
        if len(self._seen_failures) > 500:
            # 别让这张表本身成为内存泄漏
            self._seen_failures.clear()
        self._seen_failures[seen_key] = attempts
        if attempts > SEEN_RETRY_LIMIT:
            logger.error("[%s] 消息 %s 已连续失败 %s 次，放弃重试", self.name, seen_key, attempts)
            return
        try:
            await self.db.unmark_messages_seen(self.account_id, [seen_key])
            logger.warning("[%s] 消息 %s 处理失败，已撤销去重标记，下一轮会重试（第 %s 次）",
                           self.name, seen_key, attempts)
        except Exception as exc:
            logger.warning("[%s] 撤销消息 %s 的去重标记失败：%s", self.name, seen_key, exc)

    async def _handle_incoming(self, msg: IncomingMessage, *,
                               allow_auto_reply: bool = True) -> asyncio.Task[None] | None:
        """_process_incoming 的主体（去重之后再执行）。"""
        # 交易卡片本身不带昵称（reminderTitle 是「等待你发货」这类标题），
        # 先按「聊天记录」把真名找回来，再落库 —— 顺序反了的话这条流水
        # 会永久停在「买家」上，而 new-api 的令牌名正是拿它拼的。
        await self._resolve_buyer_name(msg)

        await self.db.log_message(
            account_id=self.account_id,
            chat_id=msg.chat_id,
            sender_id=msg.sender_id,
            sender_name=msg.sender_name,
            item_id=msg.item_id,
            # 标题不进 messages 表，db.log_message 会把它记到 item_titles 里，
            # 会话列表就能显示商品名而不是一串数字
            item_title=msg.item_title,
            order_id=msg.order_id,
            content=msg.content,
            kind="incoming",
            message_type=msg.message_type,
            media_url=msg.media_url or None,
            media_thumb_url=msg.media_thumb_url or None,
            media_width=msg.media_width,
            media_height=msg.media_height,
            media_duration=msg.media_duration,
            # 聊天界面按需补拉历史时靠它去重，避免同一句话出现两个气泡
            external_id=msg.message_id or None,
            # 用服务端时间，而不是落库时间：历史补拉/离线消息的发送时间
            # 不能全都变成「刚拉到的那一刻」，否则聊天顺序会乱。
            created_at=iso_from_ms(msg.created_at),
        )
        # 评价请求卡片也是系统消息（会被下面 is_ignored 跳过），
        # 所以必须在 return 之前处理；它只排后台任务，不阻塞收帧。
        await self._maybe_auto_rate(msg)
        if is_ignored(msg.content):
            logger.debug("[%s] 忽略系统消息：%s", self.name, _preview(msg.content, 80))
            return None

        logger.info("[%s] 收到 %s(%s) 关于商品 %s 的消息：%s",
                    self.name, msg.sender_name, msg.sender_id, msg.item_id or "-",
                    _preview(msg.content))

        # 充值模式可能正在等买家发「旧订单号」。命中就先把充值做完，
        # 不要再让自动回复插一句「在的」。
        if await self._maybe_handle_key_order(msg):
            return None

        # 账号模式可能正在等买家回「有没有账号 / 要什么用户名 / 确认充值」。
        if await self._maybe_handle_account_reply(msg):
            return None

        # 自动回复在这之前走：买家问「在吗」不用等到发货逻辑走完才有人搭理。
        # 返回的任务由调用方决定要不要等 —— 收帧循环不能等（会卡住紧随其后的
        # 付款卡片），拉取路径可以等（保证同一会话的回复按顺序发出去）。
        reply_task = await self._maybe_auto_reply(msg) if allow_auto_reply else None

        # 所有付款触发都先去闲鱼核实订单，卡片也不例外。
        # 卡片形态只能证明「这条消息像交易卡片」，不能证明订单属于当前买家：
        # 转发别人的卡片、伪造 dxCard 负载都能过形态判断。只有订单接口返回的
        # 状态和 buyerId 都匹配，才允许进入发货决策。
        if msg.order_id and is_paid_trigger(msg.content):
            msg.order_verified = await self._verify_order_pending(msg.order_id, msg.sender_id)

        # 配置成固定文案 / 卡密池的规则在 decide 里就把文案渲染好了，
        # 名字必须在那之前是准的 —— 否则「你好 买家」会直接发给对方。
        # 已经发过货的单不查（重复的付款卡片很常见，没必要为它多打一次接口）。
        if (msg.order_id and is_placeholder_name(msg.sender_name)
                and is_paid_trigger(msg.content)
                and not await self.db.delivered_order_exists(self.account_id, msg.order_id)):
            await self._name_from_cached_order(msg, fetch=True)

        # 规则里配了「金额区间」时必须先拿到订单实付金额，否则分不清 1 元 / 5 元 / 10 元
        # 规格，会退化成「没有规则」。金额查询本来就有缓存，后面发货还会复用。
        if msg.order_id and (is_paid_trigger(msg.content) or msg.is_system_card):
            await self._maybe_fill_amount(msg)

        decision = await self.engine.decide(self.account_id, msg)
        if not decision.should_deliver and not msg.item_id and msg.order_id:
            # 付款卡片里常常不带 itemId（只有 orderId）。
            # 规则是按商品配的，不补这一步「按商品匹配」就永远匹配不上。
            item_id = await self._item_id_from_order(msg.order_id)
            if item_id:
                msg.item_id = item_id
                logger.info("[%s] 从订单 %s 补全商品 ID：%s", self.name, msg.order_id, item_id)
                decision = await self.engine.decide(self.account_id, msg)

        if not decision.should_deliver:
            logger.info("[%s] 不发货：%s", self.name, decision.reason)
            if decision.reason == AUTH_BLOCKED_REASON or decision.reason.startswith("缺少订单号"):
                await self._record_blocked(msg, decision.reason)
            return reply_task

        if isinstance(self.client, XianyuClient):
            # 真实客户端发货要等 ack，而 ack 回包只能由收帧主循环 dispatch。
            # 在 _on_frame 里直接 await 会把自己锁死到 15 秒超时，
            # 所以这里放到后台任务里跑，主循环继续收帧/回执/分发回包。
            task = asyncio.create_task(
                self._run_delivery(msg, decision), name=f"deliver-{self.name}"
            )
            self._delivery_tasks.add(task)
            task.add_done_callback(self._delivery_done)
        else:
            # 测试替身没有 ack 能力，保持原来的同步行为，测试语义不变。
            await self._deliver(msg, decision)
        return reply_task

    async def _resolve_buyer_name(self, msg: IncomingMessage) -> None:
        """把「买家」这种占位名换成真昵称；换到了就把历史流水一起纠正。

        只查本地聊天记录（不额外打接口）：真人发过言的消息里带的就是真名。
        订单接口那条路在 _provision / _fulfil_order 里顺手做 —— 那里本来就要查订单，
        不会为了一句昵称多花一次往返。
        """
        if is_placeholder_name(msg.sender_name):
            known = await self.db.known_buyer_name(self.account_id, msg.sender_id)
            if not known:
                return
            msg.sender_name = known
            logger.debug("[%s] 买家 %s 的昵称补全为 %s", self.name, msg.sender_id, known)
        await self._remember_buyer_name(msg.sender_id, msg.sender_name)

    async def _repair_placeholder_names(self) -> None:
        """启动时扫一遍「还叫买家」的旧记录，用聊天记录里的真名纠正（一次）。"""
        if self._names_repaired:
            return
        self._names_repaired = True
        try:
            changed = await self.db.repair_placeholder_buyer_names(self.account_id)
        except Exception as exc:  # 清旧账失败不影响收消息
            logger.warning("[%s] 修复历史买家昵称失败：%s", self.name, exc)
            return
        if changed:
            logger.info("[%s] 已按聊天记录修复 %s 条「买家」占位的记录", self.name, changed)

    async def _remember_buyer_name(self, buyer_id: str, name: str) -> None:
        """拿到真名后回填历史记录（同一个人只做一次）。"""
        if not buyer_id or is_placeholder_name(name):
            return
        key = f"{buyer_id}:{name}"
        if key in self._name_backfilled:
            return
        self._name_backfilled.add(key)
        cap_set(self._name_backfilled, 2000)
        try:
            changed = await self.db.backfill_buyer_name(self.account_id, buyer_id, name)
        except Exception as exc:  # 回填失败不影响这次发货
            logger.debug("[%s] 回填买家昵称失败：%s", self.name, exc)
            return
        if changed:
            logger.info("[%s] 买家 %s 的昵称「%s」已回填 %s 条历史记录",
                        self.name, buyer_id, name, changed)

    async def _name_from_order(self, msg: IncomingMessage, order: dict[str, Any] | None) -> None:
        """订单里带的是权威昵称（buyerInfoVO.userNick），拿它补当前消息与历史。"""
        if not order or not is_placeholder_name(msg.sender_name):
            return
        nick = buyer_nick_from_order(order)
        if not nick:
            return
        msg.sender_name = nick
        logger.info("[%s] 买家 %s 的昵称从订单补全为 %s", self.name, msg.sender_id, nick)
        await self._remember_buyer_name(msg.sender_id, nick)

    async def _name_from_cached_order(self, msg: IncomingMessage, *, fetch: bool = False) -> None:
        """用订单详情补昵称；默认只捡缓存里的那份。

        为什么不默认去查订单：发货链路上每一跳都是买家多等几百毫秒，
        不该为了一句昵称单独打一次接口。真正需要查订单的那几处
        （金额、商品 ID、订单核实）会顺手把结果放进缓存，这里捡起来用。

        fetch=True 只给一个场景用：这条消息一出就要发货（付款触发语），
        而手上只有「买家」这个名字。配置成固定文案 / 卡密池的规则
        不会查订单金额，缓存里自然也没有，这时候不查就只能把「你好 买家」
        发出去 —— 相比多等一次接口，发错名字更难收场。
        """
        if not msg.order_id or not is_placeholder_name(msg.sender_name):
            return
        cached = self._order_cache.get(msg.order_id)
        if cached is None and fetch:
            await self._find_order_cached(msg.order_id)
            cached = self._order_cache.get(msg.order_id)
        if cached:
            await self._name_from_order(msg, cached[1])

    async def _maybe_auto_reply(self, msg: IncomingMessage) -> asyncio.Task[None] | None:
        """商品 / 关键词自动回复：命中就排一条后台任务，不影响发货链路。

        只在 IM 推送这条路上做：订单轮询造出来的消息不是买家说的话，
        回「在的，拍下自动发货」只会像机器人刷屏。

        发消息故意不 await：自动回复里有个「像人工」的延迟，卡在收帧循环里
        等于让紧随其后的付款卡片多等一秒 —— 那才是真正耽误事的环节。
        返回排出去的任务，方便测试直接 await 它。
        """
        if not settings.auto_reply_enabled:
            return None
        if self.client is None or not msg.chat_id:
            return None
        try:
            decision = await self.reply_engine.decide(self.account_id, msg)
        except Exception as exc:
            logger.warning("[%s] 自动回复判断出错：%s", self.name, exc)
            return None
        if not decision.should_reply:
            logger.debug("[%s] 不自动回复：%s", self.name, decision.reason)
            return None

        claim = (
            decision.chat_key,
            int((decision.rule or {}).get("id") or 0),
            int(decision.entry_id or 0),
        )
        if claim in self._replying:
            logger.debug("[%s] 这条自动回复正在发送中，跳过重复触发", self.name)
            return None
        self._replying.add(claim)
        task = asyncio.create_task(
            self._send_auto_reply(msg, decision, claim),
            name=f"autoreply-{self.name}",
        )
        self._reply_tasks.add(task)
        task.add_done_callback(self._reply_tasks.discard)
        return task

    async def _send_auto_reply(self, msg: IncomingMessage, decision: ReplyDecision,
                               claim: tuple[str, int] | None = None) -> bool:
        """真正把自动回复发出去，并记一次 hit（去重 / 冷却靠它）。"""
        try:
            rule = decision.rule or {}
            delay = settings.auto_reply_delay
            if delay > 0:
                # 秒回看起来太像机器人；留一点「人工」的间隔，也顺便错开连发
                await asyncio.sleep(delay)
            if settings.dry_run:
                logger.warning("[%s][DRY-RUN] 本应自动回复「%s」：%s",
                               self.name, rule.get("name"), _preview(decision.content))
                return False
            if self.client is None:
                return False
            try:
                await self.client.send_text_safe(msg.chat_id, msg.sender_id, decision.content)
            except XianyuError as exc:
                logger.error("[%s] 自动回复发送失败：%s", self.name, exc)
                return False

            await self.db.record_reply_hit(
                self.account_id, msg.sender_id, decision.chat_key,
                int(rule.get("id") or 0), int(decision.entry_id or 0),
            )
            await self.db.log_message(
                account_id=self.account_id, chat_id=msg.chat_id, sender_id=msg.sender_id,
                sender_name=msg.sender_name, item_id=msg.item_id, order_id=msg.order_id,
                content=decision.content, kind="outgoing",
            )
            logger.info("[%s] 已自动回复 %s：%s", self.name, msg.sender_name, decision.reason)
            return True
        finally:
            if claim is not None:
                self._replying.discard(claim)

    # ------------------------------------------------------------ 自动评价

    async def _maybe_auto_rate(self, msg: IncomingMessage) -> asyncio.Task[None] | None:
        """评价请求卡片 -> 后台给买家好评。

        只认「卡片 + 订单号来自服务端结构化字段」的请求，避免买家自己打
        「快给ta一个评价吧」+ 别人的订单号来借刀评价。真正发请求放到后台
        任务里，收帧循环不因为评价接口多等几百毫秒。
        """
        if not msg.order_id:
            return None
        if not (msg.is_system_card and msg.order_id_trusted):
            return None
        if not is_rate_request(msg.content):
            return None
        order_id = rate_request_order(msg.order_id)
        if not order_id:
            return None

        try:
            overrides = await self.db.get_all_config()
        except Exception as exc:
            logger.warning("[%s] 读取自动评价配置失败：%s", self.name, exc)
            return None
        enabled, template = resolved_auto_rate(overrides)
        if not enabled:
            return None
        if order_id in self._rate_inflight:
            return None
        if await self.db.rate_record_blocks(self.account_id, order_id):
            logger.debug("[%s] 订单 %s 已评价过（或刚失败），跳过", self.name, order_id)
            return None

        feedback = render_feedback(
            template,
            buyer_name=msg.sender_name,
            item_id=msg.item_id,
            order_id=order_id,
        )
        task = asyncio.create_task(
            self._run_auto_rate(order_id, feedback, msg),
            name=f"autorate-{self.name}",
        )
        self._rate_tasks.add(task)
        task.add_done_callback(self._rate_tasks.discard)
        return task

    async def _run_auto_rate(
        self, order_id: str, feedback: str, msg: IncomingMessage | None = None
    ) -> bool:
        """真正调用评价接口并写记录。返回是否评价成功。"""
        if order_id in self._rate_inflight:
            return False
        if await self.db.rate_record_blocks(self.account_id, order_id):
            return False
        self._rate_inflight.add(order_id)
        try:
            if settings.dry_run:
                logger.warning("[%s][DRY-RUN] 本应评价订单 %s：%s", self.name, order_id, feedback)
                return False
            client = self.client
            if client is None:
                return False
            rate_func = getattr(client, "rate_buyer", None)
            if rate_func is None:
                logger.warning("[%s] 当前客户端不支持评价接口", self.name)
                return False
            ok, detail = await rate_func(order_id, feedback)
            await self.db.record_rate(
                account_id=self.account_id,
                order_id=order_id,
                status="rated" if ok else "failed",
                feedback=feedback,
                detail=detail,
                chat_id=msg.chat_id if msg else None,
                item_id=msg.item_id if msg else None,
                buyer_id=msg.sender_id if msg else None,
                buyer_name=msg.sender_name if msg else None,
            )
            if ok:
                logger.info("[%s] 已自动评价订单 %s：%s", self.name, order_id, feedback)
            else:
                logger.warning("[%s] 自动评价订单 %s 失败：%s", self.name, order_id, detail)
            return ok
        except Exception as exc:
            logger.warning("[%s] 自动评价订单 %s 异常：%s", self.name, order_id, exc)
            try:
                await self.db.record_rate(
                    account_id=self.account_id, order_id=order_id,
                    status="failed", feedback=feedback, detail=str(exc),
                    chat_id=msg.chat_id if msg else None,
                    item_id=msg.item_id if msg else None,
                    buyer_id=msg.sender_id if msg else None,
                    buyer_name=msg.sender_name if msg else None,
                )
            except Exception:
                logger.debug("[%s] 写评价失败记录也失败了", self.name, exc_info=True)
            return False
        finally:
            self._rate_inflight.discard(order_id)

    async def _rate_poll_loop(self) -> None:
        """兜底：评价请求推送偶尔会漏，定期扫最近成交订单补一次。

        只处理「交易成功 + 卖家未评价」的订单；间隔默认 600 秒，且每轮最多
        评价 3 单，避免把评价接口当成定时炸弹。
        """
        interval = settings.auto_rate_poll_interval
        if interval <= 0:
            return
        # 启动先等半分钟：刚连上时先让订单/消息通道跑起来，别一上来就批量评价
        await asyncio.sleep(min(30.0, float(interval)))
        while True:
            try:
                await self._poll_rate_candidates()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("[%s] 自动评价轮询出错：%s", self.name, exc)
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                raise

    async def _poll_rate_candidates(self) -> int:
        """扫一次最近订单，返回这一轮成功评价的数量。"""
        client = self.client
        if client is None or not hasattr(client, "list_sold_orders"):
            return 0
        overrides = await self.db.get_all_config()
        enabled, template = resolved_auto_rate(overrides)
        if not enabled:
            return 0
        orders = await client.list_sold_orders(20)  # type: ignore[attr-defined]
        done = 0
        for order in orders:
            if done >= 3:
                break
            if str(order.get("order_status") or "") != "交易成功":
                continue
            if str(order.get("seller_rate_status") or "") == "4":
                continue
            order_id = rate_request_order(str(order.get("order_id") or ""))
            if not order_id:
                continue
            if await self.db.rate_record_blocks(self.account_id, order_id):
                continue
            feedback = render_feedback(
                template,
                buyer_name=str(order.get("buyer_name") or ""),
                item_id=str(order.get("item_id") or ""),
                order_id=order_id,
            )
            msg = IncomingMessage(
                chat_id="",
                sender_id=str(order.get("buyer_id") or ""),
                sender_name=str(order.get("buyer_name") or ""),
                content="[自动评价轮询]",
                item_id=str(order.get("item_id") or "") or None,
                order_id=order_id,
                order_id_trusted=True,
            )
            if await self._run_auto_rate(order_id, feedback, msg):
                done += 1
            await asyncio.sleep(0.5)
        return done

    # ---------------------------------------------------- 后台手动聊天

    @property
    def is_connected(self) -> bool:
        """当前有没有一条可用的长连接（后台发消息前先看这个）。"""
        return self.client is not None and getattr(self.client, "websocket", None) is not None

    async def send_manual_text(
        self,
        chat_id: str,
        to_id: str,
        text: str,
        *,
        item_id: str | None = None,
        order_id: str | None = None,
        buyer_name: str = "",
    ) -> dict[str, Any]:
        """后台聊天界面发一条消息，并把它记进流水（和自动回复同一条通道）。

        这里不走去重/冷却那一套：人点「发送」就是明确要发，
        再拿「同一会话只回一次」挡他一次只会显得后台坏了。
        """
        if not self.is_connected:
            raise XianyuError("账号当前未连接闲鱼，消息发不出去（看「账号」页的连接状态）")
        body = str(text or "").strip()
        if not body:
            raise ValueError("消息内容不能为空")
        if len(body) > MAX_CHAT_TEXT:
            raise ValueError(f"消息太长（上限 {MAX_CHAT_TEXT} 字）")

        chat = str(chat_id or "").strip()
        if not chat:
            raise ValueError("缺少会话 ID")
        to = str(to_id or "").strip() or await self.db.chat_peer_id(self.account_id, chat) or ""
        if not to:
            raise ValueError("不知道这条消息发给谁（缺少买家 ID）")

        try:
            name = str(buyer_name or "").strip() or await self.db.known_buyer_name(self.account_id, to) or ""
        except Exception:  # 名字只是展示用，查不到不能挡住发送
            name = str(buyer_name or "").strip()

        external_id = ""
        client = self.client
        assert client is not None
        # 能拿 messageId 就走「等 ack」的版本：后台要显示撤回按钮。
        # 老测试替身没有这个方法，退回 fire-and-forget，行为不变。
        if hasattr(client, "send_text_with_id"):
            external_id = await client.send_text_with_id(chat, to, body)  # type: ignore[attr-defined]
        else:
            await client.send_text_safe(chat, to, body)

        # ★ 不可逆点：消息已经到买家手上了。写流水失败可以补，绝不能再让调用方
        #   以为「没发出去」而重试一次（那样买家会收到两遍）。
        created_at = now_iso()
        message_id = 0
        try:
            message_id = await self.db.log_message(
                account_id=self.account_id,
                chat_id=chat,
                sender_id=to,
                sender_name=name,
                item_id=item_id or None,
                order_id=order_id or None,
                content=body,
                kind="outgoing",
                message_type="text",
                external_id=external_id or None,
                created_at=created_at,
            )
            # 人都在后台回消息了，这显然算「已读」
            await self.db.mark_chat_read(self.account_id, chat)
        except Exception:
            logger.exception("[%s] 消息已发出，但写聊天流水失败（请手工核对）", self.name)
        logger.info("[%s] 后台回复 %s：%s", self.name, name or to, _preview(body))
        return {
            "id": message_id,
            "account_id": self.account_id,
            "chat_id": chat,
            "sender_id": to,
            "sender_name": name,
            "item_id": item_id or None,
            "order_id": order_id or None,
            "content": body,
            "kind": "outgoing",
            "message_type": "text",
            "external_id": external_id,
            "created_at": created_at,
        }

    async def send_manual_image(
        self,
        chat_id: str,
        to_id: str,
        data: bytes,
        *,
        item_id: str | None = None,
        order_id: str | None = None,
        buyer_name: str = "",
    ) -> dict[str, Any]:
        """后台上传并发送一张图片，返回落库后的消息。"""
        if not self.is_connected:
            raise XianyuError("账号当前未连接闲鱼，图片发不出去（看「账号」页的连接状态）")
        chat = str(chat_id or "").strip()
        if not chat:
            raise ValueError("缺少会话 ID")
        to = str(to_id or "").strip() or await self.db.chat_peer_id(self.account_id, chat) or ""
        if not to:
            raise ValueError("不知道这条消息发给谁（缺少买家 ID）")
        name = str(buyer_name or "").strip() or await self.db.known_buyer_name(self.account_id, to) or ""
        client = self.client
        assert client is not None
        try:
            asset = await client.upload_image_to_cdn(data)  # type: ignore[attr-defined]
        except media_tools.MediaError as exc:
            raise XianyuError(str(exc)) from exc
        external_id = await client.send_image_with_id(  # type: ignore[attr-defined]
            chat, to, asset["url"], int(asset["width"]), int(asset["height"])
        )

        created_at = now_iso()
        message_id = 0
        try:
            message_id = await self.db.log_message(
                account_id=self.account_id,
                chat_id=chat,
                sender_id=to,
                sender_name=name,
                item_id=item_id or None,
                order_id=order_id or None,
                content="[图片]",
                kind="outgoing",
                message_type="image",
                media_url=asset["url"],
                media_width=int(asset["width"]),
                media_height=int(asset["height"]),
                external_id=external_id or None,
                created_at=created_at,
            )
            await self.db.mark_chat_read(self.account_id, chat)
        except Exception:
            logger.exception("[%s] 图片已发出，但写聊天流水失败（请手工核对）", self.name)
        logger.info("[%s] 后台发送图片给 %s：%s", self.name, name or to, asset["url"])
        return {
            "id": message_id,
            "account_id": self.account_id,
            "chat_id": chat,
            "sender_id": to,
            "sender_name": name,
            "item_id": item_id or None,
            "order_id": order_id or None,
            "content": "[图片]",
            "kind": "outgoing",
            "message_type": "image",
            "media_url": asset["url"],
            "media_width": int(asset["width"]),
            "media_height": int(asset["height"]),
            "external_id": external_id,
            "created_at": created_at,
        }

    async def send_manual_video(
        self,
        chat_id: str,
        to_id: str,
        data: bytes,
        file_name: str,
        *,
        item_id: str | None = None,
        order_id: str | None = None,
        buyer_name: str = "",
    ) -> dict[str, Any]:
        """后台上传并发送一段视频。

        视频走网页端的 FileUpload_pre/frag/ci 分片上传；封面用 ffmpeg 截第一帧
        再上传，作为接收端的视频封面。发出的视频没有可回读的 CDN URL，所以
        同时在本机留一份，聊天界面用后台鉴权接口预览。
        """
        if not self.is_connected:
            raise XianyuError("账号当前未连接闲鱼，视频发不出去（看「账号」页的连接状态）")
        chat = str(chat_id or "").strip()
        if not chat:
            raise ValueError("缺少会话 ID")
        to = str(to_id or "").strip() or await self.db.chat_peer_id(self.account_id, chat) or ""
        if not to:
            raise ValueError("不知道这条消息发给谁（缺少买家 ID）")
        name = str(buyer_name or "").strip() or await self.db.known_buyer_name(self.account_id, to) or ""
        client = self.client
        assert client is not None

        safe_name = str(file_name or "video.mp4")
        suffix = media_tools.video_extension(safe_name)
        try:
            info = await asyncio.to_thread(media_tools.probe_video, data)
            poster = await asyncio.to_thread(media_tools.video_poster, data, info)
            _, _, poster_w, poster_h = await asyncio.to_thread(media_tools.image_info, poster)
        except media_tools.MediaError as exc:
            raise XianyuError(str(exc)) from exc

        video_auth = await client.upload_media_lwp(  # type: ignore[attr-defined]
            chat,
            data,
            file_name=safe_name,
            file_type=suffix.lstrip("."),
            width=info.width,
            height=info.height,
        )
        poster_auth = await client.upload_media_lwp(  # type: ignore[attr-defined]
            chat,
            poster,
            file_name="poster.jpg",
            file_type="jpg",
            width=poster_w,
            height=poster_h,
        )
        external_id = await client.send_video_with_id(  # type: ignore[attr-defined]
            chat,
            to,
            video_media_id=video_auth,
            file_name=safe_name,
            file_size=len(data),
            duration_ms=info.duration_ms,
            width=info.width,
            height=info.height,
            pic_media_id=poster_auth,
        )

        local_media = local_poster = ""
        try:
            local_media = await asyncio.to_thread(
                media_tools.save_media_file, settings.media_dir, "chat", data, suffix
            )
            local_poster = await asyncio.to_thread(
                media_tools.save_media_file, settings.media_dir, "chat", poster, ".jpg"
            )
        except Exception as exc:
            logger.warning("[%s] 视频已发出，但保存本地预览失败：%s", self.name, exc)

        created_at = now_iso()
        message_id = 0
        try:
            message_id = await self.db.log_message(
                account_id=self.account_id,
                chat_id=chat,
                sender_id=to,
                sender_name=name,
                item_id=item_id or None,
                order_id=order_id or None,
                content="[视频]",
                kind="outgoing",
                message_type="video",
                media_name=safe_name,
                media_size=len(data),
                media_width=info.width,
                media_height=info.height,
                media_duration=info.duration_ms,
                local_media=local_media or None,
                local_poster=local_poster or None,
                external_id=external_id or None,
                created_at=created_at,
            )
            await self.db.mark_chat_read(self.account_id, chat)
        except Exception:
            logger.exception("[%s] 视频已发出，但写聊天流水失败（请手工核对）", self.name)
        logger.info("[%s] 后台发送视频给 %s：%s（%s 秒）",
                    self.name, name or to, safe_name, round(info.duration_ms / 1000, 1))
        return {
            "id": message_id,
            "account_id": self.account_id,
            "chat_id": chat,
            "sender_id": to,
            "sender_name": name,
            "item_id": item_id or None,
            "order_id": order_id or None,
            "content": "[视频]",
            "kind": "outgoing",
            "message_type": "video",
            "media_name": safe_name,
            "media_size": len(data),
            "media_width": info.width,
            "media_height": info.height,
            "media_duration": info.duration_ms,
            "local_media": local_media,
            "local_poster": local_poster,
            "external_id": external_id,
            "created_at": created_at,
        }

    async def recall_manual_message(self, external_id: str) -> None:
        """撤回一条自己发的消息（调用方负责校验时间窗与归属）。"""
        if not self.is_connected:
            raise XianyuError("账号当前未连接闲鱼，撤回不了")
        if not external_id:
            raise ValueError("这条消息没有服务端消息 ID，无法撤回")
        client = self.client
        assert client is not None
        await client.recall_message(external_id)  # type: ignore[attr-defined]
        logger.info("[%s] 已撤回消息 %s", self.name, external_id)

    async def ensure_chat(self, buyer_id: str, item_id: str = "", order_id: str = "") -> str:
        """拿到能和这个买家说话的会话 ID；没有就建一个。

        订单页点「聊天」时走这里：订单记录本身不带 chat_id，
        但同一买家通常已经聊过（复用库里的），没聊过才 create_chat。
        """
        if not self.is_connected:
            raise XianyuError("账号当前未连接闲鱼，无法打开会话")
        buyer = str(buyer_id or "").strip()
        if not buyer:
            raise ValueError("缺少买家 ID")
        item = str(item_id or "").strip()
        if not item and order_id:
            item = await self._item_id_from_order(str(order_id)) or ""
        if item:
            chat_id, error = await self._resolve_chat(buyer, item)
            if chat_id:
                return chat_id
            if error:
                raise XianyuError("创建会话失败：" + str(error))
        cached = await self.db.last_chat_id_any(self.account_id, buyer)
        if cached:
            return cached
        raise ValueError("缺少商品 ID，无法新建会话（请从订单里打开，或等买家先发消息）")

    async def sync_chat_history(self, chat_id: str, limit: int = 50) -> int:
        """按需补拉一个会话最近的历史，供后台聊天界面显示。

        为什么需要：轮询只保证「每个会话最后一条」，后台打开一个久未打开的会话时
        手上可能只有几行。这里补一次最近 N 条（一次 WS 往返），靠 messageId 去重。

        收到的和自己发出去的都记：卖家在闲鱼 App / 网页里回的消息，本机
        只有靠这里补拉才能看到。自己消息只写聊天流水，不会触发自动回复；
        发送那一刻已经记过的行，靠 messageId 和「同方向 + 同内容 + 3 分钟」
        兜底去重，不会再冒出第二条气泡。
        """
        chat = str(chat_id or "").strip()
        if not chat:
            raise ValueError("缺少会话 ID")
        if not self.is_connected:
            raise XianyuError("账号当前未连接闲鱼，同步不了历史")
        assert self.client is not None
        result = await self.client.list_conversation_messages(
            chat, limit=max(1, min(int(limit), 50))
        )
        items = list(result.get("messages") or [])
        # 历史接口通常是从新到旧；先按时间正序写库，聊天界面才是正常对话顺序
        items.sort(key=lambda item: int(item.get("created_at") or 0))

        own_id = _clean_user_id(self.client.user_id if self.client else "")
        peer = _clean_user_id(await self.db.chat_peer_id(self.account_id, chat))
        if peer == own_id:
            # 老库里可能残留「对端=自己」的脏行，不能把历史消息挂到自己名下。
            peer = ""
        known_name = await self.db.known_buyer_name(self.account_id, peer) if peer else None
        handled = 0
        learned: tuple[str, str] | None = None
        for data in items:
            content = str(data.get("content") or "").strip()
            if not content and not data.get("media_url"):
                continue
            from_me = bool(data.get("from_me"))
            # 自己发的消息没有买家 sender_id，用会话对端兜底，才能挂到同一个会话下
            buyer_id = _clean_user_id(data.get("buyer_id") or data.get("peer_id") or peer)
            if buyer_id == own_id:
                if not from_me:
                    logger.warning("[%s] 历史消息的发送方是自己，按脏数据跳过：%s",
                                   self.name, data.get("message_id") or "-")
                    continue
                buyer_id = peer or ""
            buyer_name = str(data.get("buyer_name") or "").strip() or known_name or ""
            if not from_me and buyer_id and buyer_name and not is_placeholder_name(buyer_name):
                learned = (buyer_id, buyer_name)
            await self.db.log_synced_message(
                account_id=self.account_id,
                chat_id=chat,
                sender_id=buyer_id,
                sender_name=buyer_name,
                item_id=data.get("item_id") or None,
                item_title=str(data.get("item_title") or ""),
                order_id=data.get("order_id") or None,
                content=content,
                kind="outgoing" if from_me else "incoming",
                message_type=str(data.get("message_type") or "text"),
                media_url=data.get("media_url") or None,
                media_thumb_url=data.get("media_thumb_url") or None,
                media_width=data.get("media_width"),
                media_height=data.get("media_height"),
                media_duration=data.get("media_duration"),
                external_id=str(data.get("message_id") or ""),
                # 用户正看着这个会话，补进来的历史直接算已读
                read_at=now_iso(),
                created_at=iso_from_ms(data.get("created_at")),
            )
            handled += 1
        if learned is not None:
            await self._remember_buyer_name(*learned)
        logger.debug("[%s] 已同步会话 %s 的历史：%s 条", self.name, chat, handled)
        return handled

    async def sync_order_history(self, max_orders: int = 2000) -> dict[str, Any]:
        """从闲鱼卖家订单列表补拉历史订单记录（只读 + 只插入）。

        用途：换设备/重装后本机库是空的，但闲鱼账号里的成交记录还在。
        这里把闲鱼侧的订单号、商品、买家、金额、时间拉回来，作为「本机曾经
        有这笔交易」的底账。它**不能**恢复本机才能知道的东西：发出去的卡密、
        new-api 的 Key、订单用的规则、人工备注等，所以导入行统一标成
        synced，详情里写明「仅同步订单记录」，不会伪装成本机发货成功。

        已有记录一律不覆盖：本机已经 delivered / 退款 / 拦截的明细比闲鱼
        列表更详细，用远端数据覆盖只会丢审计信息。新单后续被订单轮询命中时，
        record_order 会正常把 synced 升级成真实发货状态。
        """
        if not self.is_connected:
            raise XianyuError("账号当前未连接闲鱼，同步不了订单")
        assert self.client is not None
        orders = await self.client.list_all_sold_orders(max_orders=max_orders)
        rows: list[dict[str, Any]] = []
        ignored = 0
        for order in orders:
            order_id = str(order.get("order_id") or "").strip()
            if not order_id:
                ignored += 1
                continue
            remote_status = str(order.get("order_status") or "").strip() or "未知"
            # 卖家订单列表默认会把待付款、交易关闭也带回来；这些不是成交，
            # 导入成 synced 会让「成交/成交额」虚高，所以只补真正已付款的单。
            if remote_status in NON_SALE_ORDER_STATUSES:
                ignored += 1
                continue
            title = str(order.get("item_title") or "").strip()
            detail_parts = [f"闲鱼状态：{remote_status}"]
            if title:
                detail_parts.append(f"商品：{title}")
            if order.get("in_refund"):
                detail_parts.append("退款中")
            detail_parts.append("仅同步订单记录，本机无发货明细")
            created_at = (
                str(order.get("paid_at") or "").strip()
                or str(order.get("created_at") or "").strip()
                or now_iso()
            )
            rows.append({
                "account_id": self.account_id,
                "order_id": order_id,
                "item_id": str(order.get("item_id") or "").strip() or None,
                "buyer_id": str(order.get("buyer_id") or "").strip() or None,
                "buyer_name": str(order.get("buyer_name") or "").strip() or None,
                "amount_cny": amount_from_order_item(
                    order.get("raw") or {}, settings.order_amount_unit
                ),
                "status": "synced",
                "detail": "；".join(detail_parts),
                "created_at": created_at,
            })
        inserted, skipped = await self.db.import_orders(rows)
        logger.info(
            "[%s] 从闲鱼同步订单：拉取 %s 单，新增 %s 单，已存在/跳过 %s 单，忽略未成交 %s 单",
            self.name, len(orders), inserted, skipped, ignored,
        )
        return {
            "account_id": self.account_id,
            "account_name": self.name,
            "fetched": len(orders),
            "imported": inserted,
            "skipped": skipped,
            "ignored": ignored,
        }

    # ------------------------------------------------ 拉取式收消息（兜底）

    async def _message_poll_loop(self) -> None:
        """定期拉会话列表补消息 —— 推送只负责快，「一定有」靠这里。

        线上实测：连接正常、心跳正常时，syncPushPackage 里可能只有会话引导
        （contentType=8）和「某会话有新消息」的未读标记，**正文一个字都不给**。
        那类推送以前在日志里表现为「收到无法解析的负载」，看起来像没人说话，
        实际是推送通道本身不带正文。

        /r/Conversation/listNewestPagination 每次都能拿到每个会话的最后一条消息，
        所以这里按固定间隔拉一次，把「最后一条」当新消息处理，靠 messageId 去重。
        """
        interval = settings.message_poll_interval
        if interval <= 0:
            return
        # 启动先拉一次：断线期间别人发的消息不该等到下一个周期
        while True:
            try:
                await self._poll_recent_messages()
            except asyncio.CancelledError:
                raise
            except (RiskControl, CookieExpired, TokenExpired) as exc:
                await self._pause_for_account_error(exc)
                return
            except Exception as exc:
                logger.warning("[%s] 拉取会话消息失败：%s", self.name, exc)
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                raise

    async def _poll_recent_messages(self) -> int:
        """拉一次会话列表，把有新动静的会话补一遍消息；返回处理了几条。"""
        if self.client is None:
            return 0
        assert self.client is not None
        try:
            conversations = await self.client.list_recent_conversations(
                settings.message_poll_limit
            )
        except (RiskControl, CookieExpired, TokenExpired):
            raise
        except XianyuError as exc:
            logger.warning("[%s] 拉会话列表失败：%s", self.name, exc)
            return 0
        if not conversations:
            return 0

        keys = [self._message_key(c, c.get("message_id")) for c in conversations]
        seeking_baseline = not self._message_baseline_done
        seen = await self.db.messages_seen(self.account_id, keys)

        baseline_keys: list[str] = []
        todo: list[dict[str, Any]] = []
        for conv, key in zip(conversations, keys, strict=True):
            chat_id = str(conv.get("chat_id") or "")
            created_at = int(conv.get("created_at") or 0)
            if key in seen:
                # 已经处理过：水位至少推进到这条，别让后续历史补拉再回头看它
                if chat_id:
                    self._chat_watermarks[chat_id] = max(
                        self._chat_watermarks.get(chat_id, 0), created_at
                    )
                continue
            # 第一轮：比「本连接建立时刻」更早的都是历史消息，只记已读不回。
            # 用时间而不是「第一轮全跳过」，才不会漏掉启动瞬间刚到达的那条。
            if seeking_baseline and created_at < self._connected_at_ms:
                baseline_keys.append(key)
                # 关键：把水位推进到这条「最后一条旧消息」。
                # 否则下一次同会话来新消息、补拉历史时，比它更早的旧消息
                # 仍未 seen，会被当成新消息自动回复。
                if chat_id:
                    self._chat_watermarks[chat_id] = max(
                        self._chat_watermarks.get(chat_id, 0), created_at
                    )
                continue
            todo.append(conv)

        if baseline_keys:
            await self.db.mark_messages_seen(self.account_id, baseline_keys)
        if seeking_baseline:
            self._message_baseline_done = True
            logger.info("[%s] 已建立会话基线（%s 个会话），只处理这之后的新消息",
                        self.name, len(conversations))
        if not todo:
            return 0

        # 拉历史是一次 WS 往返，会话多的时候别一次打十几个出去：
        # 优先给「最新有动静」的几个会话补历史，其余只用列表里的最后一条。
        todo.sort(key=lambda c: int(c.get("created_at") or 0), reverse=True)
        max_history = max(0, settings.message_history_conversations)

        handled = 0
        for index, conv in enumerate(todo):
            try:
                chat_id = str(conv.get("chat_id") or "")
                # 每个会话用各自的水位：基线那一轮还没水位的用连接时刻兜底，
                # 之后的轮次则从上次处理到的位置往后看，避免重放启动前的历史。
                min_created_at = self._chat_watermarks.get(
                    chat_id,
                    self._connected_at_ms if seeking_baseline else 0,
                )
                with_history = index < max_history
                handled += await self._sync_conversation(
                    conv, min_created_at=min_created_at, with_history=with_history
                )
            except asyncio.CancelledError:
                raise
            except (RiskControl, CookieExpired, TokenExpired):
                raise
            except Exception as exc:  # 一个会话出错不影响别的会话
                logger.exception("[%s] 处理会话 %s 出错：%s",
                                 self.name, conv.get("chat_id"), exc)
        return handled

    @staticmethod
    def _message_key(conv: dict[str, Any], message_id: str | None) -> str:
        """一条消息的全局唯一键：会话 + 服务端 messageId（没有就退回时间戳）。"""
        return f"{conv.get('chat_id')}:{message_id or conv.get('created_at')}"

    async def _sync_conversation(self, conv: dict[str, Any], *, min_created_at: int = 0,
                                 with_history: bool = True) -> int:
        """会话有新动静：拉一次历史，把漏掉的几条一起处理。

        为什么要拉历史而不是只看「最后一条」：
        买家可能连发两条（「在吗」「发货吗」），或者中间夹着我们自己回的一句，
        只看最后一条就会漏 —— 关键词正好在漏掉的那条上时，规则就「看起来没生效」。
        """
        if self.client is None:
            return 0
        chat_id = str(conv.get("chat_id") or "")
        if not chat_id:
            return 0

        history: list[dict[str, Any]] = []
        if with_history:
            try:
                assert self.client is not None
                result = await self.client.list_conversation_messages(
                    chat_id, limit=settings.message_history_limit
                )
                history = list(result.get("messages") or [])
            except (RiskControl, CookieExpired, TokenExpired):
                raise
            except XianyuError as exc:
                logger.debug("[%s] 拉会话 %s 历史失败，退回只看最后一条：%s", self.name, chat_id, exc)
            except Exception as exc:
                logger.debug("[%s] 拉会话 %s 历史异常，退回只看最后一条：%s", self.name, chat_id, exc)

        candidates = history or [conv]
        keys = [self._message_key(conv, m.get("message_id")) for m in candidates]
        seen = await self.db.messages_seen(self.account_id, keys)

        fresh: list[str] = []
        ordered = [
            (m, key) for m, key in zip(candidates, keys, strict=True)
            if key not in seen and int(m.get("created_at") or 0) >= min_created_at
        ]
        # 历史是从新到旧返回的，按时间正序处理，回复顺序才符合对话直觉
        ordered.sort(key=lambda pair: int(pair[0].get("created_at") or 0))

        handled = 0
        for msg_data, key in ordered:
            fresh.append(key)
            if not str(msg_data.get("content") or "").strip() and not msg_data.get("media_url"):
                continue
            if msg_data.get("from_me"):
                outgoing = self._to_incoming(msg_data, outgoing=True)
                if outgoing is None:
                    continue
                # 自己发的消息只记流水：不能进 _process_incoming，
                # 否则自动回复/发货会把卖家自己的话当成买家需求。
                await self._record_outgoing_message(outgoing)
                handled += 1
                continue
            msg = self._to_incoming(msg_data)
            if msg is None:
                continue
            logger.debug("[%s] 拉取到 %s 的消息：%s", self.name, msg.sender_name,
                         _preview(msg.content, 60))
            # custom.type 不是 1 就是系统消息（14 提示 / 25 评价 / 26 卡片），别自动回复
            allow_reply = int(msg_data.get("custom_type") or 0) in (1, 0)
            reply_task = await self._process_incoming(msg, allow_auto_reply=allow_reply)
            if reply_task is not None:
                # 同一条会话连发两条时，挨个等回复发完，顺序才对得上
                try:
                    await reply_task
                except Exception as exc:
                    logger.warning("[%s] 自动回复任务失败：%s", self.name, exc)
            handled += 1

        # 没处理到的（自己的消息、系统消息、空内容）也要记账，否则每轮都会重看一遍
        for _, key in zip(candidates, keys, strict=True):
            if key not in fresh:
                fresh.append(key)
        await self.db.mark_messages_seen(self.account_id, fresh)
        latest = max((int(c.get("created_at") or 0) for c in candidates), default=0)
        if latest:
            self._chat_watermarks[chat_id] = max(
                self._chat_watermarks.get(chat_id, 0), latest
            )
        if handled:
            # 每个会话一条汇总就够了；逐条 INFO 在活跃账号上会把日志刷满
            logger.info("[%s] 会话 %s（%s）拉到 %s 条新消息",
                        self.name, chat_id,
                        (candidates[0].get("buyer_name") if candidates else "") or "-",
                        handled)
        return handled

    def _to_incoming(self, data: dict[str, Any], *, outgoing: bool = False) -> IncomingMessage | None:
        own_id = _clean_user_id(self.client.user_id if self.client else "")
        sender_id = _clean_user_id(data.get("buyer_id"))
        peer_id = _clean_user_id(data.get("peer_id"))
        if peer_id == own_id:
            peer_id = ""
        if outgoing:
            # 自己发的消息 buyer_id 为空，先取会话对端；仍为空也允许，
            # 由 _record_outgoing_message 从已知会话里回退。
            sender_id = sender_id or peer_id
            if sender_id == own_id:
                sender_id = ""
        elif not sender_id:
            return None
        # custom_type：1 真人文本、2 图片、0 未知；其余都是系统消息/卡片
        try:
            content_type = int(data.get("content_type") or data.get("custom_type") or 0)
        except (TypeError, ValueError):
            content_type = 0
        return IncomingMessage(
            chat_id=str(data.get("chat_id") or ""),
            sender_id=sender_id,
            sender_name=pick_display_name(data.get("buyer_name")),
            content=str(data.get("content") or ""),
            item_id=data.get("item_id") or None,
            item_title=str(data.get("item_title") or ""),
            order_id=data.get("order_id") or None,
            created_at=int(data.get("created_at") or 0),
            raw=data.get("raw") or {},
            message_id=str(data.get("message_id") or ""),
            from_me=outgoing,
            peer_id=peer_id,
            # custom.type 在部分版本里都是 1；真正区分卡片的是内容自己的
            # contentType（25 评价请求 / 26 交易卡片 / 36 卡片）。
            is_system_card=content_type not in (0, 1, 2),
            order_id_trusted=bool(data.get("order_id_trusted")),
            message_type=str(data.get("message_type") or "text"),
            media_url=str(data.get("media_url") or ""),
            media_thumb_url=str(data.get("media_thumb_url") or ""),
            media_width=data.get("media_width"),
            media_height=data.get("media_height"),
            media_duration=data.get("media_duration"),
        )

    async def _order_poll_loop(self) -> None:
        """定期扫「待发货」订单 —— IM 推送丢了也能兜底发货。

        为什么需要它：闲鱼的消息推送会因为断线、风控、版本变更而漏。
        订单状态是「拉」得到的，比推送可靠。两者互补，且都靠 delivered 去重。

        延迟 = 轮询间隔。所以这里不只用 sleep：收到任何推送都会
        （节流后）把这一觉叫醒，实际发货就从「等一轮」变成「等一两秒」。
        """
        interval = settings.order_poll_interval
        if interval <= 0:
            return
        base_gap = settings.order_hint_poll_gap
        while True:
            # 连上就先查一次：断线期间成交的订单不该再多等一个间隔
            self._poll_wakeup.clear()
            by_hint = self._poll_by_hint
            self._poll_by_hint = False
            try:
                found = await self._poll_pending_orders()
            except asyncio.CancelledError:
                raise
            except (RiskControl, CookieExpired, TokenExpired) as exc:
                await self._pause_for_account_error(exc)
                return
            except Exception as exc:
                logger.warning("[%s] 订单轮询出错：%s", self.name, exc)
                found = 0
            self._tune_hint_gap(found, by_hint, base_gap, interval)
            try:
                await asyncio.wait_for(self._poll_wakeup.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

    def _tune_hint_gap(self, found: int, by_hint: bool, base_gap: float, interval: int) -> None:
        """按「这次查到东西没有」调推送触发的节流窗口。

        查到单 → 恢复成配置值，下一单照样快；
        推送叫醒却什么都没有 → 说明闲鱼推的多半是控制包，翻倍放慢，
        最多放到 30 秒（且不超过正常轮询间隔），不会把接口打成风控。
        """
        if base_gap <= 0:
            return
        if found:
            self._hint_gap = base_gap
        elif by_hint:
            self._hint_gap = min(max(self._hint_gap, base_gap) * 2, max(base_gap, 30.0), float(interval))

    def _hint_poll(self) -> None:
        """收到推送后安排一次「提前轮询」。

        推送正文经常解析不出东西（实测大部分推送都是控制包），
        但「有推送」本身就说明可能刚有人拍下 —— 拿它当轮询的触发器，
        比死等下一个轮询周期划算得多。
        """
        gap = self._hint_gap
        if gap <= 0 or settings.order_poll_interval <= 0:
            return
        delay = self._last_poll_at + gap - time.monotonic()
        if delay <= 0:
            self._poll_by_hint = True
            self._poll_wakeup.set()
            return
        # 刚查过没多久：等节流窗口过去再补一次，不为每条推送都打一次接口
        task = self._hint_task
        if task is None or task.done():
            self._hint_task = asyncio.create_task(self._wake_up_later(delay))

    async def _wake_up_later(self, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            return
        self._poll_by_hint = True
        self._poll_wakeup.set()

    async def _poll_pending_orders(self) -> int:
        """拉一次待发货订单，没发过的就发；返回处理了几单。

        一单里的 mtop 往返（查金额、建会话）是并行的瓶颈，所以
        多张待发货订单同时处理；单张内部仍然是串行，保证顺序清晰。
        """
        if self.client is None:
            return 0
        self._last_poll_at = time.monotonic()
        pending = await self.client.list_pending_orders()
        if not pending:
            return 0

        todo: list[dict[str, Any]] = []
        for order in pending:
            order_id = str(order["order_id"])
            if order_id in self._inflight:
                continue
            if await self.db.delivery_blocked(self.account_id, order_id):
                continue
            # 订单列表里已经带了实付金额，先算好随订单带下去，
            # 后面 _provision 就不用再为同一个订单查一次详情
            if order.get("amount_cny_hint") is None:
                order["amount_cny_hint"] = amount_from_order_item(
                    order.get("raw") or {}, settings.order_amount_unit
                )
            todo.append(order)
        if not todo:
            return 0

        limit = max(1, settings.order_poll_concurrency)
        if limit == 1 or len(todo) == 1:
            delivered = 0
            for order in todo:
                delivered += 1 if await self._fulfil_order(order) else 0
            return delivered

        semaphore = asyncio.Semaphore(limit)

        async def run_one(order: dict[str, Any]) -> bool:
            async with semaphore:
                try:
                    return await self._fulfil_order(order)
                except asyncio.CancelledError:
                    raise
                except (RiskControl, CookieExpired, TokenExpired):
                    raise
                except Exception as exc:
                    logger.warning("[%s] 订单 %s 处理失败：%s", self.name, order["order_id"], exc)
                    return False

        results = await asyncio.gather(*(run_one(order) for order in todo))
        return sum(1 for ok in results if ok)

    async def _fulfil_order(self, order: dict[str, Any]) -> bool:
        """按订单（而不是按聊天消息）触发发货。返回这单是否真的发出去了。

        关键差别：订单里没有 chat_id，通常要先 create_chat 拿会话 ID 再发消息。
        但如果这个买家之前为这个商品发过消息，数据库里就有现成的会话 ——
        省掉一次 WS 往返（实测 300~600ms），也少一个可能失败的环节。
        """
        assert self.client is not None
        order_id = str(order["order_id"])
        buyer_id = order.get("buyer_id") or ""
        item_id = order.get("item_id") or ""
        if not buyer_id:
            logger.error("[%s] 订单 %s 缺少买家 ID，无法发货", self.name, order_id)
            return False
        # 认领必须发生在第一个 await 之前：协程一路跑到第一个 await 才让出，
        # 所以「检查 + 登记」在这一刻是原子的，不会两个人同时拿到同一单。
        if order_id in self._inflight:
            logger.debug("[%s] 订单 %s 正在处理中，跳过重复触发", self.name, order_id)
            return False
        self._inflight.add(order_id)
        try:
            return await self._fulfil_order_inner(order, order_id, buyer_id, item_id)
        finally:
            self._inflight.discard(order_id)

    async def _fulfil_order_inner(self, order: dict[str, Any], order_id: str,
                                  buyer_id: str, item_id: str) -> bool:
        msg = IncomingMessage(
            chat_id="",  # 需要时下面补上
            sender_id=buyer_id,
            sender_name=(
                pick_display_name(order.get("buyer_name"), fallback="")
                or buyer_nick_from_order(order.get("raw"))
                or "买家"
            ),
            content="[订单待发货]",
            item_id=item_id or None,
            order_id=order_id,
            created_at=0,
            raw=order.get("raw") or {},
            amount_cny_hint=order.get("amount_cny_hint"),
            # 订单号来自卖家「订单列表」接口，不是从聊天正文里捞的 —— 可信。
            order_id_trusted=True,
        )
        await self._maybe_fill_amount(msg)

        decision = await self.engine.decide_order(self.account_id, msg)
        try:
            if not decision.should_deliver:
                logger.info("[%s] 订单 %s 跳过：%s", self.name, order_id, decision.reason)
                return False
            rule = decision.rule or {}

            chat_id, chat_error = await self._resolve_chat(buyer_id, item_id)
            if chat_id is None:
                logger.error("[%s] 订单 %s 创建会话失败：%s", self.name, order_id, chat_error)
                await self._record_failure(msg, rule, "创建会话失败：" + str(chat_error))
                return False
            msg.chat_id = chat_id

            return await self._deliver(msg, decision, claimed=True)
        finally:
            # 决策阶段可能已经从卡密池锁了一张。任何非「已发出/结果未知」的
            # 退出路径都必须还回库存；否则 create_chat 抛非 XianyuError
            # （例如 ConnectionClosed）时这张卡会永远停在 sold。
            if (
                decision.card_id is not None
                and not decision.sent
                and not decision.sent_unknown
            ):
                await self.db.release_card(decision.card_id)

    async def _find_order_cached(self, order_id: str) -> dict[str, Any] | None:
        """查订单详情，同一个订单在 TTL 内只查一次。

        一单要查两次：付款卡片只带 orderId 时先补商品 ID，上游模式再取金额。
        同一次订单、同一个接口，没有理由打两遍 —— 每次往返 300~700ms。
        """
        if self.client is None:
            return None
        ttl = settings.order_cache_ttl
        if ttl > 0:
            cached = self._order_cache.get(order_id)
            if cached is not None and (time.monotonic() - cached[0]) < ttl:
                return cached[1]
        try:
            order = await self.client.find_order(order_id)
        except (RiskControl, CookieExpired, TokenExpired):
            # 风控/登录失效要让 worker 进入退避，不能伪装成「查不到订单」
            raise
        except XianyuError as exc:
            logger.warning("[%s] 查订单 %s 失败：%s", self.name, order_id, exc)
            return None
        if ttl > 0:
            self._order_cache[order_id] = (time.monotonic(), order)
            if len(self._order_cache) > 200:
                # 只留最近用过的那些，别让缓存无限长
                for key in sorted(self._order_cache, key=lambda k: self._order_cache[k][0])[:100]:
                    self._order_cache.pop(key, None)
        return order

    async def _resolve_chat(self, buyer_id: str, item_id: str) -> tuple[str | None, str]:
        """拿到能和买家说话的会话 ID：先查库，查不到再走 WS 建会话。

        库里有现成的就直接用（省一次往返，也少一个会失败的环节）；
        没有才 create_chat。
        """
        cached = await self.db.last_chat_id(self.account_id, buyer_id, item_id)
        if cached:
            logger.debug("[%s] 复用已有会话 %s（买家 %s）", self.name, cached, buyer_id)
            return cached, ""
        assert self.client is not None
        try:
            chat_id = await self.client.create_chat(buyer_id, item_id)
        except XianyuError as exc:
            return None, str(exc)
        return chat_id, ""

    #: 闲鱼订单列表里「可以发货」的状态。别的（待付款/已发货/交易关闭/退款中…）一律不发。
    SHIPPABLE_ORDER_STATUS = frozenset({"待发货", "等待卖家发货"})

    async def _verify_order_pending(self, order_id: str, buyer_id: str) -> bool:
        """去闲鱼查这个订单，确认「真的付款了、确实等我发货、而且是这个买家」。

        为什么必须查：发货的触发条件以前是「消息正文里有没有『我已付款』」。
        买家自己在聊天框里打下这句话，也是一个合法的文本消息 ——
        不核订单就等于谁都能白拿卡密和 Key。

        顺便挡住另一种误发：卡片自带的 buyer id 和订单真正的买家对不上
        （多端同步、转发、脏数据），那种单发出去就是发给错的人。
        """
        if self.client is None or not hasattr(self.client, "find_order"):
            logger.error("[%s] 客户端不支持查订单，无法核实订单 %s，拒绝发货",
                         self.name, order_id)
            return False
        order = await self._find_order_cached(order_id)
        if not order:
            logger.warning("[%s] 订单 %s 查不到，不能只凭聊天文本发货", self.name, order_id)
            return False

        common = order.get("commonData") if isinstance(order.get("commonData"), dict) else {}

        # 订单号必须精确匹配。find_order 在「回包只有一条且没带 orderId」时
        # 会兜底返回那一单；这里如果不复核，买家编一个假单号也能拿唯一的那一单
        # 通过核实，卡密就发到假单号名下了，真实订单之后还会被再发一次。
        returned_order_id = str(common.get("orderId") or "").strip()
        if returned_order_id and returned_order_id != str(order_id):
            logger.warning("[%s] 核实订单 %s 时上游返回的是订单 %s，订单号不一致，跳过",
                           self.name, order_id, returned_order_id)
            return False

        status = str(common.get("orderStatus") or "").strip()
        if status not in self.SHIPPABLE_ORDER_STATUS:
            logger.warning("[%s] 订单 %s 状态是「%s」，不是待发货，跳过", self.name, order_id, status or "未知")
            return False

        # 订单里带的买家 id 才是权威的；拿不到买家 ID 就不能证明这单属于这条消息
        buyer = order.get("buyerInfoVO") if isinstance(order.get("buyerInfoVO"), dict) else {}
        order_buyer = str(buyer.get("buyerId") or "").strip()
        if not order_buyer:
            logger.warning("[%s] 订单 %s 没有买家 ID，无法确认归属，跳过", self.name, order_id)
            return False
        if not buyer_id:
            logger.warning("[%s] 订单 %s 有买家 ID 但消息里没有，跳过", self.name, order_id)
            return False
        if order_buyer != str(buyer_id):
            logger.warning("[%s] 订单 %s 属于买家 %s，与消息里的 %s 不一致，跳过",
                           self.name, order_id, order_buyer, buyer_id)
            return False

        logger.info("[%s] 订单 %s 已核实（%s），允许发货", self.name, order_id, status)
        return True

    async def _item_id_from_order(self, order_id: str) -> str | None:
        """从订单详情里取商品 ID（付款卡片往往只带 orderId）。"""
        order = await self._find_order_cached(order_id)
        if not order:
            return None
        common = order.get("commonData") if isinstance(order.get("commonData"), dict) else {}
        for key in ("itemId", "item_id"):
            value = common.get(key)
            if value:
                return str(value)
        return None

    async def _deliver(self, msg: IncomingMessage, decision: Any, *, claimed: bool = False) -> bool:
        """发货入口：保证同一订单不会被并发发两次。

        现在有两条路会触发发货（IM 推送、订单轮询），推送还会提前叫醒轮询，
        两路撞在同一单上的机会比以前大很多。建 Key、出卡密都是有副作用的动作，
        所以这里先「占坑」：抢到才继续，抢不到说明这一单别人正在发。

        claimed=True 表示调用方（订单轮询）已经占过坑了，别重复占。
        """
        order_key = msg.order_id or ""
        if claimed or not order_key:
            return await self._deliver_once(msg, decision)
        if order_key in self._inflight:
            logger.info("[%s] 订单 %s 正在发货中，跳过这次重复触发", self.name, order_key)
            if decision.card_id is not None:
                # 决策阶段已经从卡密池里锁了一张，这次不发就得还回去
                await self.db.release_card(decision.card_id)
            return False
        self._inflight.add(order_key)
        try:
            return await self._deliver_once(msg, decision)
        finally:
            self._inflight.discard(order_key)

    async def _deliver_once(self, msg: IncomingMessage, decision: Any) -> bool:
        """执行一次发货，并保证「没发出去就一定把卡密还回库存」。

        决策阶段（engine._materialize）已经把卡 take_card → sold 了。
        这里任何一个分支只要没真正发出去（DRY-RUN、上游失败、发送失败、
        甚至抛异常），都必须把那张卡还回去 —— 否则它会永远停在 sold：
        不会再被任何人取到，也不会出现在任何发货记录里，等于静默丢库存。

        把归还收敛到这一层，是因为这套补偿以前散在四个分支里，
        漏掉任何一个就是一张卡人间蒸发。
        """
        try:
            sent = await self._deliver_once_inner(msg, decision)
        except BaseException:
            await self._release_leased_card(decision)
            raise
        if not sent and not getattr(decision, "sent_unknown", False):
            await self._release_leased_card(decision)
        return sent

    async def _release_leased_card(self, decision: Any) -> None:
        """把决策阶段锁住的卡密放回库存（没锁卡就是空操作）。

        **只要消息已经发出去了就不许退回**：卡在买家手里，退回库存等于
        把同一张卡再卖一次；同一单的 delivered 记录也没写成，轮询还会重发。
        """
        card_id = getattr(decision, "card_id", None)
        if card_id is None:
            return
        if getattr(decision, "sent", False) or getattr(decision, "sent_unknown", False):
            logger.error(
                "[%s] 卡密 %s 的发送结果已发出或未知，后续步骤出错也不退回库存"
                "（请人工核对这单）",
                self.name, card_id,
            )
            return
        await self.db.release_card(card_id)
        logger.warning("[%s] 卡密 %s 未发出，已退回库存", self.name, card_id)

    async def _deliver_once_inner(self, msg: IncomingMessage, decision: Any) -> bool:
        """真正的发货动作：上游模式先建卡密，再发消息 + 可选确认发货。

        返回是否真的发了出去（DRY-RUN、上游失败、发送失败都算没发）。
        """
        assert self.client is not None
        rule = decision.rule or {}

        # 订单里带的是权威昵称（buyerInfoVO.userNick）。发货链路上这个订单
        # 通常刚查过（缓存里就有），所以不额外打接口；拿到了就先补名字 ——
        # 必须早于下面生成文案和令牌名，否则「你好 买家」和「买家#订单号」
        # 已经发出去了，事后再改也追不回来。
        await self._name_from_cached_order(msg)

        card_code = decision.card_code
        amount_cny = decision.amount_cny
        quota_usd = decision.quota_usd
        content = decision.content

        # DRY-RUN 必须在最前面拦住：上游模式真的会去建 Key、建卡密、充值，不能试跑
        if settings.dry_run:
            if decision.needs_provision:
                logger.warning("[%s][DRY-RUN] 本应查订单 %s 金额 → 建 Key → 出卡密 → 发给 %s",
                               self.name, msg.order_id, msg.sender_name)
            elif decision.needs_topup:
                logger.warning("[%s][DRY-RUN] 本应查订单 %s 金额 → 充值到买家已有 Key → 发给 %s",
                               self.name, msg.order_id, msg.sender_name)
            elif decision.needs_account:
                logger.warning("[%s][DRY-RUN] 本应问买家有没有账号 → 开新号 / 充值到账号 → 发给 %s",
                               self.name, msg.sender_name)
            elif decision.needs_redeem:
                logger.warning("[%s][DRY-RUN] 本应查订单 %s 金额 → 生成 new-api 兑换码 → 发给 %s",
                               self.name, msg.order_id, msg.sender_name)
            else:
                logger.warning("[%s][DRY-RUN] 本应发货：%s", self.name, _preview(content))
            return False

        api_key_id = ""
        key_order_id = ""
        # ---- 上游模式：查订单金额 → 建 Key → 出卡密 / 充值到已有 Key / 账号
        if (decision.needs_provision or decision.needs_topup
                or decision.needs_account or decision.needs_redeem):
            # 上一次可能已经建好/充好了，只是消息没发出去。复用已有的那一份，
            # 绝不能重新执行 —— 否则一次发送失败就会留下多个令牌/卡密，
            # 充值模式更糟：重试会**重复充值**。
            prepared = await self._reuse_provision(msg)
            if prepared is None:
                if decision.needs_account:
                    prepared = await self._account(msg, rule, decision)
                elif decision.needs_redeem:
                    prepared = await self._redeem(msg, rule)
                elif decision.needs_topup:
                    prepared = await self._topup(
                        msg, rule, key_order_id=getattr(decision, "key_order_id", ""),
                        key_token_id=getattr(decision, "key_token_id", ""),
                        quota_hint=decision.quota_usd,
                    )
                else:
                    prepared = await self._provision(msg, rule)
                if prepared is None:
                    return False
                await self._record_provisioned(msg, rule, prepared)
            else:
                logger.info("[%s] 订单 %s 复用之前已建好的卡密/充值结果，不重复执行上游动作",
                            self.name, msg.order_id)
            card_code = prepared.card_code
            amount_cny = prepared.amount_cny
            quota_usd = prepared.quota_usd
            content = prepared.content
            api_key_id = prepared.api_key_id
            key_order_id = prepared.key_order_id

        # 发送前先落一条 sending：如果进程在「消息已发、记录未写」之间死掉，
        # 启动时能把它识别为「结果未知」，而不是按未发货再发一次。
        if not await self._record_sending(
            msg, rule, decision, content, amount_cny, quota_usd, card_code, api_key_id,
            key_order_id,
        ):
            return False

        external_id = ""
        sender = getattr(self.client, "send_delivery_text", None)
        if sender is not None:
            try:
                external_id, outcome = await sender(msg.chat_id, msg.sender_id, content)
            except asyncio.CancelledError:
                # 停止任务时可能已经发出去了：按结果未知处理，别退卡。
                decision.sent_unknown = True
                await self._record_send_unknown(
                    msg, rule, decision, content, amount_cny, quota_usd, card_code,
                    detail="发货任务被取消，发送结果未知；请人工核对",
                )
                raise
            if outcome == "failed":
                logger.error("[%s] 发货消息被服务端拒收", self.name)
                await self.db.record_order(
                    account_id=self.account_id, order_id=msg.order_id, item_id=msg.item_id,
                    buyer_id=msg.sender_id, buyer_name=msg.sender_name, rule_id=rule.get("id"),
                    card_id=None, content=content, status="send_failed",
                    detail="服务端明确拒收",
                    amount_cny=amount_cny, quota_usd=quota_usd, card_code=card_code,
                    api_key_id=api_key_id or None,
                    key_order_id=key_order_id or None,
                )
                return False
            if outcome == "unknown":
                # 消息可能已经到买家手上了：不能退卡，也不能自动重发。
                # 落 send_unknown，让订单轮询/推送都不再碰这一单，等人工核对。
                decision.sent_unknown = True
                logger.error(
                    "[%s] 订单 %s 发货消息发送结果未知（ack 超时/连接中断），"
                    "请到闲鱼聊天里人工核对，系统不会自动重发",
                    self.name, msg.order_id,
                )
                await self._record_send_unknown(
                    msg, rule, decision, content, amount_cny, quota_usd, card_code,
                    api_key_id=api_key_id, key_order_id=key_order_id,
                    detail="发送回包超时或连接中断，结果未知；请人工核对后处理",
                )
                return False
        else:
            # 测试替身 / 老客户端没有 ack 版本，退回原来的 fire-and-forget。
            try:
                await self.client.send_text_safe(msg.chat_id, msg.sender_id, content)
            except XianyuError as exc:
                logger.error("[%s] 发货消息发送失败：%s", self.name, exc)
                await self.db.record_order(
                    account_id=self.account_id, order_id=msg.order_id, item_id=msg.item_id,
                    buyer_id=msg.sender_id, buyer_name=msg.sender_name, rule_id=rule.get("id"),
                    card_id=None, content=content, status="send_failed", detail=str(exc),
                    amount_cny=amount_cny, quota_usd=quota_usd, card_code=card_code,
                    api_key_id=api_key_id or None,
                    key_order_id=key_order_id or None,
                )
                return False

        # ★ 不可逆点：消息已经到买家手上了。
        #   从这里往后只允许「记账失败」，绝不允许把这一单当成「没发出去」——
        #   否则会退卡（同一张卡卖给下一个人）+ 不落 delivered（轮询重发同一单）。
        decision.sent = True

        confirm_note = ""
        if decision.auto_confirm and msg.order_id:
            try:
                ok, detail = await self.client.confirm_shipping(msg.order_id)
                confirm_note = ("确认发货成功：" if ok else "确认发货失败：") + detail
                logger.info("[%s] 订单 %s %s", self.name, msg.order_id, confirm_note)
            except Exception as exc:  # 确认发货只是「加分项」，失败了也不能回滚发货
                confirm_note = "确认发货失败：" + str(exc)
                logger.warning("[%s] 订单 %s 确认发货异常（卡密已发出，不回滚）：%s",
                               self.name, msg.order_id, exc)

        await self._record_delivered(
            msg, decision, rule, content, confirm_note, amount_cny, quota_usd,
            card_code, external_id, api_key_id, key_order_id,
        )
        logger.info("[%s] 已向 %s 发货（规则：%s，%s 元 → %s USD）",
                    self.name, msg.sender_name, rule.get("name"), amount_cny, quota_usd)
        return True

    async def _record_sending(
        self,
        msg: IncomingMessage,
        rule: dict[str, Any],
        decision: Any,
        content: str,
        amount_cny: float | None,
        quota_usd: float | None,
        card_code: str | None,
        api_key_id: str = "",
        key_order_id: str = "",
    ) -> bool:
        """发送前原子认领订单，给「进程中断」留下可恢复的状态。

        返回 False 表示这一单已经发出、结果未知，或正被另一条路径发送：
        本次必须中止，否则同一次付款会发两张卡 / 充两次值。
        """
        try:
            claimed = await self.db.mark_sending(
                account_id=self.account_id, order_id=msg.order_id, item_id=msg.item_id,
                buyer_id=msg.sender_id, buyer_name=msg.sender_name, rule_id=rule.get("id"),
                card_id=decision.card_id, content=content, status="sending",
                detail="正在发送，等待服务端确认",
                amount_cny=amount_cny, quota_usd=quota_usd, card_code=card_code,
                api_key_id=api_key_id or None,
                key_order_id=key_order_id or None,
            )
        except Exception:
            # 记账失败不能阻止发货；但这一单少了中断保护，日志里要留痕。
            logger.exception("[%s] 订单 %s 发货前写 sending 记录失败",
                             self.name, msg.order_id)
            return True
        if not claimed:
            logger.warning("[%s] 订单 %s 已发出或正在由另一条路径发送，本次跳过",
                           self.name, msg.order_id)
        return claimed

    async def _record_send_unknown(
        self,
        msg: IncomingMessage,
        rule: dict[str, Any],
        decision: Any,
        content: str,
        amount_cny: float | None,
        quota_usd: float | None,
        card_code: str | None,
        *,
        detail: str,
        api_key_id: str = "",
        key_order_id: str = "",
    ) -> None:
        """落一条「发送结果未知」记录；记账失败也不能退卡/重发。"""
        try:
            await self.db.record_order(
                account_id=self.account_id, order_id=msg.order_id, item_id=msg.item_id,
                buyer_id=msg.sender_id, buyer_name=msg.sender_name, rule_id=rule.get("id"),
                card_id=decision.card_id, content=content, status="send_unknown",
                detail=detail,
                amount_cny=amount_cny, quota_usd=quota_usd, card_code=card_code,
                api_key_id=api_key_id or None,
                key_order_id=key_order_id or None,
            )
        except Exception:
            logger.exception(
                "[%s] 订单 %s 发送结果未知，但写 send_unknown 记录失败，请人工核对",
                self.name, msg.order_id,
            )

    async def _record_delivered(self, msg: IncomingMessage, decision: Any, rule: dict[str, Any],
                                content: str, confirm_note: str, amount_cny: float | None,
                                quota_usd: float | None, card_code: str | None,
                                external_id: str = "", api_key_id: str = "",
                                key_order_id: str = "") -> None:
        """发货之后的记账。

        这几种失败只记日志、不往上抛：卡密已经发出去了，抛出去只会触发
        「退回库存 / 重发」这两件更糟的事。内存里的 mark_delivered 照样打上，
        所以本进程内也不会再把同一单发一遍。
        """
        self.engine.mark_delivered(self.account_id, msg.order_id)
        try:
            await self.db.record_order(
                account_id=self.account_id, order_id=msg.order_id, item_id=msg.item_id,
                buyer_id=msg.sender_id, buyer_name=msg.sender_name, rule_id=rule.get("id"),
                card_id=decision.card_id, content=content, status="delivered",
                detail=confirm_note, amount_cny=amount_cny, quota_usd=quota_usd,
                card_code=card_code, api_key_id=api_key_id or None,
                key_order_id=key_order_id or None,
            )
        except Exception:
            logger.exception("[%s] 订单 %s 已发货，但写发货记录失败（不会重发，请手工补记录）",
                             self.name, msg.order_id)
        try:
            await self.db.log_message(
                account_id=self.account_id, chat_id=msg.chat_id, sender_id=msg.sender_id,
                sender_name=msg.sender_name, item_id=msg.item_id, order_id=msg.order_id,
                content=content, kind="outgoing",
                external_id=external_id or None,
            )
        except Exception:
            logger.exception("[%s] 订单 %s 已发货，但写消息流水失败", self.name, msg.order_id)

    async def _reuse_provision(self, msg: IncomingMessage) -> PreparedDelivery | None:
        """上一次已经建好 / 充好、但还没确认送达的内容，直接拿来重发。

        只认 provisioned / send_failed：upstream_failed / topup_failed 没有可用
        结果，delivered / send_unknown 不应该走到这里（engine 已经挡掉）。

        充值模式尤其不能重来：再充一次就是真金白银多给一份额度。
        """
        if not msg.order_id:
            return None
        row = await self.db.get_order(self.account_id, msg.order_id)
        if not row:
            return None
        if str(row.get("status") or "") not in ("provisioned", "send_failed"):
            return None
        card_code = str(row.get("card_code") or "").strip()
        content = str(row.get("content") or "")
        if not card_code or not content:
            return None
        return PreparedDelivery(
            card_code=card_code,
            amount_cny=row.get("amount_cny"),
            quota_usd=float(row.get("quota_usd") or 0.0),
            content=content,
            api_key_id=str(row.get("api_key_id") or ""),
            key_order_id=str(row.get("key_order_id") or ""),
        )

    async def _record_provisioned(
        self,
        msg: IncomingMessage,
        rule: dict[str, Any],
        prepared: PreparedDelivery,
    ) -> None:
        """在充值 / 建好 Key 之后、发送之前先落库。

        这样进程重启或发送失败时，下一轮能复用同一份结果，而不是重新执行上游动作。
        记账失败只记日志：不能因为写不进去就不发货，但会多一次孤儿风险。
        """
        mode = str(rule.get("mode") or "")
        topup = mode == "topup"
        detail = ("已充值，等待发送" if topup
                  else "已给账号加好额度，等待发送" if mode == "account"
                  else "已生成兑换码，等待发送" if mode == "redeem"
                  else "已建 Key/卡密，等待发送")
        try:
            await self.db.record_order(
                account_id=self.account_id, order_id=msg.order_id, item_id=msg.item_id,
                buyer_id=msg.sender_id, buyer_name=msg.sender_name, rule_id=rule.get("id"),
                card_id=None, content=prepared.content, status="provisioned",
                detail=detail,
                amount_cny=prepared.amount_cny, quota_usd=prepared.quota_usd,
                card_code=prepared.card_code,
                api_key_id=prepared.api_key_id or None,
                key_order_id=prepared.key_order_id or None,
            )
        except Exception:
            logger.exception("[%s] 订单 %s 已准备好发货内容但写 provisioned 记录失败",
                             self.name, msg.order_id)

    async def _cny_rate_for_site(self) -> float:
        """订单人民币 → 额度数字这一步该用的汇率。

        CNY/CUSTOM 展示站点里，quota 本身就是按人民币计价，不能再按
        XY_CNY_PER_USD 除一次 —— 否则 10 元订单在 CNY 7.3 站点上只会拿到
        ¥1.37（双重收窄）。只有 USD 展示站点才应用 XY_CNY_PER_USD。
        """
        rate = settings.cny_per_usd
        newapi = getattr(self.provisioner, "newapi", None)
        if newapi is None:
            return rate
        try:
            model = await newapi.site_quota_model()
        except Exception as exc:
            logger.warning("[%s] 读取站点货币失败，按 XY_CNY_PER_USD=%s 换算：%s",
                           self.name, rate, exc)
            return rate
        if str(getattr(model, "currency", "USD")).upper() != "USD":
            return 1.0
        return rate

    async def _maybe_fill_amount(self, msg: IncomingMessage) -> None:
        """规则里有金额区间时，先把订单实付金额查出来。

        闲鱼多规格商品在订单里**不返回 SKU 名称**，只返回实际成交价
        （priceVO.totalPrice / auctionPrice）。所以「1 元 / 5 元 / 10 元」
        这种规格只能靠金额区分；金额没拿到就先不匹配，避免用通用规则发错额度。
        """
        if msg.amount_cny_hint is not None or not msg.order_id:
            return
        rules = await self.db.list_rules(only_enabled=True)
        needs = False
        for rule in rules:
            target = str(rule.get("item_id") or "*").strip()
            if msg.item_id and target not in (str(msg.item_id), "*", "", "all"):
                continue
            if amount_bounds(rule) is not None:
                needs = True
                break
        if not needs:
            return
        order = await self._find_order_cached(msg.order_id)
        amount = amount_from_order_item(order or {}, settings.order_amount_unit)
        if amount is not None:
            msg.amount_cny_hint = amount
            logger.info("[%s] 订单 %s 取到实付金额 %.2f，用于匹配金额区间规则",
                        self.name, msg.order_id, amount)
        else:
            logger.info("[%s] 订单 %s 暂时没取到金额，金额区间规则等下一轮再匹配",
                        self.name, msg.order_id)

    async def _prepare_amount(
        self, msg: IncomingMessage, rule: dict[str, Any], *, status: str = "upstream_failed"
    ) -> tuple[float | None, float] | None:
        """查订单金额 → 校验上限 → 换算额度。

        失败时已经落库（status 决定记成「上游失败」还是「充值失败」），返回 None。
        建卡密和充值共用这一段：金额口径必须完全一致。
        """
        assert self.client is not None
        rule_fixed = float(rule.get("fixed_usd") or 0)

        # 订单轮询那条路已经带了实付金额，直接用，省掉一次订单详情往返
        amount_cny: float | None = msg.amount_cny_hint

        if amount_cny is None:
            order = await self._find_order_cached(msg.order_id or "")
            # 这一步不能晚于建令牌：new-api 的令牌名就是「买家用户名#订单号」，
            # 名字定下来之后再改，后台里留下的还是那条「买家#…」。
            await self._name_from_order(msg, order)
            if order is None:
                if rule_fixed > 0:
                    logger.warning("[%s] 查不到订单 %s 金额，改用规则里的固定额度 %s USD",
                                   self.name, msg.order_id, rule_fixed)
                else:
                    logger.error("[%s] 查不到订单 %s 的金额", self.name, msg.order_id)
                    await self._record_failure(msg, rule, "查不到订单金额", status=status)
                    return None
            else:
                try:
                    amount_cny = pick_amount(order, settings.order_amount_unit).cny
                except AmountError as exc:
                    if rule_fixed > 0:
                        logger.warning("[%s] 订单 %s 金额解析失败（%s），改用固定额度 %s USD",
                                       self.name, msg.order_id, exc, rule_fixed)
                    else:
                        logger.error("[%s] 订单 %s 金额解析失败：%s", self.name, msg.order_id, exc)
                        await self._record_failure(msg, rule, "金额解析失败", status=status)
                        return None

        if amount_cny is not None and settings.max_order_cny > 0 and amount_cny > settings.max_order_cny:
            logger.error("[%s] 订单 %s 金额 %s 元超过上限 %s 元，拒绝自动发货",
                         self.name, msg.order_id, amount_cny, settings.max_order_cny)
            await self._record_failure(
                msg, rule, "订单金额超上限", amount_cny=amount_cny, status=status
            )
            return None

        quota = quota_from_amount(
            amount_cny or 0.0,
            multiplier=float(rule.get("usd_multiplier") or 1),
            fixed_usd=rule_fixed,
            cny_per_usd=await self._cny_rate_for_site(),
        )
        if quota <= 0:
            logger.error("[%s] 订单 %s 换算额度为 0，跳过", self.name, msg.order_id)
            await self._record_failure(
                msg, rule, "换算额度为 0", amount_cny=amount_cny, status=status
            )
            return None
        return amount_cny, quota

    async def _provision(self, msg: IncomingMessage,
                         rule: dict[str, Any]) -> PreparedDelivery | None:
        """上游模式：建 Key → 出卡密 → 返回待发送内容；失败返回 None。"""
        if self.provisioner is None:
            logger.error("[%s] 规则是上游模式，但没有配置 new-api / 卡密站", self.name)
            await self._record_failure(msg, rule, "上游未配置")
            return None
        if not msg.order_id:
            logger.error("[%s] 上游模式需要订单号，但这条消息里没有", self.name)
            await self._record_failure(msg, rule, "缺少订单号")
            return None

        prepared = await self._prepare_amount(msg, rule)
        if prepared is None:
            return None
        amount_cny, quota = prepared

        # 卡密名称 = 令牌名 = 买家用户名#订单号；价格只进额度和日志
        label = build_card_label(msg.sender_name or "买家", msg.order_id)

        # 每个商品可以有自己的模型白名单（后台「设置 → 按商品指定模型」）。
        # 这里读一次库再决定这一单用哪一组：商品级 > * 兜底 > 全局。
        item_models, overrides = await self.db.model_settings()
        model_limits, model_source = resolve_model_limits(msg.item_id, item_models, overrides)
        if model_source.startswith("item:"):
            logger.info("[%s] 商品 %s 使用单独模型白名单：%s",
                        self.name, msg.item_id or "*", model_limits or "不限")

        try:
            provisioned = await self.provisioner.provision(
                quota, label, username=msg.sender_name or "买家",
                model_limits=model_limits,
            )
        except UpstreamError as exc:
            logger.error("[%s] 上游建卡密失败：%s", self.name, exc)
            await self._record_failure(
                msg, rule, "上游建卡密失败：" + str(exc),
                amount_cny=amount_cny, quota_usd=quota,
            )
            return None

        # 这一步只是「顺手记住」：建 Key 已经成功、货也快发出去了，
        # 绑定失败最多让下一单再开一个新 Key，绝不能因此不发这一单。
        await self._bind_buyer_after_provision(msg, provisioned)

        content = render(
            rule["content"],
            buyer_name=msg.sender_name,
            item_id=msg.item_id,
            order_id=msg.order_id,
            card=provisioned.card_code,
            key=provisioned.api_key,
            amount_cny=amount_cny,
            quota_usd=quota,
        )
        return PreparedDelivery(
            card_code=provisioned.card_code,
            amount_cny=amount_cny,
            quota_usd=quota,
            content=content,
            api_key_id=provisioned.token_id,
        )

    async def _redeem(self, msg: IncomingMessage,
                      rule: dict[str, Any]) -> PreparedDelivery | None:
        """兑换码模式：金额 → new-api 兑换码 → 灌进卡密 → 发卡密。

        为什么要多一层卡密：聊天里只出现卡密串，买家到卡密站兑换后才看到真正的
        new-api 兑换码。这样「一次性展示 / 留底 / 客服补发」仍然都由卡密站负责，
        兑换码本身不会躺在聊天记录里被转发。

        注意卡密站 2025-10 改版后是「一个兑换码一张卡」，卡密里**只有**兑换码，
        放不下自定义说明 —— 所以兑换步骤要写在发给买家的那条消息里。

        幂等分两段，都是为了「不重复生成码」：
          * 码生成成功后先落 redeem_card_failed + card_code=码：卡密站挂了，下一轮
            复用同一个码，不会每 30 秒往 new-api 里灌一个孤儿码；
          * 卡密建好之后由 _record_provisioned 落 provisioned：发送失败复用卡密。
        """
        if self.provisioner is None:
            logger.error("[%s] 规则是兑换码模式，但没有配置 new-api", self.name)
            await self._record_failure(msg, rule, "new-api 未配置", status="redeem_failed")
            return None
        if not msg.order_id:
            logger.error("[%s] 兑换码模式需要订单号，但这条消息里没有", self.name)
            await self._record_failure(msg, rule, "缺少订单号", status="redeem_failed")
            return None
        if not getattr(self.provisioner, "can_issue_cards", True):
            # 卡密站没配时连码都别生成：生成了也没地方放，只会每轮多一个孤儿码。
            logger.error("[%s] 规则是兑换码模式，但卡密站没有配置", self.name)
            await self._record_failure(
                msg, rule, "卡密站未配置：兑换码要先灌进卡密才能发",
                status="redeem_failed",
            )
            return None

        prepared = await self._prepare_amount(msg, rule, status="redeem_failed")
        if prepared is None:
            return None
        amount_cny, quota = prepared

        code = await self._redeem_code_for(msg, rule, quota, amount_cny)
        if not code:
            return None

        login_url = self._login_url()
        # 卡密站现在只收「兑换码」本身（一个码一张卡），不能再塞自定义文本，
        # 所以兑换步骤只能写在聊天里 —— 见下面的 content。
        label = build_card_label(msg.sender_name or "买家", msg.order_id)
        try:
            card_code = await self.provisioner.issue_card(code, quota, label)
        except Exception as exc:
            # 卡密站挂了：码已经生成并落库（下面这行把它记进 card_code），
            # 下一轮复用同一个码重试，不会重复生成。
            logger.error("[%s] 订单 %s 兑换码已生成，但卡密没建成：%s",
                         self.name, msg.order_id, exc)
            await self._record_failure(
                msg, rule, "卡密站生成卡密失败：" + str(exc),
                amount_cny=amount_cny, quota_usd=quota,
                status="redeem_card_failed", card_code=code,
            )
            return None

        content = render(
            str(rule.get("content") or "").strip() or DEFAULT_REDEEM_CONTENT,
            buyer_name=msg.sender_name, item_id=msg.item_id, order_id=msg.order_id,
            card=card_code, quota_usd=quota, amount_cny=amount_cny,
            login_url=login_url, shop_url=str(settings.cardshop_base_url or "").rstrip("/"),
        )
        if not content.strip():
            # 卡密已经建好了，不能因为文案为空就记失败（下一轮会再生成一套）
            content = "卡密：" + card_code
        logger.info("[%s] 订单 %s 已生成兑换码并灌进卡密（%s，%s 额度）",
                    self.name, msg.order_id, label, quota)
        return PreparedDelivery(
            card_code=card_code,
            amount_cny=amount_cny,
            quota_usd=quota,
            content=content,
        )

    async def _redeem_code_for(self, msg: IncomingMessage, rule: dict[str, Any],
                               quota: float, amount_cny: float | None) -> str:
        """拿到这一单的兑换码：能复用上轮的就复用，否则生成一个新的。

        复用的前提是上一轮卡在「卡密站没建成」：那时码已经落进 card_code，
        还等着被包进卡密。别的地方（比如已经发货）不会走到这里。
        """
        row = await self.db.get_order(self.account_id, msg.order_id)
        if str((row or {}).get("status") or "") == "redeem_card_failed":
            existing = str((row or {}).get("card_code") or "").strip()
            if existing:
                logger.info("[%s] 订单 %s 复用上次生成的兑换码，重试卡密站",
                            self.name, msg.order_id)
                return existing

        # 兑换码名称限 20 个字：和卡密 label 不是同一个（那个会超长）
        label = build_redeem_label(msg.sender_name or "买家", msg.order_id or "")
        try:
            valid_days = int(rule.get("redeem_expiry_days") or 0)
        except (TypeError, ValueError):
            valid_days = 0
        try:
            codes = await self.provisioner.redeem_codes(
                quota, label, valid_days=max(0, valid_days))
        except Exception as exc:
            logger.error("[%s] 订单 %s 生成兑换码失败：%s", self.name, msg.order_id, exc)
            await self._record_failure(
                msg, rule, "生成兑换码失败：" + str(exc),
                amount_cny=amount_cny, quota_usd=quota, status="redeem_failed",
            )
            return ""
        code = str(codes[0] if codes else "").strip()
        if not code:
            await self._record_failure(
                msg, rule, "生成兑换码失败：返回为空",
                amount_cny=amount_cny, quota_usd=quota, status="redeem_failed",
            )
        return code
    async def _resolve_buyer_key(self, msg: IncomingMessage, *,
                                 item_id: str = "",
                                 item_scope: str = TOPUP_ITEM_SCOPE_ANY,
                                 ) -> dict[str, Any] | None:
        """充值模式的「这个买家用哪个 Key」。

        顺序：
          1. 后台手工绑定 / 之前自动绑定的记录；
          2. 买家最近一单确实收到货的令牌（老客户不用重买一次）；
          3. 都没有 → 按规则决定开新 Key 还是拒绝发货（由 _topup 处理）。

        规则开了「只认同商品」（item_scope=item）时：绑定要先能证明它在这个商品上
        发过（后台手工绑定算运营的明确意图，照用）；历史订单只在这个商品里找。
        否则买家之前买过 kimi、这次来续 deepseek，会被自动充到 kimi 的 Key 上。
        """
        only_item = (str(item_id or "").strip()
                     if str(item_scope or "").strip().lower() == TOPUP_ITEM_SCOPE_ITEM
                     else "")
        binding = await self.db.get_buyer_key(msg.sender_id)
        if binding is not None and only_item:
            token_id = str(binding.get("token_id") or "").strip()
            if not await self.db.buyer_key_matches_item(
                    msg.sender_id, only_item, token_id):
                if str(binding.get("source") or "") == "manual":
                    logger.warning(
                        "[%s] 买家 %s 的绑定（手工）不是商品 %s 发过的 Key（令牌 %s）；"
                        "按运营意图照用",
                        self.name, msg.sender_name or msg.sender_id, only_item,
                        token_id or "-",
                    )
                else:
                    logger.info(
                        "[%s] 买家 %s 的绑定属于别的商品，按商品 %s 重新找 Key",
                        self.name, msg.sender_name or msg.sender_id, only_item,
                    )
                    binding = None
        if binding is not None:
            return binding

        # 没找到 Key 的缓存按「买家 + 商品」记：换个商品再来问不该被上一次的缓存挡住
        miss_key = msg.sender_id + "|" + only_item
        miss_at = self._buyer_key_miss.get(miss_key)
        if miss_at is not None and (time.monotonic() - miss_at) < 600:
            return None
        rows = await self.db.recent_delivered_orders(
            self.account_id, msg.sender_id, item_id=only_item)
        if not rows:
            return None

        token_id = ""
        token_name = ""
        api_key = ""
        key_mask = ""
        for row in rows:
            candidate = str(row.get("api_key_id") or "").strip()
            if candidate:
                token_id = candidate
                break

        if not token_id:
            # 早期订单没记令牌 id：令牌名一直是「买家用户名#订单号」，
            # 拿最近几单的订单号一次性反查，老客户不用重买一次。
            finder = getattr(self.provisioner, "find_tokens_for_orders", None)
            if finder is not None:
                order_ids = [str(row.get("order_id") or "") for row in rows]
                try:
                    mapping = await finder([oid for oid in order_ids if oid])
                except Exception as exc:
                    logger.warning("[%s] 买家 %s 按历史订单反查令牌失败：%s",
                                   self.name, msg.sender_id, exc)
                    mapping = {}
                for row in rows:
                    token = mapping.get(str(row.get("order_id") or ""))
                    if not token:
                        continue
                    token_id = str(token.get("id") or "").strip()
                    token_name = str(token.get("name") or "")
                    api_key = normalize_key(str(token.get("key") or ""))
                    key_mask = mask_key(api_key) if api_key else ""
                    break

        if not token_id:
            # 记一下，10 分钟内不再为这个买家（这个商品）扫令牌列表
            self._buyer_key_miss[miss_key] = time.monotonic()
            cap_mapping(self._buyer_key_miss, 1000)
            return None
        self._buyer_key_miss.pop(miss_key, None)
        try:
            await self.db.auto_bind_buyer_key({
                "buyer_id": msg.sender_id,
                "buyer_name": msg.sender_name or "买家",
                "account_id": self.account_id,
                "token_id": token_id,
                "token_name": token_name,
                "api_key": api_key,
                "key_mask": key_mask,
                "source": "history",
            })
            logger.info("[%s] 买家 %s 从历史订单回溯到令牌 %s，之后充值直接用它",
                        self.name, msg.sender_name or msg.sender_id, token_id)
        except Exception:
            logger.exception("[%s] 买家 %s 回溯历史 Key 失败", self.name, msg.sender_id)
        return await self.db.get_buyer_key(msg.sender_id)

    async def _bind_buyer_after_provision(self, msg: IncomingMessage,
                                          provisioned: Any) -> None:
        """把刚建好的 Key 记到买家名下；后台手工绑过的不覆盖。"""
        if not msg.sender_id or not getattr(provisioned, "token_id", ""):
            return
        try:
            await self.db.auto_bind_buyer_key({
                "buyer_id": msg.sender_id,
                "buyer_name": msg.sender_name or "买家",
                "account_id": self.account_id,
                "token_id": provisioned.token_id,
                "token_name": getattr(provisioned, "token_name", ""),
                "api_key": getattr(provisioned, "api_key", ""),
                "key_mask": getattr(provisioned, "key_mask", ""),
                "source": "order",
            })
        except Exception:
            logger.exception("[%s] 订单 %s 已发货，但自动记住买家 Key 失败",
                             self.name, msg.order_id)

    async def _remember_buyer_key_topup(self, msg: IncomingMessage, result: Any,
                                        quota: float) -> None:
        """充值成功后更新绑定记录与累计额度。失败只记日志，不影响发货。"""
        if not msg.sender_id:
            return
        try:
            existing = await self.db.get_buyer_key(msg.sender_id)
            if existing is None or str(existing.get("source") or "") != "manual":
                await self.db.auto_bind_buyer_key({
                    "buyer_id": msg.sender_id,
                    "buyer_name": msg.sender_name or "买家",
                    "account_id": self.account_id,
                    "token_id": getattr(result, "token_id", ""),
                    "token_name": getattr(result, "token_name", ""),
                    "api_key": getattr(result, "api_key", ""),
                    "key_mask": getattr(result, "key_mask", ""),
                    "balance": getattr(result, "balance", None),
                    "source": "order",
                })
            row = await self.db.get_buyer_key(msg.sender_id)
            if row is not None:
                await self.db.note_buyer_key_topup(
                    int(row["id"]), quota,
                    getattr(result, "balance", None),
                    getattr(result, "api_key", "") or "",
                    getattr(result, "key_mask", "") or "",
                    getattr(result, "token_name", "") or "",
                )
        except Exception:
            logger.exception("[%s] 订单 %s 充值成功，但更新买家 Key 记录失败",
                             self.name, msg.order_id)

    @staticmethod
    def _topup_item_of(item_id: Any, rule: dict[str, Any]) -> str:
        """这一单是哪个商品：优先消息 / 订单里的 item_id，其次规则的固定商品。

        规则可以是商品通配（item_id=星号），那种情况拿不到具体商品，返回空串。
        """
        item = str(item_id or "").strip()
        if item:
            return item
        declared = str(rule.get("item_id") or "").strip()
        return "" if not declared or declared == "*" else declared

    @staticmethod
    def _key_order_failure_detail(key_order_id: str, reason: str, *,
                                  token_id: str = "") -> str:
        """写进「等买家发 Key / 旧订单号」记录的 detail：这次为什么没充。

        Key 路径只写令牌 id：买家发来的 Key 是凭证，不能进订单详情、
        更不能进日志（后台是会被截图和转发的）。
        """
        reason = str(reason or "")
        why = _KEY_ORDER_FAILURE_TEXT.get(reason, "找不到对应 Key")
        if token_id:
            given = "Key（令牌 " + str(token_id) + "）"
        elif reason.startswith("key_"):
            given = "Key"
        else:
            given = "旧订单号 " + str(key_order_id)
        return "买家提供的 " + given + "：" + why

    @staticmethod
    def _prompt_default(rule: dict[str, Any]) -> str:
        """催买家发东西的默认文案：发 Key 和发订单号是两句话。"""
        source = str(rule.get("topup_key_source") or TOPUP_KEY_SOURCE_ORDER)
        source = source.strip().lower()
        return (DEFAULT_TOPUP_KEY_PROMPT if source == TOPUP_KEY_SOURCE_API_KEY
                else DEFAULT_TOPUP_PROMPT)

    async def _topup(self, msg: IncomingMessage, rule: dict[str, Any],
                     *, key_order_id: str = "", key_token_id: str = "",
                     quota_hint: float | None = None) -> PreparedDelivery | None:
        """充值模式：把这一单的额度充到买家已有的 Key 上，再生成发货文案。

        两种「用哪个 Key」策略：
          * order_id（默认）→ 买家必须先发一笔旧订单号，只充那一单对应的 Key；
            找不到就提示，订单停在 awaiting_key 等买家重发；
          * auto → 已绑定记录 / 最近一单 / 首单开新 Key（老行为）。
        """
        if self.provisioner is None:
            logger.error("[%s] 规则是充值模式，但没有配置 new-api", self.name)
            await self._record_failure(msg, rule, "new-api 未配置", status="topup_failed")
            return None
        if not msg.order_id:
            logger.error("[%s] 充值模式需要订单号，但这条消息里没有", self.name)
            await self._record_failure(msg, rule, "缺少订单号", status="topup_failed")
            return None

        prepared = await self._prepare_amount(msg, rule, status="topup_failed")
        if prepared is None:
            return None
        amount_cny, quota = prepared
        if quota_hint and quota_hint > 0:
            # 买家第一次答复「旧订单号」时，按当时算好的额度充；
            # 中途改倍率 / 固定额度不该让已经拍下的人拿到另一个数。
            quota = round(float(quota_hint), 2)
        label = build_card_label(msg.sender_name or "买家", msg.order_id)
        source = str(rule.get("topup_key_source") or TOPUP_KEY_SOURCE_ORDER).strip().lower()
        # 这一单是哪个商品：跨商品保护、以及「绑定全属于哪个商品」都按它判断。
        # 拿不到商品 ID 时不拦（没有证据就按老行为走）。
        scope = str(rule.get("topup_item_scope") or TOPUP_ITEM_SCOPE_ANY).strip().lower()
        item_hint = self._topup_item_of(msg.item_id, rule)
        if scope == TOPUP_ITEM_SCOPE_ITEM and not item_hint and msg.order_id:
            # 付款卡片经常只带 orderId，不带 itemId；规则又配了「只认同商品」时
            # 必须去订单详情补一次 —— 拿不到商品 ID 就没法拦跨商品，保护等于没有。
            item_hint = await self._item_id_from_order(msg.order_id) or ""
        if item_hint and not msg.item_id:
            # 顺手记回消息里：这一单自己也会成为买家以后可以引用的「旧订单」。
            msg.item_id = item_hint

        # 买家点名的旧订单号：优先用消息里带的，其次用「消息比订单轮询先到」时留下的提示
        explicit_key_order = str(key_order_id or "").strip()
        if not explicit_key_order:
            hint = self._key_order_hints.get(msg.sender_id)
            if hint is not None:
                if time.monotonic() - hint[1] < KEY_ORDER_HINT_TTL:
                    explicit_key_order = hint[0]
                self._key_order_hints.pop(msg.sender_id, None)
        if not explicit_key_order and msg.order_id:
            # 充值失败后的轮询重试：买家之前已经为这一单指定过旧订单号，直接复用，
            # 不要因为一次上游抖动又让买家重新发一遍。
            existing = await self.db.get_order(self.account_id, msg.order_id)
            stored = str((existing or {}).get("key_order_id") or "").strip()
            if stored:
                explicit_key_order = stored
                logger.info("[%s] 订单 %s 重试时复用买家提供的旧订单号 %s",
                            self.name, msg.order_id, stored)

        # 「买家直接发 Key」：买家给的就是令牌，不用再绕旧订单号。
        explicit_key_token = str(key_token_id or "").strip()
        if not explicit_key_token and source == TOPUP_KEY_SOURCE_API_KEY:
            hint = self._key_hints.get(msg.sender_id)
            if hint is not None:
                if time.monotonic() - hint[1] < KEY_ORDER_HINT_TTL:
                    explicit_key_token = hint[0]
                self._key_hints.pop(msg.sender_id, None)
        if not explicit_key_token and source == TOPUP_KEY_SOURCE_API_KEY and msg.order_id:
            # 充值失败后的轮询重试：上一轮已经核对过的目标令牌记在 api_key_id 上，
            # 直接复用，别因为一次上游抖动又让买家重发一遍 Key。
            existing = await self.db.get_order(self.account_id, msg.order_id)
            stored_token = str((existing or {}).get("api_key_id") or "").strip()
            if stored_token:
                explicit_key_token = stored_token
                logger.info("[%s] 订单 %s 重试时复用买家提供的 Key（令牌 %s）",
                            self.name, msg.order_id, stored_token)

        resolved = None
        key_reason = ""
        if explicit_key_token:
            lookup = await self._token_for_token_id(explicit_key_token)
            resolved = lookup.resolved
            key_reason = lookup.reason
            if key_reason == "unreadable":
                # Key 路径下「读不到令牌」= 这个 Key 多半被删/换代了，
                # 不能套用「没有找到这个订单号」那句文案。
                key_reason = "key_gone"
        elif explicit_key_order:
            lookup = await self._token_for_key_order(
                msg.sender_id, explicit_key_order,
                item_id=item_hint, item_scope=scope,
            )
            resolved = lookup.resolved
            key_reason = lookup.reason
        if (explicit_key_token or explicit_key_order) and resolved is None:
            if source in (TOPUP_KEY_SOURCE_ORDER, TOPUP_KEY_SOURCE_API_KEY):
                logger.warning("[%s] 订单 %s：买家提供的 %s 不能用来充值（%s）",
                               self.name, msg.order_id,
                               (("Key（令牌 " + explicit_key_token + "）")
                                if explicit_key_token else explicit_key_order),
                               _KEY_ORDER_FAILURE_TEXT.get(key_reason, "找不到对应 Key"))
                await self._record_awaiting_key(
                    msg, rule, amount_cny, quota,
                    detail=self._key_order_failure_detail(
                        explicit_key_order, key_reason, token_id=explicit_key_token),
                    key_order_id=explicit_key_order,
                )
                await self._send_topup_not_found(
                    msg, rule, explicit_key_order,
                    order_id=msg.order_id or "", amount_cny=amount_cny, quota_usd=quota,
                    reason=key_reason,
                )
                return None
            # auto 模式：提示里的订单号 / Key 失效就退回自动匹配，不要卡住发货
            logger.warning("[%s] 自动模式忽略失效的旧订单号 / Key：%s",
                           self.name, explicit_key_order or explicit_key_token)
            explicit_key_order = ""
            explicit_key_token = ""

        if resolved is not None:
            binding = {
                "token_id": resolved["token_id"],
                "api_key": resolved["api_key"],
                "key_order_id": resolved["key_order_id"],
            }
        elif source in (TOPUP_KEY_SOURCE_ORDER, TOPUP_KEY_SOURCE_API_KEY):
            waiting_key = source == TOPUP_KEY_SOURCE_API_KEY
            logger.info("[%s] 订单 %s 在等买家发%s，先提示不充值",
                        self.name, msg.order_id, " Key" if waiting_key else "旧订单号")
            # 先落 awaiting_key 再发提示：买家看到提示后立刻回复时，
            # 消息处理一定能查到这笔等待中的订单（顺序反过来会有竞态窗口）。
            await self._record_awaiting_key(
                msg, rule, amount_cny, quota,
                detail=("等待买家发要充值的 Key" if waiting_key
                        else "等待买家提供对应 Key 的旧订单号"),
            )
            sent = await self._send_topup_prompt(
                msg, rule, order_id=msg.order_id or "",
                amount_cny=amount_cny, quota_usd=quota,
                default_template=self._prompt_default(rule),
            )
            if sent is False:
                # 提示确实没发出去，不能停在 awaiting_key（轮询会跳过、永远等不到）；
                # 记失败让下一轮再试。冷却期内主动跳过（None）则继续等。
                await self._record_failure(
                    msg, rule, "提示买家提供旧订单号失败（消息未发出）",
                    amount_cny=amount_cny, quota_usd=quota, status="topup_failed",
                )
            return None
        else:
            binding = await self._resolve_buyer_key(
                msg, item_id=item_hint, item_scope=scope)

        token_id = str((binding or {}).get("token_id") or "").strip()
        api_key = str((binding or {}).get("api_key") or "").strip()
        if binding is not None and not token_id and not api_key:
            binding = None

        if binding is None:
            if str(rule.get("topup_missing") or "create").strip().lower() == "fail":
                reason = "买家还没有绑定 Key；到「买家 Key」页绑定后这一单会自动重试"
                logger.warning("[%s] 订单 %s：%s", self.name, msg.order_id, reason)
                await self._record_failure(
                    msg, rule, reason, amount_cny=amount_cny, quota_usd=quota,
                    status="topup_failed",
                )
                return None
            item_models, overrides = await self.db.model_settings()
            # 付款卡片经常不带 itemId：这时按规则上写的商品取模型白名单，
            # 别用全局默认给这个商品开出一把「能调所有模型」的 Key。
            model_limits, _ = resolve_model_limits(
                item_hint or msg.item_id, item_models, overrides)
            try:
                result = await self.provisioner.open_key(
                    quota, label, username=msg.sender_name or "买家",
                    model_limits=model_limits,
                )
            except Exception as exc:  # 建 Key 失败是确定的，可以安全重试
                logger.error("[%s] 买家还没有 Key，自动开 Key 失败：%s", self.name, exc)
                await self._record_failure(
                    msg, rule, "自动开 Key 失败：" + str(exc),
                    amount_cny=amount_cny, quota_usd=quota, status="topup_failed",
                )
                return None
            logger.info("[%s] 买家 %s 首次使用充值模式，已开新 Key 并绑定（令牌 %s）",
                        self.name, msg.sender_name or msg.sender_id, result.token_id)
        else:
            # ★ 充值是不可逆外呼：先落一条 topping_up，再发请求。
            #   万一进程/网络在「已扣额度、未写记录」之间断掉，下次不会当成没充过重来。
            marked = await self._record_topup_pending(
                msg, rule, binding, amount_cny, quota, key_order_id=explicit_key_order
            )
            if not marked:
                # 没有「充值意图」落库就直接扣额度，进程一崩就没人知道充过没有；
                # 宁可这一轮不发，等数据库恢复后重试。
                logger.error("[%s] 订单 %s 无法写入充值意图记录，已中止本次充值",
                             self.name, msg.order_id)
                await self._record_failure(
                    msg, rule, "无法写入充值记录，已中止（可重试）",
                    amount_cny=amount_cny, quota_usd=quota,
                    status="topup_failed", key_order_id=explicit_key_order,
                )
                return None
            try:
                result = await self.provisioner.topup(quota, token_id=token_id, api_key=api_key)
            except TopupUncertainError as exc:
                logger.error("[%s] 订单 %s 充值结果未知：%s", self.name, msg.order_id, exc)
                await self._record_failure(
                    msg, rule, "充值结果未知：" + str(exc),
                    amount_cny=amount_cny, quota_usd=quota,
                    status="topup_unknown", api_key_id=token_id,
                    key_order_id=explicit_key_order,
                )
                return None
            except Exception as exc:
                logger.error("[%s] 订单 %s 充值失败：%s", self.name, msg.order_id, exc)
                await self._record_failure(
                    msg, rule, "充值失败：" + str(exc),
                    amount_cny=amount_cny, quota_usd=quota,
                    status="topup_failed", api_key_id=token_id,
                    key_order_id=explicit_key_order,
                )
                return None
            logger.info("[%s] 订单 %s 已充值 %s 到买家 %s 的令牌 %s",
                        self.name, msg.order_id, quota, msg.sender_name or msg.sender_id, token_id)

        if not getattr(result, "topped_up", True):
            logger.warning(
                "[%s] 订单 %s 的 Key 本身不限额度，已跳过充值（仍会正常发货）",
                self.name, msg.order_id,
            )
        await self._remember_buyer_key_topup(msg, result, quota)

        # 完整 Key 拿不到时退回打码形式：买家至少能认出「是原来那个 Key」。
        # 连打码值都没有时给一句可读的说明，保证落库的 card_code 永远非空 ——
        # 否则发送失败后 _reuse_provision 会认为没准备好，下一轮又充一次（重复送钱）。
        key_text = result.api_key or result.key_mask or "（你原来购买时收到的 Key）"
        record_code = key_text
        symbol = {"USD": "$", "CNY": "¥"}.get(str(result.currency or "USD").upper(), "")
        balance_text = "" if result.balance is None else f"{symbol}{result.balance:.2f}"
        template = str(rule.get("content") or "").strip() or DEFAULT_TOPUP_CONTENT
        content = render(
            template,
            buyer_name=msg.sender_name,
            item_id=msg.item_id,
            order_id=msg.order_id,
            card=key_text,
            key=key_text,
            amount_cny=amount_cny,
            quota_usd=quota,
            balance=balance_text,
        )
        if not content.strip():
            # ★ 钱已经充进去了，不能因为文案为空就记失败 —— 那会让下一轮重试再充一次。
            content = f"已为你充值 {quota} 额度到原来的 Key：\n\n{key_text}"
        return PreparedDelivery(
            card_code=record_code,
            amount_cny=amount_cny,
            quota_usd=quota,
            content=content,
            api_key_id=result.token_id,
            key_order_id=explicit_key_order,
        )

    async def _record_topup_pending(self, msg: IncomingMessage, rule: dict[str, Any],
                                    binding: dict[str, Any], amount_cny: float | None,
                                    quota: float, *, key_order_id: str = "") -> bool:
        """充值前先落一句「正在充值」，给不可逆外呼留恢复线索。

        返回 False 表示这条意图没写进去。充值调用方必须据此**中止**：
        没有落库标记的充值，一旦进程在请求发出后崩溃，就再也说不清充没充过。
        """
        try:
            await self.db.record_order(
                account_id=self.account_id, order_id=msg.order_id, item_id=msg.item_id,
                buyer_id=msg.sender_id, buyer_name=msg.sender_name, rule_id=rule.get("id"),
                card_id=None, content="", status="topping_up",
                detail="正在把额度充到已有 Key（令牌 " + str(binding.get("token_id") or "—") + "）",
                amount_cny=amount_cny, quota_usd=quota,
                api_key_id=str(binding.get("token_id") or "") or None,
                key_order_id=key_order_id or None,
            )
            return True
        except Exception:
            logger.exception("[%s] 订单 %s 充值前写 topping_up 记录失败",
                             self.name, msg.order_id)
            return False

    def _order_id_candidates(self, content: str) -> list[str]:
        """从买家消息里挑出可能是旧订单号的数字（15~20 位，去重保序）。"""
        out: list[str] = []
        seen: set[str] = set()
        for match in _ORDER_ID_RE.finditer(str(content or "")):
            value = match.group(0)
            if value in seen:
                continue
            seen.add(value)
            out.append(value)
        return out

    async def _token_for_key_order(self, buyer_id: str, key_order_id: str, *,
                                   item_id: str = "",
                                   item_scope: str = TOPUP_ITEM_SCOPE_ANY,
                                   ) -> KeyOrderLookup:
        """买家点名的旧订单 → 那一单对应的令牌。

        只认「这个买家本人、已经发过货」的订单。订单里没记令牌 id 的老单，
        再按令牌名里的 #订单号 反查一次。

        规则配了「只认同商品」（item_scope=item）时还要核对旧订单的商品：
        买家拿 kimi 那笔订单来续费 deepseek，额度不能充到 kimi 的 Key 上。
        两边任一商品 ID 缺失时放行 —— 没有证据就按老行为走，不能让一个
        当年根本没记的字段把老客户挡在门外。
        """
        key_order = str(key_order_id or "").strip()
        if not key_order:
            return KeyOrderLookup(reason="not_found")
        row = await self.db.find_buyer_delivered_order(buyer_id, key_order)
        if row is None:
            logger.info("[%s] 买家 %s 提供的订单 %s 不是他本人已发货的订单",
                        self.name, buyer_id, key_order)
            return KeyOrderLookup(reason="not_found")

        old_item = str(row.get("item_id") or "").strip()
        wanted = str(item_id or "").strip()
        scoped = str(item_scope or TOPUP_ITEM_SCOPE_ANY).strip().lower()
        if (scoped == TOPUP_ITEM_SCOPE_ITEM and wanted and old_item
                and old_item != "*" and old_item != wanted):
            logger.warning(
                "[%s] 买家 %s 提供的旧订单 %s 属于商品 %s，本单商品是 %s；"
                "规则只认同商品，不充这一单",
                self.name, buyer_id, key_order, old_item, wanted,
            )
            return KeyOrderLookup(reason="other_item", item_id=old_item)

        token_id = str(row.get("api_key_id") or "").strip()
        api_key = ""
        token_name = ""
        if not token_id:
            finder = getattr(self.provisioner, "find_tokens_for_orders", None)
            if finder is None:
                return KeyOrderLookup(reason="no_token", item_id=old_item)
            try:
                mapping = await finder([key_order])
            except Exception as exc:
                logger.warning("[%s] 按旧订单 %s 反查令牌失败：%s", self.name, key_order, exc)
                return KeyOrderLookup(reason="unreadable", item_id=old_item)
            token = mapping.get(key_order)
            if not token:
                return KeyOrderLookup(reason="no_token", item_id=old_item)
            token_id = str(token.get("id") or "").strip()
            api_key = normalize_key(str(token.get("key") or ""))
            token_name = str(token.get("name") or "")
        if not token_id:
            return KeyOrderLookup(reason="no_token", item_id=old_item)

        # 令牌可能已经在 new-api 后台被删了：先确认还能读到，再告诉买家「找到了」
        newapi = getattr(self.provisioner, "newapi", None)
        getter = getattr(newapi, "get_token", None)
        if getter is not None:
            try:
                await getter(token_id)
            except Exception as exc:
                logger.warning("[%s] 旧订单 %s 对应的令牌 %s 读不到：%s",
                               self.name, key_order, token_id, exc)
                return KeyOrderLookup(reason="unreadable", item_id=old_item)
        return KeyOrderLookup(
            resolved={
                "token_id": token_id,
                "api_key": api_key,
                "token_name": token_name,
                "key_order_id": key_order,
                "item_id": old_item,
            },
            item_id=old_item,
        )

    def _api_key_candidates(self, content: str) -> list[str]:
        """从买家消息里挑出可能是本站 Key 的串（去重保序，最多 3 个）。

        Key 和订单号不会混：订单号 15~20 位纯数字，Key 是 sk- + 48 位字母数字。
        一条消息里可能又贴 Key 又贴订单号，所以两边都收，Key 优先。
        """
        out: list[str] = []
        seen: set[str] = set()
        for match in _API_KEY_RE.finditer(str(content or "")):
            key = normalize_key(match.group(0))
            lowered = key.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            out.append(key)
            if len(out) >= 3:
                break
        return out

    @staticmethod
    def _masked_key_present(content: str) -> bool:
        """消息里是不是发了打码后的 Key（长按复制复制到省略号的那种）。"""
        return bool(_MASKED_KEY_RE.search(str(content or "")))

    @staticmethod
    def _allow_bucket(store: dict[str, tuple[int, float]], key: str) -> bool:
        """同一 key 10 分钟内最多 5 次；窗口外自动重置，顺手清理过期桶。"""
        now = time.monotonic()
        count, first = store.get(key, (0, now))
        if now - first > 600:
            count, first = 0, now
        count += 1
        store[key] = (count, first)
        if len(store) > 500:
            for old_key, value in list(store.items()):
                if now - value[1] > 600:
                    store.pop(old_key, None)
        return count <= 5

    def _allow_key_attempt(self, buyer_id: str) -> bool:
        """同一买家 10 分钟内最多试 5 次 Key。

        每次试 Key 都可能要读令牌 / 翻列表，不设上限的话有人刷屏就能把
        上游打满；但也不能设太小，买家手抖多贴两遍是常事。
        """
        return self._allow_bucket(self._key_attempts, buyer_id)

    def _allow_account_lookup(self, buyer_id: str) -> bool:
        """同一买家 10 分钟内最多查 5 次账号名。

        账号模式允许买家发任意用户名并回显余额；不设限就等于把 new-api
        的用户名/余额查询开放给所有人当枚举接口。5 次足够正常人试错。
        """
        return self._allow_bucket(self._account_lookups, buyer_id)

    async def _token_for_token_id(self, token_id: str, *,
                                  api_key: str = "") -> KeyOrderLookup:
        """按令牌 id 取回充值目标（确认令牌还能读到，并尽量拿到明文 Key）。"""
        token_id = str(token_id or "").strip()
        if not token_id:
            return KeyOrderLookup(reason="not_found")
        newapi = getattr(self.provisioner, "newapi", None)
        getter = getattr(newapi, "get_token", None)
        token: dict[str, Any] = {}
        if getter is not None:
            try:
                token = await getter(token_id)
            except Exception as exc:
                logger.warning("[%s] 令牌 %s 读不到：%s", self.name, token_id, exc)
                return KeyOrderLookup(reason="unreadable")
        key = normalize_key(str((token or {}).get("key") or "").strip())
        if not key or "*" in key:
            reader = getattr(self.provisioner, "token_key", None)
            if reader is not None:
                try:
                    key = await reader(token_id)
                except Exception:
                    key = ""
        return KeyOrderLookup(
            resolved={
                "token_id": token_id,
                "api_key": key or api_key,
                "token_name": str((token or {}).get("name") or ""),
                "key_order_id": "",
            },
        )

    async def _token_for_api_key(self, buyer_id: str, api_key: str, *,
                                 buyer_name: str = "") -> KeyOrderLookup:
        """买家直接发 Key：只认「本站发给他本人」的令牌。

        顺序按开销从小到大：
          1. 「买家 Key」页的绑定（最常见，一次查询）；
          2. 他自己的历史订单里发出去的那串 Key（充值模式的 card_code）；
          3. 他名下出现过的令牌逐个读明文 Key 比对；
          4. 本站令牌列表：列表带明文的直接比，打码的按「昵称像他 → 其它」
             读单令牌明文（单令牌接口能拿到明文，列表接口不一定）；
          5. 列表都拿不到时才退回「按 Key 反查」。

        归属只认两种：绑定表 / 历史订单里记过这个令牌 id，或者令牌名里的
        「#订单号」是他已发货的单（老订单没记 api_key_id，但名字里有）。
        都对不上就绝不充：宁可让客服手工处理，也不能往别人的 Key 上充钱。
        """
        key = normalize_key(str(api_key or "").strip())
        if not key or key == "sk-":
            return KeyOrderLookup(reason="key_invalid")

        # 1) 「买家 Key」页的绑定
        own_token_ids: list[str] = []
        binding = await self.db.get_buyer_key(buyer_id)
        if binding is not None:
            token_id = str(binding.get("token_id") or "").strip()
            if token_id:
                own_token_ids.append(token_id)
                bound = normalize_key(str(binding.get("api_key") or ""))
                if bound and bound.lower() == key.lower():
                    return await self._token_for_token_id(token_id, api_key=key)

        # 2) 他自己历史订单里发出去的那串 Key（充值模式的 card_code）
        row = await self.db.find_buyer_key_order(buyer_id, key)
        if row is not None:
            token_id = str(row.get("api_key_id") or "").strip()
            if token_id:
                return await self._token_for_token_id(token_id, api_key=key)

        # 3) 他名下出现过的令牌，逐个读明文比对（只查自己的，天然带归属）
        for token_id in await self.db.buyer_token_ids(buyer_id, limit=20):
            if token_id not in own_token_ids:
                own_token_ids.append(token_id)
        for token_id in own_token_ids:
            got = await self._token_key_cached(token_id)
            if got and got.lower() == key.lower():
                return await self._token_for_token_id(token_id, api_key=key)

        # 4) 本站令牌列表：有的版本直接带明文，一次遍历就能比出来；
        #    打码的版本就按「本人名下 → 名字像他 → 其它」的顺序逐个读明文。
        tokens = await self._site_tokens()
        for token in tokens:
            raw = str(token.get("key") or "")
            if raw and "*" not in raw and normalize_key(raw).lower() == key.lower():
                return await self._key_token_lookup(buyer_id, token, key)
        deadline = time.monotonic() + KEY_SCAN_SECONDS
        for token in self._ordered_key_candidates(tokens, own_token_ids, buyer_name):
            if time.monotonic() > deadline:
                logger.warning(
                    "[%s] 按 Key 核对超时（站上令牌 %s 个），先按「核对不了」回",
                    self.name, len(tokens),
                )
                return KeyOrderLookup(reason="key_unavailable")
            token_id = str(token.get("id") or "").strip()
            got = await self._token_key_cached(token_id)
            if got and got.lower() == key.lower():
                return await self._key_token_lookup(buyer_id, token, key)

        # 5) 列表都拿不到（权限 / 站点异常）时才退回「按 Key 反查」
        if not tokens:
            finder = getattr(self.provisioner, "find_token_by_key", None)
            if finder is not None:
                try:
                    token = await finder(key)
                except UpstreamError as exc:
                    if "不返回明文" in str(exc):
                        logger.warning(
                            "[%s] 站点令牌列表不返回明文 Key，也没法逐条读：%s",
                            self.name, exc)
                        return KeyOrderLookup(reason="key_unavailable")
                    return KeyOrderLookup(reason="key_foreign")
                except Exception as exc:
                    logger.warning("[%s] 按 Key 反查令牌失败：%s", self.name, exc)
                    return KeyOrderLookup(reason="key_unavailable")
                return await self._key_token_lookup(buyer_id, token, key)
        return KeyOrderLookup(reason="key_foreign")

    async def _site_tokens(self) -> list[dict[str, Any]]:
        """本站令牌列表（缓存 5 分钟）。拿不到就返回空列表，不缓存失败。"""
        now = time.monotonic()
        cached = self._site_tokens_cache
        if cached is not None and now - cached[0] < TOKEN_KEY_TTL:
            return cached[1]
        lister = getattr(self.provisioner, "list_tokens", None)
        if lister is None:
            return []
        try:
            tokens = await lister()
        except Exception as exc:
            logger.warning("[%s] 拉取 new-api 令牌列表失败：%s", self.name, exc)
            return []
        self._site_tokens_cache = (now, tokens)
        return tokens

    async def _token_key_cached(self, token_id: str) -> str:
        """读令牌明文 Key（缓存 5 分钟；读不到不缓存，免得一次抖动卡住五分钟）。"""
        token_id = str(token_id or "").strip()
        if not token_id:
            return ""
        now = time.monotonic()
        cached = self._token_key_cache.get(token_id)
        if cached is not None and now - cached[1] < TOKEN_KEY_TTL:
            return cached[0]
        reader = getattr(self.provisioner, "token_key", None)
        if reader is None:
            return ""
        try:
            key = await reader(token_id)
        except Exception:
            key = ""
        if key:
            self._token_key_cache[token_id] = (key, now)
            if len(self._token_key_cache) > 2000:
                self._token_key_cache = {
                    tid: value for tid, value in self._token_key_cache.items()
                    if now - value[1] < TOKEN_KEY_TTL
                }
        return key

    @staticmethod
    def _ordered_key_candidates(tokens: list[dict[str, Any]],
                                own_token_ids: list[str],
                                buyer_name: str) -> list[dict[str, Any]]:
        """要逐个读明文的令牌顺序：本人名下 → 名字像他 → 其它（受扫描上限约束）。

        令牌名是「买家昵称#订单号」，所以按昵称能先挑出一小撮；昵称可能被截短，
        两头的前缀比较都算命中（真正的判定仍然是「明文 Key 完全相等」）。
        """
        by_id: dict[str, dict[str, Any]] = {}
        for token in tokens:
            token_id = str(token.get("id") or "").strip()
            if token_id:
                by_id[token_id] = token

        ordered: list[dict[str, Any]] = []
        seen: set[str] = set()
        for token_id in own_token_ids:
            token = by_id.get(token_id)
            if token is not None and token_id not in seen:
                ordered.append(token)
                seen.add(token_id)

        name = str(buyer_name or "").strip()
        if name:
            for token in tokens:
                token_id = str(token.get("id") or "").strip()
                if not token_id or token_id in seen:
                    continue
                head = str(token.get("name") or "").split("#", 1)[0].strip()
                if head and (head == name or head.startswith(name)
                             or name.startswith(head)):
                    ordered.append(token)
                    seen.add(token_id)

        for token in tokens:
            if len(ordered) >= KEY_SCAN_LIMIT:
                break
            token_id = str(token.get("id") or "").strip()
            if token_id and token_id not in seen:
                ordered.append(token)
                seen.add(token_id)
        return ordered

    async def _key_token_lookup(self, buyer_id: str, token: dict[str, Any],
                                key: str) -> KeyOrderLookup:
        """这把令牌是不是买家的：是就返回它，不是就明确拒绝。

        归属两种认法：绑定表 / 历史订单里记过这个令牌 id；或者令牌名里的
        「#订单号」是这个买家已发货的单 —— 老订单没记 api_key_id，但名字里有。
        """
        token_id = str((token or {}).get("id") or "").strip()
        if not token_id:
            return KeyOrderLookup(reason="key_foreign")
        if await self._token_belongs_to_buyer(buyer_id, token):
            return await self._token_for_token_id(token_id, api_key=key)
        return KeyOrderLookup(reason="key_not_yours")

    async def _token_belongs_to_buyer(self, buyer_id: str,
                                      token: dict[str, Any]) -> bool:
        """这个令牌能不能算「买家自己的」。"""
        token_id = str((token or {}).get("id") or "").strip()
        if token_id and await self.db.buyer_owns_token(buyer_id, token_id):
            return True
        hint = order_id_from_label(str((token or {}).get("name") or ""))
        if hint:
            row = await self.db.find_buyer_delivered_order(buyer_id, hint)
            if row is not None:
                return True
        return False

    async def _topup_rule_for_order(self, order: dict[str, Any]) -> dict[str, Any]:
        """按订单里记的 rule_id 取回规则；规则被删了也能继续把这一单做完。"""
        rule_id = order.get("rule_id")
        rule = None
        if rule_id not in (None, ""):
            try:
                rule = await self.db.get_rule(int(rule_id))
            except (TypeError, ValueError):
                rule = None
        if rule is None:
            return {
                "id": rule_id,
                "name": "充值订单",
                "mode": "topup",
                "content": "",
                "auto_confirm": 0,
                "topup_key_source": TOPUP_KEY_SOURCE_ORDER,
                # 规则没了就查不到「只认同商品」的配置：按老行为（不拦）继续，
                # 宁可充错也别把已经付过钱的单卡死在这里。
                "topup_item_scope": TOPUP_ITEM_SCOPE_ANY,
                "topup_prompt": "",
                "topup_not_found": "",
            }
        rule = dict(rule)
        rule["mode"] = "topup"
        return rule

    async def _send_buyer_notice(self, msg: IncomingMessage, text: str, *,
                                 order_id: str = "") -> bool:
        """发一条不带「发货」语义的提示（催买家发订单号 / 找不到 Key）。"""
        content = str(text or "").strip()
        if not content:
            return False
        if self.client is None or not msg.chat_id:
            return False
        try:
            await self.client.send_text_safe(msg.chat_id, msg.sender_id, content)
        except Exception as exc:
            logger.warning("[%s] 给买家 %s 发充值提示失败：%s", self.name, msg.sender_id, exc)
            return False
        try:
            await self.db.log_message(
                account_id=self.account_id, chat_id=msg.chat_id, sender_id=msg.sender_id,
                sender_name=msg.sender_name, item_id=msg.item_id,
                order_id=order_id or msg.order_id or None,
                content=content, kind="outgoing",
            )
        except Exception:
            logger.exception("[%s] 充值提示已发出，但写消息流水失败", self.name)
        return True

    async def _send_topup_prompt(self, msg: IncomingMessage, rule: dict[str, Any], *,
                                 order_id: str, amount_cny: float | None = None,
                                 quota_usd: float | None = None,
                                 item_id: str | None = None,
                                 force: bool = False,
                                 default_template: str = DEFAULT_TOPUP_PROMPT,
                                 ) -> bool | None:
        """提示买家发「旧订单号 / Key」。同一单 10 分钟内不重复催。

        返回值：True 已发出 / False 发送失败 / None 冷却期内主动跳过。
        「跳过」不能当成失败，否则 topup_failed 重试时会把状态来回改。
        """
        order = str(order_id or "").strip()
        now = time.monotonic()
        last = self._prompted_orders.get(order)
        if not force and order and last is not None and now - last < TOPUP_PROMPT_GAP:
            logger.info("[%s] 订单 %s 刚刚提示过，本轮不再重复催", self.name, order)
            return None
        template = (str(rule.get("topup_prompt") or "").strip()
                    or default_template or DEFAULT_TOPUP_PROMPT)
        text = render(
            template, buyer_name=msg.sender_name, item_id=item_id or msg.item_id,
            order_id=order, amount_cny=amount_cny, quota_usd=quota_usd,
        )
        sent = await self._send_buyer_notice(msg, text, order_id=order)
        if sent and order:
            if len(self._prompted_orders) >= 1000:
                cutoff = now - TOPUP_PROMPT_GAP
                self._prompted_orders = {
                    key: value for key, value in self._prompted_orders.items()
                    if value >= cutoff
                }
            self._prompted_orders[order] = now
        return sent

    async def _send_topup_not_found(self, msg: IncomingMessage, rule: dict[str, Any],
                                    key_order_id: str, *, order_id: str,
                                    amount_cny: float | None = None,
                                    quota_usd: float | None = None,
                                    reason: str = "") -> bool:
        """买家发的订单号不能用来充值时回一条。

        跨商品（reason=other_item）和「查无此单」分开说：让买家知道要发的是
        本商品的订单号，否则他会一直重发同一笔别的商品的单。
        运营自己改过文案就按运营的来 —— 不做二次加工。
        """
        template = str(rule.get("topup_not_found") or "").strip()
        if not template:
            template = _TOPUP_REJECT_TEXT.get(str(reason or ""), DEFAULT_TOPUP_NOT_FOUND)
        text = render(
            template, buyer_name=msg.sender_name, item_id=msg.item_id,
            order_id=order_id, key_order_id=key_order_id,
            amount_cny=amount_cny, quota_usd=quota_usd,
        )
        return await self._send_buyer_notice(msg, text, order_id=order_id)

    async def _record_awaiting_key(self, msg: IncomingMessage, rule: dict[str, Any],
                                   amount_cny: float | None, quota: float, *,
                                   detail: str, key_order_id: str = "",
                                   order_id: str = "",
                                   item_id: str | None = None) -> None:
        """记一条「等买家发旧订单号」。轮询会跳过它，买家回复后再继续。

        order_id / item_id 只在「买家回消息触发」的重写里给：那时的 msg 是聊天
        消息，本身没有订单号，直接用 msg.order_id 会插出一条没有订单号的脏记录。
        """
        target_order = order_id or msg.order_id
        # 显式传 item_id（哪怕空串）表示「按调用方说的来」：买家聊天所在的商品
        # 不一定就是挂起订单的商品，不能悄悄拿 msg.item_id 顶上。
        target_item = msg.item_id if item_id is None else item_id
        try:
            await self.db.record_order(
                account_id=self.account_id, order_id=target_order,
                item_id=target_item,
                buyer_id=msg.sender_id, buyer_name=msg.sender_name, rule_id=rule.get("id"),
                card_id=None, content="", status="awaiting_key", detail=detail,
                amount_cny=amount_cny, quota_usd=quota,
                key_order_id=key_order_id or None,
            )
        except Exception:
            logger.exception("[%s] 订单 %s 写 awaiting_key 记录失败", self.name, target_order)

    async def _deliver_pending_topup(self, target: dict[str, Any], rule: dict[str, Any],
                                     msg: IncomingMessage, key_order_id: str,
                                     *, key_token_id: str = "") -> bool:
        """买家给了旧订单号 / Key：用它把挂起的那笔充值做完。"""
        order_id = str(target.get("order_id") or "").strip()
        if not order_id:
            return False
        deliver_msg = IncomingMessage(
            chat_id=msg.chat_id,
            sender_id=msg.sender_id,
            sender_name=msg.sender_name,
            content=("[买家提供了 Key]" if key_token_id else "[买家提供了旧订单号]"),
            item_id=target.get("item_id"),
            order_id=order_id,
            created_at=0,
            amount_cny_hint=target.get("amount_cny"),
            order_id_trusted=True,
        )
        try:
            quoted = float(target.get("quota_usd") or 0) or None
        except (TypeError, ValueError):
            quoted = None
        decision = Decision(
            "topup",
            reason=("买家提供 Key（令牌 " + key_token_id + "）" if key_token_id
                    else "买家提供旧订单号 " + key_order_id),
            rule=rule,
            auto_confirm=bool(rule.get("auto_confirm")),
            quota_usd=quoted,
            key_order_id=key_order_id,
            key_token_id=key_token_id,
        )
        return await self._deliver(deliver_msg, decision)

    async def _finish_pending_topup(self, target: dict[str, Any], rule: dict[str, Any],
                                    msg: IncomingMessage, key_order_id: str,
                                    *, key_token_id: str = "") -> None:
        """把「买家给了旧订单号 / Key」的那笔充值做完，并提示下一笔等待中的订单。"""
        order_id = str(target.get("order_id") or "")
        try:
            row = await self.db.get_order(self.account_id, order_id) if order_id else None
            if row is None or str(row.get("status") or "") not in (
                "awaiting_key", "topup_failed"
            ):
                # 第二次回复 / 另一个路径已经把这单做完了：绝不能再来一次充值
                logger.info("[%s] 充值订单 %s 当前状态是 %s，跳过重复处理",
                            self.name, order_id, (row or {}).get("status") or "不存在")
                return
            async with self._delivery_sem:
                try:
                    ok = await self._deliver_pending_topup(
                        target, rule, msg, key_order_id, key_token_id=key_token_id)
                except (RiskControl, CookieExpired, TokenExpired) as exc:
                    await self._pause_for_account_error(exc)
                    raise
            if not ok:
                logger.warning(
                    "[%s] 买家 %s 提供了%s，但充值订单 %s 没能完成；详情见发货记录",
                    self.name, msg.sender_id,
                    ("Key（令牌 " + key_token_id + "）" if key_token_id
                     else "旧订单号 " + key_order_id),
                    order_id,
                )
            remaining = await self.db.list_awaiting_key_orders(self.account_id, msg.sender_id)
            if not remaining:
                return
            next_order = remaining[0]
            next_rule = await self._topup_rule_for_order(next_order)
            await self._send_topup_prompt(
                msg, next_rule, order_id=str(next_order.get("order_id") or ""),
                amount_cny=next_order.get("amount_cny"),
                quota_usd=next_order.get("quota_usd"),
                item_id=next_order.get("item_id"),
                default_template=self._prompt_default(next_rule),
            )
        finally:
            self._key_claims.discard(order_id)

    async def _handle_key_reply(self, msg: IncomingMessage, keys: list[str]) -> bool:
        """买家直接发了 Key（sk- 开头那一串）。

        只认「本站发给他本人」的令牌，核对不过就回一句原因、订单继续等；
        核对通过就走和旧订单号完全一样的充值链路（先落 topping_up 再扣额度）。
        """
        pending = await self.db.list_awaiting_key_orders(self.account_id, msg.sender_id)
        if not pending:
            # 订单还没轮询到：先核对归属，2 分钟内订单出现时直接用（和旧订单号同一个窗口）。
            # 这里也要限次数：没有等待中的订单还在反复发 Key 的，基本就是有人试探。
            if not self._allow_key_attempt(msg.sender_id):
                logger.warning("[%s] 买家 %s 没有等待中的充值单，却在反复发 Key，本轮忽略",
                               self.name, msg.sender_id)
                return False
            for key in keys:
                lookup = await self._token_for_api_key(
                    msg.sender_id, key, buyer_name=msg.sender_name)
                token_id = str((lookup.resolved or {}).get("token_id") or "")
                if token_id:
                    self._key_hints[msg.sender_id] = (token_id, time.monotonic())
                    cap_mapping(self._key_hints, 1000)
                    logger.info("[%s] 买家 %s 提前发了 Key（令牌 %s），先记下",
                                self.name, msg.sender_id, token_id)
                    break
            return False

        target = pending[0]
        rule = await self._topup_rule_for_order(target)
        order_id = str(target.get("order_id") or "")
        if not self._allow_key_attempt(msg.sender_id):
            logger.warning("[%s] 买家 %s 试 Key 太频繁，本轮不回充值结果",
                           self.name, msg.sender_id)
            await self._send_buyer_notice(
                msg, "试得太频繁了，请过几分钟再试，或者直接发订单号。",
                order_id=order_id,
            )
            return True

        lookup: KeyOrderLookup | None = None
        reason = "key_invalid"
        for key in keys:
            lookup = await self._token_for_api_key(
                msg.sender_id, key, buyer_name=msg.sender_name)
            if lookup.resolved is not None:
                break
            reason = lookup.reason
        token_id = str(((lookup.resolved if lookup is not None else None) or {})
                       .get("token_id") or "")
        if not token_id and reason == "unreadable":
            reason = "key_gone"
        if token_id:
            logger.info("[%s] 买家 %s 发来 Key → 令牌 %s（充值订单 %s）",
                        self.name, msg.sender_id, token_id, order_id)
            self._key_hints.pop(msg.sender_id, None)
            await self._finish_pending_topup(
                target, rule, msg, "", key_token_id=token_id)
            return True

        # 核对不过：说清楚原因，订单继续停在 awaiting_key
        await self._send_topup_not_found(
            msg, rule, "", order_id=order_id,
            amount_cny=target.get("amount_cny"), quota_usd=target.get("quota_usd"),
            reason=reason,
        )
        await self._record_awaiting_key(
            msg, rule, target.get("amount_cny"),
            float(target.get("quota_usd") or 0) or 0.0,
            detail=self._key_order_failure_detail("", reason),
            order_id=order_id, item_id=str(target.get("item_id") or ""),
        )
        logger.warning("[%s] 买家 %s 发来的 Key 不能充值（%s；充值订单 %s 继续等待）",
                       self.name, msg.sender_id,
                       _KEY_ORDER_FAILURE_TEXT.get(reason, "核对不过"), order_id)
        return True

    async def _maybe_handle_key_order(self, msg: IncomingMessage) -> bool:
        """这条买家消息是不是在回复「请发旧订单号 / Key」。

        返回 True 表示已处理（成功充值 / 发了不能充值的提示），
        调用方不要再走自动回复，免得买家收到两句不相干的话。
        """
        if msg.is_system_card or not msg.content or not msg.sender_id:
            return False
        keys = self._api_key_candidates(msg.content)
        if keys:
            return await self._handle_key_reply(msg, keys)

        candidates = self._order_id_candidates(msg.content)[:5]
        masked = self._masked_key_present(msg.content)
        if not candidates and not masked:
            return False
        pending = await self.db.list_awaiting_key_orders(self.account_id, msg.sender_id)

        if not pending:
            # 买家可能比订单轮询早一步发来旧订单号：先记在内存里，
            # 等充值订单出现时 _topup 直接用，不用再催一遍。
            for candidate in candidates:
                lookup = await self._token_for_key_order(msg.sender_id, candidate)
                if lookup.resolved is not None:
                    self._key_order_hints[msg.sender_id] = (candidate, time.monotonic())
                    cap_mapping(self._key_order_hints, 1000)
                    logger.info("[%s] 买家 %s 提前提供了旧订单号 %s，先记下",
                                self.name, msg.sender_id, candidate)
                    break
            return False

        target = pending[0]
        rule = await self._topup_rule_for_order(target)
        scope = str(rule.get("topup_item_scope") or TOPUP_ITEM_SCOPE_ANY).strip().lower()
        order_id = str(target.get("order_id") or "")

        if masked and not candidates:
            # 只有打码后的 Key（sk-ab12…wxyz）：买家以为复制全了，得说清楚
            await self._send_topup_not_found(
                msg, rule, "", order_id=order_id,
                amount_cny=target.get("amount_cny"), quota_usd=target.get("quota_usd"),
                reason="key_masked",
            )
            await self._record_awaiting_key(
                msg, rule, target.get("amount_cny"),
                float(target.get("quota_usd") or 0) or 0.0,
                detail="买家发来的是打码后的 Key，没法用来充值",
                order_id=order_id, item_id=str(target.get("item_id") or ""),
            )
            logger.warning("[%s] 买家 %s 发的 Key 是打码的（充值订单 %s 继续等待）",
                           self.name, msg.sender_id, order_id)
            return True
        item_hint = self._topup_item_of(target.get("item_id"), rule)
        if (scope == TOPUP_ITEM_SCOPE_ITEM and not item_hint and order_id):
            item_hint = await self._item_id_from_order(order_id) or ""
        chosen = ""
        resolved = None
        other_item = ""
        reason = ""
        for candidate in candidates:
            lookup = await self._token_for_key_order(
                msg.sender_id, candidate,
                item_id=item_hint, item_scope=scope,
            )
            if lookup.resolved is not None:
                chosen = candidate
                resolved = lookup.resolved
                break
            if lookup.reason == "other_item":
                other_item = other_item or candidate
            elif not reason:
                reason = lookup.reason
        if resolved is None:
            # 买家可能一口气发了好几个订单号：只要有一个是「别的商品的单」，
            # 就按这个原因回 —— 比「查无此单」更能让他知道该发哪一个。
            bad = other_item or candidates[0]
            await self._send_topup_not_found(
                msg, rule, bad, order_id=order_id,
                amount_cny=target.get("amount_cny"), quota_usd=target.get("quota_usd"),
                reason="other_item" if other_item else reason,
            )
            # 把「为什么这一单还在等」写回订单：后台能直接看到买家发错了商品，
            # 不用翻日志。注意用的是挂起订单的订单号 / 商品，不是聊天消息的。
            await self._record_awaiting_key(
                msg, rule, target.get("amount_cny"),
                float(target.get("quota_usd") or 0) or 0.0,
                detail=self._key_order_failure_detail(
                    bad, "other_item" if other_item else reason),
                order_id=order_id, item_id=str(target.get("item_id") or ""),
            )
            logger.warning(
                "[%s] 买家 %s 提供的订单号 %s 不能用来充值（%s；充值订单 %s 继续等待）",
                self.name, msg.sender_id, bad,
                _KEY_ORDER_FAILURE_TEXT.get("other_item" if other_item else reason,
                                            "没有对应 Key"),
                order_id,
            )
            return True

        self._key_order_hints.pop(msg.sender_id, None)
        if order_id in self._key_claims:
            logger.info("[%s] 充值订单 %s 正在处理买家提供的旧订单号，忽略重复回复",
                        self.name, order_id)
            return True
        self._key_claims.add(order_id)
        if isinstance(self.client, XianyuClient):
            # 真客户端发消息要等 ack，而 ack 只能由收帧主循环 dispatch；
            # 在这里直接 await 会把主循环锁死到超时。扔后台任务，和普通发货一致。
            task = asyncio.create_task(
                self._finish_pending_topup(target, rule, msg, chosen),
                name=f"topup-key-{self.name}",
            )
            self._delivery_tasks.add(task)
            task.add_done_callback(self._delivery_done)
        else:
            # 测试替身没有 ack 能力，保持同步行为
            await self._finish_pending_topup(target, rule, msg, chosen)
        return True


    # ------------------------------------------------------------ 账号模式

    @staticmethod
    def _normalize_reply(text: str) -> str:
        """把买家的一句话归一化：去空白、标点、全角符号，转小写。

        「没有。」「没有 」「没有~」都得认成同一个答案，否则买家照着提示回一句
        带句号的「没有」，机器人就当他没说。
        """
        raw = str(text or "").strip().lower()
        for ch in " \t\r\n，。！？、；：,.!?;:～~\"'“”‘’（）()【】[]":
            raw = raw.replace(ch, "")
        return raw

    @classmethod
    def _is_no_account(cls, text: str) -> bool:
        return cls._normalize_reply(text) in _ACCOUNT_NO_WORDS

    @classmethod
    def _is_confirm_account(cls, text: str) -> bool:
        return cls._normalize_reply(text) in _ACCOUNT_YES_WORDS

    @classmethod
    def _is_have_account(cls, text: str) -> bool:
        return cls._normalize_reply(text) in _ACCOUNT_HAVE_WORDS

    @classmethod
    def _is_stop_word(cls, text: str) -> bool:
        """「好的 / 谢谢 / thanks」这类回话不是账号名。"""
        return cls._normalize_reply(text) in _ACCOUNT_STOP_WORDS

    @staticmethod
    def _parse_account_name(text: str) -> str:
        """从买家这句话里认一个账号名；不像账号名就返回空串。

        买家经常会把「账号：abc」整句发过来，所以先把常见前缀剥掉。
        认不出来一律返回空串，交给自动回复处理 —— 否则「什么时候发货」会被当成
        一个非法账号名，然后回他一句「账号名不能用」。
        """
        raw = str(text or "").strip()
        for label in ("账号名", "账号", "用户名"):
            if raw.startswith(label):
                raw = raw[len(label):].lstrip("：:= 　").strip()
                break
        return raw if _ACCOUNT_NAME_RE.match(raw) else ""

    @staticmethod
    def _random_password(length: int = 12) -> str:
        """生成一个买家要手抄的密码：只用不会认错的字符。"""
        return "".join(secrets.choice(_PASSWORD_ALPHABET) for _ in range(length))

    @staticmethod
    def _login_url() -> str:
        return str(settings.newapi_base_url or "").rstrip("/")

    @staticmethod
    def _money_text(balance: float | None, currency: str = "USD") -> str:
        if balance is None:
            return "—"
        symbol = {"USD": "$", "CNY": "¥"}.get(str(currency or "USD").upper(), "")
        return f"{symbol}{balance:.2f}"

    async def _account_rule_for_order(self, order: dict[str, Any]) -> dict[str, Any]:
        """按订单里记的 rule_id 取回规则；规则被删了也能继续把这一单做完。"""
        rule_id = order.get("rule_id")
        rule = None
        if rule_id not in (None, ""):
            try:
                rule = await self.db.get_rule(int(rule_id))
            except (TypeError, ValueError):
                rule = None
        if rule is None:
            return {
                "id": rule_id,
                "name": "账号订单",
                "mode": "account",
                "content": "",
                "auto_confirm": 0,
                "account_prompt": "",
                "account_confirm": "",
                "account_card": "",
            }
        rule = dict(rule)
        rule["mode"] = "account"
        return rule

    async def _record_awaiting_account(self, msg: IncomingMessage, rule: dict[str, Any], *,
                                       status: str, detail: str,
                                       order_id: str = "",
                                       item_id: str | None = None,
                                       amount_cny: float | None = None,
                                       quota_usd: float | None = None,
                                       account_username: str = "",
                                       account_user_id: str = "") -> None:
        """记一条「等买家回话」。轮询看到这些状态就不再重复催。"""
        target_order = order_id or msg.order_id
        # 显式传 item_id（哪怕空串）表示按调用方说的来：买家聊天所在的商品
        # 不一定就是挂起订单的商品。
        target_item = msg.item_id if item_id is None else item_id
        fields: dict[str, Any] = {
            "account_id": self.account_id,
            "order_id": target_order,
            "item_id": target_item,
            "buyer_id": msg.sender_id,
            "buyer_name": msg.sender_name,
            "rule_id": rule.get("id"),
            "card_id": None,
            "content": "",
            "status": status,
            "detail": detail,
            "amount_cny": amount_cny,
            "quota_usd": quota_usd,
        }
        # 只在有值时写这两列：不然「问有没有账号」这一步会把上一轮记下的
        # 账号名 / user id 覆盖成 NULL，买家确认时又得重问一遍。
        if account_username:
            fields["account_username"] = account_username
        if account_user_id:
            fields["account_user_id"] = account_user_id
        try:
            await self.db.record_order(**fields)
        except Exception:
            logger.exception("[%s] 订单 %s 写 %s 记录失败", self.name, target_order, status)

    async def _ask_account(self, msg: IncomingMessage, rule: dict[str, Any], *,
                           order_id: str, amount_cny: float | None = None,
                           quota_usd: float | None = None) -> bool:
        """首次询问：你之前有账号吗。"""
        template = str(rule.get("account_prompt") or "").strip() or DEFAULT_ACCOUNT_PROMPT
        text = render(
            template, buyer_name=msg.sender_name, item_id=msg.item_id, order_id=order_id,
            amount_cny=amount_cny, quota_usd=quota_usd,
        )
        sent = await self._send_buyer_notice(msg, text, order_id=order_id)
        if sent:
            await self._record_awaiting_account(
                msg, rule, status=ACCOUNT_STATUS_AWAITING,
                detail="已询问买家是否有账号，等回复", order_id=order_id,
                amount_cny=amount_cny, quota_usd=quota_usd,
            )
        return sent

    async def _ask_username(self, msg: IncomingMessage, rule: dict[str, Any], *,
                            order_id: str, detail: str = "",
                            amount_cny: float | None = None,
                            quota_usd: float | None = None) -> bool:
        """买家说没有账号（或名字不能用）之后，问他想用什么账号名。"""
        text = render(
            DEFAULT_ACCOUNT_ASK_NAME, buyer_name=msg.sender_name,
            item_id=msg.item_id, order_id=order_id,
        )
        sent = await self._send_buyer_notice(msg, text, order_id=order_id)
        if sent:
            await self._record_awaiting_account(
                msg, rule, status=ACCOUNT_STATUS_USERNAME,
                detail=detail or "已请买家提供想要的账号名", order_id=order_id,
                amount_cny=amount_cny, quota_usd=quota_usd,
            )
        return sent

    async def _send_account_hint(self, msg: IncomingMessage, text: str, *,
                                 order_id: str = "") -> bool:
        return await self._send_buyer_notice(msg, str(text or ""), order_id=order_id)

    async def _record_account_pending(self, msg: IncomingMessage, rule: dict[str, Any], *,
                                      user_id: str, username: str,
                                      amount_cny: float | None, quota: float) -> bool:
        """加额度之前先落一句「正在加额度」：这是不可逆外呼，崩在中间要留线索。"""
        try:
            await self.db.record_order(
                account_id=self.account_id, order_id=msg.order_id, item_id=msg.item_id,
                buyer_id=msg.sender_id, buyer_name=msg.sender_name, rule_id=rule.get("id"),
                card_id=None, content="", status="topping_up",
                detail="正在给账号 " + (username or user_id) + " 加额度",
                amount_cny=amount_cny, quota_usd=quota,
                account_user_id=user_id or None,
                account_username=username or None,
            )
            return True
        except Exception:
            logger.exception("[%s] 订单 %s 加额度前写 topping_up 记录失败",
                             self.name, msg.order_id)
            return False

    async def _account(self, msg: IncomingMessage, rule: dict[str, Any],
                       decision: Any = None) -> PreparedDelivery | None:
        """账号模式：先问有没有账号；开新号 / 充到已有账号。

        三种出口：
          * 还不知道用哪个账号 → 发一句询问、落 awaiting_account，返回 None；
          * 新账号 → 建号 → 落绑定 → 加额度 → 出卡密 → 返回待发送内容；
          * 已有账号 → 加额度 → 返回待发送内容。

        「加额度」是不可逆外呼：每次都必须先落 topping_up 再发请求，
        否则进程在「钱已加上、记录未写」之间死掉，下一轮就会再加一次。
        """
        if self.provisioner is None:
            logger.error("[%s] 规则是账号模式，但没有配置 new-api", self.name)
            await self._record_failure(msg, rule, "new-api 未配置", status="account_failed")
            return None
        if not msg.order_id:
            logger.error("[%s] 账号模式需要订单号，但这条消息里没有", self.name)
            await self._record_failure(msg, rule, "缺少订单号", status="account_failed")
            return None

        prepared = await self._prepare_amount(msg, rule, status="account_failed")
        if prepared is None:
            return None
        amount_cny, quota = prepared
        quoted = float(getattr(decision, "quota_usd", 0) or 0)
        if quoted > 0:
            # 买家第一次回话时算好的额度：中途改倍率不该让已经拍下的人拿到另一个数
            quota = round(quoted, 2)

        username = str(getattr(decision, "account_username", "") or "").strip()
        user_id = str(getattr(decision, "account_user_id", "") or "").strip()
        login_url = self._login_url()

        if not username and not user_id:
            binding = await self.db.get_buyer_account(msg.sender_id)
            if binding is not None and str(binding.get("user_id") or ""):
                user_id = str(binding["user_id"])
                username = str(binding.get("username") or "")
                logger.info("[%s] 买家 %s 已绑定账号 %s，直接充值",
                            self.name, msg.sender_name or msg.sender_id, username or user_id)
            else:
                await self._ask_account(
                    msg, rule, order_id=msg.order_id,
                    amount_cny=amount_cny, quota_usd=quota,
                )
                return None

        label = build_card_label(msg.sender_name or "买家", msg.order_id)

        if not user_id:
            # ---- 新账号：建号 → 绑定 → 加额度 → 出卡密
            password = self._random_password()
            try:
                info = await self.provisioner.create_account(username, password)
            except Exception as exc:
                # 上游明确拒绝和网络异常都收在这里：账号没建出来就一定没额度风险，
                # 让买家换个名字重来即可（网络异常那次万一建成功了，重试会撞「已存在」，
                # 同样落在这个分支，不会把额度加到别人账上）。
                logger.error("[%s] 建账号 %s 失败：%s", self.name, username, exc)
                await self._record_failure(
                    msg, rule, "建账号失败：" + str(exc),
                    amount_cny=amount_cny, quota_usd=quota, status="account_failed",
                )
                await self._send_account_hint(
                    msg,
                    "账号名「" + username + "」建号失败（可能刚被注册），请换一个再发给我。",
                    order_id=msg.order_id,
                )
                await self._record_awaiting_account(
                    msg, rule, status=ACCOUNT_STATUS_USERNAME,
                    detail="建号失败，等买家换一个账号名", order_id=msg.order_id,
                    amount_cny=amount_cny, quota_usd=quota,
                )
                return None

            # 先把绑定落库：万一后面加额度 / 出卡密失败，重试能认出「这个号是我们
            # 刚建的」，不会再建一个同名号，也不会把额度加到别人账上。
            row_id = await self.db.auto_bind_buyer_account({
                "buyer_id": msg.sender_id,
                "buyer_name": msg.sender_name,
                "account_id": self.account_id,
                "user_id": info.user_id,
                "username": info.username,
                "password": password,
                "balance": info.balance,
                "source": "order",
            })

            if not await self._record_account_pending(
                msg, rule, user_id=info.user_id, username=info.username,
                amount_cny=amount_cny, quota=quota,
            ):
                logger.error("[%s] 订单 %s 无法写入加额度意图，已中止", self.name, msg.order_id)
                await self._record_failure(
                    msg, rule, "无法写入充值记录，已中止（可重试）",
                    amount_cny=amount_cny, quota_usd=quota, status="account_failed",
                )
                return None
            try:
                topup = await self.provisioner.topup_account(info.user_id, quota)
            except TopupUncertainError as exc:
                logger.error("[%s] 订单 %s 加额度结果未知：%s", self.name, msg.order_id, exc)
                await self._record_failure(
                    msg, rule, "加额度结果未知：" + str(exc),
                    amount_cny=amount_cny, quota_usd=quota, status="topup_unknown",
                )
                return None
            except Exception as exc:
                logger.error("[%s] 订单 %s 给新账号加额度失败：%s", self.name, msg.order_id, exc)
                await self._record_failure(
                    msg, rule, "加额度失败：" + str(exc),
                    amount_cny=amount_cny, quota_usd=quota, status="account_failed",
                )
                return None

            # 卡密内容由运营配；没配就用内置的三行（账号 / 密码 / 登录地址）
            card_template = str(rule.get("account_card") or "").strip() or DEFAULT_ACCOUNT_CARD
            card_text = render(
                card_template, buyer_name=msg.sender_name, username=info.username,
                password=password, user_id=info.user_id, login_url=login_url,
                amount_cny=amount_cny, quota_usd=quota,
                balance=self._money_text(topup.balance, topup.currency),
            )
            card_code = ""
            try:
                card_code = await self.provisioner.issue_card(card_text, quota, label)
            except Exception as exc:
                # 卡密站挂了不代表这一单失败：账号和额度都已经开出去了，
                # 这时候绝不能落「失败」——那会让下一轮再充一次额度。
                logger.error("[%s] 订单 %s 账号已开好，但卡密没建成：%s",
                             self.name, msg.order_id, exc)

            if card_code:
                content = render(
                    str(rule.get("content") or "").strip() or DEFAULT_ACCOUNT_NEW_CONTENT,
                    buyer_name=msg.sender_name, item_id=msg.item_id, order_id=msg.order_id,
                    card=card_code, username=info.username, password=password,
                    user_id=info.user_id, login_url=login_url,
                    amount_cny=amount_cny, quota_usd=quota,
                    balance=self._money_text(topup.balance, topup.currency),
                )
            else:
                content = render(
                    DEFAULT_ACCOUNT_NEW_CONTENT_NO_CARD,
                    buyer_name=msg.sender_name, order_id=msg.order_id,
                    username=info.username, password=password, login_url=login_url,
                    quota_usd=quota,
                )
            if row_id:
                # auto_bind 遇到「后台手工绑定」会原样返回那一行（不覆盖），
                # 这时它指向的是另一个账号：不能把这单的余额写上去。
                bound = await self.db.get_buyer_account_row(int(row_id))
                if str((bound or {}).get("user_id") or "") == str(info.user_id):
                    await self.db.note_buyer_account_topup(
                        int(row_id), quota, balance=topup.balance, card_code=card_code)
                else:
                    logger.warning(
                        "[%s] 买家 %s 的后台绑定是另一个账号（id=%s），本次开号 %s 不写回绑定表",
                        self.name, msg.sender_id, (bound or {}).get("user_id"), info.user_id,
                    )
            logger.info("[%s] 订单 %s 已开号 %s（%s）并充值 %s",
                        self.name, msg.order_id, info.username, info.user_id, quota)
            # card_code 必须非空：_reuse_provision 靠它判断「已经准备好了」，
            # 空串会让发送失败后的下一轮再建一次号 / 再充一次额度。
            return PreparedDelivery(
                card_code=card_code or ("account:" + info.username),
                amount_cny=amount_cny,
                quota_usd=quota,
                content=content,
            )

        # ---- 已有账号：加额度
        if not username:
            try:
                user = await self.provisioner.find_account_by_id(user_id)
                username = str(user.get("username") or "")
            except Exception:
                username = ""
        if not await self._record_account_pending(
            msg, rule, user_id=user_id, username=username,
            amount_cny=amount_cny, quota=quota,
        ):
            logger.error("[%s] 订单 %s 无法写入加额度意图，已中止", self.name, msg.order_id)
            await self._record_failure(
                msg, rule, "无法写入充值记录，已中止（可重试）",
                amount_cny=amount_cny, quota_usd=quota, status="account_failed",
            )
            return None
        try:
            topup = await self.provisioner.topup_account(user_id, quota)
        except TopupUncertainError as exc:
            logger.error("[%s] 订单 %s 加额度结果未知：%s", self.name, msg.order_id, exc)
            await self._record_failure(
                msg, rule, "加额度结果未知：" + str(exc),
                amount_cny=amount_cny, quota_usd=quota, status="topup_unknown",
            )
            return None
        except Exception as exc:
            logger.error("[%s] 订单 %s 给账号 %s 加额度失败：%s",
                         self.name, msg.order_id, username or user_id, exc)
            await self._record_failure(
                msg, rule, "加额度失败：" + str(exc),
                amount_cny=amount_cny, quota_usd=quota, status="account_failed",
            )
            return None

        username = topup.username or username or user_id
        balance_text = self._money_text(topup.balance, topup.currency)
        content = render(
            str(rule.get("content") or "").strip() or DEFAULT_ACCOUNT_CONTENT,
            buyer_name=msg.sender_name, item_id=msg.item_id, order_id=msg.order_id,
            username=username, user_id=user_id, login_url=login_url,
            balance=balance_text, amount_cny=amount_cny, quota_usd=quota,
        )
        if not content.strip():
            # 钱已经加进去了，不能因为文案为空就记失败（那会让下一轮再充一次）
            content = "已给账号 " + username + " 充值 " + f"{quota:g}" + " 额度，当前余额 " + balance_text
        binding = await self.db.get_buyer_account(msg.sender_id)
        if binding is not None:
            await self.db.note_buyer_account_topup(
                int(binding["id"]), quota, balance=topup.balance)
        logger.info("[%s] 订单 %s 已给账号 %s（%s）充值 %s",
                    self.name, msg.order_id, username, user_id, quota)
        return PreparedDelivery(
            card_code="account:" + username,
            amount_cny=amount_cny,
            quota_usd=quota,
            content=content,
        )

    @classmethod
    def _pick_account_order(cls, pending: list[dict[str, Any]], text: str) -> dict[str, Any]:
        """买家一条回复可能对着好几笔挂起的订单，按「这句话能回答哪一种」来挑。

        典型场景：老单还停在「有没有账号」，新单已经在「等确认」，买家回一句
        「确认」。以前直接取最老那一条，这句话对不上就被吞掉了，
        新单永远等不到确认（买家会一直重发）。
        """
        normalized = cls._normalize_reply(text)
        if normalized in _ACCOUNT_YES_WORDS:
            for row in pending:
                if str(row.get("status")) == ACCOUNT_STATUS_CONFIRM:
                    return row
        if normalized in _ACCOUNT_NO_WORDS or normalized in _ACCOUNT_HAVE_WORDS:
            for row in pending:
                if str(row.get("status")) == ACCOUNT_STATUS_AWAITING:
                    return row
        if cls._parse_account_name(text):
            for row in pending:
                if str(row.get("status")) == ACCOUNT_STATUS_USERNAME:
                    return row
            for row in pending:
                if str(row.get("status")) == ACCOUNT_STATUS_AWAITING:
                    return row
        return pending[0]

    async def _maybe_handle_account_reply(self, msg: IncomingMessage) -> bool:
        """这条买家消息是不是在回「有没有账号 / 要什么用户名 / 确认充值」。

        返回 True 表示已处理（发了提示 / 走完了充值），调用方不要再走自动回复，
        免得买家刚回完「没有」又收到一句「在的，拍下自动发货」。
        """
        if msg.is_system_card or not msg.content or not msg.sender_id:
            return False
        pending = await self.db.list_awaiting_account_orders(self.account_id, msg.sender_id)
        if not pending:
            return False
        target = self._pick_account_order(pending, str(msg.content or ""))
        rule = await self._account_rule_for_order(target)
        status = str(target.get("status") or "")
        order_id = str(target.get("order_id") or "")
        amount_cny = target.get("amount_cny")
        quota = float(target.get("quota_usd") or 0) or 0.0
        text = str(msg.content or "").strip()

        if status == ACCOUNT_STATUS_CONFIRM:
            if not self._is_confirm_account(text):
                # 买家说的是别的事：让自动回复去答，订单继续停在「等确认」
                return False
            user_id = str(target.get("account_user_id") or "")
            username = str(target.get("account_username") or "")
            if not user_id:
                await self._ask_account(
                    msg, rule, order_id=order_id, amount_cny=amount_cny, quota_usd=quota)
                return True
            await self._dispatch_account_reply(
                target, rule, msg, username=username, user_id=user_id)
            return True

        if status == ACCOUNT_STATUS_USERNAME:
            if self._is_stop_word(text) or self._is_no_account(text):
                # 「好的 / 谢谢」不是账号名：再问一遍，别把 thanks 注册成账号
                await self._ask_username(
                    msg, rule, order_id=order_id,
                    amount_cny=amount_cny, quota_usd=quota,
                )
                return True
            name = self._parse_account_name(text)
            if not name:
                await self._send_account_hint(
                    msg, DEFAULT_ACCOUNT_NAME_INVALID, order_id=order_id)
                return True
            if self.provisioner is not None:
                if not self._allow_account_lookup(msg.sender_id):
                    await self._send_account_hint(
                        msg, "账号查询太频繁，请 10 分钟后再试。", order_id=order_id)
                    return True
                try:
                    taken = await self.provisioner.find_account(name)
                except UpstreamError as exc:
                    logger.error("[%s] 建号前查账号名 %s 失败：%s", self.name, name, exc)
                    await self._send_account_hint(
                        msg, "账号名核对失败，请稍后重新发一次账号名。", order_id=order_id)
                    return True
                if taken is not None:
                    await self._send_account_hint(
                        msg, render(DEFAULT_ACCOUNT_TAKEN, username=name), order_id=order_id)
                    return True
            await self._dispatch_account_reply(
                target, rule, msg, username=name, user_id="")
            return True

        # ---- 还停在「你之前有账号吗」
        if self._is_no_account(text):
            await self._ask_username(
                msg, rule, order_id=order_id,
                amount_cny=amount_cny, quota_usd=quota,
            )
            return True
        if self._is_have_account(text):
            # 买家说「有」，但还没把账号名发过来
            await self._send_account_hint(
                msg, DEFAULT_ACCOUNT_ASK_EXISTING, order_id=order_id)
            await self._record_awaiting_account(
                msg, rule, status=ACCOUNT_STATUS_AWAITING,
                detail="买家说有账号，等他发账号名", order_id=order_id,
                amount_cny=amount_cny, quota_usd=quota,
            )
            return True
        name = self._parse_account_name(text)
        if not name:
            # 不像账号名：可能是「什么时候发货」，交给自动回复
            return False
        if self.provisioner is None:
            return False
        if not self._allow_account_lookup(msg.sender_id):
            await self._send_account_hint(
                msg, "账号查询太频繁，请 10 分钟后再试。", order_id=order_id)
            return True
        try:
            user = await self.provisioner.find_account(name)
        except UpstreamError as exc:
            logger.error("[%s] 查账号 %s 失败：%s", self.name, name, exc)
            await self._send_account_hint(
                msg, "账号核对失败，请稍后重新发一次账号名。", order_id=order_id)
            return True
        if user is None:
            await self._send_account_hint(
                msg, render(DEFAULT_ACCOUNT_NOT_FOUND, username=name), order_id=order_id)
            return True

        # 管理员 / 超管账号绝不从买家嘴里认领：买家随手打一个 "admin" 就把钱
        # 充进了站长的号（而且 new-api 也不允许管理员给同级账号加额度）。
        try:
            role = int(user.get("role") or 0)
        except (TypeError, ValueError):
            role = 0
        if role >= 2:
            logger.warning("[%s] 买家 %s 试图充值管理员账号 %s，已拒绝",
                           self.name, msg.sender_id, name)
            await self._send_account_hint(
                msg, "这个账号不能自动充值，请联系客服处理。", order_id=order_id)
            return True

        user_id = str(user.get("id") or "")
        balance, currency = await self.provisioner.account_balance(user)
        template = str(rule.get("account_confirm") or "").strip() or DEFAULT_ACCOUNT_CONFIRM
        confirm_text = render(
            template, buyer_name=msg.sender_name, item_id=msg.item_id, order_id=order_id,
            username=str(user.get("username") or name), user_id=user_id,
            balance=self._money_text(balance, currency),
            quota_usd=quota, amount_cny=amount_cny, login_url=self._login_url(),
        )
        sent = await self._send_buyer_notice(msg, confirm_text, order_id=order_id)
        if sent:
            # 记下「等哪个人确认哪个账号」：买家回「确认」时不用再查一遍上游，
            # 也避免这中间账号被改名 / 删除后充错地方。
            await self._record_awaiting_account(
                msg, rule, status=ACCOUNT_STATUS_CONFIRM,
                detail="已回显账号 " + name + "，等买家确认", order_id=order_id,
                amount_cny=amount_cny, quota_usd=quota,
                account_username=str(user.get("username") or name),
                account_user_id=user_id,
            )
        return True

    async def _dispatch_account_reply(self, target: dict[str, Any], rule: dict[str, Any],
                                      msg: IncomingMessage, *, username: str,
                                      user_id: str) -> None:
        """真客户端发消息要等 ack（只能由收帧主循环 dispatch），所以扔后台任务。"""
        if isinstance(self.client, XianyuClient):
            task = asyncio.create_task(
                self._finish_pending_account(target, rule, msg,
                                             username=username, user_id=user_id),
                name=f"account-{self.name}",
            )
            self._delivery_tasks.add(task)
            task.add_done_callback(self._delivery_done)
        else:
            await self._finish_pending_account(
                target, rule, msg, username=username, user_id=user_id)

    async def _finish_pending_account(self, target: dict[str, Any], rule: dict[str, Any],
                                      msg: IncomingMessage, *, username: str,
                                      user_id: str) -> None:
        """把「买家给了账号 / 确认了充值」的那一笔做完。"""
        order_id = str(target.get("order_id") or "")
        if not order_id:
            return
        try:
            row = await self.db.get_order(self.account_id, order_id)
            if row is None or str(row.get("status") or "") not in (
                ACCOUNT_STATUS_AWAITING, ACCOUNT_STATUS_USERNAME,
                ACCOUNT_STATUS_CONFIRM, "account_failed",
            ):
                # 第二个路径已经把这单做完了：绝不能再来一次加额度
                logger.info("[%s] 账号订单 %s 当前状态是 %s，跳过重复处理",
                            self.name, order_id, (row or {}).get("status") or "不存在")
                return
            if order_id in self._account_claims:
                logger.info("[%s] 账号订单 %s 正在处理买家的回复，忽略重复消息",
                            self.name, order_id)
                return
            self._account_claims.add(order_id)

            # 老用户：买家已经确认过，这时才把「买家 ↔ 账号」绑定落下来
            if user_id:
                await self.db.auto_bind_buyer_account({
                    "buyer_id": msg.sender_id,
                    "buyer_name": msg.sender_name,
                    "account_id": self.account_id,
                    "user_id": user_id,
                    "username": username,
                    "source": "order",
                })

            deliver_msg = IncomingMessage(
                chat_id=msg.chat_id,
                sender_id=msg.sender_id,
                sender_name=msg.sender_name,
                content="[买家提供了账号信息]",
                item_id=target.get("item_id"),
                order_id=order_id,
                created_at=0,
                amount_cny_hint=target.get("amount_cny"),
                order_id_trusted=True,
            )
            try:
                quoted = float(target.get("quota_usd") or 0) or None
            except (TypeError, ValueError):
                quoted = None
            decision = Decision(
                "account",
                reason=("给账号 " + (username or user_id) + " 充值" if user_id
                        else "按买家指定的用户名开新号 " + username),
                rule=rule,
                auto_confirm=bool(rule.get("auto_confirm")),
                quota_usd=quoted,
                account_username=username,
                account_user_id=user_id,
            )
            async with self._delivery_sem:
                try:
                    ok = await self._deliver(deliver_msg, decision)
                except (RiskControl, CookieExpired, TokenExpired) as exc:
                    await self._pause_for_account_error(exc)
                    raise
            if not ok:
                logger.warning("[%s] 账号订单 %s 没能完成；详情见发货记录",
                               self.name, order_id)
        finally:
            self._account_claims.discard(order_id)

    async def _record_blocked(self, msg: IncomingMessage, reason: str) -> None:
        """记一条「有人拿『我已付款』想触发发货，但没过鉴权」。

        没有订单号时不落库（否则买家刷屏就能把表写满），只留日志；
        有订单号则落一条 blocked，方便事后对照是不是有人在试别人的单。
        """
        logger.warning("[%s] 拦截可疑发货请求（买家 %s，订单 %s）：%s —— %s",
                       self.name, msg.sender_id, msg.order_id or "-", _preview(msg.content), reason)
        if not msg.order_id:
            return
        # 已经发货的单不能被 blocked 覆盖（record_order 里也有终态保护，
        # 这里先挡一次，避免把已发货记录的内容/卡密也一起冲掉）。
        if await self.db.delivery_blocked(self.account_id, msg.order_id):
            logger.info("[%s] 订单 %s 已有发货/待核对记录，保留原记录，不写 blocked",
                        self.name, msg.order_id)
            return
        await self.db.record_order(
            account_id=self.account_id, order_id=msg.order_id, item_id=msg.item_id,
            buyer_id=msg.sender_id, buyer_name=msg.sender_name, rule_id=None,
            card_id=None, content="", status="blocked", detail=reason,
        )

    async def _record_failure(
        self,
        msg: IncomingMessage,
        rule: dict[str, Any],
        reason: str,
        *,
        amount_cny: float | None = None,
        quota_usd: float | None = None,
        status: str = "upstream_failed",
        api_key_id: str = "",
        key_order_id: str = "",
        card_code: str = "",
    ) -> None:
        """记一条失败。

        金额要尽量带上：页面上的「—」看不出这单值多少钱，
        事后排查「是不是金额算错了才失败」就得多翻一遍日志。
        失败发生得早（金额还没解析出来）时才留空。

        status 默认是 upstream_failed（建 Key 链路）；充值链路传 topup_failed /
        topup_unknown，页面上才分得清「没建出来」和「可能已经充进去了」。
        """
        await self.db.record_order(
            account_id=self.account_id, order_id=msg.order_id, item_id=msg.item_id,
            buyer_id=msg.sender_id, buyer_name=msg.sender_name, rule_id=rule.get("id"),
            card_id=None, content="", status=status, detail=reason,
            amount_cny=amount_cny, quota_usd=quota_usd,
            api_key_id=api_key_id or None,
            key_order_id=key_order_id or None,
            card_code=card_code or None,
        )

class WorkerManager:
    """管理所有账号的 Worker 生命周期。"""

    def __init__(self, db: Database, provisioner: Provisioner | None = None) -> None:
        self.db = db
        self.engine = DeliveryEngine(db, cooldown=settings.delivery_cooldown)
        self.reply_engine = AutoReplyEngine(db)
        #: 上游联动。new-api 是两条路（建 Key 出卡密 / 充值到已有 Key）的共同前提；
        #: 卡密站只在「建 Key 出卡密」时需要，所以这里只按 new-api 是否配置来决定建不建。
        self.provisioner = provisioner if provisioner is not None else (
            Provisioner() if newapi_ready() else None
        )
        self.workers: dict[int, AccountWorker] = {}
        #: 后台一次性任务（上游预热等），留引用防止被 GC
        self._bg_tasks: set[asyncio.Task[Any]] = set()

    async def sync(self) -> None:
        """让运行中的 Worker 与数据库账号表保持一致。"""
        accounts = await self.db.list_accounts()
        wanted = {int(a["id"]): a for a in accounts if a["enabled"] and a["cookie"].strip()}

        for account_id in list(self.workers):
            if account_id not in wanted:
                await self.workers.pop(account_id).stop()

        for account_id, account in wanted.items():
            worker = self.workers.get(account_id)
            if worker is None:
                worker = AccountWorker(self.db, account, self.engine, self.provisioner,
                                       self.reply_engine)
                self.workers[account_id] = worker
                worker.start()
            else:
                # 扫码重新登录后 Cookie 会变；旧 worker 可能还在用旧 Cookie，
                # 或者已经因为 cookie_expired 退出了。两种情况都要重启，
                # 否则「重新登录成功」但后台一直不重连，看起来就像总是掉登录。
                cookie_changed = worker.account.get("cookie") != account.get("cookie")
                worker.account = account
                if cookie_changed or not worker.is_running:
                    await worker.stop()
                    worker.start()

        # 上游连接（DNS/TLS/登录）提前建好，免得第一单多等一次握手
        if self.provisioner is not None and wanted:
            warmup = getattr(self.provisioner, "warmup", None)
            if warmup is not None:
                task = asyncio.create_task(warmup(), name="upstream-warmup")
                # 留个引用，否则任务对象可能被 GC，异常也没人收
                self._bg_tasks.add(task)
                task.add_done_callback(self._bg_tasks.discard)

    async def stop_all(self) -> None:
        await asyncio.gather(*(w.stop() for w in self.workers.values()), return_exceptions=True)
        self.workers.clear()
        # 上游两个 httpx client 是常驻的，不关就跟着进程泄漏到底
        if self.provisioner is not None:
            close = getattr(self.provisioner, "close", None)
            if close is not None:
                try:
                    await close()
                except Exception as exc:
                    logger.warning("关闭上游连接失败：%s", exc)

    # ---------------------------------------------------------- 后台聊天

    def worker(self, account_id: int) -> AccountWorker | None:
        """按账号 ID 找运行中的 Worker（没有就返回 None）。"""
        try:
            return self.workers.get(int(account_id))
        except (TypeError, ValueError):
            return None

    def require_worker(self, account_id: int) -> AccountWorker:
        """找 Worker；找不到说明账号没启用/没 Cookie/正在重启。"""
        worker = self.worker(account_id)
        if worker is None:
            raise LookupError("账号不在运行中（未启用、没有 Cookie，或已被删除）")
        return worker

    async def send_chat_text(
        self,
        account_id: int,
        chat_id: str,
        to_id: str,
        text: str,
        *,
        item_id: str | None = None,
        order_id: str | None = None,
        buyer_name: str = "",
    ) -> dict[str, Any]:
        """后台聊天界面发消息的入口。"""
        worker = self.require_worker(account_id)
        return await worker.send_manual_text(
            chat_id, to_id, text,
            item_id=item_id, order_id=order_id, buyer_name=buyer_name,
        )

    async def send_chat_image(
        self,
        account_id: int,
        chat_id: str,
        to_id: str,
        data: bytes,
        *,
        item_id: str | None = None,
        order_id: str | None = None,
        buyer_name: str = "",
    ) -> dict[str, Any]:
        """后台聊天界面发图片。"""
        worker = self.require_worker(account_id)
        return await worker.send_manual_image(
            chat_id, to_id, data,
            item_id=item_id, order_id=order_id, buyer_name=buyer_name,
        )

    async def send_chat_video(
        self,
        account_id: int,
        chat_id: str,
        to_id: str,
        data: bytes,
        file_name: str,
        *,
        item_id: str | None = None,
        order_id: str | None = None,
        buyer_name: str = "",
    ) -> dict[str, Any]:
        """后台聊天界面发视频。"""
        worker = self.require_worker(account_id)
        return await worker.send_manual_video(
            chat_id, to_id, data, file_name,
            item_id=item_id, order_id=order_id, buyer_name=buyer_name,
        )

    async def recall_chat_message(
        self,
        account_id: int,
        chat_id: str,
        message_id: int,
    ) -> dict[str, Any]:
        """撤回一条两分钟内、由本工具发出的消息。"""
        worker = self.require_worker(account_id)
        row = await self.db.get_message(int(message_id))
        if row is None or int(row.get("account_id") or 0) != int(account_id) \
                or str(row.get("chat_id") or "") != str(chat_id):
            raise LookupError("消息不存在")
        if str(row.get("kind") or "") != "outgoing":
            raise ValueError("只能撤回自己发出的消息")
        if row.get("recalled_at"):
            raise ValueError("这条消息已经撤回过了")
        external_id = str(row.get("external_id") or "")
        if not external_id:
            raise ValueError("这条消息没有服务端消息 ID，无法撤回（旧版本发出的消息不支持）")
        if not within_recall_window(str(row.get("created_at") or "")):
            raise ValueError("消息发送超过两分钟，无法撤回")
        await worker.recall_manual_message(external_id)
        # 服务端已经撤回成功，本地标记失败只影响页面显示，不能把它当撤回失败
        try:
            await self.db.mark_message_recalled(int(message_id))
        except Exception:
            logger.exception("消息 %s 已撤回，但更新本地状态失败", message_id)
        return {"ok": True, "message_id": int(message_id)}

    async def open_chat(
        self,
        account_id: int,
        buyer_id: str,
        item_id: str = "",
        order_id: str = "",
    ) -> str:
        """后台按订单/买家打开会话，返回 chat_id。"""
        worker = self.require_worker(account_id)
        return await worker.ensure_chat(buyer_id, item_id=item_id, order_id=order_id)

    async def sync_chat(self, account_id: int, chat_id: str, limit: int = 50) -> int:
        """按需补拉一个会话的历史消息。"""
        worker = self.require_worker(account_id)
        return await worker.sync_chat_history(chat_id, limit=limit)

    async def sync_order_history(self, account_id: int | None = None) -> dict[str, Any]:
        """从闲鱼补拉历史订单；不传 account_id 时同步所有在线账号。"""
        if account_id is not None:
            worker = self.require_worker(int(account_id))
            result = await worker.sync_order_history()
            return {
                "accounts": [result],
                "fetched": result["fetched"],
                "imported": result["imported"],
                "skipped": result["skipped"],
                "ignored": int(result.get("ignored") or 0),
                "errors": [],
            }

        workers = [w for w in self.workers.values() if w.is_connected]
        if not workers:
            raise LookupError("没有在线账号，先等账号连接成功再同步")
        result: dict[str, Any] = {
            "accounts": [], "fetched": 0, "imported": 0, "skipped": 0,
            "ignored": 0, "errors": [],
        }
        for worker in workers:
            try:
                item = await worker.sync_order_history()
            except (RiskControl, CookieExpired, TokenExpired):
                raise
            except Exception as exc:
                logger.warning("[%s] 同步订单失败：%s", worker.name, exc)
                result["errors"].append(f"{worker.name}：{exc}")
                continue
            result["accounts"].append(item)
            result["fetched"] += int(item.get("fetched") or 0)
            result["imported"] += int(item.get("imported") or 0)
            result["skipped"] += int(item.get("skipped") or 0)
            result["ignored"] += int(item.get("ignored") or 0)
        if not result["accounts"]:
            raise XianyuError("；".join(result["errors"]) or "同步订单失败")
        return result

    def status(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for wid, worker in self.workers.items():
            row: dict[str, Any] = {
                "account_id": wid,
                "name": worker.name,
                "status": worker.status,
                "last_error": worker.last_error,
            }
            # 登录态是「这台机器还能不能接单」的关键，单独摊平给前端
            try:
                row.update(worker.login_report())
            except Exception:  # 纯展示，算不出来不该让整个 status 500
                logger.debug("读取登录态信息失败（account_id=%s）", wid, exc_info=True)
            rows.append(row)
        return rows
