"""账号级工作循环：连接 → 收消息 → 决策 → 发货 → 记录。"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any

from .autoreply import AutoReplyEngine, ReplyDecision
from .client import CookieExpired, RiskControl, TokenExpired, XianyuClient, XianyuError
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
from .config import settings, upstream_ready
from .db import Database
from .engine import AUTH_BLOCKED_REASON, DeliveryEngine, quota_from_amount, render
from .money import AmountError, amount_from_order_item, pick_amount
from .upstream import Provisioner, UpstreamError, build_card_label

logger = logging.getLogger(__name__)

#: 断线重连的退避序列（秒），循环使用
BACKOFF = (5, 10, 30, 60, 120)
#: 风控/滑块不是 Cookie 失效，退避要更长，避免把账号越撞越黑
RISK_BACKOFF = (60, 180, 300, 600, 900)


def _preview(text: str, limit: int = 120) -> str:
    """日志里只放前一小段，长话术文案打全会把日志刷爆。"""
    one_line = " ".join(str(text or "").split())
    if len(one_line) <= limit:
        return one_line
    return one_line[:limit] + f"…（共 {len(one_line)} 字）"


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
        self._task: asyncio.Task[None] | None = None
        #: 订单轮询兜底任务（IM 推送丢了也能发货）
        self._poll_task: asyncio.Task[None] | None = None
        #: 会话列表轮询兜底任务（推送里拿不到正文时靠它收消息）
        self._msg_poll_task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        #: 「该去查一次待发货订单了」的信号。推送本身解析不出正文（实测大多如此），
        #: 但「有推送」是「可能刚有人拍下」最便宜的线索，用它提前叫醒轮询。
        self._poll_wakeup = asyncio.Event()
        self._last_poll_at = 0.0
        #: 正在发货中的订单号，防止「推送 + 轮询」同时命中同一单发两次
        self._inflight: set[str] = set()
        #: 会话列表的第一轮只做基线（比连接时刻更早的消息不回）
        self._message_baseline_done = False
        #: 本连接建立的时刻（毫秒），用来区分「历史消息」和「新消息」
        self._connected_at_ms = 0
        #: 已经提示过「收到未读通知但没正文」的会话，避免刷屏
        self._unread_notified: set[str] = set()
        #: (chat_key, rule_id)：正在发的那条自动回复，防止同一会话被连发两次
        self._replying: set[tuple[str, int]] = set()
        #: 在飞的自动回复任务，stop() 时一并取消
        self._reply_tasks: set[asyncio.Task[None]] = set()
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
                self.last_error = str(exc)
                logger.error("[%s] Cookie 真的失效（%s），停止重连；请重新扫码", self.name, exc)
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
        self.client = XianyuClient(self.account["cookie"])
        self.client.on_cookie_refresh = self._persist_cookie
        client = self.client
        try:
            ws = await client.connect()
            self.status = "online"
            self.last_error = ""
            self.frames_seen = 0
            self.messages_seen = 0
            logger.info("[%s] 已连接闲鱼 IM（user_id=%s）", self.name, client.user_id)
            # 心跳已经由 client.connect() 在注册前启动，这里只做保活/刷 token
            maintenance = asyncio.create_task(self._maintenance_loop())
            self._poll_task = asyncio.create_task(self._order_poll_loop())
            self._msg_poll_task = asyncio.create_task(self._message_poll_loop())
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
        finally:
            # 连上不到 20 秒就断、且一帧都没收到：大概率是 token/设备不匹配，
            # 清掉缓存，别让下一次重连继续用同一个坏 token。
            if time.monotonic() - started < 20 and not getattr(self, "frames_seen", 0):
                client.invalidate_im_token()
            await client.close()
            self.client = None
            if self.status == "online":
                self.status = "disconnected"

        # 保活发现 Cookie 已真正过期：把异常抛给 run()，让它停止重连并提示扫码，
        # 不要偷偷用同一个失效 Cookie 再连一次。
        if self.status == "cookie_expired":
            raise CookieExpired(self.last_error or "Cookie 会话已过期")

    async def _maintenance_loop(self) -> None:
        """登录态保活：定期 hasLogin + 随机刷新 _m_h5_tk。

        参考 xianyu-pilot 的调度策略：
          - Cookie 保活 30 分钟一次；
          - _m_h5_tk 每 1.5~2.5 小时随机刷新（避免固定节奏）。
        这样 _m_h5_tk 不会等到过期才临时抱佛脚，长时间挂机也不容易掉登录。
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
                        await self.client.keepalive()
                except CookieExpired as exc:
                    self.status = "cookie_expired"
                    self.last_error = str(exc)
                    logger.warning("[%s] Cookie 保活发现登录态过期：%s", self.name, exc)
                    if self.client is not None and self.client.websocket is not None:
                        await self.client.websocket.close()
                    return
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("[%s] Cookie 保活失败（不影响当前连接）：%s", self.name, exc)
                next_keepalive = time.monotonic() + keepalive_interval

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
            except Exception as exc:  # 单条坏消息不该拖垮连接
                logger.exception("[%s] 处理消息出错：%s", self.name, exc)

    async def _handle_payload(self, payload: dict[str, Any]) -> None:
        import json as _json

        msg = parse_message(payload, my_id=self.client.user_id if self.client else "")
        if msg is None:
            chat_id = unread_sync_chat_id(payload)
            if chat_id:
                # 只有会话 ID、没有正文的未读通知：买家确实发了消息，
                # 只是正文不在这次推送里。这条日志是排查「自动回复不生效」的关键 ——
                # 有它就说明推送通了、是我们的解析或注册位点有问题，而不是没人在说话。
                if chat_id not in self._unread_notified:
                    self._unread_notified.add(chat_id)
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
                    logger.info("[%s][DUMP-未解析] %s", self.name,
                                _json.dumps(payload, ensure_ascii=False)[:2000])
                note_unparsed(payload)
            return
        if self.client and msg.sender_id == self.client.user_id:
            return

        if settings.debug_payload:
            logger.info("[%s][DUMP-已解析] content=%r item=%s order=%s 原文=%s",
                        self.name, msg.content[:60], msg.item_id, msg.order_id,
                        _json.dumps(payload, ensure_ascii=False)[:1200])

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
        if seen_key:
            if await self.db.messages_seen(self.account_id, [seen_key]):
                logger.debug("[%s] 消息 %s 已处理过，跳过", self.name, seen_key)
                return None
            await self.db.mark_messages_seen(self.account_id, [seen_key])

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
            order_id=msg.order_id,
            content=msg.content,
            kind="incoming",
        )
        if is_ignored(msg.content):
            logger.debug("[%s] 忽略系统消息：%s", self.name, msg.content)
            return None

        logger.info("[%s] 收到 %s(%s) 关于商品 %s 的消息：%s",
                    self.name, msg.sender_name, msg.sender_id, msg.item_id or "-", msg.content)

        # 自动回复在这之前走：买家问「在吗」不用等到发货逻辑走完才有人搭理。
        # 返回的任务由调用方决定要不要等 —— 收帧循环不能等（会卡住紧随其后的
        # 付款卡片），拉取路径可以等（保证同一会话的回复按顺序发出去）。
        reply_task = await self._maybe_auto_reply(msg) if allow_auto_reply else None

        # 真人打的字要先去闲鱼核实订单。
        # 只有「卡片形态 + 订单号来源可信」才免核实 —— 订单号如果是从正文里
        # 捞出来的，那就跟买家自己打的一句话没有区别，必须核实。
        trusted_card = msg.is_system_card and msg.order_id_trusted
        if msg.order_id and not trusted_card and is_paid_trigger(msg.content):
            msg.order_verified = await self._verify_order_pending(msg.order_id, msg.sender_id)

        # 配置成固定文案 / 卡密池的规则在 decide 里就把文案渲染好了，
        # 名字必须在那之前是准的 —— 否则「你好 买家」会直接发给对方。
        # 已经发过货的单不查（重复的付款卡片很常见，没必要为它多打一次接口）。
        if (msg.order_id and is_placeholder_name(msg.sender_name)
                and is_paid_trigger(msg.content)
                and not await self.db.delivered_order_exists(self.account_id, msg.order_id)):
            await self._name_from_cached_order(msg, fetch=True)

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

        claim = (decision.chat_key, int((decision.rule or {}).get("id") or 0))
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
                self.account_id, msg.sender_id, decision.chat_key, int(rule.get("id") or 0)
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
            if key in seen:
                continue
            # 第一轮：比「本连接建立时刻」更早的都是历史消息，只记已读不回。
            # 用时间而不是「第一轮全跳过」，才不会漏掉启动瞬间刚到达的那条。
            if seeking_baseline and int(conv.get("created_at") or 0) < self._connected_at_ms:
                baseline_keys.append(key)
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

        # 基线那一轮：比连接时刻更早的历史消息不处理（否则一启动就把旧消息全回一遍）。
        # 之后各轮只靠 messageId 去重 —— 那时「早于连接时刻」也可能是一条没见过的消息。
        min_created_at = self._connected_at_ms if seeking_baseline else 0

        # 拉历史是一次 WS 往返，会话多的时候别一次打十几个出去：
        # 优先给「最新有动静」的几个会话补历史，其余只用列表里的最后一条。
        todo.sort(key=lambda c: int(c.get("created_at") or 0), reverse=True)
        max_history = max(0, settings.message_history_conversations)

        handled = 0
        for index, conv in enumerate(todo):
            try:
                with_history = index < max_history
                handled += await self._sync_conversation(
                    conv, min_created_at=min_created_at, with_history=with_history
                )
            except asyncio.CancelledError:
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
            if msg_data.get("from_me"):
                continue
            if not str(msg_data.get("content") or "").strip():
                continue
            msg = self._to_incoming(msg_data)
            if msg is None:
                continue
            logger.debug("[%s] 拉取到 %s 的消息：%s", self.name, msg.sender_name, msg.content[:60])
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
        if handled:
            # 每个会话一条汇总就够了；逐条 INFO 在活跃账号上会把日志刷满
            logger.info("[%s] 会话 %s（%s）拉到 %s 条新消息",
                        self.name, chat_id,
                        (candidates[0].get("buyer_name") if candidates else "") or "-",
                        handled)
        return handled

    def _to_incoming(self, data: dict[str, Any]) -> IncomingMessage | None:
        sender_id = str(data.get("buyer_id") or "")
        if not sender_id:
            return None
        # custom_type：1 真人文本、2 图片、0 未知；其余都是系统消息/卡片
        try:
            custom_type = int(data.get("custom_type") or 0)
        except (TypeError, ValueError):
            custom_type = 0
        return IncomingMessage(
            chat_id=str(data.get("chat_id") or ""),
            sender_id=sender_id,
            sender_name=pick_display_name(data.get("buyer_name")),
            content=str(data.get("content") or ""),
            item_id=data.get("item_id") or None,
            order_id=data.get("order_id") or None,
            created_at=int(data.get("created_at") or 0),
            raw=data.get("raw") or {},
            message_id=str(data.get("message_id") or ""),
            is_system_card=custom_type not in (0, 1, 2),
            order_id_trusted=bool(data.get("order_id_trusted")),
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
            if await self.db.delivered_order_exists(self.account_id, order_id):
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

        decision = await self.engine.decide_order(self.account_id, msg)
        if not decision.should_deliver:
            logger.info("[%s] 订单 %s 跳过：%s", self.name, order_id, decision.reason)
            return False
        rule = decision.rule or {}

        chat_id, chat_error = await self._resolve_chat(buyer_id, item_id)
        if chat_id is None:
            logger.error("[%s] 订单 %s 创建会话失败：%s", self.name, order_id, chat_error)
            # 决策阶段已经从卡密池锁了一张，没发出去就得还回库存
            if decision.card_id is not None:
                await self.db.release_card(decision.card_id)
            await self._record_failure(msg, rule, "创建会话失败：" + str(chat_error))
            return False
        msg.chat_id = chat_id

        return await self._deliver(msg, decision, claimed=True)

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
        order = await self._find_order_cached(order_id)
        if not order:
            logger.warning("[%s] 订单 %s 查不到，不能只凭聊天文本发货", self.name, order_id)
            return False

        common = order.get("commonData") if isinstance(order.get("commonData"), dict) else {}
        status = str(common.get("orderStatus") or "").strip()
        if status not in self.SHIPPABLE_ORDER_STATUS:
            logger.warning("[%s] 订单 %s 状态是「%s」，不是待发货，跳过", self.name, order_id, status or "未知")
            return False

        # 订单里带的买家 id 才是权威的；两边都有却对不上就不发
        buyer = order.get("buyerInfoVO") if isinstance(order.get("buyerInfoVO"), dict) else {}
        order_buyer = str(buyer.get("buyerId") or "").strip()
        if order_buyer and buyer_id and order_buyer != str(buyer_id):
            logger.warning("[%s] 订单 %s 属于买家 %s，与消息里的 %s 不一致，跳过",
                           self.name, order_id, order_buyer, buyer_id)
            return False

        if order_buyer and not buyer_id:
            logger.warning("[%s] 订单 %s 有买家 ID 但消息里没有，跳过", self.name, order_id)
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
        if not sent:
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
        if getattr(decision, "sent", False):
            logger.error(
                "[%s] 卡密 %s 已经发给买家，后续步骤出错也不退回库存（请人工核对这单）",
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

        # DRY-RUN 必须在最前面拦住：上游模式真的会去建 Key、建卡密，不能试跑
        if settings.dry_run:
            if decision.needs_provision:
                logger.warning("[%s][DRY-RUN] 本应查订单 %s 金额 → 建 Key → 出卡密 → 发给 %s",
                               self.name, msg.order_id, msg.sender_name)
            else:
                logger.warning("[%s][DRY-RUN] 本应发货：%s", self.name, _preview(content))
            return False

        # ---- 上游模式：查订单金额 → 建 Key → 出卡密
        if decision.needs_provision:
            prepared = await self._provision(msg, rule)
            if prepared is None:
                return False
            card_code, amount_cny, quota_usd, content = prepared

        try:
            await self.client.send_text_safe(msg.chat_id, msg.sender_id, content)
        except XianyuError as exc:
            logger.error("[%s] 发货消息发送失败：%s", self.name, exc)
            await self.db.record_order(
                account_id=self.account_id, order_id=msg.order_id, item_id=msg.item_id,
                buyer_id=msg.sender_id, buyer_name=msg.sender_name, rule_id=rule.get("id"),
                card_id=None, content=content, status="send_failed", detail=str(exc),
                amount_cny=amount_cny, quota_usd=quota_usd, card_code=card_code,
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
            msg, decision, rule, content, confirm_note, amount_cny, quota_usd, card_code
        )
        logger.info("[%s] 已向 %s 发货（规则：%s，%s 元 → %s USD）",
                    self.name, msg.sender_name, rule.get("name"), amount_cny, quota_usd)
        return True

    async def _record_delivered(self, msg: IncomingMessage, decision: Any, rule: dict[str, Any],
                                content: str, confirm_note: str, amount_cny: float | None,
                                quota_usd: float | None, card_code: str | None) -> None:
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
                card_code=card_code,
            )
        except Exception:
            logger.exception("[%s] 订单 %s 已发货，但写发货记录失败（不会重发，请手工补记录）",
                             self.name, msg.order_id)
        try:
            await self.db.log_message(
                account_id=self.account_id, chat_id=msg.chat_id, sender_id=msg.sender_id,
                sender_name=msg.sender_name, item_id=msg.item_id, order_id=msg.order_id,
                content=content, kind="outgoing",
            )
        except Exception:
            logger.exception("[%s] 订单 %s 已发货，但写消息流水失败", self.name, msg.order_id)

    async def _provision(self, msg: IncomingMessage, rule: dict[str, Any]) -> tuple[str, float | None, float, str] | None:
        """上游模式的实际动作，返回 (卡密, 金额, 额度, 发货文案)；失败返回 None。"""
        if self.provisioner is None:
            logger.error("[%s] 规则是上游模式，但没有配置 new-api / 卡密站", self.name)
            await self._record_failure(msg, rule, "上游未配置")
            return None
        if not msg.order_id:
            logger.error("[%s] 上游模式需要订单号，但这条消息里没有", self.name)
            await self._record_failure(msg, rule, "缺少订单号")
            return None

        assert self.client is not None
        rule_fixed = float(rule.get("fixed_usd") or 0)

        # 订单轮询那条路已经带了实付金额，直接用，省掉一次订单详情往返
        amount_cny: float | None = msg.amount_cny_hint

        if amount_cny is None:
            order = await self._find_order_cached(msg.order_id)
            # 这一步不能晚于建令牌：new-api 的令牌名就是「买家用户名#订单号」，
            # 名字定下来之后再改，后台里留下的还是那条「买家#…」。
            await self._name_from_order(msg, order)
            if order is None:
                if rule_fixed > 0:
                    logger.warning("[%s] 查不到订单 %s 金额，改用规则里的固定额度 %s USD",
                                   self.name, msg.order_id, rule_fixed)
                else:
                    logger.error("[%s] 查不到订单 %s 的金额", self.name, msg.order_id)
                    await self._record_failure(msg, rule, "查不到订单金额")
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
                        await self._record_failure(msg, rule, "金额解析失败")
                        return None

        if amount_cny is not None and settings.max_order_cny > 0 and amount_cny > settings.max_order_cny:
            logger.error("[%s] 订单 %s 金额 %s 元超过上限 %s 元，拒绝自动发货",
                         self.name, msg.order_id, amount_cny, settings.max_order_cny)
            await self._record_failure(msg, rule, "订单金额超上限", amount_cny=amount_cny)
            return None

        quota = quota_from_amount(
            amount_cny or 0.0,
            multiplier=float(rule.get("usd_multiplier") or 1),
            fixed_usd=rule_fixed,
            cny_per_usd=settings.cny_per_usd,
        )
        if quota <= 0:
            logger.error("[%s] 订单 %s 换算额度为 0，跳过", self.name, msg.order_id)
            await self._record_failure(msg, rule, "换算额度为 0", amount_cny=amount_cny)
            return None
        # 卡密名称 = 令牌名 = 买家用户名#订单号；价格只进额度和日志
        label = build_card_label(msg.sender_name or "买家", msg.order_id)
        try:
            provisioned = await self.provisioner.provision(
                quota, label, username=msg.sender_name or "买家"
            )
        except UpstreamError as exc:
            logger.error("[%s] 上游建卡密失败：%s", self.name, exc)
            await self._record_failure(
                msg, rule, "上游建卡密失败：" + str(exc),
                amount_cny=amount_cny, quota_usd=quota,
            )
            return None

        content = render(
            rule["content"],
            buyer_name=msg.sender_name,
            item_id=msg.item_id,
            order_id=msg.order_id,
            card=provisioned.card_code,
            amount_cny=amount_cny,
            quota_usd=quota,
        )
        return provisioned.card_code, amount_cny, quota, content

    async def _record_blocked(self, msg: IncomingMessage, reason: str) -> None:
        """记一条「有人拿『我已付款』想触发发货，但没过鉴权」。

        没有订单号时不落库（否则买家刷屏就能把表写满），只留日志；
        有订单号则落一条 blocked，方便事后对照是不是有人在试别人的单。
        """
        logger.warning("[%s] 拦截可疑发货请求（买家 %s，订单 %s）：%s —— %s",
                       self.name, msg.sender_id, msg.order_id or "-", _preview(msg.content), reason)
        if not msg.order_id:
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
    ) -> None:
        """记一条失败。

        金额要尽量带上：页面上的「—」看不出这单值多少钱，
        事后排查「是不是金额算错了才失败」就得多翻一遍日志。
        失败发生得早（金额还没解析出来）时才留空。
        """
        await self.db.record_order(
            account_id=self.account_id, order_id=msg.order_id, item_id=msg.item_id,
            buyer_id=msg.sender_id, buyer_name=msg.sender_name, rule_id=rule.get("id"),
            card_id=None, content="", status="upstream_failed", detail=reason,
            amount_cny=amount_cny, quota_usd=quota_usd,
        )

class WorkerManager:
    """管理所有账号的 Worker 生命周期。"""

    def __init__(self, db: Database, provisioner: Provisioner | None = None) -> None:
        self.db = db
        self.engine = DeliveryEngine(db, cooldown=settings.delivery_cooldown)
        self.reply_engine = AutoReplyEngine(db)
        #: 上游联动（new-api + 卡密站）。没配置就是 None，规则选了上游模式会明确报错。
        self.provisioner = provisioner if provisioner is not None else (Provisioner() if upstream_ready() else None)
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

    def status(self) -> list[dict[str, Any]]:
        return [
            {
                "account_id": wid,
                "name": w.name,
                "status": w.status,
                "last_error": w.last_error,
            }
            for wid, w in self.workers.items()
        ]
