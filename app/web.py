"""Web 管理后台：FastAPI 路由 + 极简 HTML 面板。"""

from __future__ import annotations

import asyncio
import hmac
import ipaddress
import logging
import mimetypes
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, Response
from pydantic import BaseModel, Field

from . import media as media_tools
from .autoreply import KINDS, MATCH_TYPES, split_keywords
from .client import XianyuError, cookie_flags
from .codec import is_ignored
from .config import (
    AUTO_RATE_ENABLED_KEY,
    AUTO_RATE_FEEDBACK_KEY,
    DEFAULT_AUTO_RATE_FEEDBACK,
    MODEL_LIMITS_KEY,
    cardshop_ready,
    newapi_ready,
    resolve_cookie_key_path,
    resolved_auto_rate,
    resolved_model_limits,
    settings,
    upstream_ready,
)
from .db import Database
from .qrlogin import STATUS_TEXT, LoginError, QrLoginManager, render_qr_png
from .runner import WorkerManager
from .secrets import CookieVault
from .sign import sanitize_cookie_string
from .upstream import (
    NewApiClient,
    UpstreamError,
    mask_key,
    normalize_key,
    normalize_model_limits,
    token_expired,
)

logger = logging.getLogger(__name__)

TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"

#: 站点货币的进程内缓存：base_url -> (取回时刻, 货币)。
#: 前端每 5 秒拉一次 /api/config，不能每次都去问上游。
_SITE_CURRENCY_CACHE: dict[str, tuple[float, str]] = {}
_SITE_CURRENCY_TTL = 60.0


class AccountIn(BaseModel):
    name: str = Field(min_length=1, max_length=64)
    cookie: str = ""
    remark: str = ""
    enabled: bool = True


class RuleIn(BaseModel):
    name: str = Field(min_length=1, max_length=64)
    item_id: str = "*"
    enabled: bool = True
    #: upstream = 按订单金额建 Key 出卡密；topup = 充到买家已有 Key（不换 Key）；
    #: account = 问买家有没有账号（没有就开新号发卡密，有就直接充到账号）；
    #: redeem = 生成 new-api 兑换码直接发给买家（买家自己登录兑换）
    mode: str = Field(default="text", pattern="^(text|card|upstream|topup|account|redeem)$")
    content: str = ""
    auto_confirm: bool = False
    priority: int = 0
    remark: str = ""
    #: 规格关键词（可选）：同一商品的不同规格走不同规则；空 = 不限规格
    spec: str = Field(default="", max_length=500)
    #: 充值模式下买家还没有 Key 时：create = 开新 Key 并绑定，fail = 不发货（仅 auto 模式）
    topup_missing: str = Field(default="create", pattern="^(create|fail)$")
    #: 充值模式怎么确定 Key：order_id = 买家发旧订单号；auto = 自动绑定/历史/首单；
    #: api_key = 买家直接发 Key（只认本站发给他本人的令牌）
    topup_key_source: str = Field(default="order_id", pattern="^(order_id|auto|api_key)$")
    #: 充值模式允不允许拿「另一个商品」的旧订单当充值目标：
    #: any = 允许（老行为）；item = 只认同商品的旧订单 / 同商品发过的 Key
    topup_item_scope: str = Field(default="any", pattern="^(any|item)$")
    #: 提示买家发旧订单号 / 找不到 Key 时的回复；留空用内置默认
    topup_prompt: str = Field(default="", max_length=1000)
    topup_not_found: str = Field(default="", max_length=1000)
    #: 账号模式：首次询问买家「有没有账号」/ 老用户充值前的确认回显 / 卡密内容。
    #: 都留空就用内置文案（支持 {username} {password} {login_url} {balance} 等占位符）
    account_prompt: str = Field(default="", max_length=1000)
    account_confirm: str = Field(default="", max_length=1000)
    account_card: str = Field(default="", max_length=1000)
    #: 兑换码模式：码的有效期（天）。0 = 永不过期
    redeem_expiry_days: int = Field(default=0, ge=0, le=3650)
    #: 按订单实付金额匹配（元，含边界）；0 = 不限
    min_amount: float = Field(default=0.0, ge=0)
    max_amount: float = Field(default=0.0, ge=0)
    #: 上游 / 充值模式下：额度 = 订单金额 / cny_per_usd * usd_multiplier（fixed_usd > 0 时直接用）
    usd_multiplier: float = 1.0
    fixed_usd: float = 0.0


class BuyerKeyIn(BaseModel):
    """一条「买家 → Key」绑定。

    token_id 与 api_key 至少给一个：给 token_id 最稳（不用按 Key 反查）；
    给 api_key 时会去 new-api 令牌列表里找对应的令牌。
    """

    buyer_id: str = Field(min_length=1, max_length=64)
    buyer_name: str = Field(default="", max_length=64)
    token_id: str = Field(default="", max_length=64)
    api_key: str = Field(default="", max_length=200)
    remark: str = Field(default="", max_length=200)


class BuyerAccountIn(BaseModel):
    """一条「买家 → new-api 账号」绑定。

    user_id 与 username 至少给一个：给 id 最稳（改名也不会错），
    只给用户名时会去 new-api 里查一次确认存在。
    """

    buyer_id: str = Field(min_length=1, max_length=64)
    buyer_name: str = Field(default="", max_length=64)
    user_id: str = Field(default="", max_length=64)
    username: str = Field(default="", max_length=20)
    password: str = Field(default="", max_length=128)
    remark: str = Field(default="", max_length=200)


class ReplyRuleEntryIn(BaseModel):
    """规则里的一条「关键词 -> 回复」。

    id 有值表示这是修改已有条目；不传就是新加一条。
    """

    id: int | None = None
    keywords: str = ""
    content: str = ""


class ReplyRuleIn(BaseModel):
    """一条自动回复规则。kind=item 是商品自动回复，kind=keyword 是关键词自动回复。

    规则本身只管商品范围 / 匹配方式 / 频率；具体回什么话在 entries 里，
    一条规则可以配多条回复。顶层的 keywords / content 仅用于兼容旧接口：
    没有 entries 时当成唯一一条回复，读取时镜像第一条。
    """

    name: str = Field(default="", max_length=64)
    kind: str = Field(default="keyword", pattern="^(" + "|".join(KINDS) + ")$")
    item_id: str = "*"
    #: 兼容旧接口：没有 entries 时，这一组关键词 + content 就是唯一的一条回复
    keywords: str = ""
    #: contains 包含 / exact 完全一致 / regex 正则
    match_type: str = Field(default="contains", pattern="^(" + "|".join(MATCH_TYPES) + ")$")
    #: 兼容旧接口：见 keywords
    content: str = ""
    #: 规则里的多条回复；每条有自己的关键词和内容
    entries: list[ReplyRuleEntryIn] = Field(default_factory=list)
    enabled: bool = True
    #: 同一个会话只回一次（否则买家每发一句都会被回一遍）
    once_per_chat: bool = True
    #: 同一买家多少秒内不再回同一条回复；0 = 不限制
    cooldown: int = 60
    priority: int = 0
    remark: str = ""


class LoginIn(BaseModel):
    name: str = Field(min_length=1, max_length=64)
    remark: str = ""


class SettingsIn(BaseModel):
    """后台「设置」页里可改的项。

    只放「改错了也不至于泄漏凭据」的东西：模型白名单这类运营参数。
    凭据（access_token / 密码）仍然只从 .env 读，避免把密钥落进数据库。
    """

    #: 新建 Key 只允许使用的模型（逗号 / 中文逗号 / 空白分隔）。空串 = 不限制。
    #: 不传（None）表示这次不修改全局模型白名单。
    model_limits: str | None = Field(default=None, max_length=2000)
    #: 自动评价开关 / 文案；不传（None）表示这次不修改。
    auto_rate_enabled: bool | None = None
    auto_rate_feedback: str | None = Field(default=None, max_length=200)


class ItemModelIn(BaseModel):
    """一个商品的模型白名单覆盖（商品级配置优先于全局设置）。"""

    item_id: str = Field(min_length=1, max_length=64)
    #: 逗号 / 空格 / 换行分隔；空串 = 这个商品不限制模型（有意为之，不是「用全局值」）
    model_limits: str = Field(default="", max_length=2000)
    remark: str = Field(default="", max_length=200)


class ChatSendIn(BaseModel):
    """后台聊天界面发一条私信。"""

    account_id: int
    to_id: str = Field(default="", max_length=64)
    text: str = Field(default="", max_length=2000)
    item_id: str | None = Field(default=None, max_length=64)
    order_id: str | None = Field(default=None, max_length=64)
    buyer_name: str = Field(default="", max_length=64)


class ChatOpenIn(BaseModel):
    """按订单 / 买家打开一个会话（订单记录本身不带 chat_id）。"""

    account_id: int
    buyer_id: str = Field(min_length=1, max_length=64)
    item_id: str = Field(default="", max_length=64)
    order_id: str = Field(default="", max_length=64)
    buyer_name: str = Field(default="", max_length=64)


class ChatImageIn(BaseModel):
    """发送内置图片（图片已经在后台图库里）。"""

    account_id: int
    image_id: int
    to_id: str = Field(default="", max_length=64)
    item_id: str | None = Field(default=None, max_length=64)
    order_id: str | None = Field(default=None, max_length=64)
    buyer_name: str = Field(default="", max_length=64)


class ChatRecallIn(BaseModel):
    """撤回一条本工具发出的消息。"""

    account_id: int
    message_id: int


class CardsIn(BaseModel):
    item_id: str = "*"
    contents: list[str] = Field(default_factory=list)
    raw_text: str = ""


async def _prune_loop(db: Database, interval: float = 3600.0) -> None:
    """每小时清一次过期消息流水（XY_MESSAGE_RETENTION_DAYS，0 = 不清）。

    先睡再干：启动那一刻没必要就去扫库，也避免「刚起进程就被取消」
    把 aiosqlite 的工作线程挂在已关闭的事件循环上。
    """
    while True:
        await asyncio.sleep(interval)
        days = settings.message_retention_days
        if days > 0:
            try:
                deleted = await db.prune_history(days)
                if deleted:
                    logger.info("已清理 %s 天前的消息流水：%s 条", days, deleted)
                removed = await asyncio.to_thread(
                    media_tools.prune_media_dir, settings.media_dir, "chat", days
                )
                if removed:
                    logger.info("已清理 %s 天前的聊天媒体副本：%s 个", days, removed)
            except Exception as exc:  # 清理失败不该影响收发货
                logger.warning("清理消息流水失败：%s", exc)


class _RateLimiter:
    """进程内固定窗口限流。

    单进程部署（本项目就是）够用，重启清零。管理的这几条路要么能猜口令、
    要么会替我们往外打接口，没有节流就等于把「请求频率」交给对面决定。
    """

    def __init__(self, limit: int, window: float, *, max_keys: int = 2048) -> None:
        self.limit = limit
        self.window = window
        self.max_keys = max_keys
        self._hits: dict[str, tuple[float, int]] = {}

    def blocked_for(self, key: str) -> float:
        """还要等几秒才解除限制；0 表示没被限。"""
        entry = self._hits.get(key)
        if entry is None:
            return 0.0
        started, count = entry
        elapsed = time.monotonic() - started
        if elapsed >= self.window:
            self._hits.pop(key, None)
            return 0.0
        return (self.window - elapsed) if count >= self.limit else 0.0

    def hit(self, key: str) -> None:
        """记一次。"""
        now = time.monotonic()
        if len(self._hits) > self.max_keys:
            # 键是 IP，正常不会这么多；真被打的时候也不能让这张表变成内存洞
            self._hits.clear()
        started, count = self._hits.get(key, (now, 0))
        if now - started >= self.window:
            started, count = now, 0
        self._hits[key] = (started, count + 1)

    def reset(self, key: str) -> None:
        self._hits.pop(key, None)


def _is_ip_literal(host: str) -> bool:
    try:
        ipaddress.ip_address(host.strip("[]"))
    except ValueError:
        return False
    return True


def create_app(db: Database | None = None, manager: WorkerManager | None = None) -> FastAPI:
    database = db or Database(
        settings.db_path,
        # 生产环境密钥单独放在 data/cookie.key（跟库分开，方便「备份库但不备份密钥」）；
        # 用 XY_DB_PATH 指到临时库时密钥跟着那个库走，不碰生产密钥。
        vault=CookieVault(
            resolve_cookie_key_path(settings), enabled=settings.cookie_encryption
        ),
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # uvicorn 的 Config 在构造时会把 uvicorn.access 重新设回 log_level
        # （uvicorn/config.py: configure_logging()，log_config=None 也照设），
        # 所以 main.setup_logging() 里的压制会被它盖掉 —— 必须在这里再压一次。
        # 原因：后台页面每 5 秒轮询一批接口，access log 两小时就能刷满一个 10 MB
        # 文件，真正要看的「保活 / 登录态过期」全被挤进轮转文件里。
        logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
        await database.init()
        recovered = await database.recover_stale_sending()
        if recovered:
            logger.warning(
                "发现 %s 条上次进程中断时停在发送/充值中途的订单，已分别标为 "
                "send_unknown / topup_unknown；请到「发货记录」人工核对", recovered,
            )
        mgr = manager or WorkerManager(database)
        app.state.manager = mgr
        await mgr.sync()
        prune_task = asyncio.create_task(_prune_loop(database), name="message-prune")
        try:
            yield
        finally:
            prune_task.cancel()
            try:
                await prune_task
            except (asyncio.CancelledError, Exception):  # noqa: S110 - 退出流程里不重要
                pass
            await mgr.stop_all()
            await app.state.qr.close()

    app = FastAPI(title="闲鱼自动发货工具", version="0.1.0", lifespan=lifespan)
    app.state.db = database
    app.state.qr = QrLoginManager()

    #: 允许的 Host 名字。纯 IP（含 [::1]）一律放行：DNS rebinding 必须借域名，
    #: 用 IP 访问不是攻击面，这样局域网里用 IP 访问也不会被误伤。
    #: （这里的 0.0.0.0 / [::] 只是 Host 白名单里的字符串，不涉及任何 bind）
    _ALLOWED_HOSTS = frozenset(
        {"127.0.0.1", "localhost", "::1", "[::1]", "0.0.0.0", "[::]"}  # noqa: S104
        | {part.strip().lower() for part in settings.allowed_hosts.split(",") if part.strip()}
        | {settings.host.lower()}
    )

    def _host_ok(request: Request) -> bool:
        raw = (request.headers.get("host") or "").strip().lower()
        if not raw:
            # HTTP/1.0 之类没有 Host：交给后面的鉴权判断，不在这里挡
            return True
        if raw.startswith("["):
            host = raw.split("]")[0] + "]"
        elif ":" in raw:
            host = raw.split(":")[0]
        else:
            host = raw
        return host in _ALLOWED_HOSTS or _is_ip_literal(host)

    @app.middleware("http")
    async def _guard_requests(request: Request, call_next):
        """Host / 跨站 / 安全响应头。

        Host 校验挡的是 DNS rebinding：恶意页面把自己的域名解析到 127.0.0.1，
        浏览器就把它当同源，于是「只允许本机」对它完全无效。
        跨站校验挡的是 CSRF：表单 POST 不需要预检，能直接打到只吃 query 的
        那几个接口（toggle / reset）。
        """
        if not _host_ok(request):
            logger.warning("拒绝可疑 Host 的请求：%s %s", request.method, request.headers.get("host"))
            return JSONResponse(
                status_code=400,
                content={"detail": "Host 不在允许列表里。用域名访问请在 XY_ALLOWED_HOSTS 中配置"},
            )
        if request.method not in ("GET", "HEAD", "OPTIONS") and not _same_origin(request):
            logger.warning("拒绝跨站写请求：%s %s", request.method, request.url.path)
            return JSONResponse(status_code=403, content={"detail": "跨站请求被拒绝"})

        response = await call_next(request)
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        response.headers.setdefault(
            "Content-Security-Policy",
            # 页面是单文件内联脚本 + onclick，必须放行 inline；但依然禁止外域，
            # 注入进来的 <script src=//evil> 会被拦掉。
            # 聊天里的图片/视频来自闲鱼 CDN，必须放行 https / http；
            # 本工具自己上传的素材走 blob:。
            "default-src 'self'; img-src 'self' blob: data: https: http:; "
            "media-src 'self' blob: https: http:; style-src 'self' 'unsafe-inline'; "
            "script-src 'self' 'unsafe-inline'; connect-src 'self'; "
            "frame-ancestors 'none'; base-uri 'none'",
        )
        if request.url.path.startswith("/api/"):
            # 卡密、Cookie、买家昵称都在这些响应里，别留在浏览器磁盘缓存
            response.headers.setdefault("Cache-Control", "no-store")
        return response
    #: session_id -> 用户填的账号名/备注，扫码成功后用来建账号
    _pending: dict[str, LoginIn] = {}

    def _prune_pending(qr: QrLoginManager) -> None:
        """清掉没有对应二维码会话（或已超时）的 pending 项。"""
        for sid in list(_pending):
            session = qr.get(sid)
            if session is None or session.age > 600:
                _pending.pop(sid, None)

    def _qr(request: Request) -> QrLoginManager:
        return request.app.state.qr

    def _is_loopback(request: Request) -> bool:
        host = (request.client.host if request.client else "") or ""
        return host in ("127.0.0.1", "::1", "localhost") or host.startswith("127.")

    def _client_ip(request: Request) -> str:
        # 注意：main.py 里 uvicorn 固定 proxy_headers=False，所以这是真实对端，
        # 不会被客户端自带的 X-Forwarded-For 冒充。
        return (request.client.host if request.client else "") or "-"

    #: 口令错误次数：10 次/分钟。挡的是「挂到公网后无限猜」。
    auth_failures = _RateLimiter(limit=10, window=60.0)
    #: 会替我们往外打接口的端点（扫码登录、拉商品列表）：10 次/分钟。
    outbound_calls = _RateLimiter(limit=10, window=60.0)
    #: 后台聊天发送：60 条/分钟。人手打字打不到这么快，主要是防脚本跑飞。
    chat_sends = _RateLimiter(limit=60, window=60.0)
    #: 聊天相关的读操作（同步历史 / 打开会话）：30 次/分钟。
    #: 比「扫码登录」那类宽松些 —— 后台翻会话本来就会连续点几下。
    chat_outbound = _RateLimiter(limit=30, window=60.0)
    #: 图片/视频发送与图库上传：20 次/分钟（上传视频很重，不能刷）。
    chat_media = _RateLimiter(limit=20, window=60.0)

    def _throttle_chat_media(request: Request, what: str) -> None:
        key = f"chat-media:{_client_ip(request)}"
        wait = chat_media.blocked_for(key)
        if wait > 0:
            raise HTTPException(
                status_code=429,
                detail=f"{what}太频繁，请 {int(wait) + 1} 秒后再试",
                headers={"Retry-After": str(int(wait) + 1)},
            )
        chat_media.hit(key)

    def _throttle_chat_outbound(request: Request, what: str) -> None:
        key = f"chat-out:{_client_ip(request)}"
        wait = chat_outbound.blocked_for(key)
        if wait > 0:
            raise HTTPException(
                status_code=429,
                detail=f"{what}太频繁，请 {int(wait) + 1} 秒后再试",
                headers={"Retry-After": str(int(wait) + 1)},
            )
        chat_outbound.hit(key)

    def _throttle_chat(request: Request, what: str = "发送消息") -> None:
        key = f"chat:{_client_ip(request)}"
        wait = chat_sends.blocked_for(key)
        if wait > 0:
            raise HTTPException(
                status_code=429,
                detail=f"{what}太频繁，请 {int(wait) + 1} 秒后再试",
                headers={"Retry-After": str(int(wait) + 1)},
            )
        chat_sends.hit(key)

    def _throttle_outbound(request: Request, what: str) -> None:
        """限制会触发外部请求的接口，别让面板变成打闲鱼/上游的放大器。"""
        key = f"{what}:{_client_ip(request)}"
        wait = outbound_calls.blocked_for(key)
        if wait > 0:
            raise HTTPException(
                status_code=429,
                detail=f"{what} 调用太频繁，请 {int(wait) + 1} 秒后再试",
                headers={"Retry-After": str(int(wait) + 1)},
            )
        outbound_calls.hit(key)

    def _same_origin(request: Request) -> bool:
        """浏览器是不是从「本站」发出的请求。

        只用来挡 CSRF：带 Origin 且和 Host 对不上、或浏览器自己说
        cross-site，就拒绝。curl 之类没有这两个头，照常能用。
        """
        if request.headers.get("sec-fetch-site", "").lower() == "cross-site":
            return False
        origin = request.headers.get("origin")
        if not origin:
            return True
        expected_host = (request.headers.get("host") or "").lower()
        return origin.lower().split("://", 1)[-1] == expected_host

    async def require_admin(
        request: Request,
        x_admin_token: str | None = Header(default=None),
        authorization: str | None = Header(default=None),
    ) -> None:
        """管理接口的鉴权。

        行为：
          * 配了 XY_ADMIN_TOKEN → 必须带对的口令（X-Admin-Token 头，
            或 Authorization: Bearer <口令>），错误次数有限流。
          * 没配口令 → **只**放行本机来源，非本机一律 401。
            XY_ADMIN_ALLOW_REMOTE 在这里**不生效**：它只放宽「谁能访问」，
            不校验口令；两者都不配就等于把后台交给任何人，这种组合不能放行。

        为什么默认要拦：这些接口能改账号 Cookie、读买家昵称、导出卡密、
        删规则。以前「没配口令 = 谁都能调」，只要放到反代后面就是一扇敞开的门。
        """
        token = x_admin_token or ""
        if not token and authorization and authorization.lower().startswith("bearer "):
            token = authorization[7:].strip()

        expected = settings.admin_token
        if expected:
            fail_key = f"auth:{_client_ip(request)}"
            wait = auth_failures.blocked_for(fail_key)
            if wait > 0:
                raise HTTPException(
                    status_code=429,
                    detail=f"口令错误次数过多，请 {int(wait) + 1} 秒后再试",
                    headers={"Retry-After": str(int(wait) + 1)},
                )
            # 恒定时间比较：普通 != 会按「第几个字符不同」泄漏时间差。
            # 必须比 bytes —— compare_digest 不接受含非 ASCII 字符的 str，
            # 口令里带中文会直接 TypeError，变成每个请求都 500。
            if not hmac.compare_digest(token.encode("utf-8"), expected.encode("utf-8")):
                auth_failures.hit(fail_key)
                logger.warning("管理口令错误（来源 %s）", _client_ip(request))
                raise HTTPException(status_code=401, detail="管理口令错误")
            auth_failures.reset(fail_key)
            return

        if _is_loopback(request):
            return
        raise HTTPException(
            status_code=401,
            detail="未配置 XY_ADMIN_TOKEN，管理接口只允许本机访问"
                   "（XY_ADMIN_ALLOW_REMOTE 只放宽来源、不校验口令，没口令时不会生效）；"
                   "请在 .env 里设置 XY_ADMIN_TOKEN 后再远程使用",
        )

    guard = [Depends(require_admin)]

    def mgr(request: Request) -> WorkerManager:
        return request.app.state.manager

    # -------------------------------------------------------------- 页面

    @app.get("/", response_class=HTMLResponse)
    async def index() -> HTMLResponse:
        return HTMLResponse((TEMPLATE_DIR / "index.html").read_text(encoding="utf-8"))

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    # -------------------------------------------------------------- 账号

    @app.get("/api/accounts", dependencies=guard)
    async def list_accounts() -> list[dict[str, Any]]:
        accounts = await database.list_accounts()
        # 一次读完配置表拿「上次登录时间」，别在循环里按账号逐个查（N+1）
        config = await database.get_all_config()
        for account in accounts:
            # 顺带把「这账号的 Cookie 里有没有 cookie17」算给前端：
            # 有它才能长时间挂机，没有就注定一天一重扫。
            # 只暴露「有没有 / 缺哪几张票」，Cookie 值本身照旧打码。
            flags = cookie_flags(account.get("cookie", "") or "")
            account["has_cookie17"] = flags["has_cookie17"]
            account["missing_cookies"] = flags["missing"]
            account["login_at"] = config.get(f"account_login_at:{int(account['id'])}", "")
            # 解密失败（密钥换过 / 库是从别处拷来的）要让人看得见，
            # 不然账号只会显示「未启动」，谁都猜不到是密钥的问题。
            account["cookie_error"] = account.get("cookie_error", "")
            account["cookie"] = "***" if account["cookie"] else ""
        return accounts

    @app.post("/api/accounts", dependencies=guard)
    async def save_account(payload: AccountIn, request: Request) -> dict[str, Any]:
        cookie = payload.cookie.strip()
        #: 这次请求是不是真的带了新 Cookie（只改备注/开关时是 "***" 或空）
        pasted = bool(cookie) and cookie != "***"
        if pasted:
            # 落库前先洗：多行 / 带 Cookie 头名 / 夹带控制字符的粘贴不该进数据库，
            # 否则每次启动都要在 XianyuClient 里再洗一遍、还要多打一条告警。
            # 洗完连 unb 都没有就直接拒 —— 存一个连不上的账号只会显示「未启动」。
            cookie, dropped = sanitize_cookie_string(cookie)
            if dropped:
                logger.warning("粘贴的 Cookie 里有 %s 个键不合法，已丢弃（只记数量）", len(dropped))
            if not cookie or "unb=" not in cookie:
                raise HTTPException(
                    status_code=400,
                    detail="Cookie 里没有可用的 unb（用户 ID），请确认复制的是登录后的完整 Cookie",
                )
        if not cookie or cookie == "***":
            # 只改备注/开关时前端拿不到原 Cookie，沿用数据库里的旧值。
            # 必须走 db 层拿解密后的值，否则会把 enc:v1: 密文当 Cookie 再存一遍。
            cookie = await database.get_account_cookie_by_name(payload.name)
        account_id = await database.upsert_account(
            payload.name, cookie, payload.remark, payload.enabled
        )
        # 手工粘贴的 Cookie 也算一次「真正重新登录」：登录态 TTL 从这里起算。
        # 只改了备注/开关时（cookie 是 "***"）不能刷新它，否则预警永远不响。
        if pasted:
            await database.record_login(account_id)
        await mgr(request).sync()
        return {"id": account_id}

    @app.delete("/api/accounts/{account_id}", dependencies=guard)
    async def delete_account(account_id: int, request: Request) -> dict[str, bool]:
        await database.delete_account(account_id)
        await mgr(request).sync()
        return {"ok": True}

    @app.post("/api/accounts/{account_id}/toggle", dependencies=guard)
    async def toggle_account(account_id: int, request: Request, enabled: bool = True) -> dict[str, bool]:
        account = await database.get_account(account_id)
        if account is None:
            raise HTTPException(status_code=404, detail="账号不存在")
        await database.upsert_account(
            account["name"], account["cookie"], account["remark"], enabled
        )
        await mgr(request).sync()
        return {"ok": True}

    # ---------------------------------------------------------- 扫码登录

    @app.post("/api/login/start", dependencies=guard)
    async def login_start(payload: LoginIn, request: Request) -> dict[str, Any]:
        """生成一个闲鱼登录二维码，返回会话 id。"""
        _throttle_outbound(request, "生成二维码")
        qr = _qr(request)
        _prune_pending(qr)
        session_id = uuid.uuid4().hex
        try:
            session = await qr.start(session_id)
        except LoginError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        # 把账号名先记在会话上，扫码成功后直接建账号
        _pending[session_id] = payload
        return {
            "session_id": session_id,
            "qr_url": session.qr_url,
            "status": session.status,
            "status_text": STATUS_TEXT.get(session.status, session.status),
        }

    @app.get("/api/login/{session_id}/qrcode")
    async def login_qrcode(session_id: str, request: Request, _: None = Depends(require_admin)) -> Response:
        """二维码图片（PNG），前端直接 <img src> 显示。"""
        session = _qr(request).get(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="二维码会话不存在或已过期")
        return Response(content=render_qr_png(session.qr_url), media_type="image/png")

    @app.get("/api/login/{session_id}/poll", dependencies=guard)
    async def login_poll(session_id: str, request: Request) -> dict[str, Any]:
        """轮询扫码状态；确认后自动把账号存进数据库并开始监听。"""
        qr = _qr(request)
        session = qr.get(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="二维码会话不存在或已过期")

        try:
            session = await qr.poll(session_id)
        except LoginError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        result: dict[str, Any] = {
            "status": session.status,
            "status_text": STATUS_TEXT.get(session.status, session.status),
            "error": session.error,
        }

        if session.status in ("EXPIRED", "CANCELED", "ERROR"):
            # 不清理的话，每扫一次失败就往内存里留一条永远不走的记录
            _pending.pop(session_id, None)

        if session.status == "CONFIRMED" and session.cookie and not session.error:
            pending = _pending.pop(session_id, None)
            name = pending.name if pending else (session.nick or "闲鱼账号")
            remark = pending.remark if pending else ""
            account_id = await database.upsert_account(name, session.cookie, remark, True)
            login_at = await database.record_login(account_id)
            await qr.cancel(session_id)
            await mgr(request).sync()
            flags = cookie_flags(session.cookie)
            result.update({
                "account_id": account_id,
                "nick": session.nick,
                "unb": session.unb,
                "login_at": login_at,
                # 没有 cookie17 → 明天这个点还得再扫一次，前端直接告诉用户
                "has_cookie17": flags["has_cookie17"],
            })
            logger.info("扫码登录成功，账号 %s 已保存（id=%s）", name, account_id)

        return result

    # -------------------------------------------------------------- 规则

    @app.get("/api/rules", dependencies=guard)
    async def list_rules() -> list[dict[str, Any]]:
        return await database.list_rules()

    def _check_rule_payload(payload: RuleIn) -> None:
        if (
            payload.min_amount > 0
            and payload.max_amount > 0
            and payload.min_amount > payload.max_amount
        ):
            raise HTTPException(
                status_code=400,
                detail="最低金额不能大于最高金额（0 表示不限）",
            )

    @app.post("/api/rules", dependencies=guard)
    async def create_rule(payload: RuleIn) -> dict[str, Any]:
        _check_rule_payload(payload)
        rule_id = await database.save_rule(payload.model_dump())
        return {"id": rule_id}

    @app.put("/api/rules/{rule_id}", dependencies=guard)
    async def update_rule(rule_id: int, payload: RuleIn) -> dict[str, Any]:
        if await database.get_rule(rule_id) is None:
            raise HTTPException(status_code=404, detail="规则不存在")
        _check_rule_payload(payload)
        await database.save_rule(payload.model_dump(), rule_id)
        return {"id": rule_id}

    @app.delete("/api/rules/{rule_id}", dependencies=guard)
    async def delete_rule(rule_id: int) -> dict[str, bool]:
        await database.delete_rule(rule_id)
        return {"ok": True}

    # ---------------------------------------------------------- 自动回复

    @app.get("/api/reply-rules", dependencies=guard)
    async def list_reply_rules() -> list[dict[str, Any]]:
        return await database.list_reply_rules()

    def _auto_reply_name(kind: str, entry: dict[str, Any]) -> str:
        """规则名留空时按第一条回复自动起名，省得用户一条条想名字。"""
        if kind != "keyword":
            return "商品自动回复"
        words = split_keywords(str(entry.get("keywords") or ""))
        return " / ".join(words[:2])[:40] if words else "关键词自动回复"

    def _check_reply_payload(payload: ReplyRuleIn, fallback_name: str = "") -> dict[str, Any]:
        """校验并归一化一条规则：entries 里每条都要有内容，关键词规则还要有关键词。"""
        raw: list[dict[str, Any]] = [entry.model_dump() for entry in payload.entries]
        if not raw and (payload.keywords.strip() or payload.content.strip()):
            # 兼容旧接口：没有 entries 时，顶层的 keywords + content 就是唯一一条
            raw = [{"id": None, "keywords": payload.keywords, "content": payload.content}]
        if not raw:
            raise HTTPException(status_code=400, detail="至少要有一条回复内容")
        if len(raw) > 50:
            raise HTTPException(status_code=400, detail="一条规则最多配 50 条回复")

        entries: list[dict[str, Any]] = []
        for index, entry in enumerate(raw, start=1):
            keywords = str(entry.get("keywords") or "").strip()
            content = str(entry.get("content") or "")
            if payload.kind == "keyword" and not keywords:
                raise HTTPException(status_code=400, detail=f"第 {index} 条回复缺关键词")
            if not content.strip():
                raise HTTPException(
                    status_code=400, detail=f"第 {index} 条回复内容为空，否则买家会收到空消息"
                )
            entries.append({
                "id": entry.get("id"),
                "keywords": keywords if payload.kind == "keyword" else "",
                "content": content,
            })

        data = payload.model_dump()
        data["entries"] = entries
        # 平铺字段继续镜像第一条：旧脚本 / 旧页面读 keywords、content 时不会拿到空值
        data["keywords"] = entries[0]["keywords"]
        data["content"] = entries[0]["content"]
        name = data["name"].strip() or fallback_name.strip()
        data["name"] = name or _auto_reply_name(payload.kind, entries[0])
        return data

    @app.post("/api/reply-rules", dependencies=guard)
    async def create_reply_rule(payload: ReplyRuleIn) -> dict[str, Any]:
        rule_id = await database.save_reply_rule(_check_reply_payload(payload))
        return {"id": rule_id}

    @app.put("/api/reply-rules/{rule_id}", dependencies=guard)
    async def update_reply_rule(rule_id: int, payload: ReplyRuleIn) -> dict[str, Any]:
        existing = await database.get_reply_rule(rule_id)
        if existing is None:
            raise HTTPException(status_code=404, detail="自动回复规则不存在")
        # 不带名字的局部更新保留原名；完全留空时才按第一条回复自动起名
        data = _check_reply_payload(payload, fallback_name=str(existing["name"]))
        await database.save_reply_rule(data, rule_id)
        # 改完规则想立刻重新生效：顺手清掉这条规则的回复记录
        await database.clear_reply_hits(rule_id)
        return {"id": rule_id}

    @app.delete("/api/reply-rules/{rule_id}", dependencies=guard)
    async def delete_reply_rule(rule_id: int) -> dict[str, bool]:
        await database.delete_reply_rule(rule_id)
        await database.clear_reply_hits(rule_id)
        return {"ok": True}

    @app.post("/api/reply-rules/{rule_id}/toggle", dependencies=guard)
    async def toggle_reply_rule(rule_id: int, enabled: bool = True) -> dict[str, bool]:
        """开关一条规则。和「编辑」分开，改开关不清回复记录。"""
        rule = await database.get_reply_rule(rule_id)
        if rule is None:
            raise HTTPException(status_code=404, detail="自动回复规则不存在")
        rule["enabled"] = enabled
        await database.save_reply_rule(rule, rule_id)
        return {"ok": True}

    @app.post("/api/reply-rules/{rule_id}/reset", dependencies=guard)
    async def reset_reply_rule(rule_id: int) -> dict[str, bool]:
        """清掉这条规则的回复记录，让「同一会话只回一次」重新生效。"""
        if await database.get_reply_rule(rule_id) is None:
            raise HTTPException(status_code=404, detail="自动回复规则不存在")
        await database.clear_reply_hits(rule_id)
        return {"ok": True}

    @app.post("/api/reply-rules/{rule_id}/merge", dependencies=guard)
    async def merge_reply_rule(rule_id: int) -> dict[str, Any]:
        """把同商品、同类型的其它自动回复规则合并到这条规则里。

        老版本里一个商品配多条规则的用户，可以一键并成「一条规则多条回复」；
        共用设置以当前规则为准，历史回复记录会跟着迁移。
        """
        if await database.get_reply_rule(rule_id) is None:
            raise HTTPException(status_code=404, detail="自动回复规则不存在")
        merged, entries = await database.merge_reply_rules(rule_id)
        return {"merged": merged, "entries": entries}

    @app.get("/api/accounts/{account_id}/items", dependencies=guard)
    async def list_account_items(account_id: int, request: Request) -> list[dict[str, Any]]:
        """拉这个账号「在售」的商品，方便在规则里选商品 ID 而不是手抄。"""
        _throttle_outbound(request, "拉商品列表")
        account = await database.get_account(account_id)
        if account is None or not account["cookie"]:
            raise HTTPException(status_code=404, detail="账号不存在或没有 Cookie")

        from .client import CookieExpired, XianyuClient, XianyuError

        try:
            client = XianyuClient(account["cookie"])
        except XianyuError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        try:
            return await client.list_items()
        except CookieExpired as exc:
            raise HTTPException(status_code=400, detail="Cookie 已失效，请重新扫码登录") from exc
        finally:
            await client.close()

    # -------------------------------------------------------------- 卡密

    @app.get("/api/cards", dependencies=guard)
    async def list_cards(status: str | None = None) -> list[dict[str, Any]]:
        return await database.list_cards(status)

    @app.get("/api/cards/stock", dependencies=guard)
    async def card_stock() -> list[dict[str, Any]]:
        return await database.card_stock()

    @app.post("/api/cards", dependencies=guard)
    async def add_cards(payload: CardsIn) -> dict[str, Any]:
        contents = list(payload.contents)
        if payload.raw_text.strip():
            contents.extend(line for line in payload.raw_text.splitlines() if line.strip())
        if not contents:
            raise HTTPException(status_code=400, detail="没有可导入的卡密")
        added = await database.add_cards(contents, payload.item_id)
        return {"added": added}

    @app.delete("/api/cards/{card_id}", dependencies=guard)
    async def delete_card(card_id: int) -> dict[str, bool]:
        await database.delete_card(card_id)
        return {"ok": True}

    # -------------------------------------------------------------- 记录

    @app.get("/api/orders", dependencies=guard)
    async def list_orders(limit: int = Query(100, ge=1, le=500)) -> list[dict[str, Any]]:
        return await database.list_orders(limit)

    @app.get("/api/messages", dependencies=guard)
    async def list_messages(limit: int = Query(100, ge=1, le=500)) -> list[dict[str, Any]]:
        """消息流水（最新在前）。

        每行多带一个 system 字段：True 表示这是闲鱼的系统提示 / 交易事件
        （「[你已发货]」「快给ta一个评价吧~」「发来一条消息」…），不是买家真人发言。
        前端拿它决定要不要弹「新消息」提醒 —— 判断逻辑留在后端，
        免得那份关键词表在 Python 和 JS 里各维护一份、迟早对不上。
        """
        rows = await database.list_messages(limit)
        for row in rows:
            row["system"] = is_ignored(str(row.get("content") or ""))
        return rows

    @app.get("/api/rate-records", dependencies=guard)
    async def list_rate_records(limit: int = Query(100, ge=1, le=500)) -> list[dict[str, Any]]:
        """自动评价记录（设置页展示最近结果）。"""
        return await database.list_rate_records(limit)

    # ---------------------------------------------------------- 商品模型

    def _item_model_payload(payload: ItemModelIn) -> dict[str, Any]:
        item_id = payload.item_id.strip()
        if not item_id:
            raise HTTPException(status_code=400, detail="商品 ID 不能为空")
        return {
            "item_id": item_id,
            "model_limits": normalize_model_limits(payload.model_limits),
            "remark": payload.remark.strip(),
        }

    def _item_models_out(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        for row in rows:
            models = normalize_model_limits(str(row.get("model_limits") or ""))
            row["models"] = models
            row["unlimited"] = not models
        return rows

    @app.get("/api/item-models", dependencies=guard)
    async def list_item_models() -> list[dict[str, Any]]:
        return _item_models_out(await database.list_item_models())

    @app.post("/api/item-models", dependencies=guard)
    async def create_item_model(payload: ItemModelIn) -> dict[str, Any]:
        data = _item_model_payload(payload)
        try:
            row_id = await database.save_item_model(data)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        logger.info("商品 %s 的模型白名单已保存：%s", data["item_id"], data["model_limits"] or "不限")
        return {"id": row_id}

    @app.put("/api/item-models/{row_id}", dependencies=guard)
    async def update_item_model(row_id: int, payload: ItemModelIn) -> dict[str, Any]:
        if await database.get_item_model(row_id) is None:
            raise HTTPException(status_code=404, detail="这条商品模型配置不存在")
        data = _item_model_payload(payload)
        try:
            await database.save_item_model(data, row_id)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        logger.info("商品 %s 的模型白名单已更新：%s", data["item_id"], data["model_limits"] or "不限")
        return {"id": row_id}

    @app.delete("/api/item-models/{row_id}", dependencies=guard)
    async def delete_item_model(row_id: int) -> dict[str, bool]:
        await database.delete_item_model(row_id)
        return {"ok": True}

    @app.get("/api/item-prices", dependencies=guard)
    async def item_prices(item_id: str = Query(..., min_length=1, max_length=64)) -> dict[str, Any]:
        """这个商品历史上出现过的实付金额（用于配「金额区间」规则）。

        闲鱼订单不返回多规格的 SKU 名称，只给实际成交价；后台用这些价位提示运营。
        """
        rows = await database.item_price_tiers(item_id)
        return {
            "item_id": item_id,
            "prices": [{"amount": float(row["amount"]), "count": int(row["count"])} for row in rows],
        }

    # ---------------------------------------------------------- 买家 Key

    def _newapi_from_request(request: Request) -> tuple[NewApiClient, bool]:
        """优先复用运行中的上游客户端；没有就临时建一个（返回是否要关闭）。

        new-api 没配就直接 400 —— 绑定/校验 Key 本身就要打上游，
        存一个没校验过的 Key 只会在真发货时才炸。
        """
        if not newapi_ready():
            raise HTTPException(
                status_code=400,
                detail="new-api 还没配置，无法绑定 / 校验 Key（检查 .env 里的 XY_NEWAPI_*）",
            )
        provisioner = getattr(getattr(request.app.state, "manager", None), "provisioner", None)
        live = getattr(provisioner, "newapi", None)
        if live is not None:
            return live, False
        client = NewApiClient(
            settings.newapi_base_url,
            access_token=settings.newapi_session,
            username=settings.newapi_username,
            password=settings.newapi_password,
            group=settings.newapi_group,
            quota_per_unit=settings.newapi_quota_per_unit,
            user_id=settings.newapi_user_id,
        )
        return client, True

    async def _resolve_binding(client: NewApiClient, payload: BuyerKeyIn) -> dict[str, Any]:
        """把「令牌 ID 或 Key」解析成一条可落库的绑定。"""
        token_id = payload.token_id.strip()
        api_key = normalize_key(payload.api_key.strip())
        if not token_id and not api_key:
            raise ValueError("令牌 ID 和 Key 至少填一个")
        if token_id:
            token = await client.get_token(token_id)
            # 以服务端读到的 Key 为准：手填的 Key 可能和这个令牌对不上。
            api_key = await client.read_key_quiet(token_id) or api_key
        else:
            token = await client.find_token_by_key(api_key)
            token_id = str(token.get("id") or "").strip()
            if not api_key:
                api_key = await client.read_key_quiet(token_id)
        if not token_id:
            raise ValueError("解析不到令牌 ID")
        try:
            status = int(token.get("status") or 1)
        except (TypeError, ValueError):
            status = 1
        if status == 2:
            raise ValueError("这个令牌已被禁用，先到 new-api 后台启用再绑定")
        if status == 3 or token_expired(token):
            raise ValueError("这个令牌已过期，充值也用不了，请先续期或换一个 Key")
        return {
            "buyer_id": payload.buyer_id.strip(),
            "buyer_name": payload.buyer_name.strip(),
            "token_id": token_id,
            "token_name": str(token.get("name") or ""),
            "api_key": api_key,
            "key_mask": mask_key(api_key),
            "balance": await client.token_balance(token),
            "remark": payload.remark.strip(),
        }

    def _buyer_key_out(row: dict[str, Any]) -> dict[str, Any]:
        item = dict(row)
        # 完整 Key 不往下列：列表只给打码值，真要看去 new-api 后台。
        item.pop("api_key", None)
        item["has_key"] = bool(row.get("api_key"))
        return item

    @app.get("/api/buyer-keys", dependencies=guard)
    async def list_buyer_keys(limit: int = Query(200, ge=1, le=500)) -> list[dict[str, Any]]:
        rows = await database.list_buyer_keys(limit)
        return [_buyer_key_out(row) for row in rows]

    @app.post("/api/buyer-keys", dependencies=guard)
    async def create_buyer_key(payload: BuyerKeyIn, request: Request) -> dict[str, Any]:
        _throttle_outbound(request, "绑定买家 Key")
        client, temporary = _newapi_from_request(request)
        try:
            data = await _resolve_binding(client, payload)
            row_id = await database.save_buyer_key(data)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except UpstreamError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        finally:
            if temporary:
                await client.close()
        logger.info("买家 %s 已绑定令牌 %s（%s）", data["buyer_id"], data["token_id"],
                    data["key_mask"] or "Key 未知")
        return {"id": row_id, "key_mask": data["key_mask"], "balance": data.get("balance")}

    @app.put("/api/buyer-keys/{row_id}", dependencies=guard)
    async def update_buyer_key(row_id: int, payload: BuyerKeyIn, request: Request) -> dict[str, Any]:
        if await database.get_buyer_key_row(row_id) is None:
            raise HTTPException(status_code=404, detail="这条买家 Key 绑定不存在")
        _throttle_outbound(request, "绑定买家 Key")
        client, temporary = _newapi_from_request(request)
        try:
            data = await _resolve_binding(client, payload)
            await database.save_buyer_key(data, row_id)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except UpstreamError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        finally:
            if temporary:
                await client.close()
        logger.info("买家 %s 的 Key 已改为令牌 %s", data["buyer_id"], data["token_id"])
        return {"id": row_id, "key_mask": data["key_mask"], "balance": data.get("balance")}

    @app.delete("/api/buyer-keys/{row_id}", dependencies=guard)
    async def delete_buyer_key(row_id: int) -> dict[str, bool]:
        await database.delete_buyer_key(row_id)
        return {"ok": True}

    @app.post("/api/buyer-keys/{row_id}/refresh", dependencies=guard)
    async def refresh_buyer_key(row_id: int, request: Request) -> dict[str, Any]:
        """重新读一次余额 / 明文 Key（充值后余额会变，页面要能手动刷新）。"""
        row = await database.get_buyer_key_row(row_id)
        if row is None:
            raise HTTPException(status_code=404, detail="这条买家 Key 绑定不存在")
        _throttle_outbound(request, "刷新买家 Key")
        client, temporary = _newapi_from_request(request)
        try:
            token_id = str(row.get("token_id") or "").strip()
            if token_id:
                token = await client.get_token(token_id)
            else:
                token = await client.find_token_by_key(str(row.get("api_key") or ""))
                token_id = str(token.get("id") or "").strip()
            api_key = await client.read_key_quiet(token_id) or str(row.get("api_key") or "")
            balance = await client.token_balance(token)
            await database.snapshot_buyer_key(
                row_id, balance, api_key, mask_key(api_key), str(token.get("name") or "")
            )
        except UpstreamError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        finally:
            if temporary:
                await client.close()
        return {"ok": True, "balance": balance, "key_mask": mask_key(api_key)}


    # ---------------------------------------------------------- 买家账号

    def _buyer_account_out(row: dict[str, Any]) -> dict[str, Any]:
        """列表里不放密码：要看密码走 /secret（后台明确点什么才给）。"""
        item = dict(row)
        item.pop("password", None)
        item["has_password"] = bool(row.get("password"))
        return item

    async def _resolve_account(client: NewApiClient,
                               payload: BuyerAccountIn) -> dict[str, Any]:
        """把后台填的「账号 ID / 账号名」解析成一条可落库的绑定。"""
        user_id = str(payload.user_id or "").strip()
        username = str(payload.username or "").strip()
        if not user_id and not username:
            raise ValueError("账号 ID 和账号名至少填一个")
        user: dict[str, Any] | None
        if user_id:
            user = await client.get_user(user_id)
        else:
            user = await client.find_user(username)
            if user is None:
                raise ValueError("new-api 里没有叫 " + username + " 的账号")
        return {
            "buyer_id": payload.buyer_id.strip(),
            "buyer_name": payload.buyer_name.strip(),
            "user_id": str(user.get("id") or user_id),
            "username": str(user.get("username") or username),
            "password": payload.password.strip(),
            "balance": await client.user_balance(user),
            "remark": payload.remark.strip(),
        }

    @app.get("/api/buyer-accounts", dependencies=guard)
    async def list_buyer_accounts(limit: int = Query(200, ge=1, le=500)) -> list[dict[str, Any]]:
        rows = await database.list_buyer_accounts(limit)
        return [_buyer_account_out(row) for row in rows]

    @app.post("/api/buyer-accounts", dependencies=guard)
    async def create_buyer_account(payload: BuyerAccountIn, request: Request) -> dict[str, Any]:
        _throttle_outbound(request, "绑定买家账号")
        client, temporary = _newapi_from_request(request)
        try:
            data = await _resolve_account(client, payload)
            row_id = await database.save_buyer_account(data)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except UpstreamError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        finally:
            if temporary:
                await client.close()
        logger.info("买家 %s 已绑定账号 %s（id=%s）", data["buyer_id"], data["username"],
                    data["user_id"])
        return {"id": row_id, "username": data["username"], "balance": data.get("balance")}

    @app.put("/api/buyer-accounts/{row_id}", dependencies=guard)
    async def update_buyer_account(row_id: int, payload: BuyerAccountIn,
                                   request: Request) -> dict[str, Any]:
        if await database.get_buyer_account_row(row_id) is None:
            raise HTTPException(status_code=404, detail="这条买家账号绑定不存在")
        _throttle_outbound(request, "绑定买家账号")
        client, temporary = _newapi_from_request(request)
        try:
            data = await _resolve_account(client, payload)
            await database.save_buyer_account(data, row_id)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except UpstreamError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        finally:
            if temporary:
                await client.close()
        return {"id": row_id, "username": data["username"], "balance": data.get("balance")}

    @app.delete("/api/buyer-accounts/{row_id}", dependencies=guard)
    async def delete_buyer_account(row_id: int) -> dict[str, bool]:
        await database.delete_buyer_account(row_id)
        return {"ok": True}

    @app.get("/api/buyer-accounts/{row_id}/secret", dependencies=guard)
    async def read_buyer_account_secret(row_id: int) -> dict[str, Any]:
        """看账号密码 / 卡密：买家把卡密弄丢时，客服要能原样再发一次。

        重置密码那条路走不通 —— new-api 的重置密码接口要 2FA 二次验证，
        机器人拿不到验证码，所以生成时就把密码留下来了。
        """
        row = await database.get_buyer_account_row(row_id)
        if row is None:
            raise HTTPException(status_code=404, detail="这条买家账号绑定不存在")
        return {
            "username": row.get("username") or "",
            "password": row.get("password") or "",
            "card_code": row.get("card_code") or "",
        }

    @app.post("/api/buyer-accounts/{row_id}/refresh", dependencies=guard)
    async def refresh_buyer_account(row_id: int, request: Request) -> dict[str, Any]:
        """重新读一次余额（充值后余额会变，页面要能手动刷新）。"""
        row = await database.get_buyer_account_row(row_id)
        if row is None:
            raise HTTPException(status_code=404, detail="这条买家账号绑定不存在")
        _throttle_outbound(request, "刷新买家账号")
        client, temporary = _newapi_from_request(request)
        try:
            user = await client.get_user(str(row.get("user_id") or ""))
            balance = await client.user_balance(user)
            await database.snapshot_buyer_account(
                row_id, balance, str(user.get("username") or "")
            )
        except UpstreamError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        finally:
            if temporary:
                await client.close()
        return {"ok": True, "balance": balance}

    @app.get("/api/newapi/users", dependencies=guard)
    async def lookup_newapi_user(request: Request, username: str = "") -> list[dict[str, Any]]:
        """按账号名精确查一个人，后台绑定账号时用。"""
        name = str(username or "").strip()
        # 后台是「边打字边查」，一个字符就发一次会让上游很难受
        if len(name) < 2:
            return []
        _throttle_outbound(request, "查 new-api 账号")
        client, temporary = _newapi_from_request(request)
        try:
            user = await client.find_user(name)
            if user is None:
                return []
            return [{
                "user_id": str(user.get("id") or ""),
                "username": str(user.get("username") or ""),
                "balance": await client.user_balance(user),
            }]
        except UpstreamError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        finally:
            if temporary:
                await client.close()

    @app.get("/api/newapi/tokens", dependencies=guard)
    async def list_newapi_tokens(request: Request, keyword: str = "",
                                  limit: int = Query(100, ge=1, le=200)) -> list[dict[str, Any]]:
        """列出 new-api 令牌，方便后台按名字挑一个绑定给买家。"""
        _throttle_outbound(request, "拉 new-api 令牌")
        client, temporary = _newapi_from_request(request)
        try:
            items = await client.list_tokens(page_size=limit)
        except UpstreamError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        finally:
            if temporary:
                await client.close()
        word = keyword.strip().lower()
        out: list[dict[str, Any]] = []
        for item in items:
            token_id = str(item.get("id") or "")
            name = str(item.get("name") or "")
            if word and word not in name.lower() and word not in token_id:
                continue
            out.append({
                "id": token_id,
                "name": name,
                "status": item.get("status"),
                "unlimited_quota": bool(item.get("unlimited_quota")),
                "remain_quota": item.get("remain_quota"),
                "key_mask": mask_key(str(item.get("key") or "")),
                "created_time": item.get("created_time"),
            })
        return out

    # ---------------------------------------------------------- 内置图片

    def _builtin_image_out(row: dict[str, Any]) -> dict[str, Any]:
        item = dict(row)
        item["content_url"] = f"/api/media/images/{row['id']}/content"
        return item

    async def _read_upload(file: UploadFile, max_bytes: int, what: str) -> bytes:
        """分块读取并在超过上限时立刻中止。

        不能先 await file.read() 再 len()：Starlette 虽然会把大文件 spool 到磁盘，
        但 read() 仍会把整个文件读进内存，一个 1 GB 的上传会先吃掉 1 GB RAM
        再返回 413。这里最多读 max_bytes + 一个分块。
        """
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                raise HTTPException(
                    status_code=413,
                    detail=f"{what}太大（上限 {max_bytes // 1024 // 1024} MB）",
                )
            chunks.append(chunk)
        if total == 0:
            raise HTTPException(status_code=400, detail=f"{what}是空文件")
        return b"".join(chunks)

    @app.get("/api/media/images", dependencies=guard)
    async def list_builtin_images() -> list[dict[str, Any]]:
        return [_builtin_image_out(row) for row in await database.list_builtin_images()]

    @app.post("/api/media/images", dependencies=guard)
    async def upload_builtin_image(
        request: Request,
        file: UploadFile = File(...),
        name: str = Form(""),
    ) -> dict[str, Any]:
        """上传一张常用图片到内置图库，聊天时可一键发送。"""
        _throttle_chat_media(request, "上传内置图片")
        data = await _read_upload(file, settings.max_image_mb * 1024 * 1024, "图片")
        try:
            _, mime, width, height = await asyncio.to_thread(media_tools.image_info, data)
        except media_tools.MediaError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        ext = media_tools.extension_for_mime(mime)
        filename = await asyncio.to_thread(
            media_tools.save_media_file, settings.media_dir, "builtin", data, ext
        )
        try:
            row_id = await database.save_builtin_image(
                name=(name or file.filename or "内置图片").strip()[:80],
                filename=filename,
                mime=mime,
                size=len(data),
                width=width,
                height=height,
            )
        except Exception:
            # 数据库写失败就别在磁盘上留孤儿文件
            target = media_tools.local_media_path(settings.media_dir, "builtin", filename)
            target.unlink(missing_ok=True)
            raise
        row = await database.get_builtin_image(row_id)
        if row is None:
            raise HTTPException(status_code=500, detail="图片已保存但读不回记录")
        return {"ok": True, "image": _builtin_image_out(row)}

    @app.get("/api/media/images/{image_id}/content", dependencies=guard)
    async def builtin_image_content(image_id: int) -> Response:
        row = await database.get_builtin_image(image_id)
        if row is None:
            raise HTTPException(status_code=404, detail="图片不存在")
        path = media_tools.local_media_path(settings.media_dir, "builtin", str(row["filename"]))
        if not path.exists():
            raise HTTPException(status_code=404, detail="图片文件已丢失")
        return Response(content=path.read_bytes(), media_type=str(row["mime"] or "image/jpeg"))

    @app.delete("/api/media/images/{image_id}", dependencies=guard)
    async def delete_builtin_image(image_id: int) -> dict[str, bool]:
        row = await database.get_builtin_image(image_id)
        if row is None:
            raise HTTPException(status_code=404, detail="图片不存在")
        await database.delete_builtin_image(image_id)
        try:
            path = media_tools.local_media_path(settings.media_dir, "builtin", str(row["filename"]))
            path.unlink(missing_ok=True)
        except Exception as exc:  # 文件删不掉不影响从图库下架
            logger.warning("删除内置图片文件失败：%s", exc)
        return {"ok": True}

    # -------------------------------------------------------------- 聊天

    @app.get("/api/chats", dependencies=guard)
    async def list_chats(
        account_id: int | None = None,
        limit: int = Query(100, ge=1, le=500),
    ) -> list[dict[str, Any]]:
        """后台聊天界面的会话列表（按最后一条消息倒序）。"""
        return await database.list_chats(account_id=account_id, limit=limit)

    @app.get("/api/chats/{chat_id}/messages", dependencies=guard)
    async def list_chat_messages(
        chat_id: str,
        request: Request,
        account_id: int = Query(..., description="这个会话属于哪个账号"),
        limit: int = Query(100, ge=1, le=500),
        sync: bool = Query(False, description="是否去闲鱼补拉最近历史"),
    ) -> dict[str, Any]:
        """一个会话的消息；sync=true 时先按需补拉一次最近历史。

        补拉失败只记在 sync_error 里、照常返回本地记录 ——
        网络抖一下不该让「聊天」整页打不开。
        """
        sync_error = ""
        if sync:
            _throttle_chat_outbound(request, "同步聊天")
            try:
                await mgr(request).sync_chat(account_id, chat_id, limit=min(limit, 50))
            except (LookupError, XianyuError, ValueError) as exc:
                sync_error = str(exc)
            except Exception as exc:
                logger.warning("同步会话 %s 历史失败：%s", chat_id, exc)
                sync_error = "同步失败，先显示本地记录"
        chat = await database.get_chat(account_id, chat_id)
        messages = await database.list_chat_messages(account_id, chat_id, limit)
        return {"chat": chat, "messages": messages, "sync_error": sync_error}

    @app.post("/api/chats/{chat_id}/send", dependencies=guard)
    async def send_chat_message(chat_id: str, payload: ChatSendIn, request: Request) -> dict[str, Any]:
        """在 web 后台直接回一条闲鱼私信。"""
        text = payload.text.strip()
        if not text:
            raise HTTPException(status_code=400, detail="消息内容不能为空")
        _throttle_chat(request)
        try:
            message = await mgr(request).send_chat_text(
                payload.account_id,
                chat_id,
                payload.to_id,
                text,
                item_id=payload.item_id,
                order_id=payload.order_id,
                buyer_name=payload.buyer_name,
            )
        except LookupError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except XianyuError as exc:
            raise HTTPException(status_code=502, detail=f"发送失败：{exc}") from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"ok": True, "message": message}

    @app.post("/api/chats/{chat_id}/send-image", dependencies=guard)
    async def send_chat_image(chat_id: str, payload: ChatImageIn, request: Request) -> dict[str, Any]:
        """发送内置图库里的一张图片。"""
        _throttle_chat_media(request, "发送图片")
        row = await database.get_builtin_image(payload.image_id)
        if row is None:
            raise HTTPException(status_code=404, detail="内置图片不存在")
        path = media_tools.local_media_path(settings.media_dir, "builtin", str(row["filename"]))
        if not path.exists():
            raise HTTPException(status_code=404, detail="内置图片文件已丢失")
        try:
            message = await mgr(request).send_chat_image(
                payload.account_id,
                chat_id,
                payload.to_id,
                path.read_bytes(),
                item_id=payload.item_id,
                order_id=payload.order_id,
                buyer_name=payload.buyer_name,
            )
        except LookupError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except XianyuError as exc:
            raise HTTPException(status_code=502, detail=f"发送图片失败：{exc}") from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"ok": True, "message": message}

    @app.post("/api/chats/{chat_id}/send-image-file", dependencies=guard)
    async def send_chat_image_file(
        chat_id: str,
        request: Request,
        account_id: int = Form(...),
        to_id: str = Form(""),
        item_id: str = Form(""),
        order_id: str = Form(""),
        buyer_name: str = Form(""),
        file: UploadFile = File(...),
    ) -> dict[str, Any]:
        """上传一张本地图片并直接发到当前会话。"""
        _throttle_chat_media(request, "发送图片")
        data = await _read_upload(file, settings.max_image_mb * 1024 * 1024, "图片")
        try:
            await asyncio.to_thread(media_tools.image_info, data)
        except media_tools.MediaError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        try:
            message = await mgr(request).send_chat_image(
                account_id, chat_id, to_id, data,
                item_id=item_id or None, order_id=order_id or None, buyer_name=buyer_name,
            )
        except LookupError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except XianyuError as exc:
            raise HTTPException(status_code=502, detail=f"发送图片失败：{exc}") from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"ok": True, "message": message}

    @app.post("/api/chats/{chat_id}/send-video", dependencies=guard)
    async def send_chat_video(
        chat_id: str,
        request: Request,
        account_id: int = Form(...),
        to_id: str = Form(""),
        item_id: str = Form(""),
        order_id: str = Form(""),
        buyer_name: str = Form(""),
        file: UploadFile = File(...),
    ) -> dict[str, Any]:
        """上传一段视频并直接发到当前会话（自动截封面、分片上传到闲鱼）。"""
        _throttle_chat_media(request, "发送视频")
        data = await _read_upload(file, settings.max_video_mb * 1024 * 1024, "视频")
        if not media_tools.has_ffmpeg():
            raise HTTPException(
                status_code=503,
                detail="服务器没有安装 ffmpeg/ffprobe，暂时无法发送视频",
            )
        try:
            message = await mgr(request).send_chat_video(
                account_id, chat_id, to_id, data, file.filename or "video.mp4",
                item_id=item_id or None, order_id=order_id or None, buyer_name=buyer_name,
            )
        except LookupError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except XianyuError as exc:
            raise HTTPException(status_code=502, detail=f"发送视频失败：{exc}") from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"ok": True, "message": message}

    @app.post("/api/chats/{chat_id}/recall", dependencies=guard)
    async def recall_chat_message(
        chat_id: str,
        payload: ChatRecallIn,
        request: Request,
    ) -> dict[str, Any]:
        """撤回一条两分钟内发出的消息。"""
        try:
            result = await mgr(request).recall_chat_message(
                payload.account_id, chat_id, payload.message_id
            )
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except XianyuError as exc:
            raise HTTPException(status_code=502, detail=f"撤回失败：{exc}") from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return result

    @app.get("/api/messages/{message_id}/media", dependencies=guard)
    async def message_local_media(
        message_id: int,
        account_id: int = Query(...),
        kind: str = Query("media"),
    ) -> Response:
        """取本工具发出视频的本地预览（视频本体 / 封面）。"""
        row = await database.get_message(message_id)
        if row is None or int(row.get("account_id") or 0) != int(account_id):
            raise HTTPException(status_code=404, detail="消息不存在")
        field = "local_poster" if kind == "poster" else "local_media"
        filename = str(row.get(field) or "")
        if not filename:
            raise HTTPException(status_code=404, detail="这条消息没有本地媒体")
        path = media_tools.local_media_path(settings.media_dir, "chat", filename)
        if not path.exists():
            raise HTTPException(status_code=404, detail="本地媒体文件已丢失")
        mime = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        return Response(content=path.read_bytes(), media_type=mime)

    @app.post("/api/chats/{chat_id}/read", dependencies=guard)
    async def read_chat(chat_id: str, account_id: int = Query(...)) -> dict[str, Any]:
        """把这个会话标成已读（清掉会话列表的红点）。"""
        changed = await database.mark_chat_read(account_id, chat_id)
        return {"ok": True, "read": changed}

    @app.post("/api/chats/open", dependencies=guard)
    async def open_chat(payload: ChatOpenIn, request: Request) -> dict[str, Any]:
        """从订单 / 买家打开（必要时创建）一个闲鱼会话。"""
        _throttle_chat_outbound(request, "打开会话")
        try:
            chat_id = await mgr(request).open_chat(
                payload.account_id,
                payload.buyer_id,
                item_id=payload.item_id,
                order_id=payload.order_id,
            )
        except LookupError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except XianyuError as exc:
            raise HTTPException(status_code=502, detail=f"打开会话失败：{exc}") from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        chat = await database.get_chat(payload.account_id, chat_id)
        return {
            "chat_id": chat_id,
            "chat": chat,
            "account_id": payload.account_id,
            "buyer_id": payload.buyer_id,
            "buyer_name": payload.buyer_name,
            "item_id": payload.item_id,
            "order_id": payload.order_id,
        }

    # -------------------------------------------------------------- 设置

    async def _model_limits_state() -> dict[str, Any]:
        """模型白名单的当前生效值 + 它从哪来（后台设置页 / .env / 内置默认）。"""
        overrides = await database.get_all_config()
        value = resolved_model_limits(overrides)
        if MODEL_LIMITS_KEY in overrides:
            source = "web"
        elif settings.newapi_model_limits:
            source = "env"
        else:
            source = "default"
        return {
            "value": value,
            "normalized": normalize_model_limits(value),
            "unlimited": not normalize_model_limits(value),
            "source": source,
        }

    async def _auto_rate_state() -> dict[str, Any]:
        """自动评价的当前生效值 + 来源。"""
        overrides = await database.get_all_config()
        enabled, feedback = resolved_auto_rate(overrides)
        if AUTO_RATE_ENABLED_KEY in overrides or AUTO_RATE_FEEDBACK_KEY in overrides:
            source = "web"
        elif settings.auto_rate_enabled:
            source = "env"
        else:
            source = "default"
        return {
            "enabled": enabled,
            "feedback": feedback,
            "source": source,
            "default_feedback": DEFAULT_AUTO_RATE_FEEDBACK,
            "poll_interval": settings.auto_rate_poll_interval,
        }

    async def _site_currency() -> str:
        """站点展示货币（USD / CNY / CUSTOM）。读不到就当 USD，保持老行为。

        结果缓存 60 秒：前端每 5 秒刷新一次，而每次刷新都打一遍 new-api 的
        /api/status 既浪费又把页面拖慢（上游慢的时候单个请求能等 10 秒）。
        """
        key = settings.newapi_base_url or "-"
        cached = _SITE_CURRENCY_CACHE.get(key)
        if cached is not None and time.monotonic() - cached[0] < _SITE_CURRENCY_TTL:
            return cached[1]
        try:
            from .upstream import NewApiClient

            client = NewApiClient(
                settings.newapi_base_url,
                access_token=settings.newapi_session,
                username=settings.newapi_username,
                password=settings.newapi_password,
                user_id=settings.newapi_user_id,
            )
            try:
                model = await client.site_quota_model()
                currency = model.currency
            finally:
                await client.close()
        except Exception:
            # 纯展示用：取不到不该让整个 /api/config 失败
            logger.debug("读取站点货币失败，按 USD 展示", exc_info=True)
            currency = "USD"
        _SITE_CURRENCY_CACHE[key] = (time.monotonic(), currency)
        return currency

    @app.get("/api/config/settings", dependencies=guard)
    async def show_settings() -> dict[str, Any]:
        """后台「设置」页的初始值。"""
        return {
            "model_limits": await _model_limits_state(),
            "auto_rate": await _auto_rate_state(),
        }

    @app.put("/api/config/settings", dependencies=guard)
    async def save_settings(payload: SettingsIn, request: Request) -> dict[str, Any]:
        """保存模型白名单。保存后立刻对新订单生效，不用重启、不用改 .env。

        已经发出去的旧 Key 不受影响 —— 它们是建令牌时写进 new-api 的，
        要收紧只能去 new-api 后台单个改（这一点前端有提示）。
        """
        # model_limits=None 表示「这次只改别的字段」，不能把全局白名单顺手清空。
        if payload.model_limits is not None:
            normalized = normalize_model_limits(payload.model_limits)
            await database.set_config(MODEL_LIMITS_KEY, normalized)
            # 光写库 ≠ 生效：Provisioner 是在进程启动时按当时的配置构造的，
            # 不回推给存活的对象，后台「保存即生效」就是假的（得重启才生效）。
            provisioner = getattr(getattr(request.app.state, "manager", None), "provisioner", None)
            live_newapi = getattr(provisioner, "newapi", None)
            if live_newapi is not None:
                live_newapi.model_limits = normalized
            if not normalized:
                logger.warning("后台把模型白名单清空了：之后新建的 Key 将不限制模型")
            else:
                logger.info("后台更新模型白名单：%s（已同步到运行中的上游客户端）", normalized)
        if payload.auto_rate_enabled is not None:
            await database.set_config(AUTO_RATE_ENABLED_KEY, "1" if payload.auto_rate_enabled else "0")
            logger.info("后台%s自动评价", "开启" if payload.auto_rate_enabled else "关闭")
        if payload.auto_rate_feedback is not None:
            await database.set_config(AUTO_RATE_FEEDBACK_KEY, payload.auto_rate_feedback.strip())
        return {
            "ok": True,
            "model_limits": await _model_limits_state(),
            "auto_rate": await _auto_rate_state(),
        }

    @app.get("/api/config", dependencies=guard)
    async def show_config() -> dict[str, Any]:
        """前端用来提示「上游是否已配好」，不回显任何密钥。"""
        limits = await _model_limits_state()
        auto_rate = await _auto_rate_state()
        return {
            "dry_run": settings.dry_run,
            "auto_reply": {
                "enabled": settings.auto_reply_enabled,
                "delay": settings.auto_reply_delay,
            },
            "auto_rate": auto_rate,
            "cny_per_usd": settings.cny_per_usd,
            "max_order_cny": settings.max_order_cny,
            "newapi": {
                "base_url": settings.newapi_base_url,
                "has_credential": bool(settings.newapi_session or (settings.newapi_username and settings.newapi_password)),
                "group": settings.newapi_group,
                # 站点计价货币：CNY 时「额度」是人民币，前端别一律写 $
                "currency": await _site_currency(),
                # 生效值（后台设置优先，其次 .env），前端横幅直接用它
                "model_limits": limits["value"],
                "model_limits_source": limits["source"],
                "model_limits_unlimited": limits["unlimited"],
            },
            "cardshop": {
                "base_url": settings.cardshop_base_url,
                "has_token": bool(settings.cardshop_admin_token),
                "has_password_login": bool(settings.cardshop_username and settings.cardshop_password),
            },
            "newapi_user_id_set": bool(settings.newapi_user_id),
            # 「建 Key 出卡密」要两个上游都齐；「充值到已有 Key」只要有 new-api
            "upstream_ready": upstream_ready(),
            "newapi_ready": newapi_ready(),
            "cardshop_ready": cardshop_ready(),
        }

    @app.get("/api/status", dependencies=guard)
    async def worker_status(request: Request) -> list[dict[str, Any]]:
        return mgr(request).status()

    @app.exception_handler(Exception)
    async def unhandled(request: Request, exc: Exception) -> JSONResponse:  # pragma: no cover
        """兜底 500。

        以前这里把 str(exc) 直接回给前端 —— 上游报错里可能带着 URL、
        SQL、甚至凭据片段，等于把内部细节交给任何一个打开页面的人。
        现在完整堆栈只进日志，前端只拿到一个可以拿去 grep 的编号。
        """
        request_id = uuid.uuid4().hex[:12]
        logger.exception("未处理异常（request_id=%s）%s %s: %s",
                         request_id, request.method, request.url.path, exc)
        return JSONResponse(
            status_code=500,
            content={"detail": f"服务内部错误（request_id={request_id}），请查看服务端日志"},
        )

    return app
