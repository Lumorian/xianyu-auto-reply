"""Web 管理后台：FastAPI 路由 + 极简 HTML 面板。"""

from __future__ import annotations

import asyncio
import hmac
import ipaddress
import logging
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from pydantic import BaseModel, Field

from .autoreply import KINDS, MATCH_TYPES
from .config import MODEL_LIMITS_KEY, resolved_model_limits, settings
from .db import Database
from .qrlogin import STATUS_TEXT, LoginError, QrLoginManager, render_qr_png
from .runner import WorkerManager
from .upstream import normalize_model_limits

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
    mode: str = Field(default="text", pattern="^(text|card|upstream)$")
    content: str = ""
    auto_confirm: bool = False
    priority: int = 0
    remark: str = ""
    #: 上游模式下：额度 = 订单金额 / cny_per_usd * usd_multiplier（fixed_usd > 0 时直接用）
    usd_multiplier: float = 1.0
    fixed_usd: float = 0.0


class ReplyRuleIn(BaseModel):
    """一条自动回复规则。kind=item 是商品自动回复，kind=keyword 是关键词自动回复。"""

    name: str = Field(min_length=1, max_length=64)
    kind: str = Field(default="keyword", pattern="^(" + "|".join(KINDS) + ")$")
    item_id: str = "*"
    #: 关键词（逗号 / 分号 / 竖线 / 换行分隔），kind=keyword 时必填
    keywords: str = ""
    #: contains 包含 / exact 完全一致 / regex 正则
    match_type: str = Field(default="contains", pattern="^(" + "|".join(MATCH_TYPES) + ")$")
    content: str = ""
    enabled: bool = True
    #: 同一个会话只回一次（否则买家每发一句都会被回一遍）
    once_per_chat: bool = True
    #: 同一买家多少秒内不再回同一条规则；0 = 不限制
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
    model_limits: str = Field(default="", max_length=2000)


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
    database = db or Database(settings.db_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await database.init()
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
            "default-src 'self'; img-src 'self' blob: data:; style-src 'self' 'unsafe-inline'; "
            "script-src 'self' 'unsafe-inline'; connect-src 'self'; "
            "frame-ancestors 'none'; base-uri 'none'",
        )
        if request.url.path.startswith("/api/"):
            # 卡密、Cookie、买家昵称都在这些响应里，别留在浏览器磁盘缓存
            response.headers.setdefault("Cache-Control", "no-store")
        return response
    #: session_id -> 用户填的账号名/备注，扫码成功后用来建账号
    _pending: dict[str, LoginIn] = {}

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
        for account in accounts:
            account["cookie"] = "***" if account["cookie"] else ""
        return accounts

    @app.post("/api/accounts", dependencies=guard)
    async def save_account(payload: AccountIn, request: Request) -> dict[str, Any]:
        cookie = payload.cookie.strip()
        if not cookie or cookie == "***":
            # 只改备注/开关时前端拿不到原 Cookie，沿用数据库里的旧值
            existing = await database.fetch_one(
                "SELECT cookie FROM accounts WHERE name = ?", (payload.name,)
            )
            cookie = (existing or {}).get("cookie", "")
        account_id = await database.upsert_account(
            payload.name, cookie, payload.remark, payload.enabled
        )
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
            await qr.cancel(session_id)
            await mgr(request).sync()
            result.update({"account_id": account_id, "nick": session.nick, "unb": session.unb})
            logger.info("扫码登录成功，账号 %s 已保存（id=%s）", name, account_id)

        return result

    # -------------------------------------------------------------- 规则

    @app.get("/api/rules", dependencies=guard)
    async def list_rules() -> list[dict[str, Any]]:
        return await database.list_rules()

    @app.post("/api/rules", dependencies=guard)
    async def create_rule(payload: RuleIn) -> dict[str, Any]:
        rule_id = await database.save_rule(payload.model_dump())
        return {"id": rule_id}

    @app.put("/api/rules/{rule_id}", dependencies=guard)
    async def update_rule(rule_id: int, payload: RuleIn) -> dict[str, Any]:
        if await database.get_rule(rule_id) is None:
            raise HTTPException(status_code=404, detail="规则不存在")
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

    def _check_reply_payload(payload: ReplyRuleIn) -> dict[str, Any]:
        data = payload.model_dump()
        if payload.kind == "keyword" and not payload.keywords.strip():
            raise HTTPException(status_code=400, detail="关键词自动回复必须填关键词")
        if not payload.content.strip():
            raise HTTPException(status_code=400, detail="回复内容不能为空，否则买家会收到空消息")
        return data

    @app.post("/api/reply-rules", dependencies=guard)
    async def create_reply_rule(payload: ReplyRuleIn) -> dict[str, Any]:
        rule_id = await database.save_reply_rule(_check_reply_payload(payload))
        return {"id": rule_id}

    @app.put("/api/reply-rules/{rule_id}", dependencies=guard)
    async def update_reply_rule(rule_id: int, payload: ReplyRuleIn) -> dict[str, Any]:
        if await database.get_reply_rule(rule_id) is None:
            raise HTTPException(status_code=404, detail="自动回复规则不存在")
        await database.save_reply_rule(_check_reply_payload(payload), rule_id)
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
        return await database.list_messages(limit)

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
        return {"model_limits": await _model_limits_state()}

    @app.put("/api/config/settings", dependencies=guard)
    async def save_settings(payload: SettingsIn, request: Request) -> dict[str, Any]:
        """保存模型白名单。保存后立刻对新订单生效，不用重启、不用改 .env。

        已经发出去的旧 Key 不受影响 —— 它们是建令牌时写进 new-api 的，
        要收紧只能去 new-api 后台单个改（这一点前端有提示）。
        """
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
        return {"ok": True, "model_limits": await _model_limits_state()}

    @app.get("/api/config", dependencies=guard)
    async def show_config() -> dict[str, Any]:
        """前端用来提示「上游是否已配好」，不回显任何密钥。"""
        from .config import upstream_ready

        limits = await _model_limits_state()
        return {
            "dry_run": settings.dry_run,
            "auto_reply": {
                "enabled": settings.auto_reply_enabled,
                "delay": settings.auto_reply_delay,
            },
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
            "upstream_ready": upstream_ready(),
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
