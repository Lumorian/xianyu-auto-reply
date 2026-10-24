"""Web 管理后台：FastAPI 路由 + 极简 HTML 面板。"""

from __future__ import annotations

import hmac
import logging
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Request
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


def create_app(db: Database | None = None, manager: WorkerManager | None = None) -> FastAPI:
    database = db or Database(settings.db_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await database.init()
        mgr = manager or WorkerManager(database)
        app.state.manager = mgr
        await mgr.sync()
        try:
            yield
        finally:
            await mgr.stop_all()
            await app.state.qr.close()

    app = FastAPI(title="闲鱼自动发货工具", version="0.1.0", lifespan=lifespan)
    app.state.db = database
    app.state.qr = QrLoginManager()
    #: session_id -> 用户填的账号名/备注，扫码成功后用来建账号
    _pending: dict[str, LoginIn] = {}

    def _qr(request: Request) -> QrLoginManager:
        return request.app.state.qr

    def _is_loopback(request: Request) -> bool:
        host = (request.client.host if request.client else "") or ""
        return host in ("127.0.0.1", "::1", "localhost") or host.startswith("127.")

    async def require_admin(
        request: Request,
        x_admin_token: str | None = Header(default=None),
        authorization: str | None = Header(default=None),
    ) -> None:
        """管理接口的鉴权。

        行为：
          * 配了 XY_ADMIN_TOKEN → 必须带对的口令（X-Admin-Token 头，
            或 Authorization: Bearer <口令>）。
          * 没配口令 → 只放行本机来源；非本机一律 401，
            除非显式打开 XY_ADMIN_ALLOW_REMOTE。

        为什么默认要拦：这些接口能改账号 Cookie、读买家昵称、导出卡密、
        删规则。以前「没配口令 = 谁都能调」，只要放到反代后面就是一扇敞开的门。
        """
        token = x_admin_token or ""
        if not token and authorization and authorization.lower().startswith("bearer "):
            token = authorization[7:].strip()

        expected = settings.admin_token
        if expected:
            # 恒定时间比较：普通 != 会按「第几个字符不同」泄漏时间差
            if not hmac.compare_digest(token, expected):
                raise HTTPException(status_code=401, detail="管理口令错误")
            return

        if settings.admin_allow_remote or _is_loopback(request):
            return
        raise HTTPException(
            status_code=401,
            detail="未配置 XY_ADMIN_TOKEN，管理接口只允许本机访问；"
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
    async def list_account_items(account_id: int) -> list[dict[str, Any]]:
        """拉这个账号「在售」的商品，方便在规则里选商品 ID 而不是手抄。"""
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
    async def list_orders(limit: int = 100) -> list[dict[str, Any]]:
        return await database.list_orders(limit)

    @app.get("/api/messages", dependencies=guard)
    async def list_messages(limit: int = 100) -> list[dict[str, Any]]:
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
        """站点展示货币（USD / CNY / CUSTOM）。读不到就当 USD，保持老行为。"""
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
                return model.currency
            finally:
                await client.close()
        except Exception:
            # 纯展示用：取不到不该让整个 /api/config 失败
            logger.debug("读取站点货币失败，按 USD 展示", exc_info=True)
            return "USD"

    @app.get("/api/config/settings", dependencies=guard)
    async def show_settings() -> dict[str, Any]:
        """后台「设置」页的初始值。"""
        return {"model_limits": await _model_limits_state()}

    @app.put("/api/config/settings", dependencies=guard)
    async def save_settings(payload: SettingsIn) -> dict[str, Any]:
        """保存模型白名单。保存后立刻对新订单生效，不用重启、不用改 .env。

        已经发出去的旧 Key 不受影响 —— 它们是建令牌时写进 new-api 的，
        要收紧只能去 new-api 后台单个改（这一点前端有提示）。
        """
        normalized = normalize_model_limits(payload.model_limits)
        await database.set_config(MODEL_LIMITS_KEY, normalized)
        if not normalized:
            logger.warning("后台把模型白名单清空了：之后新建的 Key 将不限制模型")
        else:
            logger.info("后台更新模型白名单：%s", normalized)
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
