"""闲鱼网页端（goofish.com）接口封装。

包含两块能力：

* mtop HTTP 接口：换取 IM token、刷新 token、拉取商品详情；
* WebSocket 长连接：接收私信推送、发送文本消息。

协议细节来自对网页端抓包（sign = md5(token&t&appKey&data)、
LWP 帧格式、msgpack+base64 的 custom 内容）。
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import re
import time
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
import websockets

from .codec import buyer_nick_from_session, pick_display_name
from .config import settings
from .sign import (
    generate_device_id,
    generate_mid,
    generate_sign,
    generate_uuid,
    h5_token,
    now_ms,
    parse_cookies,
)

logger = logging.getLogger(__name__)

#: WebSocket 连接对象的类型。
#: websockets 13+ 里 WebSocketClientProtocol 已是废弃别名（每次引用都发
#: DeprecationWarning），正式名字是 ClientConnection；这里收口一处，
#: 将来再改名只动这一行。
WebSocket = getattr(websockets, 'ClientConnection', None) or websockets.WebSocketClientProtocol

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
)

#: 闲鱼 IM 长连接使用的客户端标识（网页端固定值）
IM_UA = UA + " DingTalk(2.1.5) OS(Windows/10) Browser(Chrome/133.0.0.0) DingWeb/2.1.5 IMPaaS DingWeb/2.1.5"

#: 换取 IM token 的接口；_m_h5_tk 刷新接口（参考 xianyu-pilot / Java refreshMH5Tk）
TOKEN_API = "mtop.taobao.idlemessage.pc.login.token"
REFRESH_MH5TK_API = "mtop.gaia.nodejs.gaia.idle.data.gw.v2.index.get"

#: accessToken 进程内缓存：unb -> (token, _m_h5_tk, expire_at)。
#: 重连时不必每次都打 token 接口，既快也少一次触发风控的机会。
_IM_TOKEN_CACHE: dict[str, tuple[str, str, str, float]] = {}


def _as_dict_local(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _b64_json_local(value: Any) -> dict[str, Any]:
    """base64 的 JSON 字符串 -> dict；解不开就空字典。"""
    if not isinstance(value, str) or not value:
        return {}
    try:
        raw = base64.b64decode(value + "=" * (-len(value) % 4))
    except (ValueError, TypeError):
        return {}
    try:
        obj = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {}
    return obj if isinstance(obj, dict) else {}


def _url_param(url: str, name: str) -> str:
    """从 fleamarket://...?a=1&b=2 这种伪 URL 里取参数。"""
    if not url or name + "=" not in url:
        return ""
    tail = url.split(name + "=", 1)[1]
    for sep in ("&", "#"):
        tail = tail.split(sep, 1)[0]
    return tail.strip()


def _dedupe_lines_local(texts: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for text in texts:
        text = text.strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


_ORDER_ID_IN_TEXT = (
    re.compile(r"order_detail\?[^\s\"']*?id=(\d+)"),
    re.compile(r"[?&]orderId=(\d+)"),
    re.compile(r"\"orderId\"\s*:\s*\"?(\d+)"),
    re.compile(r"\"bizOrderId\"\s*:\s*\"?(\d+)"),
)


def _find_order_id(text: str) -> str:
    """在一段文本/JSON 里找订单号（会话列表的正文里偶尔会带）。"""
    if not text:
        return ""
    for pattern in _ORDER_ID_IN_TEXT:
        match = pattern.search(text)
        if match:
            return match.group(1)
    return ""


def _ret_text(payload: dict[str, Any]) -> str:
    ret = payload.get("ret")
    if isinstance(ret, list):
        return " ".join(str(item) for item in ret)
    return str(ret or "")


def _is_token_expired(ret: str) -> bool:
    upper = ret.upper()
    return (
        "令牌过期" in ret
        or "令牌为空" in ret
        or "FAIL_SYS_TOKEN_EXOIRED" in upper  # 上游拼写就是 EXOIRED
        or "FAIL_SYS_TOKEN_EXPIRED" in upper
        or "FAIL_SYS_TOKEN_EMPTY" in upper
    )


def _is_risk_control(ret: str) -> bool:
    upper = ret.upper()
    return any(
        marker in upper or marker in ret
        for marker in (
            "FAIL_SYS_USER_VALIDATE",
            "RGV587",
            "被挤爆",
            "FAIL_SYS_TRAFFIC_LIMIT",
            "FAIL_SYS_ILLEGAL_ACCESS",
            "CAPTCHA",
        )
    )


def _is_session_expired(ret: str) -> bool:
    upper = ret.upper()
    return (
        "FAIL_SYS_SESSION_EXPIRED" in upper
        or "SESSION_EXPIRED" in upper
        or "USER_NOT_LOGIN" in upper
        or "请登录" in ret
        or "登录已失效" in ret
    )


def _h5_headers() -> dict[str, str]:
    """浏览器风格的 mtop 请求头。

    参考 xianyu-pilot 的关键结论：token 接口缺少 Origin / Referer 会被
    闲鱼风控判定为非浏览器请求，直接返回 FAIL_SYS_USER_VALIDATE。
    """
    return {
        "User-Agent": UA,
        "Accept": "application/json",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://www.goofish.com",
        "Referer": "https://www.goofish.com/",
        "sec-ch-ua": '"Chromium";v="138", "Google Chrome";v="138", "Not?A_Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-site": "same-site",
        "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "priority": "u=1, i",
    }


def _pick_order(response: dict[str, Any], order_id: str) -> dict[str, Any] | None:
    """在订单列表响应里找到指定订单，找不到返回 None。"""
    module = ((response.get("data") or {}).get("module")) or {}
    items = module.get("items")
    if not isinstance(items, list):
        return None
    for item in items:
        if not isinstance(item, dict):
            continue
        common = item.get("commonData") if isinstance(item.get("commonData"), dict) else {}
        if str(common.get("orderId", "")) == str(order_id):
            return item
    # 只查一单且返回就一条时，直接认它（部分接口回包不带 orderId）
    if len(items) == 1 and isinstance(items[0], dict) and "priceVO" in items[0]:
        return items[0]
    return None


class XianyuError(RuntimeError):
    """闲鱼接口调用失败。"""


class CookieExpired(XianyuError):
    """Cookie 真的失效了，需要用户重新扫码登录。

    注意：只有 FAIL_SYS_SESSION_EXPIRED / 请登录 这类明确信号才算。
    FAIL_SYS_USER_VALIDATE / RGV587 是滑块/风控，不能当成 Cookie 失效。
    """


class RiskControl(XianyuError):
    """闲鱼风控 / 滑块验证（FAIL_SYS_USER_VALIDATE、RGV587）。

    这是**临时性**错误：请求头不像浏览器、设备指纹变化、请求过于频繁
    都会触发。正确做法是退避后重试，而不是把账号标成 Cookie 失效。
    """


class TokenExpired(XianyuError):
    """_m_h5_tk 刷新失败 / token 接口仍然返回令牌过期。"""


class XianyuClient:
    """单个闲鱼账号的 HTTP + WebSocket 客户端。"""

    def __init__(
        self,
        cookie_str: str,
        *,
        http_client: httpx.AsyncClient | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        cookies = parse_cookies(cookie_str)
        if "unb" not in cookies:
            raise XianyuError("Cookie 中缺少 unb 字段，请确认复制的是登录后的完整 Cookie")
        self.cookie_str = cookie_str
        self.cookies = cookies
        self.user_id = cookies["unb"]
        #: 与账号绑定的固定设备指纹（不能每次重连都变）
        self.device_id = generate_device_id(self.user_id)
        self.websocket: WebSocket | None = None
        self.sid = ""
        #: 最近一次收到服务端帧的时间（monotonic），用于发现「假在线」的死连接
        self.last_inbound_at = time.monotonic()
        #: Cookie 被刷新后通知上层写回数据库（由 AccountWorker 注入）
        self.on_cookie_refresh: Callable[[str], Awaitable[None]] | None = None
        #: 心跳任务（register 之前就要跑起来，见 connect）
        self._heartbeat_task: asyncio.Task[None] | None = None
        #: ackDiff 周期重发任务（增量推送靠它触发）
        self._sync_poll_task: asyncio.Task[None] | None = None
        #: mid -> Future。create_chat 之类「发出去要等回包」的请求靠它拿结果。
        #: 不能直接再 recv：websockets 不允许两个协程同时 recv。
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        #: 查订单时「哪种请求参数能查到」的记忆。三种写法里通常只有一种有效，
        #: 记住它就不用每次都先撞两堵墙（每次失败都是一次完整往返）。
        self._order_query_variant = 0
        self._http = http_client
        self._transport = transport

    # ------------------------------------------------------------ HTTP

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(
                # 长连接复用 + 更短的超时：发货链路上每个请求等 20 秒没有意义，
                # 卡住时宁可早点失败早点重试
                timeout=httpx.Timeout(10.0, connect=5.0),
                headers={**_h5_headers(), "cookie": self.cookie_str},
                follow_redirects=True,
                limits=httpx.Limits(max_keepalive_connections=10, keepalive_expiry=90.0),
                transport=self._transport,
            )
        return self._http

    async def close(self) -> None:
        await self.stop_sync_poll()
        await self.stop_heartbeat()
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    def _merge_set_cookies(self, response: httpx.Response) -> None:
        """把响应里的 Set-Cookie 合并进当前 Cookie（不依赖 httpx 的域名 jar）。"""
        for raw in response.headers.get_list("set-cookie"):
            pair = raw.split(";", 1)[0].strip()
            if not pair or "=" not in pair:
                continue
            name, value = pair.split("=", 1)
            name, value = name.strip(), value.strip()
            if name and value:
                self.cookies[name] = value
        self.cookie_str = "; ".join(f"{k}={v}" for k, v in self.cookies.items())
        if self._http is not None:
            # 下一次 mtop 必须带刷新后的 Cookie，否则签名用的 token 和 Cookie 对不上
            self._http.headers["cookie"] = self.cookie_str

    async def _notify_cookie_refresh(self) -> None:
        """Cookie 变了就通知上层写回数据库；失败只记日志，不影响连接。"""
        callback = self.on_cookie_refresh
        if callback is None:
            return
        try:
            await callback(self.cookie_str)
        except Exception as exc:
            logger.warning("保存刷新后的 Cookie 失败：%s", exc)

    async def _post_refresh(self, client: httpx.AsyncClient, token: str, data_str: str) -> httpx.Response:
        t = now_ms()
        params = {
            "jsv": "2.7.2",
            "appKey": settings.app_key,
            "t": t,
            "sign": generate_sign(t, token, data_str),
            "v": "1.0",
            "type": "originaljson",
            "dataType": "json",
            "timeout": "20000",
            "api": REFRESH_MH5TK_API,
        }
        return await client.post(
            f"{settings.mtop_base}/{REFRESH_MH5TK_API}/1.0/",
            params=params,
            data={"data": data_str},
            headers={"content-type": "application/x-www-form-urlencoded"},
        )

    async def refresh_m_h5_tk(self) -> bool:
        """按官方 H5 的三步流程刷新 _m_h5_tk，成功返回 True。

        老实现只 GET 一次首页，服务端根本不会下发新的 _m_h5_tk，
        于是「刷新」完还是旧 token，mtop 永远报令牌过期。
        参考 xianyu-pilot / Java refreshMH5Tk：
          1. GET 接口，恢复会话 Cookie；
          2. 用空 token 签名 POST，触发服务端下发新 _m_h5_tk；
          3. 用新 token 再 POST 一次，激活令牌。
        """
        old_tk = self.cookies.get("_m_h5_tk", "")
        data_str = json.dumps({"bizScene": "home"}, separators=(",", ":"))
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(15.0, connect=8.0),
                headers={**_h5_headers(), "Cookie": self.cookie_str},
                follow_redirects=True,
                transport=self._transport,
            ) as client:
                # Step 1：GET 恢复会话（cookie2 / cna 等）
                get_resp = await client.get(f"{settings.mtop_base}/{REFRESH_MH5TK_API}/1.0/")
                self._merge_set_cookies(get_resp)
                client.headers["Cookie"] = self.cookie_str

                # Step 2：空 token POST，服务端会返回「令牌为空」并在 Set-Cookie 里发新 token
                post_resp = await self._post_refresh(client, "", data_str)
                self._merge_set_cookies(post_resp)
                client.headers["Cookie"] = self.cookie_str
                new_tk = self.cookies.get("_m_h5_tk", "")
                if not new_tk or new_tk == old_tk:
                    logger.warning("刷新 _m_h5_tk 失败：服务端没有下发新的令牌")
                    return False

                # Step 3：真实 token POST 激活
                token = new_tk.split("_")[0] if "_" in new_tk else new_tk
                activate_resp = await self._post_refresh(client, token, data_str)
                self._merge_set_cookies(activate_resp)
                final_tk = self.cookies.get("_m_h5_tk", "")
                if not final_tk or final_tk == old_tk:
                    logger.warning("刷新 _m_h5_tk 失败：激活后令牌没有变化")
                    return False
        except httpx.HTTPError as exc:
            logger.warning("刷新 _m_h5_tk 网络异常：%s", exc)
            return False

        logger.info("已刷新 _m_h5_tk（账号 %s）", self.user_id)
        await self._notify_cookie_refresh()
        return True

    async def mtop(self, api: str, data: dict[str, Any], version: str = "1.0") -> dict[str, Any]:
        """调用 mtop 接口，自动签名并在令牌过期时刷新 _m_h5_tk 后重试。"""
        client = await self._client()
        refreshed = False
        last_ret = ""
        for _attempt in range(3):
            t = now_ms()
            token = h5_token(self.cookie_str)
            data_str = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
            params = {
                "jsv": "2.7.2",
                "appKey": settings.app_key,
                "t": t,
                "sign": generate_sign(t, token, data_str),
                "v": version,
                "type": "originaljson",
                "accountSite": "xianyu",
                "dataType": "json",
                "timeout": "20000",
                "api": api,
                "sessionOption": "AutoLoginOnly",
                "spm_cnt": "a21ybx.im.0.0",
            }
            if api == TOKEN_API:
                # 模拟从首页侧边栏点进消息的真实路径，降低风控概率（xianyu-pilot 同款）
                params["spm_pre"] = "a21ybx.home.sidebar.1.4c053da6vYwnmf"
                params["log_id"] = "4c053da6vYwnmf"
            resp = await client.post(
                f"{settings.mtop_base}/{api}/{version}/",
                params=params,
                data={"data": data_str},
                headers={"content-type": "application/x-www-form-urlencoded"},
            )
            try:
                payload = resp.json()
            except ValueError as exc:  # pragma: no cover - 网络异常分支
                raise XianyuError(f"{api} 返回非 JSON: {resp.status_code}") from exc

            ret = _ret_text(payload)
            last_ret = ret
            if _is_token_expired(ret):
                if refreshed:
                    raise TokenExpired(f"{api} 刷新 _m_h5_tk 后仍然令牌过期: {ret}")
                refreshed = True
                if not await self.refresh_m_h5_tk():
                    raise TokenExpired(f"{api} 令牌过期，且刷新 _m_h5_tk 失败: {ret}")
                # refresh 之后 cookie 变了，后续重试必须带新 Cookie
                client.headers["cookie"] = self.cookie_str
                continue
            if _is_risk_control(ret):
                self.invalidate_im_token()
                raise RiskControl(f"{api} 触发闲鱼风控: {ret}")
            if _is_session_expired(ret):
                self.invalidate_im_token()
                raise CookieExpired(f"{api} 登录态失效: {ret}")
            return payload
        raise TokenExpired(f"{api} token 刷新后仍然失败: {last_ret}")

    async def keepalive(self) -> bool:
        """调用 passport hasLogin.do 保活 Cookie。

        闲鱼登录态并不只是「有 Cookie 就一直在」：长期不访问会逐渐失效。
        参考 xianyu-pilot，每 30 分钟做一次 hasLogin，让服务端续期并回写
        Set-Cookie（包括新的 _m_h5_tk）。
        """
        if not self.cookies.get("unb"):
            return False
        cookie_dict = dict(self.cookies)
        form_data = {
            "appName": "xianyu",
            "fromSite": "77",
            "hid": self.user_id,
            "ltl": "true",
            "appEntrance": "web",
            "_csrf_token": cookie_dict.get("XSRF-TOKEN", ""),
            "umidToken": "",
            "hsiz": cookie_dict.get("cookie2", ""),
            "bizParams": "taobaoBizLoginFrom=web",
            "mainPage": "false",
            "isMobile": "false",
            "lang": "zh_CN",
            "returnUrl": "",
            "isIframe": "true",
            "documentReferer": "https://www.goofish.com/",
            "defaultView": "hasLogin",
            "umidTag": "SERVER",
            "deviceId": cookie_dict.get("cna", ""),
        }
        headers = {
            "User-Agent": UA,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Referer": "https://passport.goofish.com/",
            "Origin": "https://passport.goofish.com",
            "Cookie": self.cookie_str,
        }
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(15.0, connect=8.0),
                follow_redirects=False,
                transport=self._transport,
            ) as client:
                resp = await client.post(
                    "https://passport.goofish.com/newlogin/hasLogin.do",
                    data=form_data,
                    headers=headers,
                )
        except httpx.HTTPError as exc:
            logger.warning("Cookie 保活网络异常：%s", exc)
            return False

        text = resp.text or ""
        if _is_risk_control(text):
            raise RiskControl("Cookie 保活触发闲鱼风控")
        if _is_session_expired(text) or '"haslogin":false' in text.replace(" ", "").lower():
            raise CookieExpired("Cookie 保活返回会话已过期")
        if resp.status_code != 200:
            logger.warning("Cookie 保活返回 HTTP %s", resp.status_code)
            return False

        self._merge_set_cookies(resp)
        await self._notify_cookie_refresh()
        logger.debug("Cookie 保活成功（账号 %s）", self.user_id)
        return True

    def invalidate_im_token(self) -> None:
        """清掉本账号缓存的 accessToken（Cookie/连接出问题时调用）。"""
        _IM_TOKEN_CACHE.pop(self.user_id, None)

    async def get_im_token(self) -> str:
        """换取 WebSocket 长连接用的 accessToken。

        优先用进程内缓存：短时间反复重连时不必每次都打 token 接口，
        这也是减少风控触发的重要手段（参考 xianyu-pilot 的 4 小时缓存）。
        """
        m_h5_tk = self.cookies.get("_m_h5_tk", "")
        ttl = settings.im_token_ttl
        if ttl > 0 and m_h5_tk:
            cached = _IM_TOKEN_CACHE.get(self.user_id)
            # Cookie 和 _m_h5_tk 都没变才命中：accessToken 与这两者绑定，
            # 只比对其中一个都可能在 Cookie 刷新后拿到「看似有效但服务端不认」的 token。
            if (
                cached
                and cached[1] == m_h5_tk
                and cached[2] == self.cookie_str
                and cached[3] > time.time()
            ):
                logger.debug("命中 IM token 缓存（账号 %s）", self.user_id)
                return cached[0]
            if cached:
                _IM_TOKEN_CACHE.pop(self.user_id, None)

        payload = await self.mtop(
            TOKEN_API,
            {"appKey": settings.im_app_key, "deviceId": self.device_id},
        )
        ret = _ret_text(payload)
        token = (payload.get("data") or {}).get("accessToken")
        if token:
            if ttl > 0:
                # 用 mtop 返回后的 _m_h5_tk 做缓存键：如果过程中刷新过 token，
                # 旧值已经不能代表这次 accessToken 实际绑定的凭据了。
                effective_tk = self.cookies.get("_m_h5_tk", "")
                _IM_TOKEN_CACHE[self.user_id] = (
                    str(token), effective_tk, self.cookie_str, time.time() + ttl
                )
            return str(token)
        if _is_risk_control(ret):
            self.invalidate_im_token()
            raise RiskControl(f"获取 IM token 触发闲鱼风控: {ret}")
        if _is_token_expired(ret):
            self.invalidate_im_token()
            raise TokenExpired(f"获取 IM token 失败（令牌过期）: {ret}")
        if _is_session_expired(ret):
            self.invalidate_im_token()
            raise CookieExpired(f"获取 IM token 失败（登录态失效）: {ret}")
        raise XianyuError(f"获取 IM token 失败: {ret}")

    async def confirm_shipping(self, order_id: str) -> tuple[bool, str]:
        """调用「无需物流发货」接口确认发货。

        虚拟商品没有物流单号，闲鱼提供 consign.dummy（免填单号发货），
        只有确认后买家侧才会显示「卖家已发货」。
        """
        data = {"orderId": str(order_id), "tradeText": "", "picList": [], "newUnconsign": True}
        try:
            payload = await self.mtop("mtop.taobao.idle.logistic.consign.dummy", data)
        except XianyuError as exc:
            return False, str(exc)
        ret = " ".join(payload.get("ret", []))
        if "SUCCESS" in ret.upper():
            return True, ret
        return False, ret or "未知返回"

    async def find_order(self, order_id: str) -> dict[str, Any] | None:
        """按订单号查卖家订单列表，拿到金额与状态。

        走 mtop.taobao.idle.trade.merchant.sold.get，这是卖家中心「订单管理」
        页面用的接口；返回 module.items[] 里每一项含 commonData / priceVO。

        orderIds 在线上有三种写法（字符串 / 数组 / 留空），只有一种能真正查到。
        每次把三种都试一遍等于多打两次接口，所以这里记住上次成功的那种，
        下次直接用它 —— 发货链路上每次往返都是买家多等的几百毫秒。
        """
        payloads: list[tuple[int, dict[str, Any]]] = [
            (0, {"pageNumber": 1, "rowsPerPage": 10, "orderIds": str(order_id), "queryCode": "ALL",
                 "orderSearchParam": "{}"}),
            (1, {"pageNumber": 1, "rowsPerPage": 10, "orderIds": [str(order_id)], "queryCode": "ALL",
                 "orderSearchParam": "{}"}),
            (2, {"pageNumber": 1, "rowsPerPage": 20, "orderIds": "", "queryCode": "ALL",
                 "orderSearchParam": "{}"}),
        ]
        preferred = self._order_query_variant
        payloads.sort(key=lambda pair: (pair[0] != preferred, pair[0]))
        for variant, data in payloads:
            try:
                resp = await self.mtop("mtop.taobao.idle.trade.merchant.sold.get", data)
            except XianyuError as exc:
                logger.warning("查订单 %s 失败: %s", order_id, exc)
                continue
            found = _pick_order(resp, order_id)
            if found is not None:
                self._order_query_variant = variant
                return found
        return None

    async def list_items(self, page_number: int = 1, page_size: int = 20) -> list[dict[str, Any]]:
        """拉自己「在售」的商品列表，供后台选商品时用。

        走 mtop.idle.web.xyh.item.list —— 也就是「我发布的」页面用的接口。
        返回精简后的 [{item_id, title, price}]，字段名对不上就跳过该条。
        """
        payload = {
            "needGroupInfo": False,
            "pageNumber": page_number,
            "pageSize": page_size,
            "groupName": "在售",
            "defaultGroup": True,
            "userId": self.user_id,
        }
        try:
            resp = await self.mtop("mtop.idle.web.xyh.item.list", payload)
        except XianyuError as exc:
            logger.warning("拉商品列表失败：%s", exc)
            return []

        ret = " ".join(resp.get("ret", []))
        if "SUCCESS" not in ret.upper():
            logger.warning("拉商品列表返回异常：%s", ret)
            return []

        cards = ((resp.get("data") or {}).get("cardList")) or []
        items: list[dict[str, Any]] = []
        for card in cards:
            if not isinstance(card, dict):
                continue
            data = card.get("cardData") if isinstance(card.get("cardData"), dict) else {}
            item_id = str(data.get("id") or "").strip()
            if not item_id:
                continue
            price_info = data.get("priceInfo") if isinstance(data.get("priceInfo"), dict) else {}
            items.append({
                "item_id": item_id,
                "title": str(data.get("title") or ""),
                "price": str(price_info.get("price") or ""),
            })
        return items

    def dispatch(self, frame: dict[str, Any]) -> bool:
        """把「我们在等的回包」交给等待者，返回是否被消费。

        由收帧主循环调用 —— 回包和普通推送走同一条流，
        所以必须由主循环分发，不能另起一个 recv。
        """
        mid = str((frame.get("headers") or {}).get("mid") or "")
        if not mid:
            return False
        future = self._pending.get(mid)
        if future is None or future.done():
            return False
        future.set_result(frame)
        return True

    async def _request(self, frame: dict[str, Any], timeout: float = 15.0) -> dict[str, Any]:
        """发一帧并等它的回包（由主循环 dispatch 回来）。"""
        if self.websocket is None:
            raise XianyuError("WebSocket 未连接")

        mid = str((frame.get("headers") or {}).get("mid") or "")
        if not mid:
            raise XianyuError("请求帧缺少 mid")

        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._pending[mid] = future
        try:
            await self.websocket.send(json.dumps(frame))
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError as exc:
            raise XianyuError("等待服务端回包超时") from exc
        finally:
            self._pending.pop(mid, None)

    async def create_chat(self, to_id: str, item_id: str) -> str:
        """确保与某个买家有会话，返回会话 cid。

        订单触发的发货没有「推送消息」可用，只能自己建会话再发消息。
        实测：/r/SingleChatConversation/create 带上 sid 才返回 200，
        回包里 singleChatConversation.cid 就是会话 ID。
        """
        mid = generate_mid()
        frame = {
            "lwp": "/r/SingleChatConversation/create",
            "headers": {"mid": mid, "sid": getattr(self, "sid", "") or ""},
            "body": [{
                "pairFirst": f"{to_id}@goofish",
                "pairSecond": f"{self.user_id}@goofish",
                "bizType": "1",
                "extension": {"itemId": str(item_id)},
                "ctx": {"appVersion": "1.0", "platform": "web"},
            }],
        }
        resp = await self._request(frame)
        body = resp.get("body") or {}
        conversation = body.get("singleChatConversation") or {}
        cid = str(conversation.get("cid") or "").split("@")[0]
        if cid:
            return cid
        raise XianyuError(f"创建会话失败（code={resp.get('code')}）")
    # ------------------------------------------------- 会话列表（拉取式收消息）

    async def list_recent_conversations(self, limit: int = 20) -> list[dict[str, Any]]:
        """拉最近的会话列表，每条带上「最后一条消息」。

        为什么需要它：私信正文并不总是走 syncPushPackage 推送 —— 线上实测
        连接稳定、心跳正常时，推送里只有会话引导（contentType=8）和
        「某会话有新消息」的未读标记，**正文一个字都没有**。
        而 /r/Conversation/listNewestPagination 每次都能拿到最后一条消息的
        完整内容（reminderContent + base64 的 custom.data）。

        所以收消息也要像收订单一样有「拉」的兜底：推送负责快，这里负责一定有。
        返回结构统一成 runner 直接能吃的形状。
        """
        frame = {
            "lwp": "/r/Conversation/listNewestPagination",
            "headers": {"mid": generate_mid(), "sid": getattr(self, "sid", "") or ""},
            # 服务器严格校验 body 只有两个元素：[游标, 条数]。
            # 游标用最大安全整数表示「从最新一页开始」。
            "body": [9007199254740991, max(1, min(int(limit), 50))],
        }
        resp = await self._request(frame)
        body = _as_dict_local(resp.get("body"))
        conversations = body.get("userConvs")
        if not isinstance(conversations, list):
            return []

        out: list[dict[str, Any]] = []
        for conv in conversations:
            if not isinstance(conv, dict):
                continue
            parsed = self._parse_conversation(conv)
            if parsed is not None:
                out.append(parsed)
        return out

    async def list_conversation_messages(self, chat_id: str, limit: int = 20,
                                         cursor: int = 9007199254740991) -> dict[str, Any]:
        """拉某个会话的消息历史（双向）。

        参考 xianyu-pilot：游标从最大安全整数开始表示「从最新一条往前翻」，
        body 是 5 个裸值组成的数组，服务器严格校验元素个数。

        为什么值得多打一次这个请求：会话列表只有**每个会话的最后一条**，
        如果买家连发两条、或者两条之间我们自己回了一句，只看最后一条就会漏。
        这里一次把最近 N 条都拿到，按 messageId 去重后逐条处理。
        """
        frame = {
            "lwp": "/r/MessageManager/listUserMessages",
            "headers": {"mid": generate_mid(), "sid": getattr(self, "sid", "") or ""},
            "body": [
                f"{chat_id}@goofish" if "@" not in str(chat_id) else str(chat_id),
                False,
                int(cursor),
                max(1, min(int(limit), 50)),
                False,
            ],
        }
        resp = await self._request(frame)
        body = _as_dict_local(resp.get("body"))
        items = body.get("userMessageModels") or body.get("messages") or []
        if not isinstance(items, list):
            return {"messages": [], "next_cursor": 0, "has_more": False}

        out: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            # 每一项是 {"message": {...}, "readStatus": …, "userExtension": …}
            message = _as_dict_local(item.get("message")) or item
            parsed = self._parse_im_message(message, chat_id_hint=str(chat_id))
            if parsed is not None:
                out.append(parsed)
        try:
            next_cursor = int(body.get("nextCursor") or 0)
        except (TypeError, ValueError):
            next_cursor = 0
        return {
            "messages": out,
            "next_cursor": next_cursor,
            "has_more": bool(body.get("hasMore")),
        }

    def _parse_conversation(self, conv: dict[str, Any]) -> dict[str, Any] | None:
        """把一条会话记录摊平成 runner 好用的字段；解不出必要信息就跳过。"""
        single = _as_dict_local(conv.get("singleChatUserConversation"))
        last = _as_dict_local(single.get("lastMessage"))
        message = _as_dict_local(last.get("message"))
        if not message:
            # 会话存在但还没有消息
            return None
        session = _as_dict_local(single.get("singleChatConversation"))
        session_ext = _as_dict_local(session.get("extension"))
        chat_id = str(message.get("cid") or session.get("cid") or "").split("@")[0].strip()
        return self._parse_im_message(message, session_ext=session_ext, chat_id_hint=chat_id)

    def _parse_im_message(self, message: dict[str, Any], *, session_ext: dict[str, Any] | None = None,
                          chat_id_hint: str = "") -> dict[str, Any] | None:
        """把一条 IM 消息（会话列表的 lastMessage 或历史里的一项）摊平。

        两个来源的 message 结构完全一致，所以共用这一段：
        会话列表只给每个会话的最后一条，历史接口给一段连续的消息。

        `session_ext` 是 `singleChatConversation.extension`，里面有权威的
        买家 / 商品 / 订单信息（比从正文里猜靠谱）。
        """
        session_ext = session_ext or {}
        extension = _as_dict_local(message.get("extension"))
        content = _as_dict_local(message.get("content"))
        custom = _as_dict_local(content.get("custom"))

        # 正文：reminderContent 是通知摘要（和网页左栏一致），
        # custom.data 是 base64 JSON，里面是原始文本，两者取更全的那个。
        texts: list[str] = []
        for value in (extension.get("reminderContent"), extension.get("detailNotice")):
            if isinstance(value, str) and value.strip():
                texts.append(value.strip())
        inner = _b64_json_local(custom.get("data"))
        for key in ("text", "tip", "title"):
            holder = _as_dict_local(inner.get(key))
            value = holder.get("text") if holder else None
            if isinstance(value, str) and value.strip():
                texts.append(value.strip())
        if isinstance(custom.get("summary"), str) and custom["summary"].strip():
            texts.append(custom["summary"].strip())
        text = "\n".join(_dedupe_lines_local(texts))

        reminder_url = str(extension.get("reminderUrl") or "")
        chat_id = str(message.get("cid") or "").split("@")[0].strip()
        if not chat_id:
            chat_id = str(chat_id_hint or "").split("@")[0].strip()
        if not chat_id:
            chat_id = _url_param(reminder_url, "sid")
        if not chat_id:
            return None

        item_id = str(session_ext.get("itemId") or "") or _url_param(reminder_url, "itemId")
        peer_id = _url_param(reminder_url, "peerUserId") or str(session_ext.get("extUserId") or "")

        sender_id = str(extension.get("senderUserId") or "").split("@")[0].strip()
        sender = _as_dict_local(message.get("sender"))
        if not sender_id:
            sender_id = str(sender.get("uid") or "").split("@")[0].strip()

        # 对方是买家还是我自己：卖家自己发的（多端同步）不该触发自动回复。
        # 自己发的那条里 reminderUrl 的 peerUserId 指向通知接收方（就是我自己），
        # 不能当买家 ID 用；这种情况留空，反正整条消息会被跳过。
        from_me = bool(sender_id) and sender_id == self.user_id
        buyer_id = "" if from_me else (sender_id or (peer_id if peer_id != self.user_id else ""))

        # 订单号的来源决定它可不可信：会话 extension / reminderUrl 是服务端字段，
        # 正文（text）和 reminderContent 是买家写的，只能兜底。
        order_id = str(session_ext.get("orderId") or "")
        order_trusted = bool(order_id)
        if not order_id:
            order_id = _url_param(reminder_url, "orderId")
            order_trusted = bool(order_id)
        if not order_id:
            order_id = _find_order_id(text) or _find_order_id(json.dumps(inner, ensure_ascii=False))
        if not order_id:
            order_id = _find_order_id(json.dumps(extension, ensure_ascii=False))

        try:
            created_at = int(message.get("createAt") or 0)
        except (TypeError, ValueError):
            created_at = 0

        # custom.type：1 = 真人文本，其余（14 系统提示、25 评价提醒、26 卡片…）都是系统消息。
        # 自动回复只该对真人文本生效 —— 回一句「在的」给「[你已发货]」很傻。
        try:
            custom_type = int(custom.get("type"))
        except (TypeError, ValueError):
            custom_type = 0

        message_id = str(message.get("messageId") or "")
        return {
            "chat_id": chat_id,
            "custom_type": custom_type,
            "item_id": item_id,
            "buyer_id": buyer_id,
            # reminderTitle 在交易卡片里是标题（「等待你发货」），不是昵称。
            # 会话 extension 里带的是对方资料，那是真名，优先用它；
            # 两处都没有才退回空串，让上层再去找（聊天记录 / 订单）。
            "buyer_name": (
                buyer_nick_from_session(session_ext, extension)
                or pick_display_name(extension.get("reminderTitle"), fallback="")
            ),
            "content": text,
            "order_id": order_id or None,
            "order_id_trusted": order_trusted,
            "created_at": created_at,
            "message_id": message_id,
            "from_me": from_me,
            "raw": message,
        }

    async def list_pending_orders(self, page_size: int = 20) -> list[dict[str, Any]]:
        """拉「待发货」订单。

        这是 IM 推送之外的兜底：订单推送丢了（断开、风控、版本变更），
        只要这里还能查到，就还能发货。
        """
        payload = {
            "pageNumber": 1,
            "rowsPerPage": page_size,
            "orderIds": "",
            "queryCode": "ALL",
            "orderSearchParam": "{}",
        }
        try:
            resp = await self.mtop("mtop.taobao.idle.trade.merchant.sold.get", payload)
        except XianyuError as exc:
            logger.warning("拉订单列表失败：%s", exc)
            return []

        ret = " ".join(resp.get("ret", []))
        if "SUCCESS" not in ret.upper():
            logger.warning("订单列表返回异常：%s", ret)
            return []

        module = _as_dict_local((resp.get("data") or {}).get("module"))
        items = module.get("items") or []
        pending: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            common = _as_dict_local(item.get("commonData"))
            if str(common.get("orderStatus") or "") != "待发货":
                continue
            order_id = str(common.get("orderId") or "").strip()
            if not order_id:
                continue
            buyer = _as_dict_local(item.get("buyerInfoVO"))
            price = _as_dict_local(item.get("priceVO"))
            pending.append({
                "order_id": order_id,
                "item_id": str(common.get("itemId") or "").strip(),
                "buyer_id": str(buyer.get("buyerId") or "").strip(),
                # 只认 buyerInfoVO 里的昵称，别把卖家名/商品标题当买家名
                "buyer_name": buyer_nick_from_session(buyer),
                "amount": price.get("totalPrice"),
                "raw": item,
            })
        return pending

    # ------------------------------------------------------- WebSocket

    async def connect(self) -> WebSocket:
        """建立长连接并完成 /reg 注册。"""
        headers = {
            "Cookie": self.cookie_str,
            "Host": "wss-goofish.dingtalk.com",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
            "User-Agent": UA,
            "Origin": "https://www.goofish.com",
            "Accept-Language": "zh-CN,zh;q=0.9",
        }
        try:
            ws = await websockets.connect(settings.ws_url, additional_headers=headers, max_size=None)
        except Exception:
            # 握手失败多半是 token/设备/Cookie 不匹配，缓存里的 token 别再用了
            self.invalidate_im_token()
            raise
        self.websocket = ws
        self.sid = ""
        self.last_inbound_at = time.monotonic()
        # 心跳必须在注册之前就动起来：注册期间长时间没有 /! 时，服务端会把连接
        # 判成「不活跃」，之后只推框架帧（/s/sync、/s/vulcan）而不再推消息正文 ——
        # 现象就是「连接看着是好的，但永远收不到买家的话」。线上实测结论。
        await self.start_heartbeat(ws)
        await self._register(ws)
        # /reg 与 ackDiff 之间必须留一口气：紧凑连发会让注册在服务端不生效，
        # 同样表现为「能连上、能收框架帧、收不到正文」。
        await asyncio.sleep(settings.register_gap)
        await self._ack_diff(ws)
        await self.start_sync_poll(ws)
        return ws

    async def start_heartbeat(self, ws: WebSocket) -> asyncio.Task[None]:
        """在注册之前就把心跳跑起来（幂等：已有就不重复起）。

        首跳**在这里直接发**，而不是等任务被调度 —— 否则 /reg 可能先出去，
        注册窗口里依然是空的，问题照旧。
        """
        task = self._heartbeat_task
        if task is not None and not task.done():
            return task
        await self._send_heartbeat(ws)
        self._heartbeat_task = asyncio.create_task(
            self.heartbeat(ws, send_first=False), name="xianyu-heartbeat"
        )
        return self._heartbeat_task

    async def _send_heartbeat(self, ws: WebSocket) -> None:
        await ws.send(json.dumps({"lwp": "/!", "headers": {"mid": generate_mid()}}))

    async def stop_heartbeat(self) -> None:
        task = self._heartbeat_task
        self._heartbeat_task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logger.debug("心跳任务退出异常：%s", exc)

    def mark_inbound(self) -> None:
        """收到任何服务端帧时调用，心跳用它判断连接是否已经静默死亡。"""
        self.last_inbound_at = time.monotonic()

    async def _register(self, ws: WebSocket) -> None:
        token = await self.get_im_token()
        frame = {
            "lwp": "/reg",
            "headers": {
                "cache-header": "app-key token ua wv",
                "app-key": settings.im_app_key,
                "token": token,
                "ua": IM_UA,
                "dt": "j",
                "wv": "im:3,au:3,sy:6",
                "sync": "0,0;0;0;",
                "did": self.device_id,
                "mid": generate_mid(),
            },
        }
        await ws.send(json.dumps(frame))
        # /reg 的回包头里有 sid，后续 create_chat 要带。
        # 注意：这里不能阻塞等回包 —— 收帧循环还没起来，直接 recv 会和它抢。
        # 改成把 sid 提取放到主循环里做（见 note_sid）。

    def note_sid(self, frame: dict[str, Any]) -> None:
        """从帧头里记下 sid（/reg 回包里有，后续请求要带）。"""
        sid = (frame.get("headers") or {}).get("sid")
        if sid and not self.sid:
            self.sid = str(sid)
            logger.debug("已获取会话 sid")

    async def start_sync_poll(self, ws: WebSocket) -> asyncio.Task[None]:
        """周期性重发 ackDiff —— 增量推送靠它触发。

        参考 xianyu-pilot：每轮 ackDiff 回包后**立刻再发一轮**（长轮询），
        服务端把增量消息作为 ackDiff 的响应推下来。只发一次的话，
        除了连接瞬间那批快照包（/s/sync、/s/vulcan）就再没有新消息了 ——
        这正是「连上了、有心跳、但收不到买家新消息」的一个成因。
        """
        task = self._sync_poll_task
        if task is not None and not task.done():
            return task
        self._sync_poll_task = asyncio.create_task(
            self._sync_poll_loop(ws), name="xianyu-sync-poll"
        )
        return self._sync_poll_task

    async def stop_sync_poll(self) -> None:
        task = self._sync_poll_task
        self._sync_poll_task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logger.debug("同步轮询任务退出异常：%s", exc)

    async def _sync_poll_loop(self, ws: WebSocket) -> None:
        interval = settings.sync_poll_interval
        if interval <= 0:
            return
        while True:
            await asyncio.sleep(interval)
            try:
                # 不能走 _request（它会等 mid 回包，而长轮询本来就常常不回），
                # 直接发出去就行；消息会以 /s/vulcan 推送的形式到达。
                await self._ack_diff(ws)
                logger.debug("已重发 ackDiff 同步位点（间隔 %.0fs）", interval)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("重发 ackDiff 失败：%s", exc)
                return

    async def _ack_diff(self, ws: WebSocket) -> None:
        import time as _time

        ts = int(_time.time() * 1000)
        frame = {
            "lwp": "/r/SyncStatus/ackDiff",
            "headers": {"mid": generate_mid()},
            "body": [
                {
                    "pipeline": "sync",
                    "tooLong2Tag": "PNM,1",
                    "channel": "sync",
                    "topic": "sync",
                    "highPts": 0,
                    "pts": ts * 1000,
                    "seq": 0,
                    "timestamp": ts,
                }
            ],
        }
        await ws.send(json.dumps(frame))

    async def ack(self, ws: WebSocket, frame: dict[str, Any]) -> None:
        """对收到的帧回执，否则服务端会重复推送。"""
        headers = frame.get("headers") or {}
        ack_headers: dict[str, Any] = {
            "mid": headers.get("mid") or generate_mid(),
            "sid": headers.get("sid", ""),
        }
        for key in ("app-key", "ua", "dt"):
            if key in headers:
                ack_headers[key] = headers[key]
        await ws.send(json.dumps({"code": 200, "headers": ack_headers}))

    async def heartbeat(self, ws: WebSocket, interval: float | None = None,
                        *, send_first: bool = True) -> None:
        """心跳循环；连接异常时退出，由上层负责重连。

        先发首跳再睡：注册期间的空窗会让服务端把连接判成不活跃，
        之后只推框架帧、不推消息正文（线上实测）。间隔默认 10 秒，
        和网页端一致。

        除了发 /!，还检查「多久没收到任何入站帧」。WebSocket 经常出现
        服务端已经把我们踢了、但 TCP 还挂着的情况：不发消息时一切正常，
        真到要发货时才发现发不出去。超过 3 个心跳周期没入站就主动断开，
        让上层重连（xianyu-pilot 同款判定）。
        """
        if interval is None:
            interval = settings.heartbeat_interval
        if send_first:
            try:
                await self._send_heartbeat(ws)
            except Exception:
                return
        while True:
            await asyncio.sleep(interval)
            idle = time.monotonic() - self.last_inbound_at
            if idle > interval * 3:
                logger.warning("WebSocket 已静默断开（%.0f 秒无入站），主动关闭以触发重连", idle)
                await ws.close()
                return
            await ws.send(json.dumps({"lwp": "/!", "headers": {"mid": generate_mid()}}))

    async def send_text(self, ws: WebSocket, chat_id: str, to_id: str, text: str) -> None:
        """在会话内发送文本消息。"""
        payload = {"contentType": 1, "text": {"text": text}}
        data = base64.b64encode(json.dumps(payload, ensure_ascii=False).encode("utf-8")).decode("utf-8")
        frame = {
            "lwp": "/r/MessageSend/sendByReceiverScope",
            "headers": {"mid": generate_mid(), "sid": getattr(self, "sid", "") or ""},
            "body": [
                {
                    "uuid": generate_uuid(),
                    "cid": f"{chat_id}@goofish",
                    "conversationType": 1,
                    "content": {"contentType": 101, "custom": {"type": 1, "data": data}},
                    "redPointPolicy": 0,
                    "extension": {"extJson": "{}"},
                    "ctx": {"appVersion": "1.0", "platform": "web"},
                    "mtags": {},
                    "msgReadStatusSetting": 1,
                },
                {"actualReceivers": [f"{to_id}@goofish", f"{self.user_id}@goofish"]},
            ],
        }
        await ws.send(json.dumps(frame))

    async def send_text_safe(self, chat_id: str, to_id: str, text: str, retry: int | None = None) -> None:
        """发送文本，失败自动重试（网络抖动很常见）。"""
        attempts = settings.send_retry if retry is None else retry
        last_error: Exception | None = None
        for i in range(attempts + 1):
            if self.websocket is None:
                raise XianyuError("WebSocket 未连接，无法发送消息")
            try:
                await self.send_text(self.websocket, chat_id, to_id, text)
                return
            except Exception as exc:
                last_error = exc
                logger.warning("发送消息失败（第 %s 次）: %s", i + 1, exc)
                await asyncio.sleep(1.5)
        raise XianyuError(f"发送消息失败: {last_error}")


MessageHandler = Callable[[dict[str, Any]], Awaitable[None]]
