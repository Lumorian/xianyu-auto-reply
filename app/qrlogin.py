"""闲鱼扫码登录（网页端 passport 流程）。

参考 cv-cat/XianYuApis 的实现，把「手工从 F12 复制 Cookie」变成「手机扫一下码」：

1. 先拿一批基础 Cookie（cna / cookie2 等，不含登录态）；
2. 打开 passport 的 mini_login 页，拿 XSRF-TOKEN 与 _tb_token_；
3. 请 passport 生成二维码，得到 codeContent（二维码内容）+ t + ck；
4. 轮询 query.do 等用户扫码；状态 NEW → SCANNED → CONFIRMED；
5. CONFIRMED 后用 token 换登录态（服务端会下发 unb / sgcookie 等）；
6. 再请求一次 mtop 接口，把 _m_h5_tk 刷成登录后的新值。

最终产出一整条 Cookie 字符串，直接喂给 :class:`app.client.XianyuClient`。
"""

from __future__ import annotations

import asyncio
import io
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from urllib.parse import quote

import httpx

from .config import DIRECT_HTTP
from .sign import clean_cookie_pair, mask_keylike

logger = logging.getLogger(__name__)

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
)

_MTOP_HEADERS = {
    "User-Agent": UA,
    "Accept": "application/json",
    "Accept-Language": "en,zh-CN;q=0.9,zh;q=0.8,zh-TW;q=0.7,ja;q=0.6",
    "Origin": "https://www.goofish.com",
    "Referer": "https://www.goofish.com/",
    "Content-Type": "application/x-www-form-urlencoded",
}

_PASSPORT_HEADERS = {
    "User-Agent": UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en,zh-CN;q=0.9,zh;q=0.8,zh-TW;q=0.7,ja;q=0.6",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
}

#: 扫码状态 → 给用户看的中文说明
STATUS_TEXT = {
    "NEW": "等待扫码",
    "SCANNED": "已扫码，请在手机上确认",
    "CONFIRMED": "已确认，正在完成登录",
    "EXPIRED": "二维码已过期",
    "CANCELED": "已取消",
    "ERROR": "登录失败",
}


class LoginError(RuntimeError):
    """扫码登录失败。"""


@dataclass(slots=True)
class QrSession:
    """一次扫码登录的进行时状态。"""

    session_id: str
    qr_url: str
    t: str
    ck: str
    created_at: float = field(default_factory=time.time)
    status: str = "NEW"
    cookie: str = ""
    nick: str = ""
    unb: str = ""
    error: str = ""
    csrf: str = ""
    cookie2: str = ""
    client: httpx.AsyncClient | None = None

    @property
    def done(self) -> bool:
        return self.status in ("CONFIRMED", "EXPIRED", "CANCELED", "ERROR")

    @property
    def age(self) -> float:
        return time.time() - self.created_at


def _mtop_params(api: str, spm: str = "a21ybx.home.0.0") -> dict[str, str]:
    return {
        "jsv": "2.7.2",
        "appKey": "34839810",
        "t": str(int(time.time() * 1000)),
        "sign": "",
        "v": "1.0",
        "type": "originaljson",
        "dataType": "json",
        "timeout": "20000",
        "api": api,
        "sessionOption": "AutoLoginOnly",
        "spm_cnt": spm,
    }


class QrLoginManager:
    """管理并发的扫码登录会话（同一时间可能有多个账号在扫）。"""

    def __init__(self, client_factory: Callable[[], httpx.AsyncClient] | None = None) -> None:
        self._sessions: dict[str, QrSession] = {}
        #: 后台清理任务。不强引用的话 create_task 的返回值可能被 GC，
        #: aclose() 就永远不执行，连接一直挂着。
        self._bg_tasks: set[asyncio.Task[None]] = set()
        #: 可注入的 httpx 客户端工厂（测试时换成打桩 transport）
        self._client_factory = client_factory

    def _new_client(self) -> httpx.AsyncClient:
        if self._client_factory is not None:
            return self._client_factory()
        return httpx.AsyncClient(
            **DIRECT_HTTP,
            timeout=httpx.Timeout(15.0),
            headers={"User-Agent": UA},
            follow_redirects=True,
        )

    def get(self, session_id: str) -> QrSession | None:
        return self._sessions.get(session_id)

    def _gc(self) -> None:
        """清掉超过 10 分钟的会话，避免内存里堆着没人要的连接。"""
        for sid in [s for s, v in self._sessions.items() if v.age > 600]:
            session = self._sessions.pop(sid, None)
            if session and session.client is not None:
                self._spawn_close(session.client)

    def _spawn_close(self, client: httpx.AsyncClient) -> None:
        """后台关连接（不能在这里 await：_gc 是同步方法）。"""
        task = asyncio.create_task(client.aclose())
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)

    async def start(self, session_id: str) -> QrSession:
        """建一个扫码会话，返回带二维码内容的 QrSession。"""
        self._gc()
        client = self._new_client()
        try:
            await self._prime_cookies(client)
            csrf, cookie2 = await self._open_passport(client)
            qr_url, qr_t, qr_ck = await self._generate_qr(client, csrf, cookie2)
        except Exception:
            await client.aclose()
            raise

        session = QrSession(
            session_id=session_id,
            qr_url=qr_url,
            t=qr_t,
            ck=qr_ck,
            client=client,
        )
        session.csrf = csrf
        session.cookie2 = cookie2
        self._sessions[session_id] = session
        logger.info("已生成闲鱼登录二维码（session=%s）", session_id)
        return session

    async def _prime_cookies(self, client: httpx.AsyncClient) -> None:
        """先访问几个站点拿基础 Cookie（cna / cookie2 / tfstk 之类）。"""
        try:
            await client.get("https://log.mmstat.com/eg.js")
        except httpx.HTTPError as exc:
            logger.debug("拿 cna 失败（不影响主流程）：%s", exc)

        cna = client.cookies.get("cna")
        if cna:
            client.cookies.set("cna", cna, domain=".goofish.com", path="/")

        for api in ("mtop.taobao.idlehome.home.webpc.feed",
                    "mtop.gaia.nodejs.gaia.idle.data.gw.v2.index.get"):
            try:
                await client.post(
                    f"https://h5api.m.goofish.com/h5/{api}/1.0/",
                    params=_mtop_params(api),
                    content="data=%7B%7D",
                    headers=_MTOP_HEADERS,
                )
            except httpx.HTTPError:
                continue

    async def _open_passport(self, client: httpx.AsyncClient) -> tuple[str, str]:
        """打开 passport 的登录页，拿 csrf 与 hsiz(cookie2)。"""
        await client.get(
            "https://passport.goofish.com/mini_login.htm",
            params={
                "lang": "zh_cn",
                "appName": "xianyu",
                "appEntrance": "web",
                "styleType": "vertical",
                "bizParams": "",
                "notLoadSsoView": "false",
                "notKeepLogin": "false",
                "isMobile": "false",
                "qrCodeFirst": "false",
                "stie": "77",
                "rnd": "0.6842814084442211",
            },
            headers={**_PASSPORT_HEADERS, "Referer": "https://www.goofish.com/"},
        )
        csrf = client.cookies.get("XSRF-TOKEN") or ""
        cookie2 = client.cookies.get("cookie2") or ""
        return csrf, cookie2

    async def _generate_qr(self, client: httpx.AsyncClient, csrf: str, cookie2: str) -> tuple[str, str, str]:
        """请 passport 生成二维码，返回 (二维码内容, t, ck)。"""
        resp = await client.get(
            "https://passport.goofish.com/newlogin/qrcode/generate.do",
            params={
                "appName": "xianyu",
                "fromSite": "77",
                "appEntrance": "web",
                "_csrf_token": csrf,
                "umidToken": "",
                "hsiz": cookie2,
                "bizParams": "taobaoBizLoginFrom=web&renderRefer=" + quote("https://www.goofish.com/"),
                "mainPage": "false",
                "isMobile": "false",
                "lang": "zh_CN",
                "returnUrl": "",
                "umidTag": "SERVER",
            },
            headers={**_PASSPORT_HEADERS, "Referer": "https://passport.goofish.com/mini_login.htm"},
        )
        try:
            data = resp.json()["content"]["data"]
        except (ValueError, KeyError, TypeError) as exc:
            raise LoginError(f"生成二维码失败（HTTP {resp.status_code}）：{resp.text[:200]}") from exc
        if not isinstance(data, dict):
            # data 为 null / 字符串时再往下 .get 就是 AttributeError → 500
            raise LoginError(f"生成二维码失败（HTTP {resp.status_code}）：返回结构异常")

        qr_url = data.get("codeContent") or ""
        if not qr_url:
            raise LoginError("passport 没返回二维码内容")
        return qr_url, str(data.get("t", "")), str(data.get("ck", ""))

    async def poll(self, session_id: str) -> QrSession:
        """查询一次扫码状态；到终态时把 Cookie 组装好。"""
        session = self._sessions.get(session_id)
        if session is None:
            raise LoginError("扫码会话不存在或已过期，请重新生成二维码")
        if session.done or session.client is None:
            return session

        if session.age > 180:
            session.status = "EXPIRED"
            await session.client.aclose()
            session.client = None
            return session

        csrf = session.csrf
        cookie2 = session.cookie2
        try:
            resp = await session.client.post(
                "https://passport.goofish.com/newlogin/qrcode/query.do?appName=xianyu&fromSite=77",
                data={
                    "appName": "xianyu",
                    "fromSite": "77",
                    "appEntrance": "web",
                    "_csrf_token": csrf,
                    "umidToken": "",
                    "hsiz": cookie2,
                    "bizParams": "taobaoBizLoginFrom=web&renderRefer=" + quote("https://www.goofish.com/"),
                    "mainPage": "false",
                    "isMobile": "false",
                    "lang": "zh_CN",
                    "returnUrl": "",
                    "umidTag": "SERVER",
                    "navlanguage": "en",
                    "navUserAgent": UA,
                    "navPlatform": "Win32",
                    "isIframe": "true",
                    "documentReferer": "https://www.goofish.com/",
                    "defaultView": "sms",
                    "deviceId": cookie2,
                    "t": session.t,
                    "ck": session.ck,
                },
                headers={
                    **_PASSPORT_HEADERS,
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Origin": "https://passport.goofish.com",
                    "Referer": "https://passport.goofish.com/mini_login.htm",
                },
            )
            payload = resp.json()["content"]["data"]
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
            logger.debug("查询扫码状态失败：%s", exc)
            return session
        if not isinstance(payload, dict):
            logger.debug("查询扫码状态返回结构异常：%r", payload)
            return session

        status = str(payload.get("qrCodeStatus") or "NEW")
        session.status = status

        if status == "EXPIRED":
            await session.client.aclose()
            session.client = None
            return session

        if status == "CONFIRMED":
            token = payload.get("token") or payload.get("lgToken")
            if token:
                await self._finish_login(session, str(token))
            # 有些版本在 CONFIRMED 的 Set-Cookie 里就带上了登录态
            if not session.cookie:
                await self._finalize(session)
        return session

    async def _finish_login(self, session: QrSession, token: str) -> None:
        assert session.client is not None
        cookie2 = session.cookie2
        try:
            await session.client.post(
                "https://passport.goofish.com/login_token/login.do",
                params={
                    "token": token,
                    "subFlow": "DIALOG_CHECK_LOGIN_RPC",
                    "nextCode": "0018",
                    "bizScene": "qrcode",
                    "confirm": "true",
                },
                data={"deviceId": cookie2},
                headers={
                    **_PASSPORT_HEADERS,
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Origin": "https://passport.goofish.com",
                    "Referer": "https://passport.goofish.com/mini_login.htm",
                },
            )
        except httpx.HTTPError as exc:
            logger.warning("login_token 调用失败（继续尝试收 Cookie）：%s", exc)
        await self._finalize(session)

    async def _finalize(self, session: QrSession) -> None:
        """登录完成后刷新 mtop token，并把 Cookie 组装成字符串。"""
        assert session.client is not None
        api = "mtop.idle.web.user.page.nav"
        try:
            await session.client.post(
                f"https://h5api.m.goofish.com/h5/{api}/1.0/",
                params=_mtop_params(api),
                content="data=%7B%7D",
                headers=_MTOP_HEADERS,
            )
        except httpx.HTTPError as exc:
            logger.debug("刷新 _m_h5_tk 失败：%s", exc)

        # 域名白名单：只收闲鱼/淘宝/天猫/统计这几个域。
        # 关键在 taobao.com —— 「保持登录」的长效票据 cookie17 是 passport
        # 下发在 .taobao.com 上的，老实现只留 .goofish.com，等于把唯一能
        # 让登录态活过 24 小时的那张票扔掉了。
        #
        # 匹配必须是「等于该域或它的子域」，不能用子串包含：
        # 子串会把 evilgoofish.com / goofish.com.attacker.tld 一起放进来，
        # 而我们这条串是要拼进发往 goofish 的 Cookie 头的。
        jar: dict[str, str] = {}
        wanted_domains = ("goofish.com", "taobao.com", "tmall.com", "mmstat.com")

        def _domain_ok(raw_domain: str) -> bool:
            host = (raw_domain or "").strip().lstrip(".").lower()
            if not host:
                return False
            return any(host == base or host.endswith("." + base) for base in wanted_domains)

        for cookie in session.client.cookies.jar:
            if not _domain_ok(cookie.domain or ""):
                continue
            # 名字/值/长度同样要洗（详见 sign.clean_cookie_pair）：这串 Cookie
            # 会被落库、并在之后每个请求里当请求头发出去。
            cleaned = clean_cookie_pair(cookie.name, cookie.value)
            if cleaned is None:
                logger.warning("扫码登录拿到的 Cookie 里有一条不能安全使用，已丢弃")
                continue
            jar[cleaned[0]] = cleaned[1]

        logger.info(
            "扫码登录拿到的 Cookie：%s（%s cookie17 长效票据）",
            ",".join(mask_keylike(name) for name in sorted(jar)),
            "含" if jar.get("cookie17") else "不含",
        )

        session.unb = jar.get("unb", "")
        session.nick = jar.get("tracknick", "") or jar.get("_nk_", "")
        session.cookie = "; ".join(f"{k}={v}" for k, v in jar.items())

        if jar and not jar.get("cookie17"):
            # 不是错误，但直接决定「明天这个点还得再扫一次」
            logger.info(
                "本次登录没有 cookie17（保持登录票据）：闲鱼网页登录态约 24 小时到期。"
                "想少扫几次，就在浏览器里勾「保持登录」后复制整条 Cookie 粘贴进来"
            )
        if not session.unb:
            session.status = "ERROR"
            session.error = "登录已确认但没拿到 unb，请重试或改用粘贴 Cookie"
        elif not jar.get("_m_h5_tk"):
            session.status = "ERROR"
            session.error = "登录已确认但没拿到 _m_h5_tk（权限令牌），请重试"

        await session.client.aclose()
        session.client = None
        if session.status == "CONFIRMED" and not session.error:
            logger.info("闲鱼扫码登录成功：%s (unb=%s)", session.nick or "未知", session.unb)

    async def cancel(self, session_id: str) -> None:
        session = self._sessions.pop(session_id, None)
        if session and session.client is not None:
            await session.client.aclose()
            session.client = None

    async def close(self) -> None:
        for session in list(self._sessions.values()):
            if session.client is not None:
                await session.client.aclose()
        self._sessions.clear()


def render_qr_png(text: str) -> bytes:
    """把二维码渲染成 PNG 字节，供网页直接显示。"""
    import qrcode

    qr = qrcode.QRCode(border=2, box_size=8)
    qr.add_data(text)
    qr.make()
    image = qr.make_image(fill_color="black", back_color="white")
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()
