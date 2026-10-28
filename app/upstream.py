"""上游联动：按订单金额在 new-api 建 Key，再把 Key 导入卡密站生成卡密。

完整链路（与你现在的操作等价，只是全自动）：

    订单实付金额(元) ──换算──> 额度(USD)
      ──> new-api POST /api/token/ 建令牌 ──> 取回 sk-xxx
      ──> 卡密站 POST /api/admin/cards 建卡 ──> 取回卡密
      ──> 卡密作为发货内容发给买家

两条链路都有「先登录拿会话」和「直接带凭据」两种用法，
配置里给哪种就用哪种，方便放在反代后面或者同机部署。
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass
from typing import Any

import httpx

from .config import DEFAULT_MODEL_LIMITS, DIRECT_HTTP, cardshop_ready, resolved_model_limits, settings

logger = logging.getLogger(__name__)

#: new-api 的 quota 与美元的换算兜底值（$1 = 500000 quota，见 common.QuotaPerUnit）
DEFAULT_QUOTA_PER_UNIT = 500_000.0


class UpstreamError(RuntimeError):
    """上游接口调用失败（可以确定这次动作没有生效）。"""


class TopupUncertainError(UpstreamError):
    """充值请求已经发出，但没拿到可信回包（超时 / 502 / 非 JSON）。

    这种时候不能当失败重试：new-api 可能已经加过额度了，再充一次就是多送钱。
    调用方要落一个「结果未知」状态，交人工到 new-api 后台核对。
    """


@dataclass(slots=True)
class QuotaModel:
    """new-api 里 quota 与「钱」的换算关系。

    关键：quota 是以**站点展示货币**计价的。站点把货币设成 CNY 时，
    quota_per_unit 表示「¥1 等于多少 quota」；设成 USD 时才是「$1 等于多少 quota」。
    """

    #: 1 个展示单位（美元或人民币）对应多少 quota
    quota_per_unit: float
    #: 展示货币代码，USD / CNY / CUSTOM
    currency: str = "USD"
    #: 美元对展示货币的汇率（CNY 用，例如 7.3）
    usd_exchange_rate: float = 1.0
    #: 实际参与换算的汇率：USD=1，CNY=usd_exchange_rate，CUSTOM=custom_currency_exchange_rate
    exchange_rate: float = 1.0


@dataclass(slots=True)
class Provisioned:
    """一次自动建号 / 充值的产物。"""

    card_code: str
    key_mask: str
    quota_usd: float
    token_id: str
    #: 完整 Key。充值模式要发给买家；建卡密模式也带上，顺便给「买家 Key」自动绑定。
    api_key: str = ""
    #: 充值后的余额（站点展示货币）。建卡密模式通常为 None。
    balance: float | None = None
    currency: str = "USD"
    token_name: str = ""
    #: False = 令牌本身不限额度 / 无需充值，跳过了一次 PUT（只用于日志与详情）。
    topped_up: bool = True


@dataclass(slots=True)
class AccountInfo:
    """一个买家账号（new-api 里的「用户」）。"""

    user_id: str
    username: str
    #: 建号时生成的密码。只在「这一单要发给买家」时使用，不写日志。
    password: str = ""
    #: 本次加了多少额度（站点展示货币）
    quota_usd: float = 0.0
    #: 加完之后的账号余额（站点展示货币）。读不到时是 None。
    balance: float | None = None
    currency: str = "USD"
    #: True = 这次真的新建了账号；False = 账号本来就在（重试 / 老用户）
    created: bool = False


@dataclass(slots=True)
class TopupInfo:
    """一次充值的结果（new-api 侧）。"""

    token_id: str
    token_name: str
    api_key: str
    key_mask: str
    quota_usd: float
    balance: float | None = None
    currency: str = "USD"
    topped_up: bool = True


class NewApiClient:
    """new-api 后台接口的最小封装（登录 + 建令牌 + 读 Key）。"""

    def __init__(self, base_url: str, *, access_token: str = "", username: str = "", password: str = "",
                 group: str = "", quota_per_unit: float = 0, user_id: str = "",
                 model_limits: str = DEFAULT_MODEL_LIMITS) -> None:
        self.base_url = base_url.rstrip("/")
        self.access_token = access_token
        #: new-api 0.13.x 要求所有后台请求带 New-Api-User（用户 id），否则 401。
        #: 留空时登录后自动从 /api/user/self 探测一次。
        self.user_id = str(user_id or "")
        self.username = username
        self.password = password
        self.group = group
        #: 新建令牌只放行的模型（逗号分隔）。留空 = 不限制。
        #: 来源是后台「设置」页（app_config 表），没配过才退回 .env。
        self.model_limits = model_limits
        self._quota_per_unit = quota_per_unit
        #: 缓存站点计价模型（只在第一次用到时读一次 /api/status）
        self._quota_model: QuotaModel | None = None
        self._http: httpx.AsyncClient | None = None
        self._logged_in = False
        #: user id 是否已经跟服务端核对过（每个进程只核对一次，省一次往返）
        self._user_id_checked = False
        #: 每个令牌一把锁：充值必须「先读余额再写回」，两单同时充同一个 Key
        #: 会丢掉其中一次的额度（read-modify-write 竞态）。
        self._token_locks: dict[str, asyncio.Lock] = {}

    async def __aenter__(self) -> NewApiClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def close(self) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None:
            headers = {"user-agent": "xianyu-auto-reply/0.1"}
            if self.access_token:
                headers["authorization"] = "Bearer " + self.access_token
            if self.user_id:
                headers["new-api-user"] = self.user_id
            self._http = httpx.AsyncClient(
                **DIRECT_HTTP,
                base_url=self.base_url,
                timeout=httpx.Timeout(10.0, connect=5.0),
                headers=headers,
                follow_redirects=True,
                # 订单是零星的：让空闲连接多活一会儿，下一单就不用重做 DNS + TLS 握手
                limits=httpx.Limits(max_keepalive_connections=10, keepalive_expiry=90.0),
            )
        return self._http

    async def login(self) -> None:
        """拿后台访问凭据。

        新版 new-api 登录会在响应体里给出 access_token（JWT），后续请求用它做
        Authorization: Bearer；老版本只下发会话 Cookie，httpx 会自动保存并沿用。
        两条路都走通，配置里直接给 access_token 时连登录都省了。
        """
        if self.access_token or self._logged_in:
            return
        if not (self.username and self.password):
            raise UpstreamError("new-api 未配置 access_token，也没有账号密码")
        client = await self._client()
        resp = await client.post(
            "/api/user/login",
            json={"username": self.username, "password": self.password},
        )
        data = _json_of(resp, "new-api 登录")
        if not data.get("success"):
            raise UpstreamError("new-api 登录失败：" + str(data.get("message") or resp.status_code))
        payload = data.get("data") or {}
        token = payload.get("access_token") if isinstance(payload, dict) else None
        if token:
            self.access_token = str(token)
            # 直接把头加给已建好的 client，别重建 —— 重建会丢掉老版本下发的会话 Cookie
            client.headers["authorization"] = "Bearer " + self.access_token
        # 0.13.x 的登录响应里有 id；新版本在 user.id 里
        if not self.user_id and isinstance(payload, dict):
            uid = payload.get("id") or (payload.get("user") or {}).get("id")
            if uid:
                self.user_id = str(uid)
                client.headers["new-api-user"] = self.user_id
                # 登录响应里的 id 就是 token 主人，不用再去 /api/user/self 核对
                self._user_id_checked = True
        self._logged_in = True
        logger.info("已登录 new-api 后台（user_id=%s）", self.user_id or "未探测")
    async def quota_per_unit(self) -> float:
        """读站点配置里「1 个展示单位」对应多少 quota。读不到就用兜底值。

        ⚠️ 这里的「1 个单位」**不一定是美元**：new-api 可以把展示货币改成
        CNY（quota_display_type=CNY），此时 quota_per_unit 的含义变成
        「¥1 = 多少 quota」。详见 site_quota_model()。
        """
        return (await self.site_quota_model()).quota_per_unit

    async def site_quota_model(self) -> QuotaModel:
        """站点当前「quota ↔ 钱」的换算关系（进程内缓存一次）。

        为什么要读这么多字段：new-api 的 quota 是按**展示货币**计价的。
        站点若把货币设成 CNY 且汇率 7.3，那么 500000 quota = ¥7.30，
        而不是 $1。以前这里无条件当成美元，结果 1 元订单发出 ¥7.30 额度，
        多给 7.3 倍 —— 这是真金白银的损失，所以必须按站点实际配置换算。
        """
        if self._quota_model is not None:
            return self._quota_model

        model = QuotaModel(quota_per_unit=0.0, currency="USD", usd_exchange_rate=1.0)
        # 只有真正读到配置才缓存。读失败时返回一个临时的「美元 1:1」兜底对象，
        # 但不写进 self._quota_model —— 否则一次网络抖动会让整个进程一直按
        # 错误汇率发额度（CNY 站点会超发一个汇率倍数），直到重启才恢复。
        fetched = False
        try:
            client = await self._client()
            resp = await client.get("/api/status")
            data = ((_json_of(resp, "new-api 状态").get("data")) or {})
            if not isinstance(data, dict):
                data = {}
            fetched = bool(data)

            per_unit = data.get("quota_per_unit")
            if isinstance(per_unit, (int, float)) and per_unit > 0:
                model.quota_per_unit = float(per_unit)
            elif self._quota_per_unit > 0:
                model.quota_per_unit = self._quota_per_unit

            # 展示货币：USD 时汇率为 1；CNY 用 usd_exchange_rate；CUSTOM 用自定义汇率
            display = str(data.get("quota_display_type") or "").upper()
            rate = data.get("usd_exchange_rate")
            if display == "CNY":
                model.currency = "CNY"
                if isinstance(rate, (int, float)) and rate > 0:
                    model.usd_exchange_rate = float(rate)
                else:
                    # 币种是 CNY 却没拿到汇率：不能猜成 1，否则少给 7 倍额度。
                    # 退回设置里的 cny_per_usd（默认 1，用户可在 .env 里调）。
                    model.usd_exchange_rate = settings.cny_per_usd or 1.0
                    logger.warning(
                        "站点展示货币是 CNY，但 /api/status 没给 usd_exchange_rate，"
                        "按 XY_CNY_PER_USD=%s 换算", model.usd_exchange_rate,
                    )
                model.exchange_rate = model.usd_exchange_rate
            elif display == "CUSTOM":
                model.currency = "CUSTOM"
                custom = data.get("custom_currency_exchange_rate")
                if isinstance(custom, (int, float)) and custom > 0:
                    model.exchange_rate = float(custom)
                else:
                    model.exchange_rate = settings.cny_per_usd or 1.0
                    logger.warning("站点使用自定义货币但没给汇率，按 XY_CNY_PER_USD=%s 换算",
                                   model.exchange_rate)
            elif display:
                model.currency = display
                model.exchange_rate = 1.0

        except (UpstreamError, httpx.HTTPError) as exc:
            logger.warning("读取 new-api 站点计价配置失败，按美元 1:1 处理：%s", exc)

        if model.quota_per_unit <= 0:
            model.quota_per_unit = self._quota_per_unit or DEFAULT_QUOTA_PER_UNIT

        if not fetched:
            logger.warning("new-api 计价配置本次读取失败，临时按美元 1:1 兜底，不缓存；下次重试")
            return model

        self._quota_model = model
        detail = (
            f"汇率={model.exchange_rate:g}" if model.exchange_rate != 1.0 else "无汇率换算"
        )
        logger.info(
            "new-api 计价：1 %s = %s quota（%s）",
            model.currency, round(model.quota_per_unit), detail,
        )
        return model

    async def quota_for_amount(self, amount: float) -> int:
        """把「订单金额（人民币）」换成 new-api 的 remain_quota。

        站点货币不管是 USD 还是 CNY，出来的额度都正好抵掉这单的钱：
        1 元订单在站点里就显示 ¥1.00，不会因为汇率多给 7 倍。
        """
        model = await self.site_quota_model()
        # new-api 自己的换算（前端 d()/l()，看站点源码得出的，别自己另立一套）：
        #     显示金额 = quota / quotaPerUnit * exchangeRate
        #     反解     = 金额 / exchangeRate * quotaPerUnit
        # 其中 exchangeRate 只在 CNY/CUSTOM 时非 1（CNY 就是 usdExchangeRate）。
        # 美元站点 exchangeRate=1，于是退化成「金额 × quotaPerUnit」，与老行为一致。
        rate = model.exchange_rate if model.exchange_rate > 0 else 1.0
        return round(amount / rate * model.quota_per_unit)

    async def ensure_user_id(self) -> None:
        """确保拿到**正确**的 New-Api-User（0.13.x 所有后台接口都强制要这个头）。

        坑一：这个头本身也是查 /api/user/self 的前提，所以「只有 access_token」
        时无法自举。能自举的只有账号密码登录 —— 登录响应里直接带 id。

        坑二：配错了 id 比不配更隐蔽 —— new-api 只会回一句
        「Unauthorized, New-Api-User does not match logged in user」，
        看起来像 token 失效，其实是「这个 id 和 token 的主人不是同一个账号」。
        所以即使配了 id，也要用 /api/user/self 核对一次（每个进程只核对一次），
        免得错误配置一直躲在后面，等到买家下单时才炸。

        坑三（实测踩过）：new-api 的「系统访问令牌」可以按 scope 授权。
        只给了 token:read/write、没给 profile:read 的令牌，调 /api/user/self
        会被 403 拒绝 —— 而建令牌其实完全不需要这个接口。此时如果
        XY_NEWAPI_USER_ID 已经显式配好，就不该为了「核对」把整条发货链路卡死：
        直接采用配置值，把核对降级成一次不影响结果的尝试。
        """
        if self._user_id_checked:
            return
        client = await self._client()
        resp = await client.get("/api/user/self")
        data = _json_of(resp, "new-api 用户信息")

        if not data.get("success"):
            message = str(data.get("message") or "")
            lowered = message.lower()
            if _is_scope_denied(lowered, message):
                # 令牌缺少 profile:read：/api/user/self 读不了，但建令牌不需要它。
                # 已经有显式 user id 就直接用，别让这层「额外核对」把发货挡死。
                if self.user_id:
                    logger.warning(
                        "new-api 令牌缺少 profile:read，跳过用户 id 核对，直接使用 "
                        "XY_NEWAPI_USER_ID=%s（若这个 id 不对，建令牌时会报 "
                        "New-Api-User does not match）", self.user_id,
                    )
                    client.headers["new-api-user"] = self.user_id
                    self._user_id_checked = True
                    return
                raise UpstreamError(
                    "new-api 令牌缺少 profile:read，无法自动探测用户 id。"
                    "请二选一：① 在 .env 里填 XY_NEWAPI_USER_ID（后台「个人设置」页可见，通常是个数字）；"
                    "② 换一个带 profile:read 的令牌，或改用 XY_NEWAPI_USERNAME / XY_NEWAPI_PASSWORD 登录。"
                )
            if "does not match" in lowered:
                # 只是一句给运维看的排查提示，不含任何实际执行的 SQL
                hint = "或 SELECT id FROM users WHERE username='<用户名>';"
                raise UpstreamError(
                    "new-api 不认配置里的 user id：XY_NEWAPI_USER_ID="
                    + (self.user_id or "（空）")
                    + "，但这串 access_token 属于别的账号（" + message + "）。"
                    "请改成 access_token 主人的用户 id：后台「个人设置」页可见，"
                    + hint
                )
            if "invalid access token" in lowered:
                raise UpstreamError(
                    "new-api 不认这串 access_token（" + message + "）："
                    "多半是复制时截断了，或者已在后台「个人设置 → 系统访问令牌」里重置过。"
                    "请重新生成，或改用 XY_NEWAPI_USERNAME / XY_NEWAPI_PASSWORD 登录。"
                )
            if "new-api-user" in lowered:
                raise UpstreamError(
                    "new-api 需要 New-Api-User 头但拿不到用户 id："
                    "该头是查询用户信息的**前提**，光有 access_token 无法自举。"
                    "请二选一：① 改用 XY_NEWAPI_USERNAME / XY_NEWAPI_PASSWORD 登录（会自动拿到 id）；"
                    "② 手工填 XY_NEWAPI_USER_ID（后台个人设置页可见）。"
                )
            raise UpstreamError(
                "拿不到 new-api 用户信息：" + (message or str(resp.status_code))
                + "。若 access_token 本身有效，多半是缺 XY_NEWAPI_USER_ID（New-Api-User 头），"
                "也可以改用账号密码登录让它自动探测。"
            )

        uid = ((data.get("data") or {}).get("id")) if isinstance(data.get("data"), dict) else None
        if not uid:
            raise UpstreamError("new-api /api/user/self 没返回 id，请手工填 XY_NEWAPI_USER_ID")
        uid = str(uid)
        if self.user_id and self.user_id != uid:
            # 理论上中间件已经按「id 必须匹配」拦过一轮，走到这里说明站点版本更宽松；
            # 以服务端返回的为准，别拿错的 id 继续发请求。
            logger.warning("配置的 XY_NEWAPI_USER_ID=%s 与 token 主人（%s）不一致，已按服务端返回为准",
                           self.user_id, uid)
        else:
            logger.info("已核对 new-api 用户 id=%s", uid)
        self.user_id = uid
        client.headers["new-api-user"] = uid
        self._user_id_checked = True

    async def create_token(self, quota_usd: float, name: str,
                           model_limits: str | None = None) -> tuple[str, str]:
        """建一个额度为 quota_usd 的令牌，返回 (token_id, 明文 key)。

        model_limits 不为 None 时按这一单实际生效的白名单建令牌
        （商品级配置走这条），否则用客户端上的全局默认值。
        """
        await self.login()
        await self.ensure_user_id()
        # 注意：这里传进来的 quota_usd 实际上是「人民币等值额度」的数，
        # 也是发给买家的「额度」展示值。真正的 remain_quota 必须按站点货币换算，
        # 否则站点用 CNY 展示时会多给一个汇率倍数。
        remain_quota = await self.quota_for_amount(quota_usd)
        if remain_quota <= 0:
            raise UpstreamError("换算后的额度为 0，请检查 XY_CNY_PER_USD / 订单金额")

        client = await self._client()
        raw_limits = self.model_limits if model_limits is None else model_limits
        model_limits = normalize_model_limits(raw_limits)
        if not model_limits:
            logger.warning("模型白名单为空：新建的令牌将**不限制模型**，买家可调用任意模型"
                           "（到后台「设置」页填写即可，无需改 .env）")
        payload: dict[str, Any] = {
            "name": fit_bytes(name),
            "remain_quota": remain_quota,
            "unlimited_quota": False,
            "expired_time": -1,
            # 必须同时打开这两个字段，new-api 才会真正拦截越权模型；
            # 只写 model_limits 不写 enabled，后台会把它当成「不限制」。
            "model_limits_enabled": bool(model_limits),
            "model_limits": model_limits,
            "allow_ips": "",
            "group": self.group or "",
            "cross_group_retry": False,
            "allow_user_quota_query": True,
        }
        resp = await client.post("/api/token/", json=payload)
        data = _json_of(resp, "new-api 建令牌")
        if not data.get("success"):
            raise UpstreamError("new-api 建令牌失败：" + str(data.get("message") or resp.status_code))

        # 有的版本建令牌直接回 id，能省掉一次列表查询（一次往返 200~400ms）
        token_id = ""
        payload_data = data.get("data")
        if isinstance(payload_data, dict) and payload_data.get("id"):
            token_id = str(payload_data["id"])
        try:
            if not token_id:
                token_id = await self._find_token_id(name)
            key = await self._read_key(token_id)
        except UpstreamError:
            # 令牌已经建出来了：拿不到 Key 就别把它留在后台白吃额度
            # （以前这里直接抛出去，后台会多一个没人知道、却还能用的 Key）
            if token_id:
                await self.delete_token_quietly(token_id)
            raise
        logger.info("new-api 已建令牌 id=%s 名称=%s 额度=%s USD 模型限制=%s",
                    token_id, name, quota_usd, model_limits or "不限")
        return token_id, key

    # ------------------------------------------------------------ 充值

    def _lock_for(self, token_id: str) -> asyncio.Lock:
        lock = self._token_locks.get(token_id)
        if lock is None:
            lock = asyncio.Lock()
            self._token_locks[token_id] = lock
        return lock

    async def get_token(self, token_id: str) -> dict[str, Any]:
        """读一个令牌的完整记录（不包含明文 Key）。"""
        token_id = str(token_id or "").strip()
        if not token_id:
            raise UpstreamError("缺少令牌 ID")
        await self.login()
        await self.ensure_user_id()
        client = await self._client()
        resp = await client.get(f"/api/token/{token_id}")
        data = _json_of(resp, "new-api 读令牌")
        if not data.get("success"):
            raise UpstreamError(
                "读不到令牌 " + token_id + "：" + str(data.get("message") or resp.status_code)
            )
        token = data.get("data")
        if not isinstance(token, dict) or not token:
            raise UpstreamError("读不到令牌 " + token_id + "（返回为空）")
        return token

    async def token_balance(self, token: dict[str, Any]) -> float | None:
        """令牌余额，按站点展示货币换算；不限额度返回 None。"""
        if token.get("unlimited_quota"):
            return None
        model = await self.site_quota_model()
        try:
            remain = float(token.get("remain_quota") or 0)
        except (TypeError, ValueError):
            remain = 0.0
        rate = model.exchange_rate if model.exchange_rate > 0 else 1.0
        unit = model.quota_per_unit if model.quota_per_unit > 0 else DEFAULT_QUOTA_PER_UNIT
        return round(remain / unit * rate, 2)

    async def topup_token(self, token_id: str, quota_usd: float,
                          api_key: str = "") -> TopupInfo:
        """把额度加到**已有**令牌上，返回充值后的余额。

        关键点（都踩过）：
          * new-api 的 PUT 是整体替换：必须先把整条记录读出来、只改
            remain_quota 再写回，否则额度、模型限制、分组会被一起冲掉；
          * 同一个 Key 并发充两单会丢额度，所以按令牌加锁；
          * 余额字段叫 remain_quota，单位是 quota 不是钱，必须按站点货币换算；
          * 已用尽的令牌（status=4）充值后要顺手改回启用，否则买家还是用不了。
        """
        token_id = str(token_id or "").strip()
        if not token_id:
            raise UpstreamError("缺少令牌 ID")
        await self.login()
        await self.ensure_user_id()

        async with self._lock_for(token_id):
            token = await self.get_token(token_id)
            name = str(token.get("name") or "")
            status = _int_or(token.get("status"), 1)
            if status == 2:
                raise UpstreamError("令牌「" + name + "」已被禁用，请先在 new-api 后台启用")
            if status == 3 or _token_expired(token):
                raise UpstreamError(
                    "令牌「" + name + "」已过期，充值也用不了；请先续期或换一个 Key"
                )

            # 以服务端读到的 Key 为准：绑定时手抄/换绑都可能留下旧值，
            # 消息里要展示的必须是这个令牌现在真正的 Key。读不到才退回传入值。
            listed_key = str(token.get("key") or "").strip()
            if listed_key and "*" not in listed_key:
                key = normalize_key(listed_key)
            else:
                key = await self._read_key_quiet(token_id) or (api_key or "").strip()

            if token.get("unlimited_quota"):
                logger.info("令牌 %s（%s）是不限额度，跳过充值", token_id, name or "-")
                return TopupInfo(
                    token_id=token_id, token_name=name, api_key=key, key_mask=_mask(key),
                    quota_usd=quota_usd, balance=None,
                    currency=(await self.site_quota_model()).currency,
                    topped_up=False,
                )

            delta = await self.quota_for_amount(quota_usd)
            if delta <= 0:
                raise UpstreamError("换算后的充值额度为 0，请检查金额 / 倍率配置")
            try:
                current = int(float(token.get("remain_quota") or 0))
            except (TypeError, ValueError):
                current = 0
            new_remain = current + delta

            payload = {k: v for k, v in token.items() if k not in ("key", "DeletedAt")}
            payload["remain_quota"] = new_remain
            if status == 4:
                # 4 = 已用尽。new-api 不会因为余额变正自动启用，必须显式改回 1。
                payload["status"] = 1

            client = await self._client()
            try:
                resp = await client.put("/api/token/", json=payload)
            except httpx.HTTPError as exc:
                raise TopupUncertainError(
                    "充值请求发出后连接异常，结果未知（new-api 可能已加额度）：" + str(exc)
                ) from exc
            try:
                data = resp.json()
            except ValueError as exc:
                raise TopupUncertainError(
                    "充值接口返回非 JSON（HTTP " + str(resp.status_code) + "），结果未知"
                ) from exc
            if not isinstance(data, dict):
                raise TopupUncertainError("充值接口返回结构异常，结果未知")
            if not data.get("success"):
                if resp.status_code >= 500:
                    # 网关/服务端 5xx：请求可能已经落到后端，不能当失败重试
                    raise TopupUncertainError(
                        "new-api 返回 HTTP " + str(resp.status_code) + "，充值结果未知"
                    )
                raise UpstreamError(
                    "new-api 充值失败：" + str(data.get("message") or resp.status_code)
                )

            model = await self.site_quota_model()
            rate = model.exchange_rate if model.exchange_rate > 0 else 1.0
            unit = model.quota_per_unit if model.quota_per_unit > 0 else DEFAULT_QUOTA_PER_UNIT
            balance = round(new_remain / unit * rate, 2)
            logger.info(
                "new-api 已充值令牌 id=%s 名称=%s +%s → remain_quota=%s（余额 %s %s）%s",
                token_id, name or "-", quota_usd, new_remain, balance, model.currency,
                "；已用尽状态已重新启用" if status == 4 else "",
            )
            return TopupInfo(
                token_id=token_id, token_name=name, api_key=key, key_mask=_mask(key),
                quota_usd=quota_usd, balance=balance, currency=model.currency, topped_up=True,
            )

    # ------------------------------------------------------------ 账号

    async def find_user(self, username: str) -> dict[str, Any] | None:
        """按账号名**精确**找一个用户；找不到返回 None。

        /api/user/search 是模糊查询（SQL LIKE），搜 "abc" 会连 "abc123" 一起返回。
        直接取第一条就等于把额度充到别人账上，所以这里必须自己再比一次用户名。
        """
        name = str(username or "").strip()
        if not name:
            return None
        await self.login()
        await self.ensure_user_id()
        client = await self._client()
        try:
            resp = await client.get(
                "/api/user/search", params={"keyword": name, "page_size": 20}
            )
        except httpx.HTTPError as exc:
            raise UpstreamError("查 new-api 用户失败：" + str(exc)) from exc
        data = _json_of(resp, "new-api 搜索用户")
        if not data.get("success"):
            raise UpstreamError(
                "查 new-api 用户失败：" + str(data.get("message") or resp.status_code)
            )
        payload = data.get("data")
        items = payload.get("items") if isinstance(payload, dict) else None
        for item in items or []:
            if isinstance(item, dict) and str(item.get("username") or "") == name:
                return item
        return None

    async def create_user(self, username: str, password: str) -> dict[str, Any]:
        """建一个普通用户（role=1），返回随后查到的用户记录。

        三个必须记住的点：
          * role 要显式给 1。省略时后端按 0（访客）建，访客登录不了后台；
          * CreateUser **不返回 id，也不接受 quota**：建完必须再查一次拿 id，
            额度另外用 add_user_quota 加；
          * 用户名 ≤20 字节、密码 8~128 位（new-api 自己的校验），先在这里拦掉，
            否则报错信息是英文的 "Invalid input …"，发给买家很难看。
        """
        name = str(username or "").strip()
        secret = str(password or "")
        if not name:
            raise UpstreamError("账号名不能为空")
        if len(name) > 20:
            raise UpstreamError("账号名最多 20 个字符")
        if len(secret) < 8:
            raise UpstreamError("密码至少 8 位（new-api 的密码规则）")
        await self.login()
        await self.ensure_user_id()
        client = await self._client()
        payload = {
            "username": name,
            "password": secret,
            "display_name": name,
            "role": 1,
        }
        try:
            resp = await client.post("/api/user/", json=payload)
        except httpx.HTTPError as exc:
            raise UpstreamError("new-api 建账号失败：" + str(exc)) from exc
        data = _json_of(resp, "new-api 建账号")
        if not data.get("success"):
            raise UpstreamError(
                "new-api 建账号失败：" + str(data.get("message") or resp.status_code)
            )
        user = await self.find_user(name)
        if user is None:
            raise UpstreamError("new-api 建账号成功，但随后查不到这个账号（" + name + "）")
        logger.info("new-api 已建账号 id=%s 名称=%s", user.get("id"), name)
        return user

    async def add_user_quota(self, user_id: str, quota_usd: float) -> dict[str, Any]:
        """给已有账号加额度，返回最新的一条用户记录（带 quota）。

        为什么是 /api/user/manage 而不是 PUT /api/user/：后者在这版 new-api 里
        只更新用户名 / 昵称 / 分组 / 备注，**根本不写 quota**，调完以为充上了，
        实际一分钱没加。add_quota 是增量操作，不需要先读再写，也就没有
        「两单同时充、其中一单被覆盖」的竞态。
        """
        uid = str(user_id or "").strip()
        if not uid:
            raise UpstreamError("缺少账号 ID")
        try:
            numeric_id = int(uid)
        except (TypeError, ValueError) as exc:
            raise UpstreamError("账号 ID 不是数字：" + uid) from exc
        delta = await self.quota_for_amount(quota_usd)
        if delta <= 0:
            raise UpstreamError("换算后的充值额度为 0，请检查金额 / 倍率配置")

        await self.login()
        await self.ensure_user_id()
        client = await self._client()
        payload = {"id": numeric_id, "action": "add_quota", "mode": "add", "value": delta}
        try:
            resp = await client.post("/api/user/manage", json=payload)
        except httpx.HTTPError as exc:
            raise TopupUncertainError(
                "加额度请求发出后连接异常，结果未知（new-api 可能已加额度）：" + str(exc)
            ) from exc
        try:
            data = resp.json()
        except ValueError as exc:
            raise TopupUncertainError(
                "加额度接口返回非 JSON（HTTP " + str(resp.status_code) + "），结果未知"
            ) from exc
        if not isinstance(data, dict):
            raise TopupUncertainError("加额度接口返回结构异常，结果未知")
        if not data.get("success"):
            if resp.status_code >= 500:
                raise TopupUncertainError(
                    "new-api 返回 HTTP " + str(resp.status_code) + "，加额度结果未知"
                )
            raise UpstreamError(
                "new-api 加额度失败：" + str(data.get("message") or resp.status_code)
            )

        logger.info(
            "new-api 已给账号 id=%s 加额度 +%s（quota +%s）",
            numeric_id, quota_usd, delta,
        )
        # ⚠️ 钱已经加上了：后面读余额失败**不能**变成异常。
        # 一旦抛出去，runner 会把它记成「确定的失败」并在下一轮重试，等于再充一次。
        # 宁可这次不显示余额，也不能让调用方以为没充成功。
        try:
            return await self.get_user(numeric_id)
        except Exception as exc:
            logger.warning(
                "已给账号 %s 加额度，但读取账号失败（余额未知，不影响本次充值）：%s",
                numeric_id, exc,
            )
            return {"id": numeric_id}

    async def get_user(self, user_id: int | str) -> dict[str, Any]:
        """读一条用户记录（含 quota）。"""
        uid = str(user_id or "").strip()
        if not uid:
            raise UpstreamError("缺少账号 ID")
        await self.login()
        await self.ensure_user_id()
        client = await self._client()
        try:
            resp = await client.get("/api/user/" + uid)
        except httpx.HTTPError as exc:
            raise UpstreamError("读 new-api 账号失败：" + str(exc)) from exc
        data = _json_of(resp, "new-api 读账号")
        if not data.get("success"):
            raise UpstreamError(
                "读不到账号 " + uid + "：" + str(data.get("message") or resp.status_code)
            )
        user = data.get("data")
        if not isinstance(user, dict) or not user:
            raise UpstreamError("读不到账号 " + uid + "（返回为空）")
        return user

    async def user_balance(self, user: dict[str, Any]) -> float | None:
        """账号余额，按站点展示货币换算成钱；读不到 quota 时返回 None。

        注意「没有 quota 字段」和「quota 是 0」是两回事：加完额度读不到账号时
        上游会返回 {"id": ...} 这种没有余额的数据，这时候显示 0.00 会吓死人，
        必须返回 None 让界面显示「—」。
        """
        if "quota" not in user or user.get("quota") is None:
            return None
        try:
            quota = float(user.get("quota") or 0)
        except (TypeError, ValueError):
            return None
        model = await self.site_quota_model()
        rate = model.exchange_rate if model.exchange_rate > 0 else 1.0
        unit = model.quota_per_unit if model.quota_per_unit > 0 else DEFAULT_QUOTA_PER_UNIT
        return round(quota / unit * rate, 2)

    # ------------------------------------------------------------ 兑换码

    async def create_redemptions(self, quota: int, name: str, *,
                                 count: int = 1, expired_time: int = 0) -> list[str]:
        """生成 new-api 兑换码，返回明文列表。

        几个必须记住的点：
          * quota 是 **quota 单位**，不是钱 —— 调用方先用 quota_for_amount 换算；
          * 名称限 1~20 个字（rune，不是字节），"买家昵称#19位订单号" 一定超，
            调用方要用 build_redeem_label 截短；
          * expired_time 是 unix 秒，**0 表示永不过期**（和令牌的 -1 不是一回事）；
          * 生成兑换码**不扣任何人的额度**，只有被兑换时才给买家加额度。
            所以「生成成功但没发出去」只是多了一个没人知道的码，不会亏钱。
        """
        await self.login()
        await self.ensure_user_id()
        client = await self._client()
        payload = {
            "name": str(name or "")[:20],
            "quota": int(quota),
            "count": int(count),
            "expired_time": int(expired_time or 0),
        }
        try:
            resp = await client.post("/api/redemption/", json=payload)
        except httpx.HTTPError as exc:
            raise UpstreamError("new-api 生成兑换码失败：" + str(exc)) from exc
        data = _json_of(resp, "new-api 生成兑换码")

        raw = data.get("data")
        codes = [str(item) for item in raw] if isinstance(raw, list) else []
        if codes and not data.get("success"):
            # 中途失败时 new-api 仍会回 200 + success=false + 已经生成的那几个码。
            # 那些码已经落库、能兑换，记下来方便运营去后台核对。
            logger.error(
                "new-api 生成兑换码时中途失败：已有 %s 个码落库（可能是孤儿），原因：%s",
                len(codes), data.get("message") or resp.status_code,
            )
        if not data.get("success"):
            message = str(data.get("message") or resp.status_code)
            if "compliance" in message.lower():
                # new-api 的合规闸门：没在后台确认过条款，兑换码生成一律拒绝。
                # 原始报错是英文长句，直接写进订单详情运营看不懂。
                message += "（需要在 new-api 后台确认「支付 / 兑换合规条款」后才能生成兑换码）"
            raise UpstreamError("new-api 生成兑换码失败：" + message)
        if len(codes) != int(count):
            raise UpstreamError(
                "new-api 生成兑换码返回数量不对（要 " + str(count) + " 个，拿到 "
                + str(len(codes)) + " 个）"
            )
        logger.info("new-api 已生成 %s 个兑换码（名称 %s）", len(codes), payload["name"])
        return codes

    async def find_token_by_key(self, api_key: str) -> dict[str, Any]:
        """按明文 Key 反查令牌记录。

        new-api 没有「按 Key 查令牌」的官方接口，只能翻令牌列表。列表里
        是否带明文 key 取决于版本：带就能查到，不带就明确告诉运营改用令牌 ID，
        不要在这里猜。
        """
        wanted = normalize_key(api_key)
        if not wanted or wanted == KEY_PREFIX:
            raise UpstreamError("Key 不能为空")
        await self.login()
        await self.ensure_user_id()
        client = await self._client()

        saw_plain_key = False
        page = 1
        while page <= 20:
            resp = await client.get("/api/token/", params={"p": page, "page_size": 100})
            data = _json_of(resp, "new-api 令牌列表")
            if not data.get("success"):
                raise UpstreamError(
                    "读 new-api 令牌列表失败：" + str(data.get("message") or resp.status_code)
                )
            items = ((data.get("data") or {}).get("items")) or []
            if not isinstance(items, list) or not items:
                break
            for item in items:
                if not isinstance(item, dict):
                    continue
                raw = str(item.get("key") or "").strip()
                if not raw or "*" in raw:
                    continue
                saw_plain_key = True
                if normalize_key(raw).lower() == wanted.lower():
                    return item
            if len(items) < 100:
                break
            page += 1

        if not saw_plain_key:
            raise UpstreamError(
                "这个 new-api 的令牌列表不返回明文 Key，没法按 Key 反查令牌。"
                "请改用「令牌 ID」绑定（new-api 后台「令牌」页点开某条，地址栏里就有 id）。"
            )
        raise UpstreamError(
            "在 new-api 的令牌列表里找不到这个 Key。确认它是本站的 Key、"
            "并且没有多复制或少复制字符。"
        )

    async def read_key_quiet(self, token_id: str) -> str:
        """公开版本：后台绑定页要用，读不到就返回空串。"""
        return await self._read_key_quiet(token_id)

    async def find_tokens_for_orders(self, order_ids: list[str]) -> dict[str, dict[str, Any]]:
        """按令牌名里的订单号反查令牌（一次性扫列表）。

        早期版本的订单没有记 api_key_id，但令牌名一直是「买家用户名#订单号」。
        拿一批订单号一次性匹配，老客户不用重买一次就能认回原来的 Key。
        """
        wanted = {str(order).strip() for order in order_ids if str(order).strip()}
        if not wanted:
            return {}
        await self.login()
        await self.ensure_user_id()
        client = await self._client()
        found: dict[str, dict[str, Any]] = {}
        page = 1
        while page <= 20:
            resp = await client.get("/api/token/", params={"p": page, "page_size": 100})
            data = _json_of(resp, "new-api 令牌列表")
            if not data.get("success"):
                raise UpstreamError(
                    "读 new-api 令牌列表失败：" + str(data.get("message") or resp.status_code)
                )
            items = ((data.get("data") or {}).get("items")) or []
            if not isinstance(items, list) or not items:
                break
            for item in items:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name") or "").strip()
                if not name:
                    continue
                tail = name.rsplit("#", 1)[-1].strip()
                if tail in wanted:
                    found.setdefault(tail, item)
                elif name in wanted:
                    found.setdefault(name, item)
            if len(found) >= len(wanted) or len(items) < 100:
                break
            page += 1
        return found

    async def _read_key_quiet(self, token_id: str) -> str:
        """尽量取到明文 Key；取不到只记日志，不影响充值本身。"""
        try:
            return await self._read_key(token_id)
        except Exception as exc:
            logger.warning("读取令牌 %s 的明文 Key 失败（充值照常）：%s", token_id, exc)
            return ""

    async def read_token_key(self, token_id: str) -> str:
        """读一个令牌的明文 Key；读不到返回空串，不抛异常。

        买家「直接发 Key 充值」靠它核对归属：只按令牌 id 读自己的令牌，
        不依赖令牌列表返回明文（有些 new-api 版本列表里是打码的）。
        """
        token_id = str(token_id or "").strip()
        if not token_id:
            return ""
        return await self.read_key_quiet(token_id)

    async def list_all_tokens(self, page_size: int = 100,
                              max_pages: int = 50) -> list[dict[str, Any]]:
        """翻页列出全部令牌（维护脚本 / 对账用）。

        list_tokens 只读第一页；「批量放开模型」这类操作必须翻完，
        否则名单一长就只改了最前面 100 个。
        """
        await self.login()
        await self.ensure_user_id()
        client = await self._client()
        out: list[dict[str, Any]] = []
        for page in range(1, max(1, int(max_pages)) + 1):
            resp = await client.get(
                "/api/token/", params={"p": page, "page_size": page_size})
            data = _json_of(resp, "new-api 令牌列表")
            items = ((data.get("data") or {}).get("items")) or []
            if not isinstance(items, list) or not items:
                break
            out.extend(item for item in items if isinstance(item, dict))
            if len(items) < page_size:
                break
        return out

    async def set_token_models(self, token_id: str, models: str) -> dict[str, Any]:
        """只改令牌的模型白名单，其它字段一个不动。

        new-api 的 PUT 是**整体替换**：先读完整记录，只改 model_limits 再写回，
        并且和 rename_token 一样刻意不带 key / DeletedAt。少写一个字段就等于
        把买家的额度、分组、过期时间冲掉。
        """
        wanted = normalize_model_limits(models)
        if not wanted:
            raise UpstreamError("模型白名单不能为空（空串 = 不限制模型，请显式确认）")
        token = await self.get_token(token_id)
        payload = {k: v for k, v in token.items() if k not in ("key", "DeletedAt")}
        payload["model_limits"] = wanted
        payload["model_limits_enabled"] = True
        client = await self._client()
        put = await client.put("/api/token/", json=payload)
        data = _json_of(put, "new-api 改令牌模型白名单")
        if not data.get("success"):
            raise UpstreamError(
                "new-api 改模型白名单失败：" + str(data.get("message") or put.status_code)
            )
        logger.info("new-api 令牌 %s 模型白名单改为 %s", token_id, wanted)
        return token

    async def delete_token(self, token_id: str) -> bool:
        """删除一个令牌（建卡密失败 / 取不到 Key 时回滚用）。

        不在这里调 ensure_user_id：能走到这里说明刚才建令牌是成功的，
        New-Api-User 头已经就位；而回滚路径上任何一个多余的往返都可能再失败一次。
        """
        await self.login()
        client = await self._client()
        resp = await client.delete(f"/api/token/{token_id}")
        data = _json_of(resp, "new-api 删除令牌")
        if not data.get("success"):
            raise UpstreamError("new-api 删除令牌失败：" + str(data.get("message") or resp.status_code))
        logger.info("new-api 已删除令牌 %s", token_id)
        return True

    async def delete_token_quietly(self, token_id: str) -> None:
        """删令牌但只记日志：回滚失败不该盖住真正的错误。"""
        try:
            await self.delete_token(token_id)
        except Exception as exc:
            logger.error("回滚删除令牌 %s 失败：%s", token_id, exc)

    async def list_tokens(self, page_size: int = 100) -> list[dict[str, Any]]:
        """列出后台所有令牌（只读，供对账 / 维护脚本用）。"""
        await self.login()
        await self.ensure_user_id()
        client = await self._client()
        resp = await client.get("/api/token/", params={"p": 1, "page_size": page_size})
        data = _json_of(resp, "new-api 令牌列表")
        items = ((data.get("data") or {}).get("items")) or []
        return [item for item in items if isinstance(item, dict)]

    async def rename_token(self, token_id: str, name: str) -> bool:
        """改令牌名，返回是否改成功。

        **必须整条记录回传**：new-api 的 PUT 是「整体替换」而不是「局部更新」，
        只带 id + name 会把额度、模型限制、分组全部冲成默认值 —— 那等于
        把买家已付费的额度清零。所以先 GET 完整记录，改掉 name 再 PUT 回去，
        并且刻意不带 key / DeletedAt 这两个字段。
        """
        await self.login()
        await self.ensure_user_id()
        client = await self._client()

        resp = await client.get(f"/api/token/{token_id}")
        data = _json_of(resp, "new-api 读令牌")
        current = data.get("data") or {}
        if not isinstance(current, dict) or not current:
            raise UpstreamError("读不到令牌 " + str(token_id))

        payload = {key: value for key, value in current.items() if key not in ("key", "DeletedAt")}
        payload["name"] = fit_bytes(name)
        put = await client.put("/api/token/", json=payload)
        result = _json_of(put, "new-api 改令牌名")
        if not result.get("success"):
            raise UpstreamError("new-api 改令牌名失败：" + str(result.get("message") or put.status_code))
        logger.info("new-api 令牌 %s 改名为 %s", token_id, payload["name"])
        return True

    async def _find_token_id(self, name: str) -> str:
        """按名字找到刚建的令牌 id（列表按创建时间倒序）。"""
        client = await self._client()
        resp = await client.get("/api/token/", params={"p": 1, "page_size": 20})
        data = _json_of(resp, "new-api 令牌列表")
        items = (((data.get("data") or {}).get("items")) or [])
        for item in items:
            if isinstance(item, dict) and item.get("name") == name and item.get("id"):
                return str(item["id"])
        raise UpstreamError("建完令牌但在列表里找不到它（可能被限流或名字重复）")

    async def _read_key(self, token_id: str) -> str:
        """取令牌明文 key，并补上 sk- 前缀。

        新版 new-api 的 /api/token/ 页面上展示的是 sk-xxx，但
        POST /api/token/:id/key 返回的**只有 48 位随机串，没有 sk-**。
        而鉴权时会 strings.TrimPrefix(key, "sk-") —— 两种都能过，但用户
        拿到的卡密应该和他在后台看到的一致，否则会以为 Key 是坏的。
        """
        client = await self._client()
        resp = await client.post(f"/api/token/{token_id}/key")
        data = _json_of(resp, "new-api 取 Key")
        key = ((data.get("data") or {}).get("key"))
        if not key:
            raise UpstreamError("拿不到令牌明文 Key")
        return normalize_key(str(key))


class CardShopClient:
    """卡密站（new-api-cost）管理接口封装。"""

    def __init__(self, base_url: str, *, admin_token: str = "", username: str = "", password: str = "",
                 groups: int = 5) -> None:
        self.base_url = base_url.rstrip("/")
        self.admin_token = admin_token
        self.username = username
        self.password = password
        self.groups = groups
        self._http: httpx.AsyncClient | None = None
        self._logged_in = False

    async def close(self) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(
                **DIRECT_HTTP,
                base_url=self.base_url,
                timeout=httpx.Timeout(10.0, connect=5.0),
                headers={"user-agent": "xianyu-auto-reply/0.1"},
                follow_redirects=True,
                limits=httpx.Limits(max_keepalive_connections=10, keepalive_expiry=90.0),
            )
        return self._http

    async def _headers(self) -> dict[str, str]:
        if self.admin_token:
            return {"Authorization": "Bearer " + self.admin_token}
        return {}

    async def login(self) -> None:
        """没配 Bearer 时用后台账号密码登录拿会话 Cookie。"""
        if self.admin_token or self._logged_in:
            return
        if not (self.username and self.password):
            raise UpstreamError("卡密站未配置 ADMIN_TOKEN，也没有账号密码")
        client = await self._client()
        resp = await client.post(
            "/api/admin/login",
            json={"username": self.username, "password": self.password},
        )
        data = _json_of(resp, "卡密站登录")
        if not data.get("ok"):
            raise UpstreamError("卡密站登录失败：" + str(data.get("error") or resp.status_code))
        self._logged_in = True
        logger.info("已登录卡密站后台")

    @staticmethod
    def _session_expired(resp: httpx.Response) -> bool:
        """卡密站用 200 + {"error": "未登录"} 表示 Cookie 会话已失效。"""
        if resp.status_code == 401:
            return True
        try:
            data = resp.json()
        except ValueError:
            return False
        return isinstance(data, dict) and str(data.get("error") or "").strip().startswith("未登录")

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """带后台会话请求；会话过期时重登一次，再原样重试。

        只在服务端明确拒绝认证时重试，不重试超时或 5xx，避免重复建卡。
        """
        await self.login()
        client = await self._client()
        headers = dict(kwargs.pop("headers", None) or {})
        headers.update(await self._headers())
        for attempt in range(2):
            resp = await client.request(method, path, headers=headers, **kwargs)
            if self.admin_token or attempt or not self._session_expired(resp):
                return resp
            logger.warning("卡密站登录会话已过期，正在重新登录")
            self._logged_in = False
            client.cookies.clear()
            await self.login()
        raise AssertionError("unreachable")

    async def create_card(self, reward_code: str, quota_usd: float, label: str) -> str:
        """把一个 new-api 额度兑换码灌成一张卡密，返回卡密明文。

        ⚠️ 卡密站改版了（2025-10）：**一张卡密 = 一个 new-api 兑换码**，1:1。
        旧字段 key / count / maxRedeems 现在会直接 400 ——
        卡密内容也不再是任意文本，只能是兑换码本身。

        兑换码是一次性的，所以站里还加了 reward_hash 唯一约束：
        同一个码绑第二张卡会被拒（409），这正是我们想要的防重复。
        """
        code = str(reward_code or "").strip()
        if not code:
            raise UpstreamError("兑换码不能为空")
        label = (label or "")[:100]
        payload = {
            "code": code,
            "label": label,
            "quotaUsd": quota_usd,
            "groups": self.groups,
        }
        resp = await self._request("POST", "/api/admin/cards", json=payload)
        data = _json_of(resp, "卡密站建卡")
        if not data.get("ok"):
            error = str(data.get("error") or resp.status_code)
            # 有的卡密站版本会先写卡、再因会话校验返回失败。下一次请求会撞
            # reward_hash 唯一约束；此时服务端返回的卡 id 就是本单的产出，
            # 取回这张卡继续发货，不能永远重复创建，也不能换一个码重复给额度。
            duplicate = re.search(r"已经绑定到卡密\s*#(\d+)", error)
            if duplicate:
                for card in await self.list_cards():
                    existing_code = str(card.get("code") or "").strip()
                    if str(card.get("id") or "") == duplicate.group(1) and existing_code:
                        logger.info("卡密站建卡此前已生效，复用已有卡密 #%s", duplicate.group(1))
                        return existing_code
            raise UpstreamError("卡密站建卡失败：" + error)
        created = data.get("created") or []
        if not created or not created[0].get("code"):
            raise UpstreamError("卡密站建卡成功但没返回卡密")
        return str(created[0]["code"])


    async def list_cards(self, page_size: int = 200) -> list[dict[str, Any]]:
        """列出所有卡密（只读，供对账 / 维护脚本用）。"""
        resp = await self._request(
            "GET", "/api/admin/cards", params={"page": 1, "pageSize": page_size},
        )
        data = _json_of(resp, "卡密站卡密列表")
        items = (((data.get("list") or {}).get("items")) or [])
        return [item for item in items if isinstance(item, dict)]

    async def can_rename_card(self) -> bool:
        """这个卡密站支不支持改 label。

        只在维护脚本里用：拿一个必然不存在的 id 去探 PATCH，
        405 = 没有这个路由，404 = 有路由只是没这张卡。
        """
        resp = await self._request("PATCH", "/api/admin/cards/0", json={"label": "__probe__"})
        return resp.status_code != 405

    async def rename_card(self, card_id: str | int, label: str) -> bool:
        """改卡密的后台标签（label）。卡密本身和额度都不受影响。

        老版本卡密站没有这个接口（返回 405），那种情况抛 UpstreamError 让调用方
        决定是跳过还是提示手工处理。
        """
        resp = await self._request(
            "PATCH", f"/api/admin/cards/{card_id}", json={"label": (label or "")[:100]},
        )
        if resp.status_code == 405:
            raise UpstreamError("这个卡密站不支持改标签")
        data = _json_of(resp, "卡密站改标签")
        if not data.get("ok"):
            raise UpstreamError("卡密站改标签失败：" + str(data.get("error") or resp.status_code))
        return True


class Provisioner:
    """把「额度」变成「卡密 / 充值到已有 Key」的入口，供发货流程调用。"""

    @property
    def can_issue_cards(self) -> bool:
        """卡密站配好了才能走「建 Key + 出卡密」；充值模式不需要卡密站。"""
        return cardshop_ready()

    def __init__(self) -> None:
        self.newapi = NewApiClient(
            settings.newapi_base_url,
            access_token=settings.newapi_session,
            username=settings.newapi_username,
            password=settings.newapi_password,
            group=settings.newapi_group,
            quota_per_unit=settings.newapi_quota_per_unit,
            user_id=settings.newapi_user_id,
            model_limits=resolved_model_limits(),
        )
        self.cardshop = CardShopClient(
            settings.cardshop_base_url,
            admin_token=settings.cardshop_admin_token,
            username=settings.cardshop_username,
            password=settings.cardshop_password,
            groups=settings.cardshop_groups,
        )

    async def close(self) -> None:
        await self.newapi.close()
        await self.cardshop.close()

    async def warmup(self) -> None:
        """启动时先把两条上游的连接建好（DNS + TLS + 登录）。

        不做这一步的话，第一单要额外多等一次完整握手（几百毫秒）。
        失败无所谓 —— 真发货时该报错还是会报错。
        """
        async def touch_newapi() -> None:
            try:
                # 先把凭据核对一遍（登录 / user id / token），配置错了现在就说，
                # 别等买家下单时才在日志里喊「建卡密失败」
                await self.newapi.login()
                await self.newapi.ensure_user_id()
                await self.newapi.quota_per_unit()
                client = await self.newapi._client()
                await client.get("/api/status")
            except UpstreamError as exc:
                logger.error("new-api 凭据有问题，自动发货会全部失败：%s", exc)
            except Exception as exc:
                logger.info("new-api 预热失败（不影响后续发货）：%s", exc)

        async def touch_cardshop() -> None:
            try:
                client = await self.cardshop._client()
                await client.get("/")
            except Exception as exc:
                logger.info("卡密站预热失败（不影响后续发货）：%s", exc)

        tasks = [touch_newapi()]
        if self.can_issue_cards:
            tasks.append(touch_cardshop())
        await asyncio.gather(*tasks)

    @staticmethod
    def _check_quota(quota_usd: float) -> None:
        if quota_usd <= 0:
            raise UpstreamError("额度必须大于 0")
        if quota_usd > 1_000_000:
            raise UpstreamError("换算后的额度过大（" + str(quota_usd) + "），请检查倍率配置")

    def _token_name(self, label: str, username: str) -> str:
        order_hint = order_id_from_label(label)
        return build_token_name(username, order_hint, prefix=settings.newapi_token_prefix or "")

    async def open_key(self, quota_usd: float, label: str, username: str = "",
                       model_limits: str | None = None) -> Provisioned:
        """只建一个新的令牌（不建卡密）。

        两种情况用得上：
          * 「充值到已有 Key」模式的首单：买家还没有 Key，先给他开一个并绑定；
          * 后台补开 / 维护脚本。
        """
        self._check_quota(quota_usd)
        name = self._token_name(label, username)
        token_id, api_key = await self.newapi.create_token(
            quota_usd, name, model_limits=model_limits
        )
        try:
            currency = (await self.newapi.site_quota_model()).currency
        except Exception:
            currency = "USD"
        return Provisioned(
            card_code="", key_mask=_mask(api_key), quota_usd=quota_usd, token_id=token_id,
            api_key=api_key, balance=round(quota_usd, 2), currency=currency, token_name=name,
        )

    async def provision(self, quota_usd: float, label: str, username: str = "",
                        model_limits: str | None = None) -> Provisioned:
        """额度 → 建令牌 → 建卡密。任一步失败抛 UpstreamError。

        ⚠️ 这条路**现在走不通了**：卡密站改版后只接受 new-api 额度兑换码，
        不再接受 API Key（连 sk- 都会明确拒收）。所以卡密这一步必然报错，
        报错文案（「额度兑换码不能是 API Key」）会原样进订单详情 ——
        比我们猜一句更准确。建出来的令牌在这一层回滚，不会留下孤儿 Key。
        要发 Key 的场景请改用「兑换码」或「充值到已有 Key」模式。
        """
        if not self.can_issue_cards:
            raise UpstreamError("卡密站没有配置，无法生成卡密；可以把规则改成「充值到已有 Key」")
        opened = await self.open_key(quota_usd, label, username, model_limits=model_limits)
        try:
            code = await self.cardshop.create_card(opened.api_key, quota_usd, label)
        except Exception:
            # 卡密没建成（不管是接口报错还是网络异常），都要把刚建的令牌删掉，
            # 否则会留下一个没人知道却能用、还占着额度的 Key。
            await self._revoke_token(opened.token_id)
            raise
        opened.card_code = code
        return opened

    async def find_tokens_for_orders(self, order_ids: list[str]) -> dict[str, dict[str, Any]]:
        """按订单号找回早期订单对应的令牌（老客户迁移用）。"""
        return await self.newapi.find_tokens_for_orders(order_ids)

    async def find_token_by_key(self, api_key: str) -> dict[str, Any]:
        """按明文 Key 反查本站令牌记录。

        买家「直接发 Key 充值」时用它区分「本站但不是他买的」和「根本不是本站的」，
        好回一句准确的话。令牌列表不返回明文 Key 的站点会抛 UpstreamError。
        """
        return await self.newapi.find_token_by_key(api_key)

    async def token_key(self, token_id: str) -> str:
        """读令牌明文 Key；读不到返回空串（不抛异常）。"""
        return await self.newapi.read_token_key(token_id)

    async def list_tokens(self, page_size: int = 100) -> list[dict[str, Any]]:
        """列出本站全部令牌（只读）。

        买家直接发 Key 时，列表里的 Key 可能被打码，但令牌名 / id 还能用：
        先按名字缩小范围，再逐个读明文核对。
        """
        return await self.newapi.list_all_tokens(page_size=page_size)

    async def topup(self, quota_usd: float, *, token_id: str = "",
                    api_key: str = "") -> Provisioned:
        """把额度充到买家已有的令牌上；只给了 Key 时先反查令牌。"""
        self._check_quota(quota_usd)
        token_id = str(token_id or "").strip()
        if not token_id:
            if not api_key:
                raise UpstreamError("既没有令牌 ID 也没有 Key，无法充值")
            token = await self.newapi.find_token_by_key(api_key)
            token_id = str(token.get("id") or "").strip()
            if not token_id:
                raise UpstreamError("按 Key 反查到的令牌没有 id，无法充值")
            api_key = api_key or normalize_key(str(token.get("key") or ""))
        info = await self.newapi.topup_token(token_id, quota_usd, api_key=api_key)
        return Provisioned(
            card_code="", key_mask=info.key_mask, quota_usd=info.quota_usd, token_id=token_id,
            api_key=info.api_key, balance=info.balance, currency=info.currency,
            token_name=info.token_name, topped_up=info.topped_up,
        )

    # ------------------------------------------------------------ 账号

    async def create_account(self, username: str, password: str) -> AccountInfo:
        """在 new-api 上开一个普通账号（不加额度）。"""
        user = await self.newapi.create_user(username, password)
        user_id = str(user.get("id") or "").strip()
        if not user_id:
            raise UpstreamError("new-api 返回的账号没有 id，无法继续加额度")
        try:
            currency = (await self.newapi.site_quota_model()).currency
        except Exception:
            currency = "USD"
        return AccountInfo(
            user_id=user_id,
            username=str(user.get("username") or username),
            password=password,
            balance=await self.newapi.user_balance(user),
            currency=currency,
            created=True,
        )

    async def topup_account(self, user_id: str, quota_usd: float) -> AccountInfo:
        """把额度加到买家已有的账号上。"""
        self._check_quota(quota_usd)
        user = await self.newapi.add_user_quota(user_id, quota_usd)
        try:
            currency = (await self.newapi.site_quota_model()).currency
        except Exception:
            currency = "USD"
        return AccountInfo(
            user_id=str(user.get("id") or user_id),
            username=str(user.get("username") or ""),
            quota_usd=quota_usd,
            balance=await self.newapi.user_balance(user),
            currency=currency,
        )

    async def find_account(self, username: str) -> dict[str, Any] | None:
        """按账号名找人（只读，给「老用户提供账号」那一步核对用）。"""
        return await self.newapi.find_user(username)

    async def find_account_by_id(self, user_id: str) -> dict[str, Any]:
        """按 id 读账号（只读）。"""
        return await self.newapi.get_user(user_id)

    async def account_balance(self, user: dict[str, Any]) -> tuple[float | None, str]:
        """账号余额和它用的是哪种展示货币。"""
        try:
            currency = (await self.newapi.site_quota_model()).currency
        except Exception:
            currency = "USD"
        return await self.newapi.user_balance(user), currency

    async def redeem_codes(self, quota_usd: float, label: str, *, count: int = 1,
                           valid_days: int = 0) -> list[str]:
        """按「这一单该给多少额度」生成 new-api 兑换码。

        quota_usd 是**展示货币**的额度（10 元订单 = 10），这里换算成 quota 单位；
        valid_days=0 表示永不过期。
        """
        self._check_quota(quota_usd)
        quota = await self.newapi.quota_for_amount(quota_usd)
        if quota <= 0:
            raise UpstreamError("换算后的兑换码额度为 0，请检查金额 / 倍率配置")
        expired = 0
        if valid_days > 0:
            expired = int(time.time()) + int(valid_days) * 86400
        return await self.newapi.create_redemptions(
            quota, label, count=count, expired_time=expired
        )

    async def issue_card(self, reward_code: str, quota_usd: float, label: str) -> str:
        """把 new-api 额度兑换码灌成一张卡密并返回卡密串。

        卡密站只收兑换码了（不再收任意文本），所以「账号密码灌卡密」那条老路
        在这里必然失败 —— 账号模式的调用方会退化成「直接把账号密码发给买家」。
        """
        if not self.can_issue_cards:
            raise UpstreamError("卡密站没有配置，无法生成卡密")
        return await self.cardshop.create_card(reward_code, quota_usd, label)

    async def _revoke_token(self, token_id: str) -> None:
        """卡密没建成时把刚建的令牌删掉（失败只记日志，不影响原本要抛的错）。"""
        await self.newapi.delete_token_quietly(token_id)


#: new-api 用 Go 的 len() 校验令牌名，单位是**字节**不是字符。
#: 「示例买家昵称八#<19位订单号>」= 44 字节，刚好能放下；
#: 名字再长就优先截短买家用户名，订单号始终保留完整。
TOKEN_NAME_MAX_BYTES = 50


def fit_bytes(text: str, limit: int = TOKEN_NAME_MAX_BYTES) -> str:
    """按 UTF-8 字节数截断，绝不切坏多字节字符。"""
    encoded = text.encode("utf-8")
    if len(encoded) <= limit:
        return text
    # 逐字符累加，超过就停 —— 简单但绝不会产出半个汉字
    out: list[str] = []
    used = 0
    for ch in text:
        size = len(ch.encode("utf-8"))
        if used + size > limit:
            break
        out.append(ch)
        used += size
    return "".join(out)


def order_id_from_label(label: str) -> str:
    """从卡密名称里取出订单号（名称形如「买家用户名#2503…」）。"""
    parts = (label or "").split("#")
    return parts[-1].strip() if len(parts) >= 2 else ""


def _clean_name_part(text: str) -> str:
    """清理令牌名里的一段：去控制字符、合并空白、把 # 换成全角，避免和分隔符打架。"""
    return " ".join(str(text or "").replace("#", "＃").split())


#: 卡密名称上限（卡密站 label 字段 ≤ 120，接口层再截到 100，这里与之一致）
CARD_LABEL_MAX_CHARS = 100


def build_card_label(username: str, order_id: str) -> str:
    """拼卡密名称：`买家用户名#订单号`。

    名称里不放价格，也不放平台前缀 —— 卡密后台要认的是「哪个买家、哪一单」。
    超长时优先截短用户名，订单号一个字符都不动。
    """
    user = _clean_name_part(username) or "买家"
    order = _clean_name_part(order_id)
    if not order:
        return user[:CARD_LABEL_MAX_CHARS]
    tail = "#" + order
    full = user + tail
    if len(full) <= CARD_LABEL_MAX_CHARS:
        return full
    budget = CARD_LABEL_MAX_CHARS - len(tail)
    if budget > 0:
        return user[:budget].rstrip() + tail
    # 订单号本身就超限（现实中不会发生）：只保留尾部，至少还能对上号
    return "#" + order[-(CARD_LABEL_MAX_CHARS - 1):]


#: new-api 兑换码名称上限：1~20 个字（rune）。比卡密标签短得多。
REDEEM_NAME_MAX_CHARS = 20


def build_redeem_label(username: str, order_id: str) -> str:
    """拼兑换码名称：「买家名#订单号后8位」。

    new-api 的名称只有 20 个字，而一个闲鱼订单号就 19 位，原样塞进去必然超限
    （直接复用 build_card_label 会被后台回「长度必须 1-20」）。对账主要靠
    「谁的、哪一单」，所以留昵称前 6 个字 + 订单号后 8 位：尾部足够在订单列表里
    定位，昵称用来肉眼扫。
    """
    user = _clean_name_part(username) or "买家"
    order = _clean_name_part(order_id)
    if len(user) > 6:
        user = user[:6]
    tail = ("#" + order[-8:]) if order else ""
    return (user + tail)[:REDEEM_NAME_MAX_CHARS]


def build_token_name(username: str, order_id: str, prefix: str = "") -> str:
    """拼一个既不超 50 字节、又能一眼认出是哪个买家的令牌名。

    规则：`[前缀]买家用户名#订单号`。价格不再进名字 —— 在 new-api 的令牌列表里
    金额没有辨识度，买家用户名才是客诉/对账时要找的东西。

    超限时优先截短用户名，保住完整订单号；订单号本身异常长时只留后 10 位。
    """
    user = _clean_name_part(username) or "买家"
    order = _clean_name_part(order_id)
    head = prefix or ""
    if not order:
        return fit_bytes(head + user)

    tail = "#" + order
    full = head + user + tail
    if len(full.encode("utf-8")) <= TOKEN_NAME_MAX_BYTES:
        return full

    # 先砍用户名，订单号一个字符都不动
    budget = TOKEN_NAME_MAX_BYTES - len(head.encode("utf-8")) - len(tail.encode("utf-8"))
    if budget > 0:
        user = fit_bytes(user, budget).rstrip()
        return head + user + tail

    # 前缀 + 订单号本身就超限：退回订单号后 10 位
    short_tail = "#" + order[-10:]
    budget = TOKEN_NAME_MAX_BYTES - len(head.encode("utf-8")) - len(short_tail.encode("utf-8"))
    if budget > 0:
        user = fit_bytes(user, budget).rstrip()
        return head + user + short_tail
    return fit_bytes(head + short_tail)


def normalize_model_limits(raw: str) -> str:
    """把「逗号 / 中文逗号 / 空白」分隔的模型名整理成 new-api 要的逗号串。

    去空、去重，顺序保持不变。new-api 的 model_limits 字段是**逗号分隔字符串**，
    空串表示不限制 —— 所以调用方必须同时设置 model_limits_enabled。
    """
    text = (raw or "").replace("，", ",")
    models: list[str] = []
    for chunk in text.split(","):
        for name in chunk.split():
            if name and name not in models:
                models.append(name)
    return ",".join(models)


#: new-api 页面展示与客户端连接时的统一前缀
KEY_PREFIX = "sk-"


def normalize_key(key: str) -> str:
    """统一成带 sk- 的形式。

    new-api 的接口返回裸 key（48 位随机串），页面却显示 sk-xxx。
    发给买家的必须是后者 —— 他拿去填客户端时，教程里都是 sk- 开头。
    已经带前缀的就不重复加。
    """
    text = (key or "").strip()
    if not text:
        return text
    if text.lower().startswith(KEY_PREFIX):
        return text
    return KEY_PREFIX + text


def _mask(key: str) -> str:
    """打码展示：保留 sk- + 前 4 位，末尾 4 位。"""
    text = str(key or "").strip()
    if not text:
        return ""
    if len(text) <= 12:
        return text[:3] + "***"
    return text[:7] + "…" + text[-4:]


def mask_key(key: str) -> str:
    """公开的打码函数（后台展示 Key 用）。"""
    return _mask(key)


def token_expired(token: dict[str, Any]) -> bool:
    """公开的过期判断（后台绑定前校验用）。"""
    return _token_expired(token)


def _int_or(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _token_expired(token: dict[str, Any]) -> bool:
    """令牌是否过期。new-api 用 -1 表示永不过期；有的版本给毫秒。"""
    raw = token.get("expired_time")
    if raw is None or isinstance(raw, bool):
        return False
    try:
        stamp = float(raw)
    except (TypeError, ValueError):
        return False
    if stamp <= 0:
        return False
    if stamp > 1e12:  # 毫秒时间戳
        stamp /= 1000.0
    return stamp < time.time()


def _is_scope_denied(lowered: str, message: str) -> bool:
    """new-api 的「令牌作用域不够」错误。

    形如：{"code":"ACCESS_TOKEN_SCOPE_DENIED","message":"The access token lacks
    the required permission: profile:read","message_key":"The access token lacks
    the required permission: {{scope}}"}。
    文案可能随版本变，所以认 code / message_key / 关键字三种写法。
    """
    return (
        "access_token_scope_denied" in lowered
        or "lacks the required permission" in lowered
        or "profile:read" in lowered
        or "access token lacks" in lowered
    )


def _json_of(resp: httpx.Response, what: str) -> dict[str, Any]:
    try:
        data = resp.json()
    except ValueError as exc:
        raise UpstreamError(what + "返回非 JSON（HTTP " + str(resp.status_code) + "）") from exc
    if not isinstance(data, dict):
        raise UpstreamError(what + "返回结构异常")
    return data
