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
from dataclasses import dataclass
from typing import Any

import httpx

from .config import DEFAULT_MODEL_LIMITS, resolved_model_limits, settings

logger = logging.getLogger(__name__)

#: new-api 的 quota 与美元的换算兜底值（$1 = 500000 quota，见 common.QuotaPerUnit）
DEFAULT_QUOTA_PER_UNIT = 500_000.0


class UpstreamError(RuntimeError):
    """上游接口调用失败。"""


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
    """一次自动建号的产物。"""

    card_code: str
    key_mask: str
    quota_usd: float
    token_id: str


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
        try:
            client = await self._client()
            resp = await client.get("/api/status")
            data = ((_json_of(resp, "new-api 状态").get("data")) or {})
            if not isinstance(data, dict):
                data = {}

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

    async def create_token(self, quota_usd: float, name: str) -> tuple[str, str]:
        """建一个额度为 quota_usd 的令牌，返回 (token_id, 明文 key)。"""
        await self.login()
        await self.ensure_user_id()
        # 注意：这里传进来的 quota_usd 实际上是「人民币等值额度」的数，
        # 也是发给买家的「额度」展示值。真正的 remain_quota 必须按站点货币换算，
        # 否则站点用 CNY 展示时会多给一个汇率倍数。
        remain_quota = await self.quota_for_amount(quota_usd)
        if remain_quota <= 0:
            raise UpstreamError("换算后的额度为 0，请检查 XY_CNY_PER_USD / 订单金额")

        client = await self._client()
        model_limits = normalize_model_limits(self.model_limits)
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

    async def create_card(self, api_key: str, quota_usd: float, label: str) -> str:
        """用给定 Key 生成一张卡密，返回卡密明文。"""
        await self.login()
        label = (label or "")[:100]
        client = await self._client()
        payload = {
            "key": api_key,
            "count": 1,
            "label": label,
            "quotaUsd": quota_usd,
            "groups": self.groups,
            "maxRedeems": 1,
        }
        resp = await client.post("/api/admin/cards", json=payload, headers=await self._headers())
        data = _json_of(resp, "卡密站建卡")
        if not data.get("ok"):
            raise UpstreamError("卡密站建卡失败：" + str(data.get("error") or resp.status_code))
        created = data.get("created") or []
        if not created or not created[0].get("code"):
            raise UpstreamError("卡密站建卡成功但没返回卡密")
        return str(created[0]["code"])


    async def list_cards(self, page_size: int = 200) -> list[dict[str, Any]]:
        """列出所有卡密（只读，供对账 / 维护脚本用）。"""
        await self.login()
        client = await self._client()
        resp = await client.get(
            "/api/admin/cards", params={"page": 1, "pageSize": page_size},
            headers=await self._headers(),
        )
        data = _json_of(resp, "卡密站卡密列表")
        items = (((data.get("list") or {}).get("items")) or [])
        return [item for item in items if isinstance(item, dict)]

    async def can_rename_card(self) -> bool:
        """这个卡密站支不支持改 label。

        只在维护脚本里用：拿一个必然不存在的 id 去探 PATCH，
        405 = 没有这个路由，404 = 有路由只是没这张卡。
        """
        await self.login()
        client = await self._client()
        resp = await client.patch(
            "/api/admin/cards/0", json={"label": "__probe__"}, headers=await self._headers()
        )
        return resp.status_code != 405

    async def rename_card(self, card_id: str | int, label: str) -> bool:
        """改卡密的后台标签（label）。卡密本身和额度都不受影响。

        老版本卡密站没有这个接口（返回 405），那种情况抛 UpstreamError 让调用方
        决定是跳过还是提示手工处理。
        """
        await self.login()
        client = await self._client()
        resp = await client.patch(
            f"/api/admin/cards/{card_id}", json={"label": (label or "")[:100]},
            headers=await self._headers(),
        )
        if resp.status_code == 405:
            raise UpstreamError("这个卡密站不支持改标签")
        data = _json_of(resp, "卡密站改标签")
        if not data.get("ok"):
            raise UpstreamError("卡密站改标签失败：" + str(data.get("error") or resp.status_code))
        return True


class Provisioner:
    """把「额度」变成「卡密」的入口，供发货流程调用。"""

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

        await asyncio.gather(touch_newapi(), touch_cardshop())

    async def provision(self, quota_usd: float, label: str, username: str = "") -> Provisioned:
        """额度 → 建令牌 → 建卡密。任一步失败抛 UpstreamError。

        令牌名 = 买家用户名 + 订单号；卡密名称同样是「买家用户名#订单号」，
        两个后台都按「谁买的、哪一单」来认，价格不进名字。
        """
        order_hint = order_id_from_label(label)
        name = build_token_name(username, order_hint, prefix=settings.newapi_token_prefix or "")
        if quota_usd <= 0:
            raise UpstreamError("额度必须大于 0")
        if quota_usd > 1_000_000:
            raise UpstreamError("换算后的额度过大（" + str(quota_usd) + " USD），请检查倍率配置")
        token_id, api_key = await self.newapi.create_token(quota_usd, name)
        try:
            code = await self.cardshop.create_card(api_key, quota_usd, label)
        except Exception:
            # 卡密没建成（不管是接口报错还是网络异常），都要把刚建的令牌删掉，
            # 否则会留下一个没人知道却能用、还占着额度的 Key
            # 卡密没建成，就把刚建的令牌删掉，避免留下一个没人知道的可用 Key
            await self._revoke_token(token_id)
            raise
        return Provisioned(code, _mask(api_key), quota_usd, token_id)

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
    if len(key) <= 12:
        return key[:3] + "***"
    return key[:7] + "…" + key[-4:]


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
