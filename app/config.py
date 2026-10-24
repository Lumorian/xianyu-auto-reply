"""运行期配置，全部来自环境变量。"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent


def _load_dotenv(path: Path) -> None:
    """把 .env 里的键值对塞进 os.environ（已存在的变量优先）。"""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_dotenv(BASE_DIR / ".env")


#: new-api 新建令牌默认只放行的模型。改这里等于改所有没显式配置的部署。
DEFAULT_MODEL_LIMITS = "deepseek-v4.1-flash"


def _float(name: str, default: float) -> float:
    """读一个浮点配置；没配或者填错就用默认值。"""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _int(name: str, default: int) -> int:
    """读一个整数配置；没配、填空或者填错都退回默认值。

    以前这里直接 int(os.getenv(...))，一个 XY_PORT=abc 就会让整个进程
    在 import 期崩掉，而报错里只有 invalid literal for int()，
    看不出是哪个变量写错了。
    """
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default


@dataclass(slots=True)
class Settings:
    """进程级配置。

    所有字段都可以通过 XY_ 前缀的环境变量覆盖，方便 Docker / systemd 部署。
    """

    db_path: Path = field(default_factory=lambda: Path(os.getenv("XY_DB_PATH", str(BASE_DIR / "data" / "app.db"))))
    host: str = field(default_factory=lambda: os.getenv("XY_HOST", "127.0.0.1"))
    port: int = field(default_factory=lambda: _int("XY_PORT", 8000))
    admin_token: str = field(default_factory=lambda: os.getenv("XY_ADMIN_TOKEN", ""))
    #: 没配 XY_ADMIN_TOKEN 时，是否允许非本机来源访问管理接口。
    #: 默认 False：管理接口能改 Cookie、读卡密、删规则，绝不能默认敞开。
    admin_allow_remote: bool = field(default_factory=lambda: _bool("XY_ADMIN_ALLOW_REMOTE", False))
    log_level: str = field(default_factory=lambda: os.getenv("XY_LOG_LEVEL", "INFO"))
    #: 日志文件路径。留空则只输出到控制台（交给 systemd / docker 收走）
    log_file: str = field(default_factory=lambda: os.getenv("XY_LOG_FILE", "./logs/server.log"))
    #: 单个日志文件上限（字节），默认 10 MB
    log_max_bytes: int = field(default_factory=lambda: _int("XY_LOG_MAX_BYTES", 10 * 1024 * 1024))
    #: 保留几份历史日志，默认 5
    log_backup_count: int = field(default_factory=lambda: _int("XY_LOG_BACKUP_COUNT", 5))

    # 发货行为
    delivery_cooldown: int = field(default_factory=lambda: _int("XY_DELIVERY_COOLDOWN", 600))
    send_retry: int = field(default_factory=lambda: _int("XY_SEND_RETRY", 2))
    dry_run: bool = field(default_factory=lambda: _bool("XY_DRY_RUN", False))

    # 上游联动 new-api（按订单金额自动建 Key）
    newapi_base_url: str = field(default_factory=lambda: os.getenv("XY_NEWAPI_BASE_URL", ""))
    newapi_username: str = field(default_factory=lambda: os.getenv("XY_NEWAPI_USERNAME", ""))
    newapi_password: str = field(default_factory=lambda: os.getenv("XY_NEWAPI_PASSWORD", ""))
    newapi_session: str = field(default_factory=lambda: os.getenv("XY_NEWAPI_ACCESS_TOKEN", ""))
    newapi_group: str = field(default_factory=lambda: os.getenv("XY_NEWAPI_GROUP", ""))
    # 令牌名 = [前缀]买家用户名#订单号；默认不加前缀。想加就填，比如「闲鱼-」。
    newapi_token_prefix: str = field(default_factory=lambda: os.getenv("XY_NEWAPI_TOKEN_PREFIX", ""))
    # 新建令牌只允许使用的模型（逗号分隔）。默认只放行 deepseek-v4.1-flash，
    # 避免买家拿 Key 去调别的贵模型；留空 = 不限制（老行为，慎用）。
    newapi_model_limits: str = field(default_factory=lambda: os.getenv("XY_NEWAPI_MODEL_LIMITS", DEFAULT_MODEL_LIMITS))
    # 多少元人民币换 1 美元额度。默认 1 = 订单 10 元发 10 美元额度（1:1，有意为之的让利）。
    # 想按真实汇率收窄就设成 7.3 之类的数字。
    cny_per_usd: float = field(default_factory=lambda: _float("XY_CNY_PER_USD", 1.0))
    # quota_per_unit 兜底值，正常会从 /api/status 读到真实值
    newapi_quota_per_unit: float = field(default_factory=lambda: _float("XY_NEWAPI_QUOTA_PER_UNIT", 0.0))
    # new-api 0.13.x 必填：所有后台接口都要带 New-Api-User（用户 id）。留空会在登录后自动探测。
    newapi_user_id: str = field(default_factory=lambda: os.getenv("XY_NEWAPI_USER_ID", ""))

    # 上游联动 new-api-cost（把 Key 换成卡密）
    cardshop_base_url: str = field(default_factory=lambda: os.getenv("XY_CARDSHOP_BASE_URL", "https://shop.lumorian.org"))
    cardshop_admin_token: str = field(default_factory=lambda: os.getenv("XY_CARDSHOP_ADMIN_TOKEN", ""))
    cardshop_username: str = field(default_factory=lambda: os.getenv("XY_CARDSHOP_USERNAME", ""))
    cardshop_password: str = field(default_factory=lambda: os.getenv("XY_CARDSHOP_PASSWORD", ""))
    cardshop_groups: int = field(default_factory=lambda: _int("XY_CARDSHOP_GROUPS", 5))

    # WebSocket 心跳间隔（秒）。对齐网页端：首跳立即发，之后每 10 秒一次。
    heartbeat_interval: float = field(default_factory=lambda: _float("XY_HEARTBEAT_INTERVAL", 10.0))
    # ackDiff 同步位点重发间隔（秒）。0 = 只发一次（不推荐）。
    # 增量推送是服务端对 ackDiff 的响应，只发一次就基本收不到新消息。
    sync_poll_interval: float = field(default_factory=lambda: _float("XY_SYNC_POLL_INTERVAL", 30.0))
    # /reg 与 ackDiff 之间的间隔（秒）。紧凑连发会让注册在服务端不生效，
    # 表现为「能连上、能收框架帧、但永远收不到私信正文」。
    register_gap: float = field(default_factory=lambda: _float("XY_REGISTER_GAP", 1.0))

    # 会话列表轮询间隔（秒）。0 = 关闭。
    # 私信正文不一定会走 WebSocket 推送（线上实测经常只有「未读标记」），
    # 所以另外按这个节奏拉一次「最近会话」，把最后一条消息当新消息处理。
    message_poll_interval: float = field(default_factory=lambda: _float("XY_MESSAGE_POLL_INTERVAL", 20.0))
    # 每次拉多少个会话（1~50）
    message_poll_limit: int = field(default_factory=lambda: _int("XY_MESSAGE_POLL_LIMIT", 20))
    # 发现会话有新消息时，往回拉多少条历史（1~50）。
    # 只看「最后一条」会漏掉连发或多条之间的系统消息，所以按会话补一段。
    message_history_limit: int = field(default_factory=lambda: _int("XY_MESSAGE_HISTORY_LIMIT", 20))
    # 每轮最多给几个会话补拉历史（每次是一趟 WS 往返）。0 = 永远只看最后一条。
    message_history_conversations: int = field(
        default_factory=lambda: _int("XY_MESSAGE_HISTORY_CONVERSATIONS", 5)
    )
    # 消息流水保留天数，0 = 永久保留。只清 messages 表：seen_messages / reply_hits
    # 删了会让「同一会话只回一次」失效（详见 Database.prune_history）。
    message_retention_days: int = field(
        default_factory=lambda: _int("XY_MESSAGE_RETENTION_DAYS", 30)
    )

    # 自动回复总开关。关掉后商品 / 关键词自动回复全部不生效（只发货）。
    auto_reply_enabled: bool = field(default_factory=lambda: _bool("XY_AUTO_REPLY", True))
    # 自动回复前先等几秒，避免「秒回」显得像机器人（也顺便错开连发）。0 = 立刻回。
    auto_reply_delay: float = field(default_factory=lambda: _float("XY_AUTO_REPLY_DELAY", 1.0))

    # 订单轮询间隔（秒）。0 = 关闭。IM 推送丢了时靠它兜底发货。
    # 这是「一直没有推送」时的兜底节奏；有推送时下面那项会把轮询提前叫醒，
    # 所以实际发货延迟通常是几秒，而不是这个值。
    order_poll_interval: int = field(default_factory=lambda: _int("XY_ORDER_POLL_INTERVAL", 30))
    # 收到任何 IM 推送就提前补一次订单轮询（秒级节流）。0 = 关闭这个加速。
    # 推送本身常解析不出内容，但「有推送」是「可能刚有人拍下」最便宜的信号。
    order_hint_poll_gap: float = field(default_factory=lambda: _float("XY_ORDER_HINT_POLL_GAP", 5.0))
    # 一轮轮询里同时处理多少张待发货订单。1 = 串行。
    order_poll_concurrency: int = field(default_factory=lambda: _int("XY_ORDER_POLL_CONCURRENCY", 3))
    # 订单详情缓存时长（秒）：补 itemId 与取金额共用同一次查询。0 = 不缓存。
    order_cache_ttl: float = field(default_factory=lambda: _float("XY_ORDER_CACHE_TTL", 30.0))
    # 1 = 把收到的每条原始消息负载写进日志（排查「为什么没发货」时打开）
    debug_payload: bool = field(default_factory=lambda: _bool("XY_DEBUG_PAYLOAD", False))

    # 订单金额单位：auto 会按数值大小猜（整数且很大时视为「分」），也可强制 yuan / fen
    order_amount_unit: str = field(default_factory=lambda: os.getenv("XY_ORDER_AMOUNT_UNIT", "auto"))
    # 单笔订单允许的最大金额（元），超过则不自动发货，防止标错价被刷
    max_order_cny: float = field(default_factory=lambda: _float("XY_MAX_ORDER_CNY", 0.0))

    # ---- 登录态保活（参考 xianyu-pilot）----
    # accessToken 进程内缓存时长（秒）。0 = 不缓存。缓存能显著减少重连时的
    # token 接口调用，也就少一次触发风控的机会。
    im_token_ttl: float = field(default_factory=lambda: _float("XY_IM_TOKEN_TTL", 4 * 3600))
    # _m_h5_tk 主动刷新间隔（秒），每次在 [min, max] 内随机，避免固定节奏像机器人。
    # 0 = 关闭主动刷新（仍然会在遇到「令牌过期」时被动刷新）。
    m_h5_tk_refresh_min: float = field(default_factory=lambda: _float("XY_MH5TK_REFRESH_MIN", 1.5 * 3600))
    m_h5_tk_refresh_max: float = field(default_factory=lambda: _float("XY_MH5TK_REFRESH_MAX", 2.5 * 3600))
    # Cookie 保活间隔（秒），0 = 关闭。调用 passport hasLogin.do 让登录态续期。
    session_keepalive_interval: float = field(
        default_factory=lambda: _float("XY_SESSION_KEEPALIVE_INTERVAL", 30 * 60)
    )

    # 连接参数（一般不需要改）
    ws_url: str = "wss://wss-goofish.dingtalk.com/"
    app_key: str = "34839810"
    im_app_key: str = "444e9908a51d1cb236a27862abc769c9"
    mtop_base: str = "https://h5api.m.goofish.com/h5"



#: 后台「设置」页可覆盖的配置项 → app_config 表里的 key。
#: 这些项在 .env 里仍然是默认值，但 web 后台保存过就以数据库为准。
MODEL_LIMITS_KEY = "newapi_model_limits"


def resolved_model_limits(overrides: dict[str, str] | None = None) -> str:
    """新建 Key 实际使用的模型白名单。

    优先级：后台「设置」页保存的值 > XY_NEWAPI_MODEL_LIMITS > DEFAULT_MODEL_LIMITS。
    留空 = 不限制模型（会打警告，等于主动关掉这层保护）。

    overrides 传入 app_config 的整张表；不传时直接读数据库（读不到就退回环境变量），
    这样没有 DB 会话的脚本也能用同一个口径。
    """
    if overrides is None:
        overrides = _config_overrides_from_db()
    if MODEL_LIMITS_KEY in overrides:
        return overrides[MODEL_LIMITS_KEY]
    return settings.newapi_model_limits


def _config_overrides_from_db() -> dict[str, str]:
    """同步读一次 app_config。仅在没有现成 dict 时兜底用（异步环境请传 overrides）。"""
    import sqlite3

    try:
        if not settings.db_path.exists():
            return {}
        conn = sqlite3.connect(settings.db_path)
        try:
            rows = conn.execute("SELECT key, value FROM app_config").fetchall()
        finally:
            conn.close()
        return {str(k): str(v) for k, v in rows}
    except sqlite3.Error:
        # 读不到配置不该让发货链路炸掉，退回环境变量
        return {}


def upstream_ready() -> bool:
    """new-api 与卡密站是否都配置齐全（缺一个都不能走自动建 Key）。"""
    newapi = bool(settings.newapi_base_url) and bool(
        settings.newapi_session or (settings.newapi_username and settings.newapi_password)
    )
    cardshop = bool(settings.cardshop_base_url) and bool(
        settings.cardshop_admin_token or (settings.cardshop_username and settings.cardshop_password)
    )
    return newapi and cardshop


settings = Settings()
