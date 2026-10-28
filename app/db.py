"""SQLite 存储层。

故意保持轻量：单文件 SQLite + aiosqlite，部署不需要额外中间件。
所有写操作都是显式的，方便在测试里把 DB 换成临时文件。
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import aiosqlite

from .secrets import CookieVault, is_encrypted

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    cookie TEXT NOT NULL DEFAULT '',
    enabled INTEGER NOT NULL DEFAULT 1,
    remark TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    item_id TEXT NOT NULL DEFAULT '*',
    enabled INTEGER NOT NULL DEFAULT 1,
    mode TEXT NOT NULL DEFAULT 'text',
    content TEXT NOT NULL DEFAULT '',
    auto_confirm INTEGER NOT NULL DEFAULT 0,
    usd_multiplier REAL NOT NULL DEFAULT 1,
    fixed_usd REAL NOT NULL DEFAULT 0,
    priority INTEGER NOT NULL DEFAULT 0,
    remark TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cards (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id TEXT NOT NULL DEFAULT '*',
    content TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'available',
    order_id TEXT,
    buyer_id TEXT,
    sold_at TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cards_pool ON cards (status, item_id);

CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    order_id TEXT,
    item_id TEXT,
    buyer_id TEXT,
    buyer_name TEXT,
    rule_id INTEGER,
    card_id INTEGER,
    content TEXT,
    amount_cny REAL,
    quota_usd REAL,
    api_key_id TEXT,
    card_code TEXT,
    status TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_dedup ON orders (account_id, order_id) WHERE order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_orders_created ON orders (created_at DESC);
-- 充值模式按「买家提供的旧订单号」反查归属：不带 account_id 也能走索引
CREATE INDEX IF NOT EXISTS idx_orders_order_buyer ON orders (order_id, buyer_id, status);
-- 按买家找「等旧订单号 / 最近已发货」的单
CREATE INDEX IF NOT EXISTS idx_orders_buyer ON orders (account_id, buyer_id, status, id DESC);
-- 后台用历史成交价提示「这个商品有哪些价格规格」
CREATE INDEX IF NOT EXISTS idx_orders_item_amount ON orders (item_id, amount_cny);

CREATE TABLE IF NOT EXISTS reply_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'keyword',
    item_id TEXT NOT NULL DEFAULT '*',
    keywords TEXT NOT NULL DEFAULT '',
    match_type TEXT NOT NULL DEFAULT 'contains',
    content TEXT NOT NULL DEFAULT '',
    enabled INTEGER NOT NULL DEFAULT 1,
    once_per_chat INTEGER NOT NULL DEFAULT 1,
    cooldown INTEGER NOT NULL DEFAULT 60,
    priority INTEGER NOT NULL DEFAULT 0,
    remark TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- 一条自动回复规则里的「关键词 -> 回复」条目。
-- 规则本身只管商品范围 / 匹配方式 / 频率；具体回什么话都在这里，一条规则可以有多条。
CREATE TABLE IF NOT EXISTS reply_rule_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id INTEGER NOT NULL,
    keywords TEXT NOT NULL DEFAULT '',
    content TEXT NOT NULL DEFAULT '',
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_reply_rule_entries_rule
    ON reply_rule_entries (rule_id, sort_order, id);

CREATE TABLE IF NOT EXISTS seen_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    msg_key TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_seen_messages ON seen_messages (account_id, msg_key);

CREATE TABLE IF NOT EXISTS reply_hits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    sender_id TEXT NOT NULL DEFAULT '',
    chat_key TEXT NOT NULL DEFAULT '',
    rule_id INTEGER NOT NULL,
    -- 命中的具体条目：一条规则里每个回复各自去重 / 冷却
    entry_id INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_reply_hits_dedup ON reply_hits (account_id, sender_id, chat_key, rule_id);
CREATE INDEX IF NOT EXISTS idx_reply_hits_rule ON reply_hits (account_id, sender_id, rule_id, created_at);
-- entry_id 的索引在 _apply_migrations 里等列补齐后再建：
-- 老库的 reply_hits 还没有这一列，SCHEMA 执行时直接建会报 no such column。

CREATE TABLE IF NOT EXISTS app_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
);

-- 买家已经绑定的 Key：发货模式 topup 直接充值到这里，不再每单新建。
-- buyer_id 是闲鱼用户 ID（跨账号全局唯一），一个买家只认一个 Key；
-- token_id 也唯一，避免同一个令牌被两个买家同时认领。
CREATE TABLE IF NOT EXISTS buyer_keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    buyer_id TEXT NOT NULL,
    buyer_name TEXT NOT NULL DEFAULT '',
    account_id INTEGER,
    token_id TEXT NOT NULL DEFAULT '',
    token_name TEXT NOT NULL DEFAULT '',
    api_key TEXT NOT NULL DEFAULT '',
    key_mask TEXT NOT NULL DEFAULT '',
    -- 最近一次读到的余额（站点展示货币）
    balance REAL,
    -- 累计充值额度（站点展示货币，运营参考）
    total_usd REAL NOT NULL DEFAULT 0,
    topup_count INTEGER NOT NULL DEFAULT 0,
    -- manual 后台手工绑定 / order 首次购买自动绑定 / history 从历史订单回溯
    source TEXT NOT NULL DEFAULT 'manual',
    remark TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_buyer_keys_buyer ON buyer_keys (buyer_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_buyer_keys_token ON buyer_keys (token_id) WHERE token_id != '';

-- 买家在 new-api 上注册的账号：账号模式（卡密换账号 / 直接充值到账号）用。
-- 一个买家只认一个账号；但同一个账号可以被多个买家指向（一家人共用一个号是合法的），
-- 所以 user_id 只建普通索引，不做唯一约束。
CREATE TABLE IF NOT EXISTS buyer_accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    buyer_id TEXT NOT NULL,
    buyer_name TEXT NOT NULL DEFAULT '',
    account_id INTEGER,
    -- new-api 的用户 id 和用户名：id 是主键，用户名只是展示用（可以改）
    user_id TEXT NOT NULL DEFAULT '',
    username TEXT NOT NULL DEFAULT '',
    -- 我们生成的密码：买家把卡密弄丢时还能重发，也是唯一能补救的线索
    -- （通过接口重置密码需要 2FA 二次验证，机器人拿不到验证码）
    password TEXT NOT NULL DEFAULT '',
    -- 最近一次读到的余额（站点展示货币）
    balance REAL,
    -- 累计充值额度（站点展示货币，运营参考）
    total_usd REAL NOT NULL DEFAULT 0,
    topup_count INTEGER NOT NULL DEFAULT 0,
    -- 发给买家的卡密（账号密码就在里面），卡还没领时可以直接重发
    card_code TEXT NOT NULL DEFAULT '',
    -- manual 后台手工绑定 / order 发货时自动绑定
    source TEXT NOT NULL DEFAULT 'order',
    remark TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_buyer_accounts_buyer ON buyer_accounts (buyer_id);
CREATE INDEX IF NOT EXISTS idx_buyer_accounts_user ON buyer_accounts (user_id);

-- 商品级模型白名单：同一个站点上，不同商品可以发不同权限的 Key。
-- 没有出现在这张表里的商品沿用 app_config 里的全局值。
CREATE TABLE IF NOT EXISTS item_models (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id TEXT NOT NULL UNIQUE,
    model_limits TEXT NOT NULL DEFAULT '',
    remark TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- 内置图片库：在后台传一次，聊天时一键发送。
CREATE TABLE IF NOT EXISTS builtin_images (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL DEFAULT '',
    filename TEXT NOT NULL UNIQUE,
    mime TEXT NOT NULL DEFAULT 'image/jpeg',
    size INTEGER NOT NULL DEFAULT 0,
    width INTEGER NOT NULL DEFAULT 0,
    height INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

-- 自动评价记录：同一账号 + 订单只保留一条，成功后不再重复评。
CREATE TABLE IF NOT EXISTS rate_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    order_id TEXT NOT NULL,
    chat_id TEXT,
    item_id TEXT,
    buyer_id TEXT,
    buyer_name TEXT,
    feedback TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'rated',
    detail TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_rate_records_dedup ON rate_records (account_id, order_id);
CREATE INDEX IF NOT EXISTS idx_rate_records_created ON rate_records (created_at DESC);

CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER,
    chat_id TEXT,
    sender_id TEXT,
    sender_name TEXT,
    item_id TEXT,
    order_id TEXT,
    content TEXT,
    kind TEXT NOT NULL DEFAULT 'incoming',
    -- text / image / video
    message_type TEXT NOT NULL DEFAULT 'text',
    -- 闲鱼 CDN 上的图片 / 视频地址（收到的媒体来自服务端解析）
    media_url TEXT,
    -- 视频封面地址
    media_thumb_url TEXT,
    media_width INTEGER,
    media_height INTEGER,
    media_duration INTEGER,
    media_name TEXT,
    media_size INTEGER,
    -- 本工具发出、但闲鱼没有回传可访问 URL 的视频：本地副本，供后台预览
    local_media TEXT,
    local_poster TEXT,
    -- 服务端 messageId：撤回、以及聊天界面补拉历史时按它去重
    external_id TEXT,
    -- 后台看过的时间；NULL = 还没读过（会话列表的未读红点）
    read_at TEXT,
    -- 撤回时间；NULL = 没撤回
    recalled_at TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_created ON messages (created_at DESC);
"""


#: 后加的列：(表, 列, 类型定义)。老库升级时自动补齐，新库建表后也会跑一遍（幂等）。
MIGRATIONS: tuple[tuple[str, str, str], ...] = (
    ("rules", "usd_multiplier", "REAL NOT NULL DEFAULT 1"),
    ("rules", "fixed_usd", "REAL NOT NULL DEFAULT 0"),
    # 规格关键词（可选）：同一商品的不同规格可以走不同规则；空 = 不限规格
    ("rules", "spec", "TEXT NOT NULL DEFAULT ''"),
    # 充值模式找不到已绑定 Key 时：create = 开一个新 Key 并绑定，fail = 不发货
    ("rules", "topup_missing", "TEXT NOT NULL DEFAULT 'create'"),
    # 充值模式怎么确定 Key：order_id = 买家必须发旧订单号；auto = 自动用绑定/历史/首单
    ("rules", "topup_key_source", "TEXT NOT NULL DEFAULT 'order_id'"),
    # 充值模式允不允许跨商品：any = 买家任意已发货订单都能当充值目标（老行为），
    # item = 只认同商品的旧订单，避免把额度充到另一个商品（另一套模型白名单）的 Key 上
    ("rules", "topup_item_scope", "TEXT NOT NULL DEFAULT 'any'"),
    # 让买家提供旧订单号的提示 / 找不到 Key 时的回复（留空用内置默认）
    ("rules", "topup_prompt", "TEXT NOT NULL DEFAULT ''"),
    ("rules", "topup_not_found", "TEXT NOT NULL DEFAULT ''"),
    # 账号模式：首次询问 / 老用户确认 / 卡密内容（留空用内置默认）
    ("rules", "account_prompt", "TEXT NOT NULL DEFAULT ''"),
    ("rules", "account_confirm", "TEXT NOT NULL DEFAULT ''"),
    ("rules", "account_card", "TEXT NOT NULL DEFAULT ''"),
    # 兑换码模式：有效期（天）。0 = 永不过期（new-api 用 0 表示不过期，
    # 和令牌的 -1 不是一回事，别照抄）
    ("rules", "redeem_expiry_days", "INTEGER NOT NULL DEFAULT 0"),
    # 兑换码模式：卡密内容模板（买家在卡密站兑换后看到的就是这段）
    ("rules", "redeem_card", "TEXT NOT NULL DEFAULT ''"),
    # 按订单实付金额匹配（同一商品不同价格规格）：0 = 不限
    ("rules", "min_amount", "REAL NOT NULL DEFAULT 0"),
    ("rules", "max_amount", "REAL NOT NULL DEFAULT 0"),
    ("orders", "amount_cny", "REAL"),
    ("orders", "quota_usd", "REAL"),
    ("orders", "api_key_id", "TEXT"),
    ("orders", "card_code", "TEXT"),
    # 充值模式下买家指定的「旧订单号」（要充到哪一笔订单对应的 Key）
    ("orders", "key_order_id", "TEXT"),
    # 账号模式：这一单充到哪个 new-api 账号（id 用来对账，用户名用来展示）
    ("orders", "account_user_id", "TEXT"),
    ("orders", "account_username", "TEXT"),
    ("messages", "external_id", "TEXT"),
    ("messages", "read_at", "TEXT"),
    ("messages", "message_type", "TEXT NOT NULL DEFAULT 'text'"),
    ("messages", "media_url", "TEXT"),
    ("messages", "media_thumb_url", "TEXT"),
    ("messages", "media_width", "INTEGER"),
    ("messages", "media_height", "INTEGER"),
    ("messages", "media_duration", "INTEGER"),
    ("messages", "media_name", "TEXT"),
    ("messages", "media_size", "INTEGER"),
    ("messages", "local_media", "TEXT"),
    ("messages", "local_poster", "TEXT"),
    ("messages", "recalled_at", "TEXT"),
    # 自动回复的命中记录细化到「规则里的哪一条回复」：一条规则多条回复时各自去重
    ("reply_hits", "entry_id", "INTEGER NOT NULL DEFAULT 0"),
)


def now_iso() -> str:
    """本地时间秒级 ISO 字符串，便于人读。"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


async def _backfill_reply_entries(conn: aiosqlite.Connection) -> None:
    """把老库里的自动回复规则补成「一条规则至少一条回复条目」。

    老版本里 keywords / content 直接存在 reply_rules 上；升级后引擎只读条目，
    所以这里给每条还没有条目的规则建一条，并把该规则的历史命中记录挂到它下面，
    否则升级后「同一会话只回一次」会全部失效、把老会话重新回一遍。

    函数是幂等的：已经有条目的规则不会重复插入。
    """
    ts = now_iso()
    cur = await conn.execute(
        "SELECT id, keywords, content FROM reply_rules r"
        " WHERE NOT EXISTS (SELECT 1 FROM reply_rule_entries e WHERE e.rule_id = r.id)"
    )
    for row in await cur.fetchall():
        cur2 = await conn.execute(
            "INSERT INTO reply_rule_entries"
            " (rule_id, keywords, content, sort_order, created_at, updated_at)"
            " VALUES (?, ?, ?, 0, ?, ?)",
            (row["id"], row["keywords"] or "", row["content"] or "", ts, ts),
        )
        entry_id = cur2.lastrowid
        if entry_id:
            await conn.execute(
                "UPDATE reply_hits SET entry_id = ? WHERE rule_id = ? AND entry_id = 0",
                (entry_id, row["id"]),
            )
    # 规则已经有条目、命中记录却还是 0（上一版代码写下的）：统一挂到第一条。
    await conn.execute(
        "UPDATE reply_hits SET entry_id = ("
        "  SELECT e.id FROM reply_rule_entries e WHERE e.rule_id = reply_hits.rule_id"
        "  ORDER BY e.sort_order, e.id LIMIT 1"
        ") WHERE entry_id = 0 AND EXISTS ("
        "  SELECT 1 FROM reply_rule_entries e WHERE e.rule_id = reply_hits.rule_id"
        ")"
    )


async def _apply_migrations(conn: aiosqlite.Connection) -> None:
    """给已有数据库补上后加的列（SQLite 没有 IF NOT EXISTS 加列语法）。"""
    added: set[tuple[str, str]] = set()
    for table, column, ddl in MIGRATIONS:
        cur = await conn.execute(f"PRAGMA table_info({table})")
        rows = await cur.fetchall()
        existing = {row[1] for row in rows}
        if column in existing:
            continue
        await conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
        added.add((table, column))
    if ("messages", "read_at") in added:
        # 升级前没有任何「已读」概念：把历史消息一次性标成已读，
        # 否则聊天界面一上线，每个老会话都挂着几十上百条未读红点。
        await conn.execute("UPDATE messages SET read_at = created_at WHERE read_at IS NULL")
    # 索引必须等列补齐之后再建：老库先跑 SCHEMA 时还没有 external_id / read_at。
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_messages_chat"
        " ON messages (account_id, chat_id, created_at, id)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_messages_external"
        " ON messages (account_id, chat_id, external_id)"
    )
    # 买家昵称回填 / 复用会话都按 (account_id, sender_id[, item_id]) 查；
    # 没有这两个索引时，消息表一大就要扫全表再做临时排序。
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_messages_sender"
        " ON messages (account_id, sender_id, created_at DESC, id DESC)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_messages_sender_item"
        " ON messages (account_id, sender_id, item_id, created_at DESC)"
    )
    # external_id 去重从「先查后插」改成唯一索引兜底。升级老库时先清掉
    # 历史重复行（同一会话同一 messageId 只留最早一条），否则建索引会失败。
    await conn.execute(
        "DELETE FROM messages WHERE external_id IS NOT NULL AND id NOT IN ("
        "  SELECT MIN(id) FROM messages WHERE external_id IS NOT NULL"
        "  GROUP BY account_id, chat_id, external_id"
        ")"
    )
    await conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_external_unique"
        " ON messages (account_id, chat_id, external_id)"
        " WHERE external_id IS NOT NULL"
    )
    # 自动回复：entry_id 的索引也要等列补齐后再建（老库原本没有这一列）
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_reply_hits_entry"
        " ON reply_hits (account_id, sender_id, entry_id, created_at)"
    )
    # 自动回复：老规则补条目、老命中记录补 entry_id（必须在 entry_id 列补好之后）
    await _backfill_reply_entries(conn)
    await conn.commit()


#: 自动回复规则的插入语句，字段顺序只在这里定义一次。
_INSERT_REPLY_RULE = (
    "INSERT INTO reply_rules (name, kind, item_id, keywords, match_type, content, enabled,"
    " once_per_chat, cooldown, priority, remark, created_at, updated_at)"
    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

#: 规则里每条「关键词 -> 回复」的插入语句
_INSERT_REPLY_ENTRY = (
    "INSERT INTO reply_rule_entries"
    " (rule_id, keywords, content, sort_order, created_at, updated_at)"
    " VALUES (?, ?, ?, ?, ?, ?)"
)


def _reply_rule_entries(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """把请求里的 entries（或旧的平铺 keywords/content）统一成条目列表。"""
    raw = payload.get("entries")
    if raw:
        return [
            {
                "id": item.get("id"),
                "keywords": item.get("keywords") or "",
                "content": item.get("content") or "",
            }
            for item in raw
        ]
    # 兼容旧接口 / 老测试：没有 entries 时，keywords + content 就是唯一的一条
    return [{
        "id": None,
        "keywords": payload.get("keywords") or "",
        "content": payload.get("content") or "",
    }]


def _reply_rule_values(payload: dict[str, Any]) -> tuple[Any, ...]:
    """规则行上的可写字段；顺序和 _INSERT_REPLY_RULE 的参数一一对应。

    keywords / content 两列是旧接口的兼容镜像，总是写第一条条目的值；
    引擎和新界面都只读 reply_rule_entries。
    """
    entries = _reply_rule_entries(payload)
    first = entries[0]
    return (
        payload["name"], payload["kind"], payload.get("item_id") or "*",
        first.get("keywords") or "", payload.get("match_type") or "contains",
        first.get("content") or "", int(bool(payload.get("enabled", True))),
        int(bool(payload.get("once_per_chat", True))), int(payload.get("cooldown") or 0),
        int(payload.get("priority") or 0), payload.get("remark") or "",
    )


async def _sync_reply_entries(
    conn: aiosqlite.Connection, rule_id: int, entries: list[dict[str, Any]], ts: str
) -> None:
    """把规则的条目同步成 entries：带 id 的更新，新的插入，少掉的删掉。

    删条目时顺手删它的命中记录，免得 reply_hits 里留下永远对不上的 entry_id。
    """
    cur = await conn.execute("SELECT id FROM reply_rule_entries WHERE rule_id = ?", (rule_id,))
    existing = {int(row["id"]) for row in await cur.fetchall()}
    kept: set[int] = set()
    for order, entry in enumerate(entries):
        raw_id = entry.get("id")
        entry_id = int(raw_id) if raw_id else 0
        if entry_id and entry_id in existing:
            await conn.execute(
                "UPDATE reply_rule_entries SET keywords = ?, content = ?, sort_order = ?,"
                " updated_at = ? WHERE id = ? AND rule_id = ?",
                (entry.get("keywords") or "", entry.get("content") or "", order, ts,
                 entry_id, rule_id),
            )
            kept.add(entry_id)
        else:
            cur = await conn.execute(
                _INSERT_REPLY_ENTRY,
                (rule_id, entry.get("keywords") or "", entry.get("content") or "", order, ts, ts),
            )
            if cur.lastrowid:
                kept.add(int(cur.lastrowid))
    for entry_id in existing - kept:
        await conn.execute("DELETE FROM reply_hits WHERE entry_id = ?", (entry_id,))
        await conn.execute("DELETE FROM reply_rule_entries WHERE id = ?", (entry_id,))


@dataclass(slots=True)
class CardAllocation:
    """一次卡密出库的结果。"""

    card_id: int | None
    content: str | None


class Database:
    """极简仓储层。

    每个方法自己开关连接，避免长连接跨事件循环带来的问题；
    SQLite 打开 WAL 后并发读写足够应付几十个账号。
    """

    def __init__(self, path: str | Path, *, vault: CookieVault | None = None) -> None:
        self.path = Path(path)
        #: 凭据加解密。默认把密钥放在「数据库文件旁边」（app.db -> app.db.key）：
        #: 测试用临时库时密钥也落在临时目录，不会碰到真实密钥。
        #: 生产环境由 create_app() 显式传入 settings.cookie_key_path（data/cookie.key）。
        self.vault = vault or CookieVault(Path(str(self.path) + ".key"))

    async def connect(self) -> aiosqlite.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = await aiosqlite.connect(self.path)
        conn.row_factory = sqlite3.Row
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA foreign_keys=ON")
        await conn.execute("PRAGMA busy_timeout=5000")
        # 删/改过的内容用 0 覆盖，别让它以「空闲页」的形式留在文件里。
        # 这条和 Cookie 加密是一套的：加密之后还留着旧的明文页，等于白加密
        # （拿库文件 grep 一下照样能捞出 Cookie）。开销可以忽略。
        await conn.execute("PRAGMA secure_delete=ON")
        return conn

    def _chmod_private(self) -> None:
        """DB 里有 Cookie、卡密、Key：不要让同机其他用户读到。"""
        for suffix in ("", "-wal", "-shm"):
            try:
                Path(str(self.path) + suffix).chmod(0o600)
            except OSError:
                pass

    async def init(self) -> None:
        conn = await self.connect()
        try:
            await conn.executescript(SCHEMA)
            await conn.commit()
            await _apply_migrations(conn)
            await asyncio.to_thread(self._chmod_private)
        finally:
            await conn.close()
        # 密钥不可用要在这里就炸掉：等第一条 Cookie 落库时才发现，
        # 那时候可能已经有一半账号在用明文跑了。
        if self.vault.enabled:
            self.vault.ensure_key()
        logger.info("凭据加密：%s（密钥 %s）", self.vault.mode, self.vault.key_path)
        await self._migrate_cookie_encryption()

    async def _migrate_cookie_encryption(self) -> int:
        """把历史遗留的明文 Cookie 加密掉。幂等：已经是密文的行不动。"""
        if not self.vault.enabled:
            return 0
        rows = await self.fetch_all("SELECT id, cookie FROM accounts")
        migrated = 0
        for row in rows:
            raw = str(row.get("cookie") or "")
            if not raw or is_encrypted(raw):
                continue
            try:
                sealed = self.vault.encrypt(raw)
            except Exception:
                logger.error("账号 %s 的 Cookie 加密失败，保持明文", row.get("id"), exc_info=True)
                continue
            await self.execute("UPDATE accounts SET cookie = ? WHERE id = ?", (sealed, row.get("id")))
            migrated += 1
        if migrated:
            logger.warning("已把 %s 个账号的明文 Cookie 就地加密（一次性迁移）", migrated)
            # 只改行不收拾文件是不够的：旧明文仍可能躺在空闲页里。
            # VACUUM 会重写整库，把那些页真正吐掉。启动阶段没有 worker 在跑，安全。
            await asyncio.to_thread(self._vacuum_now)
        return migrated

    def _vacuum_now(self) -> None:
        """同步 VACUUM（在线程里跑）：把迁移前留下的旧明文页彻底清掉。"""
        conn = sqlite3.connect(self.path)
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            conn.execute("VACUUM")
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            logger.info("已 VACUUM：迁移前的旧 Cookie 明文页不再留在库文件里")
        except sqlite3.Error:
            logger.warning("VACUUM 失败（不影响加密，只是旧明文页可能还在文件里）", exc_info=True)
        finally:
            conn.close()

    # ---------------------------------------------------------------- 通用

    async def fetch_all(self, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
        conn = await self.connect()
        try:
            cur = await conn.execute(sql, params)
            rows = await cur.fetchall()
            return [dict(row) for row in rows]
        finally:
            await conn.close()

    async def fetch_one(self, sql: str, params: Sequence[Any] = ()) -> dict[str, Any] | None:
        rows = await self.fetch_all(sql, params)
        return rows[0] if rows else None

    async def execute(self, sql: str, params: Sequence[Any] = ()) -> int:
        conn = await self.connect()
        try:
            cur = await conn.execute(sql, params)
            await conn.commit()
            return cur.lastrowid or 0
        finally:
            await conn.close()

    # ---------------------------------------------------------------- 配置

    async def get_config(self, key: str) -> str | None:
        """读一条后台配置；没有就返回 None。"""
        row = await self.fetch_one("SELECT value FROM app_config WHERE key = ?", (key,))
        return None if row is None else str(row["value"])

    async def get_all_config(self) -> dict[str, str]:
        rows = await self.fetch_all("SELECT key, value FROM app_config")
        return {str(row["key"]): str(row["value"]) for row in rows}

    async def set_config(self, key: str, value: str) -> None:
        """写一条后台配置（没有就插入）。"""
        conn = await self.connect()
        try:
            await conn.execute(
                "INSERT INTO app_config (key, value, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
                (key, value, now_iso()),
            )
            await conn.commit()
        finally:
            await conn.close()

    async def delete_config(self, key: str) -> None:
        await self.execute("DELETE FROM app_config WHERE key = ?", (key,))

    async def model_settings(self) -> tuple[dict[str, str], dict[str, str]]:
        """一次读完「商品级模型白名单」和「全局后台配置」。

        发货路径每单都会调用一次，开两次连接没必要。返回值：
          * 第一个 dict 是 {item_id: model_limits}（商品级覆盖，含 * 兜底）；
          * 第二个 dict 是 app_config 整张表（全局模型白名单在这里）。
        """
        conn = await self.connect()
        try:
            cur = await conn.execute("SELECT item_id, model_limits FROM item_models")
            item_models = {
                str(row["item_id"]): str(row["model_limits"] or "") for row in await cur.fetchall()
            }
            cur = await conn.execute("SELECT key, value FROM app_config")
            overrides = {str(row["key"]): str(row["value"]) for row in await cur.fetchall()}
        finally:
            await conn.close()
        return item_models, overrides

    # ------------------------------------------------------------ 商品模型

    async def list_item_models(self) -> list[dict[str, Any]]:
        """列出所有商品级模型配置（* 兜底排最后，方便前端展示优先级）。"""
        return await self.fetch_all(
            "SELECT * FROM item_models ORDER BY CASE WHEN item_id = '*' THEN 1 ELSE 0 END, item_id"
        )

    async def get_item_model(self, row_id: int) -> dict[str, Any] | None:
        return await self.fetch_one("SELECT * FROM item_models WHERE id = ?", (row_id,))

    async def get_item_model_by_item(self, item_id: str) -> dict[str, Any] | None:
        return await self.fetch_one(
            "SELECT * FROM item_models WHERE item_id = ?", (str(item_id).strip(),)
        )

    async def save_item_model(self, payload: dict[str, Any], row_id: int | None = None) -> int:
        """新增 / 更新一条商品模型配置。

        同一个商品只能有一条：前端「新增」时如果商品已经配过，就地改成更新，
        而不是抛 IntegrityError —— 运营看到的是保存成功，心里想的就是改掉它。
        """
        item_id = str(payload.get("item_id") or "").strip()
        if not item_id:
            raise ValueError("商品 ID 不能为空")
        model_limits = str(payload.get("model_limits") or "")
        remark = str(payload.get("remark") or "")
        ts = now_iso()
        existing = await self.get_item_model_by_item(item_id)

        if row_id:
            if existing is not None and int(existing["id"]) != int(row_id):
                raise ValueError(f"商品 {item_id} 已经有单独配置了")
            await self.execute(
                "UPDATE item_models SET item_id = ?, model_limits = ?, remark = ?, updated_at = ?"
                " WHERE id = ?",
                (item_id, model_limits, remark, ts, row_id),
            )
            return row_id
        if existing is not None:
            await self.execute(
                "UPDATE item_models SET model_limits = ?, remark = ?, updated_at = ? WHERE id = ?",
                (model_limits, remark, ts, int(existing["id"])),
            )
            return int(existing["id"])
        return await self.execute(
            "INSERT INTO item_models (item_id, model_limits, remark, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (item_id, model_limits, remark, ts, ts),
        )

    async def delete_item_model(self, row_id: int) -> None:
        await self.execute("DELETE FROM item_models WHERE id = ?", (row_id,))

    # ------------------------------------------------------------ 买家 Key

    async def list_buyer_keys(self, limit: int = 200) -> list[dict[str, Any]]:
        return await self.fetch_all(
            "SELECT * FROM buyer_keys ORDER BY updated_at DESC, id DESC LIMIT ?", (limit,)
        )

    async def get_buyer_key(self, buyer_id: str | None) -> dict[str, Any] | None:
        """按买家 ID 取绑定的 Key；没有就返回 None。"""
        key = str(buyer_id or "").strip()
        if not key:
            return None
        return await self.fetch_one("SELECT * FROM buyer_keys WHERE buyer_id = ?", (key,))

    async def get_buyer_key_row(self, row_id: int) -> dict[str, Any] | None:
        return await self.fetch_one("SELECT * FROM buyer_keys WHERE id = ?", (row_id,))

    async def item_price_tiers(self, item_id: str, limit: int = 20) -> list[dict[str, Any]]:
        """这个商品历史上出现过的实付金额（后台配「金额区间」时做提示）。

        闲鱼订单接口不会返回多规格的 SKU 名称，只会给实际成交价；所以对
        「同一商品 1 元 / 5 元 / 10 元」这种多规格商品，金额就是唯一可靠的区分依据。
        """
        item = str(item_id or "").strip()
        if not item:
            return []
        return await self.fetch_all(
            "SELECT amount_cny AS amount, COUNT(*) AS count FROM orders"
            " WHERE item_id = ? AND amount_cny IS NOT NULL AND amount_cny > 0"
            " GROUP BY amount_cny ORDER BY amount_cny LIMIT ?",
            (item, max(1, int(limit))),
        )

    async def list_awaiting_key_orders(self, account_id: int,
                                       buyer_id: str | None) -> list[dict[str, Any]]:
        """还在等买家提供「旧订单号」的充值订单（按购买顺序）。"""
        buyer = str(buyer_id or "").strip()
        if not buyer:
            return []
        return await self.fetch_all(
            "SELECT * FROM orders WHERE account_id = ? AND buyer_id = ?"
            " AND status = 'awaiting_key' ORDER BY id",
            (account_id, buyer),
        )

    async def find_buyer_delivered_order(self, buyer_id: str | None,
                                         order_id: str | None) -> dict[str, Any] | None:
        """按订单号找「这个买家的、已经发过货的」订单。

        充值模式验证买家提供的旧订单号时用：不限定账号（同一买家可能在多个
        闲鱼号下过单），但必须同时匹配 buyer_id —— 只核对订单号等于把
        别人订单对应的 Key 充给当前买家。
        """
        buyer = str(buyer_id or "").strip()
        order = str(order_id or "").strip()
        if not buyer or not order:
            return None
        return await self.fetch_one(
            "SELECT * FROM orders WHERE order_id = ? AND buyer_id = ?"
            " AND status = 'delivered' ORDER BY id DESC LIMIT 1",
            (order, buyer),
        )

    async def find_buyer_key_order(self, buyer_id: str | None,
                                   api_key: str) -> dict[str, Any] | None:
        """按 Key 找「这个买家、已经发过货、发出去的就是这串 Key」的订单。

        「买家直接发 Key」模式下用它把 Key 认回某一单：充值模式的
        orders.card_code 存的就是发给买家的那串 Key（建卡密模式存的是卡密，
        认不出来也正常）。
        """
        buyer = str(buyer_id or "").strip()
        key = str(api_key or "").strip()
        if not buyer or not key:
            return None
        bare = key[3:] if key.lower().startswith("sk-") else key
        return await self.fetch_one(
            "SELECT * FROM orders WHERE buyer_id = ?"
            " AND (card_code = ? OR card_code = ?)"
            " AND status = 'delivered' ORDER BY id DESC LIMIT 1",
            (buyer, key, bare),
        )

    async def buyer_token_ids(self, buyer_id: str | None,
                              limit: int = 20) -> list[str]:
        """这个买家名下出现过的令牌 id（最近的在前，去重）。

        买家直接发 Key 时按它逐个读明文 Key 核对 —— 只查他自己的令牌，
        既便宜又天然带归属，不用全站扫描。
        """
        buyer = str(buyer_id or "").strip()
        if not buyer:
            return []
        rows = await self.fetch_all(
            "SELECT api_key_id FROM orders WHERE buyer_id = ?"
            " AND api_key_id IS NOT NULL AND api_key_id != ''"
            " ORDER BY id DESC LIMIT ?",
            (buyer, max(1, int(limit))),
        )
        out: list[str] = []
        for row in rows:
            token_id = str(row["api_key_id"] or "").strip()
            if token_id and token_id not in out:
                out.append(token_id)
        return out

    async def buyer_owns_token(self, buyer_id: str | None, token_id: str) -> bool:
        """这个令牌是不是「这个买家的」：绑定表或历史订单里出现过。"""
        buyer = str(buyer_id or "").strip()
        token = str(token_id or "").strip()
        if not buyer or not token:
            return False
        if await self.fetch_one(
                "SELECT id FROM buyer_keys WHERE buyer_id = ? AND token_id = ? LIMIT 1",
                (buyer, token)) is not None:
            return True
        return await self.fetch_one(
            "SELECT id FROM orders WHERE buyer_id = ? AND api_key_id = ? LIMIT 1",
            (buyer, token),
        ) is not None

    async def recent_delivered_orders(self, account_id: int, buyer_id: str | None,
                                      limit: int = 5, *,
                                      item_id: str = "") -> list[dict[str, Any]]:
        """买家最近几单**确实收到货**的记录（按时间倒序）。

        充值模式要靠它认「这个买家原来用哪个 Key」：
          * 新单会直接记 orders.api_key_id；
          * 早期版本的订单没记令牌 id，但订单号就在令牌名里
            （「买家用户名#订单号」），可以拿订单号去 new-api 按名字反查。
        只认 delivered：卡在发送失败/结果未知的单，买家手里不一定有 Key。

        item_id 非空时只返回这个商品的订单（充值规则配了「只认同商品」时用）。
        早期订单没记商品 ID（NULL / 空串 / 星号）仍然算数 —— 老客户不能被一个
        当年根本没记的字段挡在门外；这些行的归属由上层按「未知 = 放行」处理。
        """
        buyer = str(buyer_id or "").strip()
        if not buyer:
            return []
        sql = (
            "SELECT order_id, item_id, api_key_id, created_at FROM orders"
            " WHERE account_id = ? AND buyer_id = ? AND status = 'delivered'"
        )
        params: list[Any] = [account_id, buyer]
        item = str(item_id or "").strip()
        if item:
            sql += " AND (item_id = ? OR item_id IS NULL OR item_id = '' OR item_id = '*')"
            params.append(item)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(max(1, int(limit)))
        return await self.fetch_all(sql, params)

    async def buyer_key_matches_item(self, buyer_id: str, item_id: str,
                                     token_id: str) -> bool:
        """这个令牌是不是在这个商品上发给过这个买家。

        充值规则开了「只认同商品」时要判断「买家 Key」页那条绑定到底属于哪个商品 ——
        绑定表按买家只存一行，买家买第二个商品时会被覆盖，光看绑定分不出来。
        同样把早期没记商品 ID 的订单当「未知 = 算数」。
        """
        buyer = str(buyer_id or "").strip()
        token = str(token_id or "").strip()
        item = str(item_id or "").strip()
        if not buyer or not token or not item:
            return False
        row = await self.fetch_one(
            "SELECT id FROM orders WHERE buyer_id = ? AND api_key_id = ?"
            " AND status = 'delivered'"
            " AND (item_id = ? OR item_id IS NULL OR item_id = '' OR item_id = '*')"
            " LIMIT 1",
            (buyer, token, item),
        )
        return row is not None

    async def _check_buyer_key_conflict(self, buyer_id: str, token_id: str,
                                        row_id: int | None = None) -> None:
        """同一个令牌不能被两个买家认领。"""
        if not token_id:
            return
        row = await self.fetch_one(
            "SELECT id, buyer_id FROM buyer_keys WHERE token_id = ?", (token_id,)
        )
        if row is None:
            return
        if row_id is not None and int(row["id"]) == int(row_id):
            return
        if str(row["buyer_id"]) == buyer_id:
            # 同一个买家重复保存同一个令牌：这是幂等操作，不是冲突
            return
        raise ValueError(
            "这个令牌已经绑定给买家 " + str(row["buyer_id"]) + " 了，请先解绑那一条"
        )

    async def save_buyer_key(self, payload: dict[str, Any], row_id: int | None = None) -> int:
        """后台手工绑定 / 修改一条「买家 → Key」。

        同一个买家只保留一条：前端新增时如果买家已经绑过，就地更新
        （运营的意思就是「换成这个 Key」）。token_id 冲突会抛 ValueError。
        """
        buyer_id = str(payload.get("buyer_id") or "").strip()
        token_id = str(payload.get("token_id") or "").strip()
        if not buyer_id:
            raise ValueError("买家 ID 不能为空")
        if not token_id:
            raise ValueError("令牌 ID 不能为空")
        await self._check_buyer_key_conflict(buyer_id, token_id, row_id)
        if row_id:
            owner = await self.get_buyer_key(buyer_id)
            if owner is not None and int(owner["id"]) != int(row_id):
                raise ValueError(
                    "买家 " + buyer_id + " 已经绑定了另一条 Key，请直接编辑那一条"
                )

        ts = now_iso()
        buyer_name = str(payload.get("buyer_name") or "").strip()
        token_name = str(payload.get("token_name") or "").strip()
        api_key = str(payload.get("api_key") or "").strip()
        key_mask = str(payload.get("key_mask") or "").strip()
        balance = payload.get("balance")
        account_id = payload.get("account_id")
        remark = str(payload.get("remark") or "").strip()

        if row_id:
            await self.execute(
                "UPDATE buyer_keys SET buyer_id = ?, buyer_name = ?, token_id = ?, token_name = ?,"
                " api_key = ?, key_mask = ?, balance = ?, account_id = ?, source = 'manual',"
                " remark = ?, updated_at = ? WHERE id = ?",
                (buyer_id, buyer_name, token_id, token_name, api_key, key_mask, balance,
                 account_id, remark, ts, row_id),
            )
            return row_id
        existing = await self.get_buyer_key(buyer_id)
        if existing is not None:
            await self.execute(
                "UPDATE buyer_keys SET buyer_name = ?, token_id = ?, token_name = ?, api_key = ?,"
                " key_mask = ?, balance = ?, account_id = ?, source = 'manual', remark = ?,"
                " updated_at = ? WHERE id = ?",
                (buyer_name or existing.get("buyer_name") or "", token_id, token_name,
                 api_key or existing.get("api_key") or "", key_mask or existing.get("key_mask") or "",
                 balance, account_id if account_id is not None else existing.get("account_id"),
                 remark, ts, int(existing["id"])),
            )
            return int(existing["id"])
        return await self.execute(
            "INSERT INTO buyer_keys (buyer_id, buyer_name, account_id, token_id, token_name,"
            " api_key, key_mask, balance, source, remark, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'manual', ?, ?, ?)",
            (buyer_id, buyer_name, account_id, token_id, token_name, api_key, key_mask,
             balance, remark, ts, ts),
        )

    async def auto_bind_buyer_key(self, payload: dict[str, Any]) -> int | None:
        """发货链路自动绑定：后台手工绑过的（source=manual）一律不覆盖。

        首次购买 / 历史订单回溯都走这里。返回绑定行 id；不绑定返回 None。
        """
        buyer_id = str(payload.get("buyer_id") or "").strip()
        token_id = str(payload.get("token_id") or "").strip()
        if not buyer_id or not token_id:
            return None

        existing = await self.get_buyer_key(buyer_id)
        if existing is not None and str(existing.get("source") or "") == "manual":
            logger.debug("买家 %s 的 Key 是后台手工绑定的，不用自动绑定覆盖", buyer_id)
            return int(existing["id"])
        conflict = await self.fetch_one(
            "SELECT id, buyer_id FROM buyer_keys WHERE token_id = ? AND buyer_id != ?",
            (token_id, buyer_id),
        )
        if conflict is not None:
            logger.warning(
                "令牌 %s 已经绑定给买家 %s，跳过为买家 %s 自动绑定",
                token_id, conflict["buyer_id"], buyer_id,
            )
            return None

        ts = now_iso()
        buyer_name = str(payload.get("buyer_name") or "").strip()
        token_name = str(payload.get("token_name") or "").strip()
        api_key = str(payload.get("api_key") or "").strip()
        key_mask = str(payload.get("key_mask") or "").strip()
        balance = payload.get("balance")
        account_id = payload.get("account_id")
        source = str(payload.get("source") or "order").strip() or "order"

        if existing is not None:
            # 换成另一个令牌时不能沿用旧行的 api_key / key_mask：那是上一个令牌的
            # Key，留着会在「充值结果没回 Key」时把别的 Key 发给买家。
            same_token = str(existing.get("token_id") or "") == token_id
            await self.execute(
                "UPDATE buyer_keys SET buyer_name = ?, account_id = ?, token_id = ?, token_name = ?,"
                " api_key = ?, key_mask = ?, balance = ?, source = ?, updated_at = ? WHERE id = ?",
                (buyer_name or existing.get("buyer_name") or "", account_id, token_id, token_name,
                 api_key or (existing.get("api_key") if same_token else "") or "",
                 key_mask or (existing.get("key_mask") if same_token else "") or "",
                 balance, source, ts, int(existing["id"])),
            )
            return int(existing["id"])
        return await self.execute(
            "INSERT INTO buyer_keys (buyer_id, buyer_name, account_id, token_id, token_name,"
            " api_key, key_mask, balance, source, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (buyer_id, buyer_name, account_id, token_id, token_name, api_key, key_mask,
             balance, source, ts, ts),
        )

    async def note_buyer_key_topup(self, row_id: int, quota_usd: float,
                                   balance: float | None = None,
                                   api_key: str = "", key_mask: str = "",
                                   token_name: str = "") -> None:
        """记一次充值：累计额度、次数、最新余额（运营对账用）。"""
        await self.execute(
            "UPDATE buyer_keys SET total_usd = total_usd + ?, topup_count = topup_count + 1,"
            " balance = COALESCE(?, balance),"
            " api_key = CASE WHEN ? != '' THEN ? ELSE api_key END,"
            " key_mask = CASE WHEN ? != '' THEN ? ELSE key_mask END,"
            " token_name = CASE WHEN ? != '' THEN ? ELSE token_name END,"
            " updated_at = ? WHERE id = ?",
            (float(quota_usd or 0), balance, api_key, api_key, key_mask, key_mask,
             token_name, token_name, now_iso(), row_id),
        )

    async def snapshot_buyer_key(self, row_id: int, balance: float | None = None,
                                 api_key: str = "", key_mask: str = "",
                                 token_name: str = "") -> None:
        """只刷新展示字段（余额 / Key / 令牌名），不动累计额度和次数。"""
        await self.execute(
            "UPDATE buyer_keys SET balance = COALESCE(?, balance),"
            " api_key = CASE WHEN ? != '' THEN ? ELSE api_key END,"
            " key_mask = CASE WHEN ? != '' THEN ? ELSE key_mask END,"
            " token_name = CASE WHEN ? != '' THEN ? ELSE token_name END,"
            " updated_at = ? WHERE id = ?",
            (balance, api_key, api_key, key_mask, key_mask,
             token_name, token_name, now_iso(), row_id),
        )

    async def delete_buyer_key(self, row_id: int) -> None:
        await self.execute("DELETE FROM buyer_keys WHERE id = ?", (row_id,))

    # ------------------------------------------------------------ 买家账号

    async def list_buyer_accounts(self, limit: int = 200) -> list[dict[str, Any]]:
        return await self.fetch_all(
            "SELECT * FROM buyer_accounts ORDER BY updated_at DESC, id DESC LIMIT ?", (limit,)
        )

    async def get_buyer_account(self, buyer_id: str | None) -> dict[str, Any] | None:
        """按买家 ID 取绑定的账号；没有就返回 None。"""
        key = str(buyer_id or "").strip()
        if not key:
            return None
        return await self.fetch_one(
            "SELECT * FROM buyer_accounts WHERE buyer_id = ?", (key,)
        )

    async def get_buyer_account_row(self, row_id: int) -> dict[str, Any] | None:
        return await self.fetch_one("SELECT * FROM buyer_accounts WHERE id = ?", (row_id,))

    async def buyer_accounts_of_user(self, user_id: str) -> list[dict[str, Any]]:
        """这个 new-api 账号被哪些买家认领过（同一个号被多人指向时给运营看的）。"""
        uid = str(user_id or "").strip()
        if not uid:
            return []
        return await self.fetch_all(
            "SELECT * FROM buyer_accounts WHERE user_id = ? ORDER BY id", (uid,)
        )

    async def save_buyer_account(self, payload: dict[str, Any],
                                 row_id: int | None = None) -> int:
        """后台手工绑定 / 修改一条「买家 → 账号」。

        同一个买家只保留一条：新增时如果买家已经绑过，就地更新
        （运营的意思就是「改成这个账号」）。
        """
        buyer_id = str(payload.get("buyer_id") or "").strip()
        user_id = str(payload.get("user_id") or "").strip()
        username = str(payload.get("username") or "").strip()
        if not buyer_id:
            raise ValueError("买家 ID 不能为空")
        if not user_id and not username:
            raise ValueError("账号 ID 和账号名至少填一个")
        if row_id:
            owner = await self.get_buyer_account(buyer_id)
            if owner is not None and int(owner["id"]) != int(row_id):
                raise ValueError(
                    "买家 " + buyer_id + " 已经绑定了另一个账号，请直接编辑那一条"
                )
        ts = now_iso()
        values = {
            "buyer_name": str(payload.get("buyer_name") or "").strip(),
            "account_id": payload.get("account_id"),
            "user_id": user_id,
            "username": username,
            "password": str(payload.get("password") or "").strip(),
            "balance": payload.get("balance"),
            "remark": str(payload.get("remark") or "").strip(),
        }
        if row_id:
            # 后台的表单里没有「密码」这一栏：留空就当「不改」，否则编辑一次绑定
            # 就会把开号时存的密码抹掉，买家丢了卡密就再也补不回来了。
            await self.execute(
                "UPDATE buyer_accounts SET buyer_id = ?, buyer_name = ?, account_id = ?,"
                " user_id = ?, username = ?,"
                " password = CASE WHEN ? != '' THEN ? ELSE password END,"
                " balance = ?, source = 'manual', remark = ?, updated_at = ? WHERE id = ?",
                (buyer_id, values["buyer_name"], values["account_id"], values["user_id"],
                 values["username"], values["password"], values["password"], values["balance"],
                 values["remark"], ts, row_id),
            )
            return row_id
        existing = await self.get_buyer_account(buyer_id)
        if existing is not None:
            await self.execute(
                "UPDATE buyer_accounts SET buyer_name = ?, account_id = ?, user_id = ?,"
                " username = ?, password = CASE WHEN ? != '' THEN ? ELSE password END,"
                " balance = ?, source = 'manual', remark = ?, updated_at = ? WHERE id = ?",
                (values["buyer_name"] or existing.get("buyer_name") or "",
                 values["account_id"] if values["account_id"] is not None
                 else existing.get("account_id"),
                 values["user_id"] or existing.get("user_id") or "",
                 values["username"] or existing.get("username") or "",
                 values["password"], values["password"],
                 values["balance"] if values["balance"] is not None else existing.get("balance"),
                 values["remark"] or existing.get("remark") or "",
                 ts, int(existing["id"])),
            )
            return int(existing["id"])
        return await self.execute(
            "INSERT INTO buyer_accounts (buyer_id, buyer_name, account_id, user_id, username,"
            " password, balance, source, remark, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, 'manual', ?, ?, ?)",
            (buyer_id, values["buyer_name"], values["account_id"], values["user_id"],
             values["username"], values["password"], values["balance"], values["remark"], ts, ts),
        )

    async def auto_bind_buyer_account(self, payload: dict[str, Any]) -> int | None:
        """发货链路自动绑定：后台手工绑过的（source=manual）一律不覆盖。"""
        buyer_id = str(payload.get("buyer_id") or "").strip()
        user_id = str(payload.get("user_id") or "").strip()
        if not buyer_id or not user_id:
            return None

        existing = await self.get_buyer_account(buyer_id)
        if existing is not None and str(existing.get("source") or "") == "manual":
            logger.debug("买家 %s 的账号是后台手工绑定的，不用自动绑定覆盖", buyer_id)
            return int(existing["id"])

        ts = now_iso()
        buyer_name = str(payload.get("buyer_name") or "").strip()
        username = str(payload.get("username") or "").strip()
        password = str(payload.get("password") or "").strip()
        balance = payload.get("balance")
        account_id = payload.get("account_id")
        source = str(payload.get("source") or "order").strip() or "order"

        if existing is not None:
            # 换绑到另一个账号时必须把旧密码清掉：那是上一个账号的密码，
            # 留着会在「重发卡密」时把别人的账号密码发给买家。
            same_user = str(existing.get("user_id") or "") == user_id
            await self.execute(
                "UPDATE buyer_accounts SET buyer_name = ?, account_id = ?, user_id = ?,"
                " username = ?, password = ?, balance = ?, source = ?, updated_at = ?"
                " WHERE id = ?",
                (buyer_name or existing.get("buyer_name") or "", account_id, user_id, username,
                 password or (existing.get("password") if same_user else "") or "",
                 balance, source, ts, int(existing["id"])),
            )
            return int(existing["id"])
        try:
            return await self.execute(
                "INSERT INTO buyer_accounts (buyer_id, buyer_name, account_id, user_id, username,"
                " password, balance, source, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (buyer_id, buyer_name, account_id, user_id, username, password, balance, source,
                 ts, ts),
            )
        except Exception:
            # 并发首单：同一买家两单同时发货时，两边都查不到既有绑定，后插入的那个
            # 会撞 buyer_id 唯一索引。退回「更新已有那一行」，别让刚开好的账号丢掉绑定。
            existing = await self.get_buyer_account(buyer_id)
            if existing is None:
                raise
            await self.execute(
                "UPDATE buyer_accounts SET buyer_name = ?, account_id = ?, user_id = ?,"
                " username = ?, password = ?, balance = ?, source = ?, updated_at = ?"
                " WHERE id = ?",
                (buyer_name or existing.get("buyer_name") or "", account_id, user_id, username,
                 password, balance, source, ts, int(existing["id"])),
            )
            return int(existing["id"])

    async def note_buyer_account_topup(self, row_id: int, quota_usd: float,
                                       balance: float | None = None,
                                       card_code: str = "") -> None:
        """记一次充值：累计额度、次数、最新余额（运营对账用）。"""
        await self.execute(
            "UPDATE buyer_accounts SET total_usd = total_usd + ?, topup_count = topup_count + 1,"
            " balance = COALESCE(?, balance),"
            " card_code = CASE WHEN ? != '' THEN ? ELSE card_code END,"
            " updated_at = ? WHERE id = ?",
            (float(quota_usd or 0), balance, card_code, card_code, now_iso(), row_id),
        )

    async def snapshot_buyer_account(self, row_id: int, balance: float | None = None,
                                     username: str = "") -> None:
        """只刷新余额 / 用户名，不改来源、不动密码和累计额度。

        后台点「刷新余额」走这里：借道 save_buyer_account 会把 source 改成
        manual，之后自动绑定就再也不会更新这条了。
        """
        await self.execute(
            "UPDATE buyer_accounts SET balance = COALESCE(?, balance),"
            " username = CASE WHEN ? != '' THEN ? ELSE username END,"
            " updated_at = ? WHERE id = ?",
            (balance, username, username, now_iso(), row_id),
        )

    async def delete_buyer_account(self, row_id: int) -> None:
        await self.execute("DELETE FROM buyer_accounts WHERE id = ?", (row_id,))

    async def list_awaiting_account_orders(self, account_id: int,
                                           buyer_id: str | None) -> list[dict[str, Any]]:
        """还在等买家回「有没有账号 / 要什么用户名 / 确认充值」的订单（按购买顺序）。"""
        buyer = str(buyer_id or "").strip()
        if not buyer:
            return []
        return await self.fetch_all(
            "SELECT * FROM orders WHERE account_id = ? AND buyer_id = ?"
            " AND status IN ('awaiting_account', 'awaiting_username', 'awaiting_confirm')"
            " ORDER BY id",
            (account_id, buyer),
        )

    # ------------------------------------------------------------ 内置图片

    async def list_builtin_images(self) -> list[dict[str, Any]]:
        return await self.fetch_all("SELECT * FROM builtin_images ORDER BY id DESC")

    async def get_builtin_image(self, image_id: int) -> dict[str, Any] | None:
        return await self.fetch_one("SELECT * FROM builtin_images WHERE id = ?", (image_id,))

    async def save_builtin_image(
        self,
        *,
        name: str,
        filename: str,
        mime: str,
        size: int,
        width: int,
        height: int,
    ) -> int:
        return await self.execute(
            "INSERT INTO builtin_images (name, filename, mime, size, width, height, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (name, filename, mime, int(size), int(width), int(height), now_iso()),
        )

    async def delete_builtin_image(self, image_id: int) -> None:
        await self.execute("DELETE FROM builtin_images WHERE id = ?", (image_id,))

    # ------------------------------------------------------------ 自动评价

    async def get_rate_record(self, account_id: int, order_id: str) -> dict[str, Any] | None:
        if not order_id:
            return None
        return await self.fetch_one(
            "SELECT * FROM rate_records WHERE account_id = ? AND order_id = ?",
            (int(account_id), str(order_id)),
        )

    async def rate_record_blocks(
        self, account_id: int, order_id: str, *, retry_after_seconds: int = 3600
    ) -> bool:
        """这个订单现在还要不要尝试评价。

        成功过的永远跳过；失败过的在冷却窗口内也跳过，避免同一条系统消息
        被反复推送时把评价接口刷爆。
        """
        row = await self.get_rate_record(account_id, order_id)
        if row is None:
            return False
        if str(row.get("status") or "") == "rated":
            return True
        if retry_after_seconds <= 0:
            return False
        return await self.fetch_one(
            "SELECT id FROM rate_records WHERE account_id = ? AND order_id = ?"
            " AND created_at >= datetime('now', 'localtime', ?) LIMIT 1",
            (int(account_id), str(order_id), f"-{int(retry_after_seconds)} seconds"),
        ) is not None

    async def record_rate(
        self,
        *,
        account_id: int,
        order_id: str,
        status: str,
        feedback: str = "",
        detail: str = "",
        chat_id: str | None = None,
        item_id: str | None = None,
        buyer_id: str | None = None,
        buyer_name: str | None = None,
    ) -> int:
        """记录一次自动评价（同一订单覆盖上一次失败记录）。"""
        if not order_id:
            return 0
        values = (
            account_id, order_id, chat_id or None, item_id or None, buyer_id or None,
            buyer_name or None, feedback or "", status, detail or "", now_iso(),
        )
        conn = await self.connect()
        try:
            cur = await conn.execute(
                "INSERT INTO rate_records (account_id, order_id, chat_id, item_id, buyer_id,"
                " buyer_name, feedback, status, detail, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                " ON CONFLICT(account_id, order_id) DO UPDATE SET"
                " chat_id = excluded.chat_id, item_id = excluded.item_id,"
                " buyer_id = excluded.buyer_id, buyer_name = excluded.buyer_name,"
                " feedback = excluded.feedback, status = excluded.status,"
                " detail = excluded.detail, created_at = excluded.created_at",
                values,
            )
            await conn.commit()
            return int(cur.lastrowid or 0)
        finally:
            await conn.close()

    async def list_rate_records(self, limit: int = 100) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        return await self.fetch_all(
            "SELECT r.*, a.name AS account_name FROM rate_records r"
            " LEFT JOIN accounts a ON a.id = r.account_id"
            " ORDER BY r.id DESC LIMIT ?",
            (limit,),
        )

    # ---------------------------------------------------------------- 账号

    async def list_accounts(self, only_enabled: bool = False) -> list[dict[str, Any]]:
        sql = "SELECT * FROM accounts"
        if only_enabled:
            sql += " WHERE enabled = 1"
        rows = await self.fetch_all(sql + " ORDER BY id")
        return [self._open_account(row) for row in rows]

    async def get_account(self, account_id: int) -> dict[str, Any] | None:
        row = await self.fetch_one("SELECT * FROM accounts WHERE id = ?", (account_id,))
        return None if row is None else self._open_account(row)

    async def get_account_cookie_by_name(self, name: str) -> str:
        """按名字取解密后的 Cookie（后台「只改备注」时要沿用旧值）。"""
        row = await self.fetch_one("SELECT cookie FROM accounts WHERE name = ?", (name,))
        return "" if row is None else self.vault.decrypt(str(row.get("cookie") or ""))

    def _open_account(self, row: dict[str, Any]) -> dict[str, Any]:
        """把库里的一行账号摊开：Cookie 解密，解不开就标 cookie_error。

        解不开基本只有两种原因：密钥换过/丢了，或者库是从别处拷来的。
        这时交出空 Cookie 最安全（不会被当成可用账号去连闲鱼），
        但要留一句人话给后台，别让人对着「未启动」瞎猜。
        """
        out = dict(row)
        raw = str(out.get("cookie") or "")
        out["cookie"] = self.vault.decrypt(raw)
        out["cookie_error"] = "" if out["cookie"] or not raw else (
            self.vault.last_error or "Cookie 解密失败"
        )
        return out

    async def upsert_account(self, name: str, cookie: str, remark: str = "", enabled: bool = True) -> int:
        ts = now_iso()
        # 落库前加密（明文不落盘）。加密失败就让异常往上冒：宁可这次保存失败，
        # 也不能悄悄把明文写进去 —— 那正是这层要防的事。
        sealed = self.vault.encrypt(cookie)
        existing = await self.fetch_one("SELECT id FROM accounts WHERE name = ?", (name,))
        if existing:
            await self.execute(
                "UPDATE accounts SET cookie = ?, remark = ?, enabled = ?, updated_at = ? WHERE id = ?",
                (sealed, remark, int(enabled), ts, existing["id"]),
            )
            return int(existing["id"])
        return await self.execute(
            "INSERT INTO accounts (name, cookie, remark, enabled, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
            (name, sealed, remark, int(enabled), ts, ts),
        )

    async def update_account_cookie(self, account_id: int, cookie: str) -> None:
        """把刷新后的 Cookie（含新的 _m_h5_tk）写回数据库。

        不写回的话，进程重启又会拿旧 token 去连，等于每次重启都从头踩一遍
        「令牌过期 → 风控 → Cookie 失效」。
        """
        if not cookie:
            return
        await self.execute(
            "UPDATE accounts SET cookie = ?, updated_at = ? WHERE id = ?",
            (self.vault.encrypt(cookie), now_iso(), account_id),
        )

    async def record_login(self, account_id: int) -> str:
        """记下这次「真正重新登录」的时间点。

        为什么不用 accounts.updated_at：那个字段每次刷新 _m_h5_tk 都会改，
        而闲鱼网页登录态是按**登录时刻**算 TTL 的（实测 24 小时整）。
        后台要提示「还能撑多久」，就必须分开记这两件事。
        """
        ts = now_iso()
        await self.set_config(f"account_login_at:{int(account_id)}", ts)
        return ts

    async def get_login_at(self, account_id: int) -> str | None:
        """上次真正重新登录的时间（ISO 字符串）；没记录过返回 None。"""
        return await self.get_config(f"account_login_at:{int(account_id)}")

    async def delete_account(self, account_id: int) -> None:
        await self.execute("DELETE FROM accounts WHERE id = ?", (account_id,))

    # ---------------------------------------------------------------- 规则

    async def list_rules(self, only_enabled: bool = False) -> list[dict[str, Any]]:
        sql = "SELECT * FROM rules"
        if only_enabled:
            sql += " WHERE enabled = 1"
        return await self.fetch_all(sql + " ORDER BY priority DESC, id")

    async def get_rule(self, rule_id: int) -> dict[str, Any] | None:
        return await self.fetch_one("SELECT * FROM rules WHERE id = ?", (rule_id,))

    async def save_rule(self, payload: dict[str, Any], rule_id: int | None = None) -> int:
        """新增或更新发货规则。"""
        ts = now_iso()
        values = (
            payload["name"], payload["item_id"], int(payload["enabled"]), payload["mode"],
            payload["content"], int(payload["auto_confirm"]), int(payload["priority"]),
            payload["remark"], float(payload.get("usd_multiplier", 1) or 1),
            float(payload.get("fixed_usd", 0) or 0),
            str(payload.get("spec") or "").strip(),
            str(payload.get("topup_missing") or "create").strip() or "create",
            str(payload.get("topup_key_source") or "order_id").strip() or "order_id",
            str(payload.get("topup_item_scope") or "any").strip() or "any",
            str(payload.get("topup_prompt") or "").strip(),
            str(payload.get("topup_not_found") or "").strip(),
            float(payload.get("min_amount") or 0),
            float(payload.get("max_amount") or 0),
            str(payload.get("account_prompt") or "").strip(),
            str(payload.get("account_confirm") or "").strip(),
            str(payload.get("account_card") or "").strip(),
            max(0, int(payload.get("redeem_expiry_days") or 0)),
        )
        if rule_id:
            await self.execute(
                "UPDATE rules SET name = ?, item_id = ?, enabled = ?, mode = ?, content = ?,"
                " auto_confirm = ?, priority = ?, remark = ?, usd_multiplier = ?, fixed_usd = ?,"
                " spec = ?, topup_missing = ?, topup_key_source = ?, topup_item_scope = ?,"
                " topup_prompt = ?, topup_not_found = ?, min_amount = ?, max_amount = ?,"
                " account_prompt = ?, account_confirm = ?, account_card = ?,"
                " redeem_expiry_days = ?, updated_at = ? WHERE id = ?",
                (*values, ts, rule_id),
            )
            return rule_id
        return await self.execute(
            "INSERT INTO rules (name, item_id, enabled, mode, content, auto_confirm, priority, remark,"
            " usd_multiplier, fixed_usd, spec, topup_missing, topup_key_source,"
            " topup_item_scope, topup_prompt, topup_not_found, min_amount, max_amount,"
            " account_prompt, account_confirm, account_card, redeem_expiry_days,"
            " created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (*values, ts, ts),
        )
    async def delete_rule(self, rule_id: int) -> None:
        await self.execute("DELETE FROM rules WHERE id = ?", (rule_id,))

    # ---------------------------------------------------------- 自动回复

    async def list_reply_rules(self, only_enabled: bool = False) -> list[dict[str, Any]]:
        sql = "SELECT * FROM reply_rules"
        if only_enabled:
            sql += " WHERE enabled = 1"
        return await self._attach_reply_entries(
            await self.fetch_all(sql + " ORDER BY priority DESC, id")
        )

    async def _attach_reply_entries(
        self, rules: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """给规则挂上各自的回复条目（一次查完，避免 N+1）。"""
        if not rules:
            return rules
        placeholders = ",".join("?" * len(rules))
        rows = await self.fetch_all(
            # placeholders 只由规则条数拼出来，值仍然走参数绑定，不是拼接用户输入
            "SELECT * FROM reply_rule_entries WHERE rule_id IN (" + placeholders + ")"  # noqa: S608
            " ORDER BY rule_id, sort_order, id",
            [rule["id"] for rule in rules],
        )
        by_rule: dict[int, list[dict[str, Any]]] = {}
        for row in rows:
            by_rule.setdefault(int(row["rule_id"]), []).append(row)
        for rule in rules:
            entries = by_rule.get(int(rule["id"]), [])
            if not entries:
                # 极端兼容：回填还没跑到时，至少让旧字段还能被引擎用上
                entries = [{
                    "id": 0,
                    "rule_id": rule["id"],
                    "keywords": rule.get("keywords") or "",
                    "content": rule.get("content") or "",
                    "sort_order": 0,
                }]
            rule["entries"] = entries
        return rules

    async def get_reply_rule(self, rule_id: int) -> dict[str, Any] | None:
        rule = await self.fetch_one("SELECT * FROM reply_rules WHERE id = ?", (rule_id,))
        if rule is None:
            return None
        return (await self._attach_reply_entries([rule]))[0]

    async def save_reply_rule(self, payload: dict[str, Any], rule_id: int | None = None) -> int:
        """新增或更新一条自动回复规则（规则本身 + 里面的多条回复条目）。"""
        ts = now_iso()
        entries = _reply_rule_entries(payload)
        values = _reply_rule_values(payload)
        conn = await self.connect()
        try:
            if rule_id:
                cur = await conn.execute("SELECT id FROM reply_rules WHERE id = ?", (rule_id,))
                if await cur.fetchone() is None:
                    raise KeyError(f"reply rule {rule_id} not found")
                await conn.execute(
                    "UPDATE reply_rules SET name = ?, kind = ?, item_id = ?, keywords = ?,"
                    " match_type = ?, content = ?, enabled = ?, once_per_chat = ?, cooldown = ?,"
                    " priority = ?, remark = ?, updated_at = ? WHERE id = ?",
                    (*values, ts, rule_id),
                )
                target_id = rule_id
            else:
                cur = await conn.execute(_INSERT_REPLY_RULE, (*values, ts, ts))
                target_id = int(cur.lastrowid or 0)
                if not target_id:
                    raise RuntimeError("自动回复规则写入失败")
            await _sync_reply_entries(conn, target_id, entries, ts)
            await conn.commit()
            return target_id
        except Exception:
            await conn.rollback()
            raise
        finally:
            await conn.close()

    async def delete_reply_rule(self, rule_id: int) -> None:
        """删规则，连同它的回复条目和命中记录（共用一个事务）。"""
        conn = await self.connect()
        try:
            await conn.execute("DELETE FROM reply_hits WHERE rule_id = ?", (rule_id,))
            await conn.execute("DELETE FROM reply_rule_entries WHERE rule_id = ?", (rule_id,))
            await conn.execute("DELETE FROM reply_rules WHERE id = ?", (rule_id,))
            await conn.commit()
        finally:
            await conn.close()

    async def merge_reply_rules(self, target_id: int) -> tuple[int, int]:
        """把同商品、同类型的其它规则并进 target，返回（合并了几条规则, 合并后条目数）。

        目标规则在前，其它规则按 priority DESC, id 追加；关键词+内容完全相同的条目不重复
        插入。历史命中记录会搬到对应的新条目上，合并后不会把老会话重新回一遍。
        共用设置（匹配方式 / 冷却 / 优先级 / 启停）以目标规则为准。
        """
        conn = await self.connect()
        try:
            cur = await conn.execute("SELECT * FROM reply_rules WHERE id = ?", (target_id,))
            target = await cur.fetchone()
            if target is None:
                raise KeyError(f"reply rule {target_id} not found")

            cur = await conn.execute(
                "SELECT * FROM reply_rule_entries WHERE rule_id = ? ORDER BY sort_order, id",
                (target_id,),
            )
            target_entries = await cur.fetchall()
            existing: dict[tuple[str, str], int] = {
                (entry["keywords"] or "", entry["content"] or ""): int(entry["id"])
                for entry in target_entries
            }
            next_order = max(
                (int(entry["sort_order"]) for entry in target_entries), default=-1
            ) + 1

            cur = await conn.execute(
                "SELECT * FROM reply_rules WHERE id != ? AND item_id = ? AND kind = ?"
                " ORDER BY priority DESC, id",
                (target_id, target["item_id"], target["kind"]),
            )
            siblings = await cur.fetchall()
            if not siblings:
                await conn.commit()
                return 0, len(target_entries)

            ts = now_iso()
            merged = 0
            for sibling in siblings:
                cur = await conn.execute(
                    "SELECT * FROM reply_rule_entries WHERE rule_id = ? ORDER BY sort_order, id",
                    (sibling["id"],),
                )
                sibling_entries = await cur.fetchall()
                first_entry_id = 0
                for entry in sibling_entries:
                    key = (entry["keywords"] or "", entry["content"] or "")
                    entry_id = existing.get(key)
                    if entry_id is None:
                        cur = await conn.execute(
                            _INSERT_REPLY_ENTRY,
                            (target_id, entry["keywords"], entry["content"],
                             next_order, ts, ts),
                        )
                        entry_id = int(cur.lastrowid or 0)
                        next_order += 1
                        existing[key] = entry_id
                    if not first_entry_id:
                        first_entry_id = entry_id
                    await conn.execute(
                        "UPDATE reply_hits SET rule_id = ?, entry_id = ?"
                        " WHERE rule_id = ? AND entry_id = ?",
                        (target_id, entry_id, sibling["id"], entry["id"]),
                    )
                # 老记录 / 指向已删条目的记录：挂到这条规则搬过来的第一条回复上
                fallback = first_entry_id or (next(iter(existing.values())) if existing else 0)
                await conn.execute(
                    "UPDATE reply_hits SET rule_id = ?, entry_id = ? WHERE rule_id = ?",
                    (target_id, fallback, sibling["id"]),
                )
                await conn.execute(
                    "DELETE FROM reply_rule_entries WHERE rule_id = ?", (sibling["id"],)
                )
                await conn.execute("DELETE FROM reply_rules WHERE id = ?", (sibling["id"],))
                merged += 1
            if existing:
                # 顶层 keywords/content 是第一条条目的兼容镜像：目标原本没条目时也要补上
                first_keywords, first_content = next(iter(existing))
                await conn.execute(
                    "UPDATE reply_rules SET keywords = ?, content = ?, updated_at = ? WHERE id = ?",
                    (first_keywords, first_content, ts, target_id),
                )
            cur = await conn.execute(
                "SELECT COUNT(*) AS n FROM reply_rule_entries WHERE rule_id = ?", (target_id,)
            )
            row = await cur.fetchone()
            await conn.commit()
            return merged, int(row["n"]) if row else 0
        except Exception:
            await conn.rollback()
            raise
        finally:
            await conn.close()

    async def record_reply_hit(self, account_id: int, sender_id: str, chat_key: str,
                               rule_id: int, entry_id: int = 0) -> int:
        """记一次「这条回复已经回过这个会话」，用于去重与冷却。

        entry_id 是规则里的具体回复条目：一条规则配多条回复时，各自独立去重。
        """
        return await self.execute(
            "INSERT INTO reply_hits"
            " (account_id, sender_id, chat_key, rule_id, entry_id, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (account_id, sender_id or "", chat_key or "", rule_id,
             int(entry_id or 0), now_iso()),
        )

    async def reply_hit_exists(self, account_id: int, sender_id: str, chat_key: str,
                              rule_id: int, entry_id: int | None = None) -> bool:
        """这条回复是否已经在这个会话里回过（once_per_chat 用）。

        entry_id 不传 = 这条规则下任意一条回复回过都算（老调用方的语义）；
        传具体值 = 只看那一条回复。
        """
        sql = (
            "SELECT id FROM reply_hits WHERE account_id = ? AND sender_id = ?"
            " AND chat_key = ? AND rule_id = ?"
        )
        params: list[Any] = [account_id, sender_id or "", chat_key or "", rule_id]
        if entry_id is not None:
            sql += " AND entry_id = ?"
            params.append(int(entry_id or 0))
        row = await self.fetch_one(sql + " LIMIT 1", params)
        return row is not None

    async def reply_hit_recent(self, account_id: int, sender_id: str, rule_id: int,
                               seconds: int, entry_id: int | None = None) -> bool:
        """这条回复最近 seconds 秒内是否回过这个买家（冷却用）。

        entry_id 的口径和 reply_hit_exists 一样：不传看整条规则，传了只看那一条回复。
        """
        if seconds <= 0:
            return False
        sql = (
            "SELECT id FROM reply_hits WHERE account_id = ? AND sender_id = ? AND rule_id = ?"
            " AND created_at >= datetime('now', 'localtime', ?)"
        )
        params: list[Any] = [account_id, sender_id or "", rule_id, f"-{int(seconds)} seconds"]
        if entry_id is not None:
            sql += " AND entry_id = ?"
            params.append(int(entry_id or 0))
        row = await self.fetch_one(sql + " LIMIT 1", params)
        return row is not None

    async def clear_reply_hits(self, rule_id: int | None = None) -> None:
        """清空回复记录：改了规则想立刻重新生效时用。"""
        if rule_id is None:
            await self.execute("DELETE FROM reply_hits")
        else:
            await self.execute("DELETE FROM reply_hits WHERE rule_id = ?", (rule_id,))

    # ---------------------------------------------------------------- 卡密

    async def add_cards(self, contents: Iterable[str], item_id: str = "*") -> int:
        """导入卡密；同一串内容（不分商品池）只保留一份。

        同一张卡被粘两次就会在池子里出现两行，take_card() 会分别卖给两个人。
        这里在写事务里先查已有内容，再做去重；跨商品池也按全局内容去重，
        因为同一张卡密不应该在两个池子里各卖一次。
        """
        ts = now_iso()
        cleaned: list[str] = []
        seen: set[str] = set()
        for content in contents:
            text = str(content or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            cleaned.append(text)
        if not cleaned:
            return 0

        conn = await self.connect()
        try:
            await conn.execute("BEGIN IMMEDIATE")
            existing: set[str] = set()
            chunk_size = 500  # 留足 SQLite 变量数量余量
            for start in range(0, len(cleaned), chunk_size):
                chunk = cleaned[start:start + chunk_size]
                placeholders = ", ".join("?" for _ in chunk)
                cur = await conn.execute(
                    f"SELECT content FROM cards WHERE content IN ({placeholders})",  # noqa: S608 - 只有占位符
                    tuple(chunk),
                )
                existing.update(str(row["content"]) for row in await cur.fetchall())
            rows = [
                (item_id, content, "available", ts)
                for content in cleaned if content not in existing
            ]
            if not rows:
                await conn.commit()
                return 0
            await conn.executemany(
                "INSERT INTO cards (item_id, content, status, created_at) VALUES (?, ?, ?, ?)",
                rows,
            )
            await conn.commit()
            return len(rows)
        except Exception:
            await conn.rollback()
            raise
        finally:
            await conn.close()

    async def list_cards(self, status: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        sql = "SELECT * FROM cards"
        params: list[Any] = []
        if status:
            sql += " WHERE status = ?"
            params.append(status)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        return await self.fetch_all(sql, tuple(params))

    async def card_stock(self) -> list[dict[str, Any]]:
        return await self.fetch_all(
            "SELECT item_id, COUNT(*) AS available FROM cards WHERE status = 'available' GROUP BY item_id"
        )

    async def take_card(self, item_id: str, order_id: str | None, buyer_id: str | None) -> CardAllocation:
        """原子地取出一张卡密。

        先匹配该商品专属的卡密，再退化到通配池；取出时立刻标记 sold，
        依靠 UPDATE ... WHERE status = 'available' 保证不会重复发同一张。
        """
        conn = await self.connect()
        try:
            await conn.execute("BEGIN IMMEDIATE")
            cur = await conn.execute(
                "SELECT id, content FROM cards WHERE status = 'available'"
                " AND (item_id = ? OR item_id = '*')"
                " ORDER BY CASE WHEN item_id = ? THEN 0 ELSE 1 END, id LIMIT 1",
                (item_id, item_id),
            )
            row = await cur.fetchone()
            if row is None:
                await conn.rollback()
                return CardAllocation(card_id=None, content=None)
            cur = await conn.execute(
                "UPDATE cards SET status = 'sold', order_id = ?, buyer_id = ?, sold_at = ?"
                " WHERE id = ? AND status = 'available'",
                (order_id, buyer_id, now_iso(), row["id"]),
            )
            if cur.rowcount == 0:
                await conn.rollback()
                return CardAllocation(card_id=None, content=None)
            await conn.commit()
            return CardAllocation(card_id=int(row["id"]), content=str(row["content"]))
        except Exception:
            await conn.rollback()
            raise
        finally:
            await conn.close()

    async def release_card(self, card_id: int) -> None:
        await self.execute(
            "UPDATE cards SET status = 'available', order_id = NULL, buyer_id = NULL, sold_at = NULL"
            " WHERE id = ?",
            (card_id,),
        )

    async def delete_card(self, card_id: int) -> None:
        await self.execute("DELETE FROM cards WHERE id = ?", (card_id,))

    # ---------------------------------------------------------------- 订单

    async def get_order(self, account_id: int, order_id: str | None) -> dict[str, Any] | None:
        """按 (账号, 订单号) 取发货记录；没有就返回 None。"""
        if not order_id:
            return None
        return await self.fetch_one(
            "SELECT * FROM orders WHERE account_id = ? AND order_id = ?",
            (account_id, str(order_id)),
        )

    async def delivered_order_exists(self, account_id: int, order_id: str | None) -> bool:
        """这个订单是否已经成功发过货。

        只认 delivered：失败记录（upstream_failed / send_failed）不算，
        否则改好配置后同一单永远重试不了。
        """
        if not order_id:
            return False
        row = await self.fetch_one(
            "SELECT id FROM orders WHERE account_id = ? AND order_id = ? AND status = 'delivered'",
            (account_id, order_id),
        )
        return row is not None

    async def delivery_blocked(self, account_id: int, order_id: str | None) -> bool:
        """自动发货是否必须停手。

        delivered：已经发过；
        send_unknown：消息可能已经到买家手上（ack 超时/连接中途断开）；
        sending：正在发送，另一个路径别插队；
        topping_up / topup_unknown：充值请求已发出或结果未知 —— 再充一次可能多送钱；
        awaiting_key：已经提示买家发旧订单号，等买家回复，轮询不要再催一遍。
        这些情况都不允许自动重发，结果未知的交给人工核对。
        """
        if not order_id:
            return False
        row = await self.fetch_one(
            "SELECT id FROM orders WHERE account_id = ? AND order_id = ?"
            " AND status IN ('delivered', 'send_unknown', 'sending',"
            " 'topping_up', 'topup_unknown', 'awaiting_key',"
            " 'awaiting_account', 'awaiting_username', 'awaiting_confirm')",
            (account_id, str(order_id)),
        )
        return row is not None

    async def recover_stale_sending(self) -> int:
        """进程启动时把上次残留的 sending 单标成 send_unknown。

        能留下 sending 说明进程在「已落库、还没等到发送结果」时被杀掉：
        消息可能已经发出去了，不能自动重发，也不能退卡。
        """
        conn = await self.connect()
        try:
            cur = await conn.execute(
                "UPDATE orders SET status = 'send_unknown',"
                " detail = CASE WHEN detail != '' THEN detail || '；' ELSE '' END"
                " || '进程在发送过程中中断，结果未知；请人工核对后处理'"
                " WHERE status = 'sending'"
            )
            changed = cur.rowcount or 0
            # 充值也是不可逆的外呼：进程在「请求已发出、记录未写」之间死掉时，
            # 绝不能当没充过再充一次 —— 标成 topup_unknown，等人工去 new-api 核对余额。
            cur = await conn.execute(
                "UPDATE orders SET status = 'topup_unknown',"
                " detail = CASE WHEN detail != '' THEN detail || '；' ELSE '' END"
                " || '进程在充值过程中中断，结果未知；请到 new-api 后台核对余额后处理'"
                " WHERE status = 'topping_up'"
            )
            changed += cur.rowcount or 0
            # awaiting_key 本身没有不可逆动作，但进程可能死在「已落库、提示还没发」之间。
            # 重启后退回 topup_failed，让轮询重新提示一次，避免订单永远卡住。
            cur = await conn.execute(
                "UPDATE orders SET status = 'topup_failed',"
                " detail = CASE WHEN detail != '' THEN detail || '；' ELSE '' END"
                " || '进程重启，重新提示买家发旧订单号'"
                " WHERE status = 'awaiting_key'"
            )
            changed += cur.rowcount or 0
            await conn.commit()
            return changed
        finally:
            await conn.close()

    #: 允许写进 orders / messages 的列名。字段名是代码里写死的常量，
    #: 但拼 SQL 的地方显式收口一下，免得哪天有人把外部字符串递进来。
    _ORDER_COLUMNS = frozenset({
        "account_id", "order_id", "item_id", "buyer_id", "buyer_name", "rule_id",
        "card_id", "content", "amount_cny", "quota_usd", "api_key_id", "card_code",
        "key_order_id", "account_user_id", "account_username", "status", "detail",
        "created_at",
    })
    _MESSAGE_COLUMNS = frozenset({
        "account_id", "chat_id", "sender_id", "sender_name", "item_id", "order_id",
        "content", "kind", "message_type", "media_url", "media_thumb_url",
        "media_width", "media_height", "media_duration", "media_name", "media_size",
        "local_media", "local_poster", "external_id", "read_at", "recalled_at", "created_at",
    })

    @classmethod
    def _checked_columns(cls, fields: dict[str, Any], allowed: frozenset[str]) -> list[str]:
        unknown = set(fields) - allowed
        if unknown:
            raise ValueError(f"不允许的列名：{sorted(unknown)}")
        return list(fields)

    async def record_order(self, **fields: Any) -> int:
        """记录一次发货。

        同一 (账号, 订单号) 只保留一行：重试时覆盖上一次的失败记录，
        这样「哪个订单还没发出去」永远用一条 SQL 就能查干净。

        「查 + 写」必须在同一个写事务里（BEGIN IMMEDIATE）。旧写法是先 SELECT
        再 INSERT/UPDATE：推送和轮询同时处理同一单时，两边都查不到就都 INSERT，
        后一个直接撞唯一索引抛 IntegrityError —— 而那一步在发货之后，
        会被上层当成「没发出去」（退卡 + 重发）。

        另外，原来那条 blocked（有人在拿别人的订单号试发货）被后来的 delivered
        覆盖时，会把原因并进 detail，审计信息不会凭空消失。
        """
        fields.setdefault("created_at", now_iso())
        names = self._checked_columns(fields, self._ORDER_COLUMNS)
        account_id = fields.get("account_id")
        order_id = fields.get("order_id")

        if not order_id:
            # 没有订单号的行不参与 idx_orders_dedup（它的 WHERE 是 order_id IS NOT NULL），
            # 直接插入；也就不存在并发覆盖的问题。
            columns = ", ".join(names)
            placeholders = ", ".join("?" for _ in names)
            return await self.execute(
                f"INSERT INTO orders ({columns}) VALUES ({placeholders})",  # noqa: S608 - 列名已过白名单
                tuple(fields.values()),
            )

        conn = await self.connect()
        try:
            await conn.execute("BEGIN IMMEDIATE")
            cur = await conn.execute(
                "SELECT id, status, detail, created_at FROM orders"
                " WHERE account_id = ? AND order_id = ?",
                (account_id, order_id),
            )
            existing = await cur.fetchone()
            if existing is not None:
                update_fields = dict(fields)
                # 重试/状态变化不应该把「首次创建时间」改成现在：
                # 页面上的「今日发货」按 created_at 统计，旧单会被算成今天。
                update_fields.pop("created_at", None)
                if not update_fields:
                    await conn.commit()
                    return int(existing["id"])
                previous_status = str(existing["status"] or "")
                new_status = str(update_fields.get("status") or previous_status)

                # delivered 是终态：后续核实失败/上游失败不能把已发货记录降级，
                # 否则 delivered_order_exists() 会变 False，订单轮询可能再发一次。
                # 只在 detail 里追加审计信息，核心发货字段一个都不动。
                if previous_status == "delivered" and new_status != "delivered":
                    note = str(update_fields.get("detail") or "").strip()
                    merged = str(existing["detail"] or "").strip()
                    if note:
                        merged = (merged + "；" if merged else "") + (
                            f"（已发货后收到 {new_status}：{note}）"
                        )
                    await conn.execute(
                        "UPDATE orders SET detail = ? WHERE id = ?",
                        (merged, int(existing["id"])),
                    )
                    await conn.commit()
                    return int(existing["id"])

                # topup_unknown 和 send_unknown 一样接近终态：只允许人工确认后的
                # delivered 覆盖它。充值可能已经生效，被别的状态顺手改掉会有重复充值的风险。
                if previous_status == "topup_unknown" and new_status not in ("topup_unknown", "delivered"):
                    note = str(update_fields.get("detail") or "").strip()
                    merged = str(existing["detail"] or "").strip()
                    if note:
                        merged = (merged + "；" if merged else "") + (
                            f"（充值结果未知后收到 {new_status}：{note}）"
                        )
                    await conn.execute(
                        "UPDATE orders SET detail = ? WHERE id = ?",
                        (merged, int(existing["id"])),
                    )
                    await conn.commit()
                    return int(existing["id"])

                # send_unknown 也接近终态：只允许人工确认后的 delivered 覆盖它。
                # 不能被 blocked/upstream_failed 顺手改掉，否则同一单可能被重新发送。
                if previous_status == "send_unknown" and new_status not in ("send_unknown", "delivered"):
                    note = str(update_fields.get("detail") or "").strip()
                    merged = str(existing["detail"] or "").strip()
                    if note:
                        merged = (merged + "；" if merged else "") + (
                            f"（发送结果未知后收到 {new_status}：{note}）"
                        )
                    await conn.execute(
                        "UPDATE orders SET detail = ? WHERE id = ?",
                        (merged, int(existing["id"])),
                    )
                    await conn.commit()
                    return int(existing["id"])

                if (
                    previous_status == "blocked"
                    and update_fields.get("status") != "blocked"
                    and "detail" in update_fields
                ):
                    update_fields["detail"] = (
                        str(update_fields.get("detail") or "")
                        + "（此前曾拦截：" + str(existing["detail"] or "") + "）"
                    )
                assignments = ", ".join(
                    f"{key} = ?" for key in update_fields if key != "account_id"
                )
                values = tuple(
                    value for key, value in update_fields.items() if key != "account_id"
                )
                await conn.execute(
                    f"UPDATE orders SET {assignments} WHERE id = ?",  # noqa: S608 - 列名已过白名单
                    (*values, int(existing["id"])),
                )
                await conn.commit()
                return int(existing["id"])

            columns = ", ".join(names)
            placeholders = ", ".join("?" for _ in names)
            cur = await conn.execute(
                f"INSERT INTO orders ({columns}) VALUES ({placeholders})",  # noqa: S608 - 列名已过白名单
                tuple(fields.values()),
            )
            await conn.commit()
            return int(cur.lastrowid or 0)
        except Exception:
            await conn.rollback()
            raise
        finally:
            await conn.close()

    async def list_orders(self, limit: int = 100) -> list[dict[str, Any]]:
        return await self.fetch_all("SELECT * FROM orders ORDER BY id DESC LIMIT ?", (limit,))

    # ---------------------------------------------------------------- 消息

    async def log_message(self, **fields: Any) -> int:
        """记一条消息流水。

        带 external_id（服务端 messageId）时先查一次：推送、轮询、手动同步
        可能拿到同一条消息，重复写会让聊天界面出现两条一模一样的对话。
        """
        fields.setdefault("created_at", now_iso())
        external_id = str(fields.get("external_id") or "").strip()
        fields["external_id"] = external_id or None
        names = self._checked_columns(fields, self._MESSAGE_COLUMNS)
        columns = ", ".join(names)
        placeholders = ", ".join("?" for _ in names)
        conn = await self.connect()
        try:
            # 唯一索引负责最终去重：推送/轮询/历史同步并发写同一条时，
            # 先 INSERT OR IGNORE，再读回赢家那一行的 id。
            cur = await conn.execute(
                f"INSERT OR IGNORE INTO messages ({columns}) VALUES ({placeholders})",  # noqa: S608
                tuple(fields.values()),
            )
            if (cur.rowcount or 0) == 0 and external_id and fields.get("account_id") and fields.get("chat_id"):
                existing = await conn.execute(
                    "SELECT id FROM messages WHERE account_id = ? AND chat_id = ?"
                    " AND external_id = ? LIMIT 1",
                    (fields["account_id"], fields["chat_id"], external_id),
                )
                row = await existing.fetchone()
                await conn.commit()
                return int(row["id"]) if row is not None else 0
            await conn.commit()
            return int(cur.lastrowid or 0)
        except Exception:
            await conn.rollback()
            raise
        finally:
            await conn.close()

    async def log_synced_message(self, **fields: Any) -> int:
        """记一条「按需补拉」来的历史消息，尽量不产生重复。

        聊天界面第一次打开某个会话时会去闲鱼拉一次最近历史。那些消息
        可能已经被推送/轮询路径记过一遍了 —— 老库里没存 messageId，
        所以除了按 external_id 去重，还按「同会话 + 同方向 + 同内容 +
        前后 3 分钟」兜一次底。宁可少一条重复气泡，也不要让页面看着像买家复读。
        """
        fields.setdefault("created_at", now_iso())
        account_id = fields.get("account_id")
        chat_id = fields.get("chat_id") or ""
        external_id = str(fields.get("external_id") or "").strip()
        kind = fields.get("kind") or "incoming"
        content = fields.get("content") or ""

        if external_id:
            row = await self.fetch_one(
                "SELECT id FROM messages WHERE account_id = ? AND chat_id = ? AND external_id = ?"
                " LIMIT 1",
                (account_id, chat_id, external_id),
            )
            if row is not None:
                return int(row["id"])
        if content:
            row = await self.fetch_one(
                "SELECT id FROM messages WHERE account_id = ? AND chat_id = ? AND kind = ?"
                " AND content = ? AND created_at BETWEEN datetime(?, '-180 seconds')"
                " AND datetime(?, '+180 seconds') ORDER BY id DESC LIMIT 1",
                (account_id, chat_id, kind, content, fields["created_at"], fields["created_at"]),
            )
            if row is not None:
                if external_id:
                    try:
                        await self.execute(
                            "UPDATE messages SET external_id = ?"
                            " WHERE id = ? AND external_id IS NULL",
                            (external_id, int(row["id"])),
                        )
                    except sqlite3.IntegrityError:
                        # 极窄的并发：另一个路径刚把这串 external_id 写到别的行上。
                        # 这里已经找到同内容的历史行，不需要再为索引冲突报错。
                        pass
                return int(row["id"])
        return await self.log_message(**fields)

    async def last_chat_id(self, account_id: int, sender_id: str, item_id: str = "") -> str | None:
        """这个买家针对**这个商品**最近一次出现在哪个会话里。

        订单触发的发货本来要 create_chat 走一次 WS 往返才能拿到会话 ID；
        如果他之前聊过天（绝大多数情况都聊过），数据库里就有现成的，直接用。

        必须连商品一起匹配：闲鱼的会话是按「买家 × 商品」分开的，
        拿另一个商品的会话发消息，买家会在错误的窗口里看到卡密。
        """
        if not sender_id or not item_id:
            return None
        row = await self.fetch_one(
            "SELECT chat_id FROM messages WHERE account_id = ? AND sender_id = ? AND item_id = ?"
            " AND chat_id IS NOT NULL AND chat_id != '' ORDER BY id DESC LIMIT 1",
            (account_id, sender_id, item_id),
        )
        if not row:
            return None
        chat_id = str(row["chat_id"] or "").strip()
        return chat_id or None

    async def backfill_buyer_name(self, account_id: int, buyer_id: str, name: str) -> int:
        """把这个买家历史上「没名字」的记录补成真昵称，返回改动行数。

        为什么要回填：交易卡片本身不带昵称，付款那一刻只知道「有人下单了」，
        于是流水里先写成「买家」。等从订单接口拿到真名（或者他终于说了第一句话），
        之前那几条已经落库了 —— 不回填的话页面和令牌名里就一直是「买家」，
        事后根本对不上是谁买的。

        「要不要覆盖」交给 is_placeholder_name 判断，和解析层用同一把尺子，
        所以「等待你发货」这类被误当昵称的卡片标题也会一并纠正。
        """
        from .codec import is_placeholder_name

        if not account_id or not buyer_id or not name or is_placeholder_name(name):
            return 0

        changed = 0
        conn = await self.connect()
        try:
            for table, id_column, name_column in (
                ("messages", "sender_id", "sender_name"),
                ("orders", "buyer_id", "buyer_name"),
            ):
                cur = await conn.execute(
                    f"SELECT id, {name_column} AS name FROM {table}"  # noqa: S608 - 表名/列名都是字面量
                    f" WHERE account_id = ? AND {id_column} = ?",
                    (account_id, buyer_id),
                )
                rows = await cur.fetchall()
                stale = [(int(row["id"]),) for row in rows if is_placeholder_name(row["name"])]
                if not stale:
                    continue
                await conn.executemany(
                    f"UPDATE {table} SET {name_column} = ? WHERE id = ?",  # noqa: S608
                    [(name, row_id) for (row_id,) in stale],
                )
                changed += len(stale)
            await conn.commit()
        finally:
            await conn.close()
        return changed
    async def repair_placeholder_buyer_names(self, account_id: int) -> int:
        """把库里「名字不是名字」的记录，按同一买家的聊天记录纠正一遍。

        只查本地：一个买家只要在聊天里说过一句话，我们手上就有真名，
        没必要为了修一条旧记录去打接口（那才是真的会招风控）。

        存在的理由：付款卡片本身不带昵称，发货那一刻如果还没拿到真名，
        记录就先落地成「买家」或卡片标题了。新逻辑会在发货前补名，
        但历史数据得有人扫一遍，否则页面上永远显示「买家」。返回改动行数。
        """
        rows = await self.fetch_all(
            "SELECT buyer_id AS who FROM orders WHERE account_id = ? AND buyer_id != ''"
            " UNION SELECT sender_id AS who FROM messages WHERE account_id = ? AND sender_id != ''"
            " LIMIT 500",
            (account_id, account_id),
        )
        if not rows:
            return 0

        from .codec import is_placeholder_name

        changed = 0
        for row in rows:
            buyer_id = str(row["who"] or "").strip()
            if not buyer_id:
                continue
            name = await self.known_buyer_name(account_id, buyer_id)
            if not name or is_placeholder_name(name):
                continue
            changed += await self.backfill_buyer_name(account_id, buyer_id, name)
        return changed
    async def known_buyer_name(self, account_id: int, sender_id: str) -> str | None:
        """这个买家**可信**的昵称（以前在真人聊天里出现过的那个）。

        为什么需要它：交易卡片里的 reminderTitle 是卡片标题（「等待你发货」），
        不是昵称。只靠当条消息取名，就会在 new-api 里留下
        「等待你发货#2500000000000000007」这种谁也认不出来的令牌名。
        聊天记录里他真实说过的消息带的是真昵称，翻一条出来用。
        """
        from .codec import is_placeholder_name

        if not sender_id:
            return None
        rows = await self.fetch_all(
            "SELECT sender_name FROM messages WHERE account_id = ? AND sender_id = ?"
            " AND sender_name IS NOT NULL AND sender_name != '' ORDER BY id DESC LIMIT 20",
            (account_id, sender_id),
        )
        for row in rows:
            name = str(row["sender_name"] or "").strip()
            if name and not is_placeholder_name(name):
                return name
        return None

    # ---------------------------------------------------------- 聊天界面

    #: 会话列表的「最后一条消息」按时间取，而不是按 id：
    #: 聊天界面按需补拉历史时，会插入比本地更早的消息，id 顺序不等于对话顺序。
    _CHAT_LAST_SQL = (
        "m.id = (SELECT x.id FROM messages x WHERE x.account_id = m.account_id"
        " AND x.chat_id = m.chat_id ORDER BY x.created_at DESC, x.id DESC LIMIT 1)"
    )

    _CHAT_SELECT = (
        "SELECT m.id, m.account_id, a.name AS account_name, m.chat_id, m.sender_id,"
        " m.sender_name, m.item_id, m.order_id, m.content, m.kind, m.created_at,"
        " (SELECT COUNT(*) FROM messages u WHERE u.account_id = m.account_id"
        "  AND u.chat_id = m.chat_id AND u.kind = 'incoming' AND u.read_at IS NULL) AS unread,"
        " (SELECT u.sender_id FROM messages u WHERE u.account_id = m.account_id"
        "  AND u.chat_id = m.chat_id AND u.sender_id IS NOT NULL AND u.sender_id != ''"
        "  ORDER BY u.created_at DESC, u.id DESC LIMIT 1) AS buyer_id"
        " FROM messages m JOIN accounts a ON a.id = m.account_id"
    )

    async def list_chats(self, account_id: int | None = None, limit: int = 100) -> list[dict[str, Any]]:
        """后台聊天界面的会话列表：每个会话只留最后一条消息 + 未读数。

        只列还有账号的会话：账号删了就没有连接能发消息，留在列表里只会点一次错一次。
        """
        limit = max(1, min(int(limit), 500))
        sql = self._CHAT_SELECT + " WHERE m.chat_id IS NOT NULL AND m.chat_id != '' AND " + self._CHAT_LAST_SQL
        params: list[Any] = []
        if account_id is not None:
            sql += " AND m.account_id = ?"
            params.append(int(account_id))
        sql += " ORDER BY m.created_at DESC, m.id DESC LIMIT ?"
        params.append(limit)
        return await self.fetch_all(sql, tuple(params))

    async def get_chat(self, account_id: int, chat_id: str) -> dict[str, Any] | None:
        """单个会话的摘要（最后一条消息 + 未读数 + 买家 ID）。"""
        sql = self._CHAT_SELECT + " WHERE m.account_id = ? AND m.chat_id = ? AND " + self._CHAT_LAST_SQL
        return await self.fetch_one(sql, (int(account_id), str(chat_id)))

    async def list_chat_messages(self, account_id: int, chat_id: str,
                                 limit: int = 100) -> list[dict[str, Any]]:
        """一个会话的消息（按对话顺序，旧 → 新）。"""
        limit = max(1, min(int(limit), 500))
        rows = await self.fetch_all(
            "SELECT * FROM (SELECT * FROM messages WHERE account_id = ? AND chat_id = ?"
            " ORDER BY created_at DESC, id DESC LIMIT ?) ORDER BY created_at, id",
            (int(account_id), str(chat_id), limit),
        )
        return rows

    async def chat_peer_id(self, account_id: int, chat_id: str) -> str | None:
        """这个会话对面的买家 ID（最新一条带 sender_id 的消息）。"""
        row = await self.fetch_one(
            "SELECT sender_id FROM messages WHERE account_id = ? AND chat_id = ?"
            " AND sender_id IS NOT NULL AND sender_id != ''"
            " ORDER BY created_at DESC, id DESC LIMIT 1",
            (int(account_id), str(chat_id)),
        )
        value = str((row or {}).get("sender_id") or "").strip()
        return value or None

    async def last_chat_id_any(self, account_id: int, sender_id: str) -> str | None:
        """这个买家最近出现在哪个会话里（不限定商品）。

        订单里没带商品 ID 时，至少还能复用他聊过的上一个会话，
        总比直接放弃好 —— 反正发出去以后买家是在真实会话里看到的。
        """
        if not sender_id:
            return None
        row = await self.fetch_one(
            "SELECT chat_id FROM messages WHERE account_id = ? AND sender_id = ?"
            " AND chat_id IS NOT NULL AND chat_id != ''"
            " ORDER BY created_at DESC, id DESC LIMIT 1",
            (int(account_id), str(sender_id)),
        )
        value = str((row or {}).get("chat_id") or "").strip()
        return value or None

    async def get_message(self, message_id: int) -> dict[str, Any] | None:
        return await self.fetch_one("SELECT * FROM messages WHERE id = ?", (int(message_id),))

    async def mark_message_recalled(self, message_id: int) -> None:
        """标记一条消息已撤回（内容保留在库里，前端按 recalled_at 渲染成系统提示）。"""
        await self.execute(
            "UPDATE messages SET recalled_at = ? WHERE id = ? AND recalled_at IS NULL",
            (now_iso(), int(message_id)),
        )

    async def mark_chat_read(self, account_id: int, chat_id: str) -> int:
        """把会话里的收到消息标成已读（清掉红点），返回改动条数。"""
        conn = await self.connect()
        try:
            cur = await conn.execute(
                "UPDATE messages SET read_at = ? WHERE account_id = ? AND chat_id = ?"
                " AND kind = 'incoming' AND read_at IS NULL",
                (now_iso(), int(account_id), str(chat_id)),
            )
            changed = cur.rowcount or 0
            await conn.commit()
            return changed
        finally:
            await conn.close()

    # ---------------------------------------------------- 外部消息去重

    async def messages_seen(self, account_id: int, keys: Sequence[str]) -> set[str]:
        """这些消息键里，哪些已经处理过了。

        推送和「拉会话列表」两条路会拿到同一条消息，重启后又会把最近一页
        再拉一遍；不记一下就会重复自动回复。用独立的表记，别污染消息流水。
        """
        keys = [k for k in keys if k]
        if not keys:
            return set()
        placeholders = ", ".join("?" for _ in keys)
        rows = await self.fetch_all(
            # 只拼接 "?" 占位符，值全部走参数绑定
            f"SELECT msg_key FROM seen_messages WHERE account_id = ? AND msg_key IN ({placeholders})",  # noqa: S608
            (account_id, *keys),
        )
        return {str(row["msg_key"]) for row in rows}

    async def claim_message_seen(self, account_id: int, key: str) -> bool:
        """原子地「认领」一条消息；返回 False 表示别人已经处理过。

        INSERT OR IGNORE + rowcount 是唯一可靠的判据：先 messages_seen()
        再 mark 的写法在推送/轮询并发时两边都能看到 False，然后各处理一遍。
        """
        if not key:
            return False
        conn = await self.connect()
        try:
            cur = await conn.execute(
                "INSERT OR IGNORE INTO seen_messages (account_id, msg_key, created_at)"
                " VALUES (?, ?, ?)",
                (account_id, key, now_iso()),
            )
            changed = cur.rowcount or 0
            await conn.commit()
            return changed == 1
        finally:
            await conn.close()

    async def mark_messages_seen(self, account_id: int, keys: Iterable[str]) -> None:
        keys = [k for k in keys if k]
        if not keys:
            return
        ts = now_iso()
        conn = await self.connect()
        try:
            await conn.executemany(
                "INSERT OR IGNORE INTO seen_messages (account_id, msg_key, created_at) VALUES (?, ?, ?)",
                [(account_id, key, ts) for key in keys],
            )
            await conn.commit()
        finally:
            await conn.close()

    async def unmark_messages_seen(self, account_id: int, keys: Iterable[str]) -> int:
        """撤销「这条已经处理过」的标记。

        消息是在处理**之前**就标记的（否则推送和轮询会各回一遍）。万一下游
        处理抛异常，这条消息就永久丢了 —— 这时候把标记撤掉，下一轮还能重来。
        只在异常路径上调，正常流程不会走到这里。
        """
        keys = [k for k in keys if k]
        if not keys:
            return 0
        placeholders = ", ".join("?" for _ in keys)
        conn = await self.connect()
        try:
            cur = await conn.execute(
                # 只拼 "?" 占位符，值全部走参数绑定
                f"DELETE FROM seen_messages WHERE account_id = ? AND msg_key IN ({placeholders})",  # noqa: S608
                (account_id, *keys),
            )
            changed = cur.rowcount or 0
            await conn.commit()
            return changed
        finally:
            await conn.close()

    async def prune_history(self, days: int) -> int:
        """删掉 N 天前的**消息流水**，返回删除条数；days <= 0 = 不删。

        为什么只清 messages：
          * messages 是最占地方的（每条带完整正文），长期跑会无限涨；
          * seen_messages 和 reply_hits 一旦删掉，「同一会话只回一次」「这条已经
            处理过」就同时失效 —— 那会导致给买家重复回复、同一单重复处理，
            所以宁可留着它们（一行只有几十字节，一年也就几十 MB）。
        """
        if days <= 0:
            return 0
        cutoff = f"-{int(days)} days"
        conn = await self.connect()
        try:
            cur = await conn.execute(
                "DELETE FROM messages WHERE created_at < datetime('now', 'localtime', ?)",
                (cutoff,),
            )
            deleted = cur.rowcount or 0
            await conn.commit()
            return deleted
        finally:
            await conn.close()

    async def list_messages(self, limit: int = 100) -> list[dict[str, Any]]:
        return await self.fetch_all("SELECT * FROM messages ORDER BY id DESC LIMIT ?", (limit,))
