"""SQLite 存储层。

故意保持轻量：单文件 SQLite + aiosqlite，部署不需要额外中间件。
所有写操作都是显式的，方便在测试里把 DB 换成临时文件。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import aiosqlite

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
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_reply_hits_dedup ON reply_hits (account_id, sender_id, chat_key, rule_id);
CREATE INDEX IF NOT EXISTS idx_reply_hits_rule ON reply_hits (account_id, sender_id, rule_id, created_at);

CREATE TABLE IF NOT EXISTS app_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
);

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
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_created ON messages (created_at DESC);
"""


#: 后加的列：(表, 列, 类型定义)。老库升级时自动补齐，新库建表后也会跑一遍（幂等）。
MIGRATIONS: tuple[tuple[str, str, str], ...] = (
    ("rules", "usd_multiplier", "REAL NOT NULL DEFAULT 1"),
    ("rules", "fixed_usd", "REAL NOT NULL DEFAULT 0"),
    ("orders", "amount_cny", "REAL"),
    ("orders", "quota_usd", "REAL"),
    ("orders", "api_key_id", "TEXT"),
    ("orders", "card_code", "TEXT"),
)


def now_iso() -> str:
    """本地时间秒级 ISO 字符串，便于人读。"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


async def _apply_migrations(conn: aiosqlite.Connection) -> None:
    """给已有数据库补上后加的列（SQLite 没有 IF NOT EXISTS 加列语法）。"""
    for table, column, ddl in MIGRATIONS:
        cur = await conn.execute(f"PRAGMA table_info({table})")
        rows = await cur.fetchall()
        existing = {row[1] for row in rows}
        if column in existing:
            continue
        await conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
    await conn.commit()


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

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    async def connect(self) -> aiosqlite.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = await aiosqlite.connect(self.path)
        conn.row_factory = sqlite3.Row
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA foreign_keys=ON")
        await conn.execute("PRAGMA busy_timeout=5000")
        return conn

    async def init(self) -> None:
        conn = await self.connect()
        try:
            await conn.executescript(SCHEMA)
            await conn.commit()
            await _apply_migrations(conn)
        finally:
            await conn.close()

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

    # ---------------------------------------------------------------- 账号

    async def list_accounts(self, only_enabled: bool = False) -> list[dict[str, Any]]:
        sql = "SELECT * FROM accounts"
        if only_enabled:
            sql += " WHERE enabled = 1"
        return await self.fetch_all(sql + " ORDER BY id")

    async def get_account(self, account_id: int) -> dict[str, Any] | None:
        return await self.fetch_one("SELECT * FROM accounts WHERE id = ?", (account_id,))

    async def upsert_account(self, name: str, cookie: str, remark: str = "", enabled: bool = True) -> int:
        ts = now_iso()
        existing = await self.fetch_one("SELECT id FROM accounts WHERE name = ?", (name,))
        if existing:
            await self.execute(
                "UPDATE accounts SET cookie = ?, remark = ?, enabled = ?, updated_at = ? WHERE id = ?",
                (cookie, remark, int(enabled), ts, existing["id"]),
            )
            return int(existing["id"])
        return await self.execute(
            "INSERT INTO accounts (name, cookie, remark, enabled, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
            (name, cookie, remark, int(enabled), ts, ts),
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
            (cookie, now_iso(), account_id),
        )

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
        )
        if rule_id:
            await self.execute(
                "UPDATE rules SET name = ?, item_id = ?, enabled = ?, mode = ?, content = ?,"
                " auto_confirm = ?, priority = ?, remark = ?, usd_multiplier = ?, fixed_usd = ?,"
                " updated_at = ? WHERE id = ?",
                (*values, ts, rule_id),
            )
            return rule_id
        return await self.execute(
            "INSERT INTO rules (name, item_id, enabled, mode, content, auto_confirm, priority, remark,"
            " usd_multiplier, fixed_usd, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (*values, ts, ts),
        )
    async def delete_rule(self, rule_id: int) -> None:
        await self.execute("DELETE FROM rules WHERE id = ?", (rule_id,))

    # ---------------------------------------------------------- 自动回复

    async def list_reply_rules(self, only_enabled: bool = False) -> list[dict[str, Any]]:
        sql = "SELECT * FROM reply_rules"
        if only_enabled:
            sql += " WHERE enabled = 1"
        return await self.fetch_all(sql + " ORDER BY priority DESC, id")

    async def get_reply_rule(self, rule_id: int) -> dict[str, Any] | None:
        return await self.fetch_one("SELECT * FROM reply_rules WHERE id = ?", (rule_id,))

    async def save_reply_rule(self, payload: dict[str, Any], rule_id: int | None = None) -> int:
        """新增或更新一条自动回复规则（商品 / 关键词两种都走这里）。"""
        ts = now_iso()
        values = (
            payload["name"], payload["kind"], payload.get("item_id") or "*",
            payload.get("keywords") or "", payload.get("match_type") or "contains",
            payload.get("content") or "", int(bool(payload.get("enabled", True))),
            int(bool(payload.get("once_per_chat", True))), int(payload.get("cooldown") or 0),
            int(payload.get("priority") or 0), payload.get("remark") or "",
        )
        if rule_id:
            await self.execute(
                "UPDATE reply_rules SET name = ?, kind = ?, item_id = ?, keywords = ?, match_type = ?,"
                " content = ?, enabled = ?, once_per_chat = ?, cooldown = ?, priority = ?, remark = ?,"
                " updated_at = ? WHERE id = ?",
                (*values, ts, rule_id),
            )
            return rule_id
        return await self.execute(
            "INSERT INTO reply_rules (name, kind, item_id, keywords, match_type, content, enabled,"
            " once_per_chat, cooldown, priority, remark, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (*values, ts, ts),
        )

    async def delete_reply_rule(self, rule_id: int) -> None:
        await self.execute("DELETE FROM reply_rules WHERE id = ?", (rule_id,))

    async def record_reply_hit(self, account_id: int, sender_id: str, chat_key: str,
                               rule_id: int) -> int:
        """记一次「这条规则已经回过这个会话」，用于去重与冷却。"""
        return await self.execute(
            "INSERT INTO reply_hits (account_id, sender_id, chat_key, rule_id, created_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (account_id, sender_id or "", chat_key or "", rule_id, now_iso()),
        )

    async def reply_hit_exists(self, account_id: int, sender_id: str, chat_key: str,
                              rule_id: int) -> bool:
        """这条规则是否已经在这个会话里回过（once_per_chat 用）。"""
        row = await self.fetch_one(
            "SELECT id FROM reply_hits WHERE account_id = ? AND sender_id = ? AND chat_key = ?"
            " AND rule_id = ? LIMIT 1",
            (account_id, sender_id or "", chat_key or "", rule_id),
        )
        return row is not None

    async def reply_hit_recent(self, account_id: int, sender_id: str, rule_id: int,
                               seconds: int) -> bool:
        """这条规则最近 seconds 秒内是否回过这个买家（冷却用）。"""
        if seconds <= 0:
            return False
        row = await self.fetch_one(
            "SELECT id FROM reply_hits WHERE account_id = ? AND sender_id = ? AND rule_id = ?"
            " AND created_at >= datetime('now', 'localtime', ?) LIMIT 1",
            (account_id, sender_id or "", rule_id, f"-{int(seconds)} seconds"),
        )
        return row is not None

    async def clear_reply_hits(self, rule_id: int | None = None) -> None:
        """清空回复记录：改了规则想立刻重新生效时用。"""
        if rule_id is None:
            await self.execute("DELETE FROM reply_hits")
        else:
            await self.execute("DELETE FROM reply_hits WHERE rule_id = ?", (rule_id,))

    # ---------------------------------------------------------------- 卡密

    async def add_cards(self, contents: Iterable[str], item_id: str = "*") -> int:
        ts = now_iso()
        rows = [(item_id, c.strip(), "available", ts) for c in contents if c and c.strip()]
        if not rows:
            return 0
        conn = await self.connect()
        try:
            await conn.executemany(
                "INSERT INTO cards (item_id, content, status, created_at) VALUES (?, ?, ?, ?)",
                rows,
            )
            await conn.commit()
        finally:
            await conn.close()
        return len(rows)

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

    #: 允许写进 orders / messages 的列名。字段名是代码里写死的常量，
    #: 但拼 SQL 的地方显式收口一下，免得哪天有人把外部字符串递进来。
    _ORDER_COLUMNS = frozenset({
        "account_id", "order_id", "item_id", "buyer_id", "buyer_name", "rule_id",
        "card_id", "content", "amount_cny", "quota_usd", "api_key_id", "card_code",
        "status", "detail", "created_at",
    })
    _MESSAGE_COLUMNS = frozenset({
        "account_id", "chat_id", "sender_id", "sender_name", "item_id", "order_id",
        "content", "kind", "created_at",
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
                "SELECT id, status, detail FROM orders WHERE account_id = ? AND order_id = ?",
                (account_id, order_id),
            )
            existing = await cur.fetchone()
            if existing is not None:
                update_fields = dict(fields)
                previous_status = str(existing["status"] or "")
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
        fields.setdefault("created_at", now_iso())
        names = self._checked_columns(fields, self._MESSAGE_COLUMNS)
        columns = ", ".join(names)
        placeholders = ", ".join("?" for _ in names)
        return await self.execute(
            f"INSERT INTO messages ({columns}) VALUES ({placeholders})",  # noqa: S608 - 列名已过白名单
            tuple(fields.values()),
        )

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
