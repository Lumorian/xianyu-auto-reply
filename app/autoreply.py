"""自动回复：商品自动回复 + 关键词自动回复。

和「发货」是两条独立的路子：

* **商品自动回复**：买家针对某个商品发来任何一句话，就回一条预设文案
  （常见用法：「在的，拍下自动发货」）。
* **关键词自动回复**：买家说的话里命中关键词才回（常见用法：
  「发货」「怎么用」「能便宜点吗」）。

这一层不碰网络，纯匹配 + 渲染，方便单元测试；真正发消息交给 runner。
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from typing import Any

from .codec import IncomingMessage, is_ignored, is_paid_trigger
from .db import Database
from .template import render_placeholders

logger = logging.getLogger(__name__)

#: 关键词分隔符：中英文逗号、分号、半角/全角竖线、换行都算
_SPLIT_RE = re.compile(r"[,，;；|｜\n]+")

#: 匹配方式
MATCH_CONTAINS = "contains"
MATCH_EXACT = "exact"
MATCH_REGEX = "regex"
MATCH_TYPES = (MATCH_CONTAINS, MATCH_EXACT, MATCH_REGEX)

#: 规则类型
KIND_ITEM = "item"
KIND_KEYWORD = "keyword"
KINDS = (KIND_ITEM, KIND_KEYWORD)


def split_keywords(raw: str | None) -> list[str]:
    """把规则里的关键词串拆成列表（去空、去重、保序）。"""
    if not raw:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for part in _SPLIT_RE.split(raw):
        word = part.strip()
        if not word or word in seen:
            continue
        seen.add(word)
        out.append(word)
    return out


def item_matches(rule: dict[str, Any], item_id: str | None) -> bool:
    """规则的商品范围是否覆盖这条消息。* / 空 / all 视为「全部商品」。"""
    target = str(rule.get("item_id") or "*").strip()
    if target in ("*", "", "all"):
        return True
    if not item_id:
        return False
    return target == str(item_id).strip()


def reply_entries(rule: dict[str, Any]) -> list[dict[str, Any]]:
    """规则里的「关键词 -> 回复」条目。

    新库每条规则至少有一条；这里仍兜底兼容「只有平铺 keywords/content」的
    老对象（测试里手搓规则、或回填还没跑到的极老库）。
    """
    entries = rule.get("entries")
    if entries:
        return entries
    return [{
        "id": 0,
        "keywords": rule.get("keywords") or "",
        "content": rule.get("content") or "",
    }]


#: 关键词正则的长度上限。正常人写的关键词远短于这个数，
#: 而会触发灾难性回溯的畸形正则通常又长又嵌套。
MAX_REGEX_LEN = 200

#: 用正则匹配时只看正文的前 N 个字符。
#: 回溯是输入长度的指数函数，限长能把最坏情况压到可接受范围；
#: 关键词是「命中任意一段即可」，截断尾部的代价远小于卡死一个账号。
MAX_REGEX_SUBJECT = 500

#: 已编译的正则缓存：规则是常驻的，每来一条消息重编译一次纯属浪费。
#: 值为 None 表示这条规则被判为不可用（太长/写错/会造成灾难性回溯）。
_REGEX_CACHE: dict[str, re.Pattern[str] | None] = {}

#: 「嵌套量词」形状：一个分组里带量词，分组本身又带量词。
#: 形如 (a+)+ / (a*b)* / (?:x+){2,} —— 这就是灾难性回溯的经典构造。
_NESTED_QUANTIFIER = re.compile(
    r"\((?=[^()]*(?:[+*]|\{[0-9]+,))[^()]*\)\s*(?:[+*]|\{[0-9]+,)"
)


def looks_catastrophic(pattern: str) -> bool:
    """粗略判断这条正则会不会把事件循环卡死。

    不去追求完美判定（那需要真正的正则分析器），只挡住最常见的一种：
    嵌套量词。规则是管理员在后台手填的，误填才是现实风险，
    为此付出的代价是一条规则不生效 —— 相比整个账号静默卡死太划算了。
    """
    return bool(_NESTED_QUANTIFIER.search(pattern))


def compile_keyword_regex(pattern: str) -> re.Pattern[str] | None:
    """编译一条关键词正则；太长、写错、或有回溯风险都返回 None。"""
    if pattern in _REGEX_CACHE:
        return _REGEX_CACHE[pattern]
    compiled: re.Pattern[str] | None = None
    if len(pattern) > MAX_REGEX_LEN:
        logger.warning("关键词正则过长（%s 字符，上限 %s），已跳过：%s",
                       len(pattern), MAX_REGEX_LEN, pattern[:60])
    elif looks_catastrophic(pattern):
        # 不放进线程里「超时兜底」：线程取消不掉，畸形正则会把一个核占死，
        # 而且 executors 的线程不是 daemon，连进程都退不掉。直接不编译最干净。
        logger.warning("关键词正则可能导致灾难性回溯（嵌套量词），已跳过：%s", pattern[:60])
    else:
        try:
            compiled = re.compile(pattern, re.IGNORECASE)
        except re.error as exc:
            logger.warning("关键词规则里的正则写错了，已跳过：%s（%s）", pattern, exc)
    _REGEX_CACHE[pattern] = compiled
    return compiled


def _regex_search(pattern: str, text: str) -> bool:
    compiled = compile_keyword_regex(pattern)
    if compiled is None:
        return False
    return compiled.search(text[:MAX_REGEX_SUBJECT]) is not None


async def match_keyword_async(content: str, keywords: list[str],
                              match_type: str = MATCH_CONTAINS) -> str | None:
    """match_keyword 的异步壳子。

    目前是直接调同步版（危险的形状已经在编译期被拒掉了），
    保留 async 签名是为了让调用方不必因为将来换实现而改动。
    """
    return match_keyword(content, keywords, match_type)


def match_keyword(content: str, keywords: list[str], match_type: str = MATCH_CONTAINS) -> str | None:
    """命中的关键词；没命中返回 None。

    contains / exact 不区分大小写；regex 用 re.search，写错了只记日志不算命中。
    """
    if not content or not keywords:
        return None
    text = content.strip()
    lowered = text.lower()
    if match_type == MATCH_REGEX:
        for pattern in keywords:
            if _regex_search(pattern, text):
                return pattern
        return None
    if match_type == MATCH_EXACT:
        for word in keywords:
            if lowered == word.lower():
                return word
        return None
    for word in keywords:
        if word.lower() in lowered:
            return word
    return None


@dataclass(slots=True)
class ReplyDecision:
    """自动回复的结论。"""

    action: str = "skip"  # "reply" | "skip"
    reason: str = ""
    rule: dict[str, Any] | None = None
    #: 命中的具体回复条目（一条规则里可以配多条「关键词 -> 回复」）
    entry: dict[str, Any] | None = None
    entry_id: int = 0
    content: str = ""
    keyword: str = ""
    #: 会话标识：优先用 chat_id，没有就用「买家+商品」拼一个，保证去重粒度正确
    chat_key: str = ""

    @property
    def should_reply(self) -> bool:
        return self.action == "reply"


def render_reply(template: str, *, buyer_name: str = "", item_id: str | None = None,
                 order_id: str | None = None, keyword: str = "",
                 date: str | None = None) -> str:
    """渲染自动回复文案，支持 {buyer} {item_id} {order_id} {keyword} {date}。"""
    values = {
        "buyer": buyer_name or "亲",
        "item_id": item_id or "",
        "order_id": order_id or "",
        "keyword": keyword,
        "date": date or time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    return render_placeholders(template, values)


def chat_key_of(msg: IncomingMessage) -> str:
    """给一次会话算个稳定的键，用于「同一会话只回一次」。"""
    if msg.chat_id:
        return str(msg.chat_id)
    if msg.sender_id:
        return f"{msg.sender_id}@{msg.item_id or '*'}"
    return ""


class AutoReplyEngine:
    """根据买家消息挑一条自动回复规则。"""

    def __init__(self, db: Database) -> None:
        self.db = db

    async def decide(self, account_id: int, msg: IncomingMessage) -> ReplyDecision:
        if not msg.sender_id:
            return ReplyDecision(reason="缺少买家 ID")
        content = (msg.content or "").strip()
        if not content:
            return ReplyDecision(reason="空消息")
        # 系统卡片（已付款待发货 / 交易关闭 / 退款申请…）不自动回复：
        # 付款那单该走发货，回一句「在的」只会干扰买家。
        # 不要只靠文案列表判断：闲鱼会不断新增卡片类型，is_system_card
        # 才是「这条不是买家真人打的字」的稳定信号。
        if msg.is_system_card:
            return ReplyDecision(reason="系统卡片")
        if is_ignored(content) or is_paid_trigger(content):
            return ReplyDecision(reason="系统提示消息")

        rules = await self.db.list_reply_rules(only_enabled=True)
        if not rules:
            return ReplyDecision(reason="没有启用中的自动回复规则")

        chat_key = chat_key_of(msg)
        candidates = await self._rank(rules, msg, content)
        if not candidates:
            return ReplyDecision(reason="没有命中自动回复规则")

        empty_rules: list[str] = []
        for rule, entry, keyword in candidates:
            reason = await self._blocked(account_id, msg, rule, entry, chat_key)
            if reason:
                logger.debug("自动回复规则 %s 跳过：%s", rule.get("name"), reason)
                continue
            rendered = render_reply(
                entry.get("content") or "",
                buyer_name=msg.sender_name,
                item_id=msg.item_id,
                order_id=msg.order_id,
                keyword=keyword,
            )
            if not rendered.strip():
                # 这一条是空的就继续看后面的条目，别让一条写坏的回复挡住整条规则
                empty_rules.append(str(rule.get("name")))
                logger.warning("自动回复规则「%s」里有一条回复内容为空，已跳过", rule.get("name"))
                continue
            return ReplyDecision(
                action="reply",
                reason=(
                    f"命中{('关键词' if rule.get('kind') == KIND_KEYWORD else '商品')}"
                    f"自动回复「{rule.get('name')}」"
                ),
                rule=rule,
                entry=entry,
                entry_id=int(entry.get("id") or 0),
                content=rendered,
                keyword=keyword,
                chat_key=chat_key,
            )
        if empty_rules:
            return ReplyDecision(reason="命中的回复内容为空")
        return ReplyDecision(reason="命中的规则都在冷却或已回过")

    async def _rank(self, rules: list[dict[str, Any]], msg: IncomingMessage,
                    content: str) -> list[tuple[dict[str, Any], dict[str, Any], str]]:
        """按优先级排出候选条目（规则里的每一条回复都是一个候选）。

        顺序：关键词规则 > 商品规则；同一类里按 rules 的
        priority DESC, id 升序（数据库已经排好，这里保持相对顺序）；
        同一条规则内按 sort_order, id 升序。
        """
        keyword_hits: list[tuple[dict[str, Any], dict[str, Any], str]] = []
        item_hits: list[tuple[dict[str, Any], dict[str, Any], str]] = []
        for rule in rules:
            if not item_matches(rule, msg.item_id):
                continue
            kind = str(rule.get("kind") or KIND_KEYWORD)
            entries = reply_entries(rule)
            if kind == KIND_KEYWORD:
                match_type = str(rule.get("match_type") or MATCH_CONTAINS)
                for entry in entries:
                    keyword = await match_keyword_async(
                        content, split_keywords(entry.get("keywords")), match_type
                    )
                    if keyword:
                        keyword_hits.append((rule, entry, keyword))
            else:
                for entry in entries:
                    item_hits.append((rule, entry, ""))
        # 商品专属规则优先于 * 通配，通配规则只在没有专属规则时兜底
        def spec(rule: dict[str, Any]) -> int:
            return 0 if str(rule.get("item_id") or "*") not in ("*", "", "all") else 1

        keyword_hits.sort(key=lambda hit: spec(hit[0]))
        item_hits.sort(key=lambda hit: spec(hit[0]))
        return keyword_hits + item_hits

    async def _blocked(self, account_id: int, msg: IncomingMessage, rule: dict[str, Any],
                       entry: dict[str, Any], chat_key: str) -> str:
        """这条回复此刻不该回的原因；可以回则返回空串。

        去重 / 冷却按「规则里的条目」算：同一条规则里配了「发货」和「退款」，
        在同一个会话里回完「发货」不影响「退款」该不该回。
        """
        rule_id = int(rule["id"])
        entry_id = int(entry.get("id") or 0)
        if int(rule.get("once_per_chat") or 0) and await self.db.reply_hit_exists(
            account_id, msg.sender_id, chat_key, rule_id, entry_id
        ):
            return "这个会话已经回过"
        cooldown = int(rule.get("cooldown") or 0)
        if cooldown > 0 and await self.db.reply_hit_recent(
            account_id, msg.sender_id, rule_id, cooldown, entry_id=entry_id
        ):
            return f"冷却中（{cooldown} 秒）"
        return ""
