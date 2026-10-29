"""闲鱼 IM 消息解包与结构化。

闲鱼 WebSocket 推送的消息里，``body.syncPushPackage.data[0].data``
 是一段 base64（可能再做 msgpack 编码）的负载。本模块负责：

1. base64 解码 + msgpack 解包；
2. 把嵌套的字典结构（键全是数字字符串）规整成便于业务使用的结构；
3. 从卡片消息里提取 itemId / orderId。
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any

import msgpack

from .memory import cap_mapping

logger = logging.getLogger(__name__)

#: 卖家侧「买家已付款」类系统卡片文案，命中即视为待发货。
PAID_KEYWORDS: tuple[str, ...] = (
    "[我已付款，等待你发货]",
    "我已付款，等待你发货",
    "[已付款，待发货]",
    "已付款，待发货",
    "[记得及时发货]",
    "等待卖家发货",
)

#: 只记录不发货的系统提示。
IGNORED_KEYWORDS: tuple[str, ...] = (
    "[我已拍下，待付款]",
    "等待买家付款",
    "交易关闭",
    "[你关闭了订单，钱款已原路退返]",
    "发来一条消息",
    "发来一条新消息",
    "[买家确认收货，交易成功]",
    "快给ta一个评价吧~",
    "快给ta一个评价吧～",
    "卖家人不错？送Ta闲鱼小红花",
    "[你已确认收货，交易成功]",
    "[你已发货]",
)

#: 这些串出现在 reminderTitle 里时，它是**卡片标题**，不是买家昵称。
#: 线上真实取值：「等待你发货」「买家已拍下，待付款」「我发起了退款申请」
#: 「等待买家收货」「交易关闭」……同一个买家 id 会因为这些卡片拿到好几个
#: 不同的「名字」，于是 new-api 里出现「等待你发货#2500000000000000007」这种
#: 根本对不上人的令牌名。
PLACEHOLDER_NAME_PARTS: tuple[str, ...] = (
    "等待你发货",
    "等待卖家发货",
    "等待买家付款",
    "等待买家收货",
    "买家已拍下",
    "我已付款",
    "我发起了退款",
    "退款成功",
    "交易关闭",
    "交易成功",
    "你已发货",
    "你已确认收货",
    "你已确认收款",
    "你关闭了订单",
    "买家确认收货",
    "评价",
    "系统消息",
    "发来一条消息",
    "发来一条新消息",
    # 会话列表 / 系统会话的标题，同样是「不是昵称的名字」：
    # 「交易消息」「工作台通知」会被 reminderTitle 带进 sender_name，
    # 一旦被当成昵称存下来，后面所有回查都会先撞上它。
    "交易消息",
    "工作台通知",
    "我完成了评价",
    "去评价",
)

#: 这些昵称本身没有信息量，等同于「没拿到」。
_EMPTY_NAMES = frozenset({"", "买家", "对方", "未知", "闲鱼用户", "null", "None"})


def is_placeholder_name(name: str | None) -> bool:
    """这个名字是不是「其实不是昵称」——卡片标题、空值、通用占位。

    reminderTitle 在真人文本消息里是昵称（「测试者-winkee」），
    在交易卡片里却变成标题（「等待你发货」）。两者共用一个字段，
    所以用之前必须过一遍这个判断。
    """
    text = str(name or "").strip()
    if text in _EMPTY_NAMES:
        return True
    if len(text) > 24:
        # 真昵称不会这么长，多半是把整段提示文案塞进来了
        return True
    return any(part in text for part in PLACEHOLDER_NAME_PARTS)


def pick_display_name(*candidates: str | None, fallback: str = "买家") -> str:
    """从若干候选里挑一个能当昵称用的；都不行就用 fallback。"""
    for candidate in candidates:
        text = str(candidate or "").strip()
        if text and not is_placeholder_name(text):
            return text
    return fallback


#: 订单详情里买家昵称的字段名（按可信度）。卖家订单接口把买家信息放在
#: buyerInfoVO 里，userNick 就是真昵称 —— 新买家还没说过话时，这是唯一来源。
_ORDER_NICK_KEYS: tuple[str, ...] = ("userNick", "buyerNick", "nick", "nickName", "userName")


#: 会话对象里可能带买家昵称的字段（会话列表 / 历史消息的 extension）。
_SESSION_NICK_KEYS: tuple[str, ...] = ("extUserName", "buyerNick", "userNick", "nick", "nickName")


def buyer_nick_from_session(*nodes: Any) -> str:
    """从会话结构里取买家昵称，取不到返回空串。

    为什么值得单独找：付款卡片本身不带昵称，聊天记录里也可能一条都没有
    （新买家第一句就是下单）。而会话列表的 singleChatConversation.extension
    里是带着对方资料的 —— 拿得到就不用为了一个名字去查订单接口。
    只认明确的昵称字段，绝不把会话标题、商品标题当名字。
    """
    for node in nodes:
        source = _as_dict(node)
        if not source:
            continue
        for key in _SESSION_NICK_KEYS:
            name = pick_display_name(source.get(key), fallback="")
            if name:
                return name
    return ""


def buyer_nick_from_order(order: dict[str, Any] | None) -> str:
    """从订单详情里取买家昵称；取不到返回空串。

    为什么必须只认 buyerInfoVO：订单里还有商品、卖家自己的昵称，
    随手扫一遍 nick 字段很容易把「卖家名」或「商品标题」当成买家名，
    那比留空更糟（令牌名会挂到别人头上）。
    """
    if not isinstance(order, dict) or not order:
        return ""

    buyer = _as_dict(order.get("buyerInfoVO"))
    if not buyer:
        # 有的回包把它埋在别的层级，按 key 名找，但仍然只认 buyerInfoVO 这棵树
        for key, value in walk_pairs(order):
            if key == "buyerInfoVO" and isinstance(value, dict):
                buyer = value
                break
    for key in _ORDER_NICK_KEYS:
        name = pick_display_name(buyer.get(key), fallback="")
        if name:
            return name
    return ""


_ORDER_ID_PATTERNS = (
    re.compile(r"order_detail\?id=(\d+)"),
    re.compile(r"[?&]orderId=(\d+)"),
    re.compile(r"bizOrderId=(\d+)"),
)
#: flat 负载里 content.custom.data 是 base64 的 JSON，要先解开再找 id

_ITEM_ID_PATTERNS = (
    re.compile(r"[?&]itemId=(\d+)"),
    re.compile(r"\"itemId\"\s*:\s*\"?(\d+)"),
)


class DecodeError(ValueError):
    """消息负载无法解包。"""


def decode_payloads(data: Any) -> list[dict[str, Any]]:
    """把单条推送负载解成**若干个**字典。

    闲鱼一个 data 里可能塞了不止一条消息：base64 解开后是连续拼接的
    多个 msgpack 对象（流式拼接），也可能整体就是 base64(JSON)。
    只取第一个会漏消息，所以这里全部取出来。
    """
    objects = _unpack_all(data)
    out = [obj for obj in objects if isinstance(obj, dict)]
    if not out:
        raise DecodeError("负载里没有解析出任何对象")
    return out


def _unpack_all(data: Any) -> list[Any]:
    """宽容地解开负载，返回所有解出来的对象。"""
    if isinstance(data, (dict, list)):
        return [data]

    if isinstance(data, bytes):
        raw = data
    elif isinstance(data, str):
        text = data.strip()
        if not text:
            return []
        try:
            raw = base64.b64decode(text + "=" * (-len(text) % 4))
        except (binascii.Error, ValueError) as exc:  # pragma: no cover - 异常分支
            raise DecodeError(f"负载不是合法 base64: {exc}") from exc
    else:  # pragma: no cover - 类型防御
        raise DecodeError(f"不支持的负载类型: {type(data)!r}")

    # 策略 1：base64 里直接是 JSON 文本
    try:
        text = raw.decode("utf-8").strip()
    except UnicodeDecodeError:
        text = ""
    if text.startswith("{") or text.startswith("["):
        try:
            parsed = json.loads(text)
        except ValueError:
            parsed = None
        if parsed is not None:
            return parsed if isinstance(parsed, list) else [parsed]

    # 策略 2：流式 msgpack，能读多少读多少（多对象拼接就靠它）
    for kwargs in ({"raw": False}, {"raw": True}):
        try:
            unpacker = msgpack.Unpacker(strict_map_key=False, **kwargs)
            unpacker.feed(raw)
            objects = []
            for obj in unpacker:
                objects.append(obj)
            if objects:
                return [_normalize_unpacked(obj) for obj in objects]
        except Exception:  # noqa: S112 - 策略试探：这一种装不下就换下一种，
            # 逐条记日志只会变成噪声；三种都失败时调用方会拿到明确的 DecodeError
            continue

    # 策略 3：单对象解包
    for kwargs in ({"raw": False}, {"raw": True}):
        try:
            obj = msgpack.unpackb(raw, strict_map_key=False, **kwargs)
        except Exception:  # noqa: S112 - 同上
            continue
        return [_normalize_unpacked(obj)]

    raise DecodeError("负载既不是 msgpack 也不是 JSON")


def _normalize_unpacked(value: Any) -> Any:
    """把 msgpack 的 bytes 键值递归转成 str，省得下游到处判断类型。"""
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.decode("utf-8", errors="ignore")
    if isinstance(value, list):
        return [_normalize_unpacked(item) for item in value]
    if isinstance(value, dict):
        return {str(_normalize_unpacked(k)): _normalize_unpacked(v) for k, v in value.items()}
    return value


def decode_payload(data: Any) -> dict[str, Any]:
    """把负载解成单个字典（取第一条）。保留给只关心一条的调用方。"""
    return decode_payloads(data)[0]


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _walk_strings(node: Any) -> list[str]:
    """深度收集结构里的所有字符串，用于兜底提取 id。"""
    out: list[str] = []
    stack = [node]
    while stack:
        cur = stack.pop()
        if isinstance(cur, str):
            out.append(cur)
        elif isinstance(cur, dict):
            stack.extend(cur.values())
        elif isinstance(cur, (list, tuple)):
            stack.extend(cur)
    return out


def walk_pairs(node: Any) -> Any:
    """深度遍历，产出所有 (键, 值) 对，方便按字段名找金额等。"""
    stack = [node]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            for key, value in cur.items():
                yield (str(key), value)
                stack.append(value)
        elif isinstance(cur, (list, tuple)):
            stack.extend(cur)


def _search(patterns: tuple[re.Pattern[str], ...], texts: list[str]) -> str | None:
    for text in texts:
        for pattern in patterns:
            match = pattern.search(text)
            if match:
                return match.group(1)
    return None


def extract_item_id(raw: dict[str, Any]) -> str | None:
    """从消息里提取商品 ID。"""
    detail = _as_dict(_as_dict(raw.get("1")).get("10"))
    reminder_url = detail.get("reminderUrl")
    candidates: list[str] = []
    if isinstance(reminder_url, str):
        candidates.append(reminder_url)
    candidates.extend(_walk_strings(raw))
    return _search(_ITEM_ID_PATTERNS, candidates)


def extract_order_id(raw: dict[str, Any]) -> str | None:
    """从卡片消息里提取订单 ID。

    付款卡片把跳转地址放在 ``1.6.3.5``（一段 JSON 字符串）里，
    形如 ``fleamarket://order_detail?id=xxx&role=seller``。
    """
    card = _as_dict(_as_dict(_as_dict(raw.get("1")).get("6")).get("3"))
    candidates: list[str] = []
    card_json = card.get("5")
    if isinstance(card_json, str):
        candidates.append(card_json)
    candidates.extend(_walk_strings(raw))
    return _search(_ORDER_ID_PATTERNS, candidates)


#: contentType 里只有这两个是买家真人内容：1 文本、2 图片。
#: 其余（8 引导 / 14 系统提示 / 25 评价 / 26 交易卡片 / 36 卡片）都是系统消息。
_HUMAN_CONTENT_TYPES = frozenset({1, 2})

#: 卡片负载里一定会出现的**键名**。买家在聊天框里只能写字，写不出「键」，
#: 所以按键名判断是安全的 —— 有些卡片推送里根本没带 "4"，
#: 只看 contentType 会把真卡片的 content_type 默认成 1（文本）。
#:
#: ⚠️ 绝对不能改成「对整段 JSON 做子串匹配」：正文（买家可控）就存在
#: payload 的 text 值里，扫值等于把「是不是交易卡片」这个信任判断交给买家。
#: 曾经的写法把 order_detail 当标记扫全文，买家只要在聊天框里打一句
#: 「我已付款，等待你发货 fleamarket://order_detail?id=1234」就能被当成
#: 系统交易卡片，跳过订单核实直接领走卡密/Key。
_CARD_KEYS = frozenset({"dxcard", "trade_paid_done", "ordercard", "ordercardinfo"})

#: 交易卡片的消息类型（26 = 交易卡片，36 = 卡片消息）
_CARD_CONTENT_TYPES = frozenset({26, 36})


def _payload_is_card(payload: dict[str, Any] | None) -> bool:
    """负载的**结构**里是不是带了一张交易卡片。

    只看键名与 contentType，不看任何字符串值 —— 值里可能是买家打的字。
    """
    if not isinstance(payload, dict) or not payload:
        return False
    stack: list[Any] = [payload]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            for key, value in cur.items():
                lowered = str(key).lower()
                if lowered in _CARD_KEYS:
                    return True
                if lowered == "contenttype":
                    try:
                        if int(value) in _CARD_CONTENT_TYPES:
                            return True
                    except (TypeError, ValueError):
                        pass
                if isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(cur, list):
            stack.extend(cur)
    return False


def _is_card_type(content_type: Any, payload: dict[str, Any] | None = None) -> bool:
    """这条消息是不是系统卡片（而不是买家真人打的字）。

    两个信号任一成立就算卡片：
      1. 负载结构里带着 dxCard 这类只有交易链路才产出的**键**；
      2. contentType 不是 1/2（文本、图片），或压根认不出来。

    注意：这里只是「消息形态」判断，**不是信任判断**。自动发货不再依赖
    order_id_trusted：runner 对所有付款触发都会核实订单归属。自动评价仍用
    (is_system_card and order_id_trusted) 判断订单号是否来自结构化字段。
    """
    if _payload_is_card(payload):
        return True
    if content_type is None:
        return False
    try:
        return int(content_type) not in _HUMAN_CONTENT_TYPES
    except (TypeError, ValueError):
        # 认不出的类型（服务端给了非数字）：当卡片处理，避免把系统消息
        # 当成买家真人文本去自动回复；自动评价是否可信仍看 order_id_trusted，
        # 自动发货则一律以 runner 的订单核实结果为准。
        return True


def is_paid_trigger(content: str) -> bool:
    """判断一条消息是否为「已付款，请发货」触发语。"""
    if not content:
        return False
    return any(word in content for word in PAID_KEYWORDS)


def is_ignored(content: str) -> bool:
    """判断一条消息是否属于「只记录不处理」的系统提示。"""
    if not content:
        return False
    return any(word in content for word in IGNORED_KEYWORDS)


@dataclass(slots=True)
class IncomingMessage:
    """规整后的聊天消息。"""

    chat_id: str
    sender_id: str
    sender_name: str
    content: str
    item_id: str | None = None
    #: 商品标题（会话 extensions 里的 itemTitle）。只用于后台把「商品 1093…」
    #: 显示成商品名；不参与任何发货判断 —— 它只是卖家给自己的商品起的名字。
    item_title: str = ""
    order_id: str | None = None
    created_at: int = 0
    raw: dict[str, Any] = field(default_factory=dict)
    #: 订单轮询造出来时附带的实付金额（元）。有它就免掉一次订单详情查询。
    amount_cny_hint: float | None = None
    #: 服务端消息 id。推送和「拉会话列表」两条路会用同一个 id，
    #: 拿它去做跨路径去重，否则同一条消息会被处理两遍。
    message_id: str = ""
    #: 订单号是不是来自**服务端结构化字段**（卡片 JSON / reminderUrl 参数 /
    #: 会话 extension / 订单接口），而不是从正文里 grep 出来的。
    #:
    #: 这是鉴权的第二道闸：买家能控制正文，也就控制得了「正文里出现的订单号」。
    #: 只有「卡片形态 + 订单号来源可信」两个条件同时成立，才允许跳过
    #: 「去闲鱼核实订单」这一步（见 engine.decide）。
    order_id_trusted: bool = False
    #: 订单号是否已经找闲鱼核实过「确实付了款、等我发货」。
    #: 真人文本消息必须核实过才允许发货（见 engine.decide）。
    order_verified: bool = False
    #: 是系统卡片（付款/发货/退款…）还是买家真人打的字。
    #:
    #: 这个字段是发货鉴权的关键：买家自己在聊天框里打下
    #: 「我已付款，等待你发货」也是合法的 content，光看正文根本分不出来。
    #: 真人文本必须额外核对订单状态才允许发货（见 runner._verify_order_pending）。
    is_system_card: bool = False
    #: text / image / video。聊天界面按它决定渲染气泡还是媒体。
    message_type: str = "text"
    media_url: str = ""
    media_thumb_url: str = ""
    media_width: int | None = None
    media_height: int | None = None
    media_duration: int | None = None


def _b64_json(value: Any) -> dict[str, Any]:
    """把 base64 的 JSON 字符串解开；解不开返回空字典。"""
    if not isinstance(value, str) or not value:
        return {}
    try:
        raw = base64.b64decode(value + "=" * (-len(value) % 4))
    except (binascii.Error, ValueError):
        return {}
    try:
        obj = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {}
    return obj if isinstance(obj, dict) else {}


def decode_custom(content: dict[str, Any]) -> dict[str, Any]:
    """flat 负载的正文藏在 content.custom.data 里（base64 JSON），这里解开。

    形如 {"custom": {"type": 26, "data": "<base64>"}}，
    解开后可能是 {"contentType": 26, "dxCard": {...}} 或 {"contentType": 1, "text": {...}}。
    """
    custom = _as_dict(content.get("custom"))
    return _b64_json(custom.get("data"))


def extract_media_ref(payload: dict[str, Any] | None) -> tuple[str, str, str, int | None, int | None, int | None]:
    """从消息体里提取图片/视频引用。

    闲鱼图片：{"contentType":2, "image": {"pics":[{"url":..., "width":..}]}}
    闲鱼视频：{"contentType":4, "video": {"url":..., "snapshot":..., "width":.., "duration":..}}
    也兼容 photo / 顶层 pics 这些历史写法。
    返回 (message_type, url, thumb_url, width, height, duration)。
    """
    if not isinstance(payload, dict):
        return "text", "", "", None, None, None

    video = payload.get("video")
    if isinstance(video, dict):
        url = str(video.get("url") or "").strip()
        thumb = str(video.get("snapshot") or video.get("thumbUrl") or "").strip()
        width = _positive_int(video.get("width"))
        height = _positive_int(video.get("height"))
        duration = _positive_int(video.get("duration"))
        if url or thumb:
            return "video", url, thumb, width, height, duration

    image = payload.get("image") or payload.get("photo") or payload
    if isinstance(image, dict):
        url = str(image.get("url") or image.get("picUrl") or "").strip()
        pics = image.get("pics")
        if not url and isinstance(pics, list) and pics:
            first = pics[0] if isinstance(pics[0], dict) else {}
            url = str(first.get("url") or first.get("picUrl") or "").strip()
            if isinstance(first, dict):
                image = first
        if url:
            return (
                "image", url, "",
                _positive_int(image.get("width")),
                _positive_int(image.get("height")),
                None,
            )
    return "text", "", "", None, None, None


def _positive_int(value: Any) -> int | None:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def collect_texts(*nodes: Any) -> list[str]:
    """从一个或多个结构里，按「像不像正文」的顺序收集候选文本。

    优先级：正文 > 卡片标题 > 系统提示 > 任意字符串。
    后面 parse_message 拿到的是合并结果，用换行连接，这样关键词匹配能命中任意一段。
    """
    strong: list[str] = []
    titles: list[str] = []
    others: list[str] = []

    stack = [n for n in nodes if n]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            for key, value in cur.items():
                key_l = str(key).lower()
                if key_l in ("text", "remindercontent", "detailnotice", "content"):
                    if isinstance(value, str) and value:
                        strong.append(value)
                    elif isinstance(value, dict):
                        stack.append(value)
                elif key_l in ("title", "remindertitle"):
                    if isinstance(value, str) and value:
                        titles.append(value)
                elif isinstance(value, (dict, list)):
                    stack.append(value)
                elif isinstance(value, str) and value:
                    others.append(value)
        elif isinstance(cur, list):
            stack.extend(cur)

    ordered = strong + titles
    if ordered:
        # 去重但保序
        seen: set[str] = set()
        out: list[str] = []
        for item in ordered:
            if item in seen:
                continue
            seen.add(item)
            out.append(item)
        return out
    return others[:3]


def _flat_ids(raw: dict[str, Any]) -> tuple[str | None, str | None, bool]:
    """flat 负载里取 (item_id, order_id, order_id_trusted)。

    先解 custom（服务端结构），再全文兜底；只有前者出来的订单号算可信。
    """
    content = _as_dict(_as_dict(raw.get("operation")).get("content"))
    inner = decode_custom(content)
    session_info = _as_dict(_as_dict(raw.get("operation")).get("sessionInfo"))
    extensions = _as_dict(session_info.get("extensions"))

    item_id = str(extensions.get("itemId") or "").strip() or None

    texts: list[str] = []
    for node in (inner, content, raw):
        texts.extend(_walk_strings(node))

    if item_id is None:
        item_id = _search(_ITEM_ID_PATTERNS, texts)

    order_id: str | None = None
    order_trusted = False
    if _payload_is_card(inner):
        found = _search(_ORDER_ID_PATTERNS, _walk_strings(inner))
        if found:
            order_id, order_trusted = found, True
    if order_id is None:
        order_id = _search(_ORDER_ID_PATTERNS, texts)
    return item_id, order_id, order_trusted


def _parse_flat(raw: dict[str, Any], my_id: str) -> IncomingMessage | None:
    """解析闲鱼当前的 flat 消息格式。

    真实结构（线上抓包确认）：
        {
          "chatType": 1, "incrementType": 1, "sessionId": "61051610079",
          "operation": {
            "content": {"contentType": 26, "custom": {"type": 26, "data": "<base64 JSON>"}},
            "receiverIds": ["<我的unb>"],
            "sessionInfo": {
              "sessionId": "...", "ownerUserId": "<卖家>",
              "extensions": {"itemId": "...", "itemTitle": "...", "extUserId": "<买家>",
                             "itemSellerId": "...", "ownerUserId": "..."}
            }
          }
        }

    contentType 语义：1 文本、2 图片、8 会话引导（要忽略）、26 卡片（付款之类）。
    """
    operation = _as_dict(raw.get("operation"))
    content = _as_dict(operation.get("content"))
    session_info = _as_dict(operation.get("sessionInfo"))
    extensions = _as_dict(session_info.get("extensions"))

    content_type = content.get("contentType")
    if content_type == 8:
        # 会话引导（「现在买可以做吗？」这类系统建议），不是真消息
        return None

    chat_id = str(raw.get("sessionId") or session_info.get("sessionId") or "").strip()
    if not chat_id:
        return None

    owner_id = str(session_info.get("ownerUserId") or extensions.get("ownerUserId") or "").strip()
    ext_id = str(extensions.get("extUserId") or "").strip()
    seller_id = str(extensions.get("itemSellerId") or "").strip()

    # 谁发的：owner 是会话主人（卖家）。我是 owner 就说明是买家发来的。
    if my_id and owner_id == my_id:
        sender_id = ext_id
    elif my_id and ext_id == my_id:
        sender_id = owner_id or seller_id
    else:
        sender_id = ext_id or owner_id

    if my_id and sender_id == my_id:
        # 自己发的（含多端同步），交给上层过滤
        return None

    inner = decode_custom(content)
    texts = collect_texts(inner, content)
    content_text = "\n".join(texts).strip()

    item_id, order_id, order_trusted = _flat_ids(raw)
    # sessionInfo.extensions 是服务端结构，itemTitle 就在这里，和 itemId 成对出现
    item_title = " ".join(str(extensions.get("itemTitle") or "").split())

    try:
        created_at = int(session_info.get("createTime") or raw.get("createTime") or 0)
    except (TypeError, ValueError):
        created_at = 0

    nickname = pick_display_name(
        extensions.get("extUserName"), extensions.get("buyerNick"), fallback=""
    ) or ("买家" if (my_id and owner_id == my_id) else "对方")
    msg_type, media_url, media_thumb, media_w, media_h, media_d = extract_media_ref(inner)

    return IncomingMessage(
        chat_id=chat_id,
        sender_id=sender_id,
        sender_name=nickname,
        content=content_text,
        item_id=item_id,
        item_title=item_title,
        order_id=order_id,
        created_at=created_at,
        raw=raw,
        message_id=str(raw.get("messageId") or ""),
        is_system_card=_is_card_type(content_type, inner),
        order_id_trusted=order_trusted,
        message_type=msg_type,
        media_url=media_url,
        media_thumb_url=media_thumb,
        media_width=media_w,
        media_height=media_h,
        media_duration=media_d,
    )


# ---------------------------------------------------------------- 嵌套（数字键）

_URL_PREFIXES = ("http://", "https://", "fleamarket://", "//", "taobao://", "tbopen://")


def _usable_text(value: str) -> str:
    """判断一段字符串是否值得作为正文。

    卡片里的 targetUrl（fleamarket://order_detail?id=...）是跳转地址，
    不是买家该看到的话；混进正文会让关键词匹配失效。
    """
    text = value.strip()
    if not text:
        return ""
    lowered = text.lower()
    if lowered.startswith(_URL_PREFIXES):
        return ""
    return text


def _dedupe_lines(items: list[str]) -> list[str]:
    """按「去掉方括号与空白」归一化后去重，保留首次出现的原文。"""
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        raw = _usable_text(item)
        if not raw:
            continue
        key = raw.strip("[]【】 　\t")
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(raw)
    return out


def _has_cjk(text: str) -> bool:
    """是否含中日韩字符。用来识别「字段错位」（本该是 ID 的位置塞了正文）。"""
    return any("\u4e00" <= ch <= "\u9fff" for ch in text)


def _maybe_json(value: Any) -> dict[str, Any]:
    """字符串可能是纯 JSON、也可能是 base64 包的 JSON，两种都试。"""
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value.strip():
        return {}
    text = value.strip()
    if text.startswith("{"):
        try:
            obj = json.loads(text)
        except ValueError:
            return {}
        return obj if isinstance(obj, dict) else {}
    return _b64_json(text)


def _find_msg_info(raw: dict[str, Any]) -> dict[str, Any]:
    """在嵌套格式里找到真正的「消息体」块。

    闲鱼真实结构（抓包确认）：
        {"1": {"1": {"1": "<senderId>@goofish"}, "2": "<chatId>@goofish",
               "3": "<pnmId>", "5": <毫秒时间戳>,
               "6": {"1": 101, "3": {"1": "<base64>", "2": "<正文>", "4": 26, "5": "<卡片JSON>"}},
               "10": {"reminderContent": "...", "reminderUrl": "...", "senderUserId": "..."}}}

    判定标准：含 "6"（正文块），或同时含 "2"（会话）与 "5"（时间）。
    少数推送把消息体摊平在顶层，所以两层都看。
    """

    def looks_like(node: dict[str, Any]) -> bool:
        # "6" 是正文块；("2"+"5") 是会话+时间；"3" 是 pnmId（参考实现都用它判定）；
        # "10" 带 reminderContent 是最老的一代提醒格式
        if "6" in node or ("2" in node and "5" in node) or ("3" in node and "2" in node):
            return True
        reminder = _as_dict(node.get("10"))
        return bool(reminder.get("reminderContent"))

    first = _as_dict(raw.get("1"))
    if first and looks_like(first):
        return first
    if looks_like(raw):
        return raw

    # 兜底：有些包多套了一层，广度优先找
    queue: list[Any] = [v for v in raw.values() if isinstance(v, (dict, list))]
    while queue:
        cur = queue.pop(0)
        if isinstance(cur, dict):
            if looks_like(cur):
                return cur
            queue.extend(v for v in cur.values() if isinstance(v, (dict, list)))
        elif isinstance(cur, list):
            queue.extend(v for v in cur if isinstance(v, (dict, list)))
    return {}


@dataclass(slots=True)
class ContentBlock:
    """解出来的正文。"""

    content_type: int = 1
    text: str = ""
    payload: dict[str, Any] = field(default_factory=dict)


def _decode_content_block(msg_info: dict[str, Any]) -> ContentBlock:
    """解出消息正文。

    "6" 是内容块，真实形状：
        "6": {"1": 101, "3": {"1": "<base64>", "2": "<正文>", "4": <真实 contentType>, "5": "<JSON>"}}

    "1" 在外面是「自定义消息」标记（101），真正的 contentType 在 "3"."4" 里（1 文本、2 图片、8 引导、26 卡片）。
    """
    content_obj = _as_dict(msg_info.get("6")) or _as_dict(msg_info.get("content"))
    if not content_obj:
        return ContentBlock()

    inner = _as_dict(content_obj.get("3")) or content_obj

    content_type = 1
    for key in ("4", "contentType"):
        value = inner.get(key)
        if isinstance(value, int):
            content_type = value
            break
        if isinstance(value, str) and value.isdigit():
            content_type = int(value)
            break

    payload: dict[str, Any] = {}
    for key in ("5", "1", "data"):
        obj = _maybe_json(inner.get(key))
        if obj:
            nested_type = obj.get("contentType")
            if isinstance(nested_type, int):
                content_type = nested_type
            elif isinstance(nested_type, str) and nested_type.isdigit():
                content_type = int(nested_type)
            payload = payload or obj

    texts: list[str] = []
    for key in ("2", "text", "content"):
        value = inner.get(key)
        if isinstance(value, str) and value.strip():
            texts.append(value.strip())
    if payload:
        texts = collect_texts(payload) + texts

    ordered: list[str] = []
    for item in texts:
        if item and item not in ordered:
            ordered.append(item)
    return ContentBlock(content_type, "\n".join(ordered), payload)


def _is_control_packet(raw: dict[str, Any]) -> bool:
    """判断是不是控制包/成员状态包（不是聊天消息）。

    真实形状：{"1": [{"1": 1, "2": 2, "3": 3}]} —— "1" 是数组，每项只有
    编号类字段（1/2/3/4），没有正文、没有 reminder。
    这类包如果不排除，会被误当成「内容为空的会话消息」记进消息表。
    """
    first = raw.get("1")
    if not isinstance(first, list) or not first:
        return False
    if not all(isinstance(item, dict) for item in first):
        return False
    numbered_only = all(set(map(str, item.keys())).issubset({"1", "2", "3", "4"}) for item in first)
    if not numbered_only:
        return False
    # 只要顶层还带真正的消息字段，就不是控制包
    return not any(key in raw for key in ("10", "6", "content", "msgType", "sessionInfo"))


def _looks_like_message_object(node: Any) -> bool:
    """判断是不是「message 对象」——`cid` + `content` 同时在。

    这是闲鱼第三种真实格式（从 SingleChatConversation/create 回包实测拿到）：

        {
          "messageId": "4368627977983.PNM",
          "createAt": 1792740873447,
          "cid": "68100583197@goofish",
          "sender": {"uid": "4100000003@goofish"},
          "content": {"contentType": 101,
                      "custom": {"type": 26, "data": "<base64 JSON>"}},
          "extension": {"reminderContent": "[你当前宝贝拍下未付款]",
                        "senderUserId": "...", "reminderUrl": "fleamarket://message_chat?itemId=..."}
        }

    注意 `content.contentType` 是 101（表示「自定义消息」），
    真正的业务类型在 `content.custom.type` 里（1 文本 / 26 卡片）。
    """
    return (
        isinstance(node, dict)
        and isinstance(node.get("content"), dict)
        and bool(node.get("cid") or node.get("sessionId"))
    )


def _find_message_object(node: Any) -> dict[str, Any]:
    """在（可能多层包裹的）负载里找 message 对象。"""
    if _looks_like_message_object(node):
        return node

    first = node.get("1") if isinstance(node, dict) else None
    if _looks_like_message_object(first):
        return first

    stack: list[Any] = [node]
    while stack:
        cur = stack.pop(0)
        if _looks_like_message_object(cur):
            return cur
        if isinstance(cur, dict):
            stack.extend(v for v in cur.values() if isinstance(v, (dict, list)))
        elif isinstance(cur, list):
            stack.extend(v for v in cur if isinstance(v, (dict, list)))
    return {}


def _decode_message_object(obj: dict[str, Any], my_id: str) -> IncomingMessage | None:
    """解析 message 对象格式。"""
    content = _as_dict(obj.get("content"))
    extension = _as_dict(obj.get("extension"))
    sender = _as_dict(obj.get("sender"))

    # 101 = 自定义消息，真实类型在 custom.type；其余情况 content.contentType 就是真实类型
    custom = _as_dict(content.get("custom"))
    inner = _b64_json(custom.get("data"))

    content_type = content.get("contentType")
    if isinstance(content_type, str) and content_type.isdigit():
        content_type = int(content_type)
    if content_type in (None, 101) and isinstance(custom.get("type"), int):
        content_type = custom["type"]
    nested_type = inner.get("contentType")
    if isinstance(nested_type, int) and (content_type in (None, 101)):
        content_type = nested_type

    if content_type == 8:
        # 会话引导，不是买家消息
        return None

    cid_raw = str(obj.get("cid") or obj.get("sessionId") or "")
    chat_id = cid_raw.split("@")[0] if "@" in cid_raw else cid_raw
    if not chat_id or _has_cjk(chat_id):
        return None

    sender_id = str(sender.get("uid") or "").split("@")[0].strip()
    if not sender_id:
        sender_id = str(extension.get("senderUserId") or "").split("@")[0].strip()

    texts: list[str] = []
    for value in (extension.get("reminderContent"), extension.get("detailNotice")):
        if isinstance(value, str) and value.strip():
            texts.append(value.strip())
    if inner:
        texts.extend(collect_texts(inner))
    if isinstance(custom.get("summary"), str) and custom["summary"].strip():
        texts.append(custom["summary"].strip())
    content_text = "\n".join(_dedupe_lines(texts))

    reminder_url = str(extension.get("reminderUrl") or "")
    probes: list[Any] = [reminder_url, inner, extension, obj]
    item_id = _search(_ITEM_ID_PATTERNS, _walk_strings(probes[0]))
    if item_id is None:
        item_id = _search(_ITEM_ID_PATTERNS, _walk_strings(probes[1:]))

    # 订单号只认服务端结构：reminderUrl 的参数，或卡片形态的 inner（dxCard）。
    # extension 和整条 obj 里带 reminderContent（买家正文），只能兜底、不可信。
    order_id: str | None = None
    order_trusted = False
    if reminder_url:
        found = _search(_ORDER_ID_PATTERNS, [reminder_url])
        if found:
            order_id, order_trusted = found, True
    if order_id is None and _payload_is_card(inner):
        found = _search(_ORDER_ID_PATTERNS, _walk_strings(inner))
        if found:
            order_id, order_trusted = found, True
    if order_id is None:
        order_id = _search(_ORDER_ID_PATTERNS, _walk_strings(probes))

    try:
        created_at = int(obj.get("createAt") or 0)
    except (TypeError, ValueError):
        created_at = 0

    if not content_text.strip() and not sender_id:
        return None

    # 自己发的消息要丢掉（避免自问自答），但「已付款」类交易卡片例外：
    # 实测这类系统卡片会把 senderUserId 记成卖家自己，一律过滤会把订单丢掉。
    if my_id and sender_id == my_id and not is_paid_trigger(content_text):
        return None

    msg_type, media_url, media_thumb, media_w, media_h, media_d = extract_media_ref(inner)
    return IncomingMessage(
        chat_id=chat_id,
        sender_id=sender_id or "",
        # reminderTitle 在卡片消息里是标题（「等待你发货」），不是昵称 ——
        # 会一路传到 new-api 的令牌名上，必须过滤。
        sender_name=pick_display_name(extension.get("reminderTitle")),
        content=content_text,
        item_id=item_id,
        order_id=order_id,
        created_at=created_at,
        raw=obj,
        message_id=str(obj.get("messageId") or ""),
        is_system_card=_is_card_type(content_type, inner),
        message_type=msg_type,
        media_url=media_url,
        media_thumb_url=media_thumb,
        media_width=media_w,
        media_height=media_h,
        media_duration=media_d,
        order_id_trusted=order_trusted,
    )


def _parse_numbered(raw: dict[str, Any], my_id: str) -> IncomingMessage | None:
    """解析嵌套数字键格式（闲鱼 IM 的真实聊天消息格式）。"""
    if _is_control_packet(raw):
        return None

    message_obj = _find_message_object(raw)
    if message_obj:
        parsed = _decode_message_object(message_obj, my_id)
        if parsed is not None:
            return parsed

    msg_info = _find_msg_info(raw)
    if not msg_info:
        return None

    # "2": 2 是已读回执，没有正文块
    if raw.get("2") == 2 and "6" not in msg_info:
        return None

    block = _decode_content_block(msg_info)
    if block.content_type == 8:
        # 会话引导（「现在买可以做吗？」这类系统建议），不是买家消息
        return None

    reminder = _as_dict(msg_info.get("10")) or _as_dict(msg_info.get("reminder"))
    sender_info = _as_dict(msg_info.get("1"))

    chat_raw = str(msg_info.get("2") or msg_info.get("sId") or "")
    chat_id = chat_raw.split("@")[0] if "@" in chat_raw else chat_raw
    if not chat_id or _has_cjk(chat_id):
        # 会话 ID 位置出现中文，说明字段错位，宁可丢掉也不要错发
        return None

    sender_id = str(
        sender_info.get("1") or sender_info.get("senderUserId") or reminder.get("senderUserId") or ""
    )
    sender_id = sender_id.split("@")[0].strip()
    if my_id and sender_id == my_id:
        return None

    sender_name = pick_display_name(
        sender_info.get("2"), reminder.get("senderNick"), reminder.get("reminderTitle"), fallback=""
    )

    texts: list[str] = []
    for value in (reminder.get("reminderContent"), reminder.get("detailNotice")):
        if isinstance(value, str) and value.strip():
            texts.append(value.strip())
    if block.text:
        texts.extend(block.text.split("\n"))

    # 卡片消息里标题、内层文本、reminderContent 常常是同一句话，
    # 只差方括号/空白。归一化后去重，免得发给买家一串重复。
    content = "\n".join(_dedupe_lines(texts))

    reminder_url = reminder.get("reminderUrl")
    probe: dict[str, Any] = {"msg": msg_info, "reminder": reminder, "payload": block.payload}
    item_id = None
    if isinstance(reminder_url, str):
        item_id = _search(_ITEM_ID_PATTERNS, [reminder_url])
    if item_id is None:
        item_id = _search(_ITEM_ID_PATTERNS, _walk_strings(probe))

    # 可信来源只有两个：reminderUrl 的参数、卡片形态的 payload。
    # msg_info / reminder 里带 reminderContent（买家正文），只能兜底、不可信。
    order_id: str | None = None
    order_trusted = False
    if isinstance(reminder_url, str) and reminder_url:
        found = _search(_ORDER_ID_PATTERNS, [reminder_url])
        if found:
            order_id, order_trusted = found, True
    if order_id is None and block.payload and _payload_is_card(block.payload):
        found = _search(_ORDER_ID_PATTERNS, _walk_strings(block.payload))
        if found:
            order_id, order_trusted = found, True
    if order_id is None:
        order_id = _search(_ORDER_ID_PATTERNS, _walk_strings(probe))

    if not content.strip() and not sender_id:
        # 既没正文也没有发送方 —— 大概率是控制包/回执的变体，不是买家消息
        return None

    try:
        created_at = int(msg_info.get("5") or 0)
    except (TypeError, ValueError):  # pragma: no cover - 脏数据防御
        created_at = 0

    msg_type, media_url, media_thumb, media_w, media_h, media_d = extract_media_ref(block.payload)
    return IncomingMessage(
        chat_id=chat_id,
        sender_id=sender_id,
        sender_name=sender_name or "买家",
        content=content,
        item_id=item_id,
        order_id=order_id,
        created_at=created_at,
        raw=raw,
        message_id=str(msg_info.get("3") or ""),
        is_system_card=_is_card_type(block.content_type, block.payload),
        order_id_trusted=order_trusted,
        message_type=msg_type,
        media_url=media_url,
        media_thumb_url=media_thumb,
        media_width=media_w,
        media_height=media_h,
        media_duration=media_d,
    )


def parse_message(raw: dict[str, Any], my_id: str = "") -> IncomingMessage | None:
    """把一条推送负载解析成 :class:`IncomingMessage`；不是买家消息时返回 ``None```。

    闲鱼一共推过三种格式，都要认，否则真实消息会被整条丢掉 ——
    而「丢掉了」在日志里看起来跟「没人发消息」一模一样：

    1. **嵌套数字键**（真实聊天消息）：`"1"` → `"6"` → `"3"` → 正文
    2. **flat**（会话引导、部分通知）：`chatType` + `operation`
    3. **老的提醒格式**：`"1"` → `"10"` → `reminderContent`
    """
    if unread_sync_chat_id(raw):
        # 未读通知：只有会话 ID，没有正文，当消息处理只会得到空内容
        return None

    if "operation" in raw and "chatType" in raw:
        parsed = _parse_flat(raw, my_id)
        if parsed is not None:
            return parsed
        if "1" not in raw:
            return None

    return _parse_numbered(raw, my_id)


#: 见到但不认识的负载形状 -> 次数。第一次出现时打一条日志，
#: 免得闲鱼再改格式时又变成「什么都没发生」。
_UNKNOWN_SHAPES: dict[tuple[str, ...], int] = {}


def describe_payload(raw: dict[str, Any]) -> tuple[str, ...]:
    """给一个负载做个能对上的形状指纹。

    只打键名不够用（很多包都只有一个 "1"），所以把 contentType 和有没正文也带进来。
    """
    if unread_sync_chat_id(raw):
        # 先认它：否则会被 _find_msg_info 误判成「有会话有 pnmId 的消息体」，
        # 日志里就变成一条看不懂的 nested/1/0
        return ("unread",)

    content = _as_dict(_as_dict(raw.get("operation")).get("content"))
    if content:
        return ("flat", str(content.get("contentType")))

    message_obj = _find_message_object(raw)
    if message_obj:
        custom = _as_dict(_as_dict(message_obj.get("content")).get("custom"))
        return ("message", str(custom.get("type")))

    msg_info = _find_msg_info(raw)
    if msg_info:
        block = _decode_content_block(msg_info)
        has_text = "1" if block.text else "0"
        return ("nested", str(block.content_type), has_text)

    return tuple(sorted(str(k) for k in raw))[:6]


def is_guide_payload(raw: dict[str, Any]) -> bool:
    """是不是「会话引导」这类已知噪声（contentType=8）。

    它们一次推送就有上百条，如果每条都 dump，日志会被瞬间刷满，
    真正的买家消息反而找不到了。
    """
    content = _as_dict(_as_dict(raw.get("operation")).get("content"))
    if content and content.get("contentType") == 8:
        return True
    message_obj = _find_message_object(raw)
    if message_obj:
        custom = _as_dict(_as_dict(message_obj.get("content")).get("custom"))
        if custom.get("type") == 8:
            return True
    msg_info = _find_msg_info(raw)
    if msg_info:
        return _decode_content_block(msg_info).content_type == 8
    return False


def unread_sync_chat_id(raw: dict[str, Any]) -> str | None:
    """如果这是「有新消息」的未读同步包，返回它的会话 ID。

    真实形状（抓包）：
        {"1": [{"1": "<chatId>@goofish", "2": 1, "3": 1, "4": "<我的unb>@goofish"}]}

    它只有会话 ID 和「未读/已读」标记，**不带正文**。
    认出它的意义在于：能明确区分「买家发消息了但我们没拿到正文」
    和「根本没人发消息」—— 这两件事在日志里原本长得一模一样。
    """
    first = raw.get("1")
    if not isinstance(first, list) or not first:
        return None
    if not all(isinstance(item, dict) for item in first):
        return None
    if not all(set(map(str, item.keys())).issubset({"1", "2", "3", "4"}) for item in first):
        return None
    # 纯数字的第一项是控制包（成员状态之类），不是会话 ID；
    # 会话 ID 一定长得像 "68432961704@goofish"。
    raw_chat = str(first[0].get("1") or "")
    if not raw_chat or raw_chat.isdigit():
        return None
    chat = raw_chat.split("@")[0].strip()
    return chat or None


def note_unparsed(raw: dict[str, Any]) -> None:
    """记录一个解不出消息的负载。

    同形状只详详细细打一次 —— 第二次以后只计数。第一次必须把内容打出来，
    否则「收到不认识的消息」这句话本身毫无信息量，谁也修不了。
    """
    shape = describe_payload(raw)
    if shape in _UNKNOWN_SHAPES:
        _UNKNOWN_SHAPES[shape] += 1
        return
    _UNKNOWN_SHAPES[shape] = 1
    # 闲鱼改格式时可能出现成千上万种「键组合」；不设上限的话这张表
    # 会跟着进程一起长，512 MB 的机器扛不住。超出就丢最早的计数。
    cap_mapping(_UNKNOWN_SHAPES, 256)

    if shape == ("unread",):
        logger.warning(
            "会话 %s 有新消息（未读通知），但这次推送里没有正文。"
            "如果你正好发了一条测试消息，说明正文走的是我们没读的那条通道。",
            unread_sync_chat_id(raw),
        )
        return
    try:
        preview = json.dumps(raw, ensure_ascii=False)[:2000]
    except (TypeError, ValueError):  # pragma: no cover - 兜底
        preview = repr(raw)[:2000]
    logger.warning(
        "收到无法解析为买家消息的负载（形状 %s）。如果这正好是买家下单，请把下面这段发我：%s",
        shape, preview,
    )


def sync_payloads(frame: dict[str, Any]) -> list[dict[str, Any]]:
    """从 WebSocket 帧里拆出所有业务负载。"""
    return sync_payloads_detailed(frame)[0]


def sync_payloads_detailed(
    frame: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[tuple[int, str]]]:
    """从 WebSocket 帧里拆出业务负载，并**报告解不开的那几条**。

    以前解不开的 data 会被静默跳过 —— 于是「闲鱼改版了」在日志里
    和「没人发消息」长得一模一样，谁也查不出来。现在把失败的条目
    连同序号一起返回，由调用方记日志。

    另外加了一条兜底：如果 syncPushPackage 里什么都没有，就把整个 body
    当候选重新扫一遍。闲鱼的推送并不只有 syncPushPackage 一条通道，
    正文走别的 lwp 时老代码会整条丢掉。
    """
    body = _as_dict(frame.get("body"))
    package = _as_dict(body.get("syncPushPackage"))
    items = package.get("data")

    payloads: list[dict[str, Any]] = []
    failures: list[tuple[int, str]] = []
    if isinstance(items, list):
        for index, item in enumerate(items):
            if not isinstance(item, dict) or "data" not in item:
                continue
            try:
                payloads.extend(decode_payloads(item["data"]))
            except DecodeError as exc:
                failures.append((index, str(exc)))

    if not payloads:
        payloads.extend(_body_candidates(body))
    return payloads, failures


def _body_candidates(body: Any) -> list[dict[str, Any]]:
    """把 body 里所有「可能是消息」的字典挑出来，交给 parse_message 判断。

    parse_message 本身很保守（认不出就返回 None），所以这里可以放宽：
    宁可多试几个候选，也别让一条真消息因为换了容器就消失。
    """
    out: list[dict[str, Any]] = []
    seen: set[int] = set()

    def visit(node: Any, depth: int) -> None:
        if depth > 6 or len(out) > 40:
            return
        if isinstance(node, dict):
            if _looks_like_message_object(node) or _find_msg_info(node) or _is_flat(node):
                if id(node) not in seen:
                    seen.add(id(node))
                    out.append(node)
                return
            for value in node.values():
                visit(value, depth + 1)
        elif isinstance(node, list):
            for value in node:
                visit(value, depth + 1)

    visit(body, 0)
    return out


def frame_shape(frame: dict[str, Any]) -> str:
    """一帧长什么样：lwp + body 的键。排查「推送来了但没解析出东西」时看它。"""
    lwp = str(frame.get("lwp") or frame.get("headers", {}).get("dt") or "-")
    body = frame.get("body")
    if isinstance(body, dict):
        keys = ",".join(sorted(str(k) for k in body))[:120]
        return f"lwp={lwp} body={{{keys}}}"
    if isinstance(body, list):
        return f"lwp={lwp} body=[{len(body)} 项]"
    return f"lwp={lwp} body={type(body).__name__}"


def _is_flat(node: dict[str, Any]) -> bool:
    """flat 格式：chatType + operation 同时在。"""
    return "chatType" in node and isinstance(node.get("operation"), dict)
