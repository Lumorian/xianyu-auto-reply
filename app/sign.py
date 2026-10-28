"""闲鱼（Goofish）请求签名与设备指纹。

闲鱼 H5 接口的 sign 算法为 md5(token & t & appKey & data)，
其中 token 取自 Cookie _m_h5_tk 的下划线前一段，t 为毫秒时间戳。
"""

from __future__ import annotations

import hashlib
import random
import re
import time

_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

#: 网页端固定的 mtop appKey
MTOP_APP_KEY = "34839810"


def generate_mid() -> str:
    """生成消息 id。"""
    return f"{int(1000 * random.random())}{int(time.time() * 1000)} 0"


def generate_uuid() -> str:
    """生成发送消息用的 uuid。"""
    return f"-{int(time.time() * 1000)}1"


def generate_device_id(user_id: str) -> str:
    """按闲鱼前端逻辑生成设备 id，尾部拼接用户 id。

    关键：同一账号必须每次生成**完全相同**的 deviceId。

    闲鱼的风控会把 deviceId 当成设备指纹：如果每次重连都随机换一个，
    服务端会认为「同一账号突然换设备登录」，很容易返回
    FAIL_SYS_USER_VALIDATE / RGV587_ERROR（滑块/被挤爆）。
    参考 xianyu-pilot 的做法，用 unb 当随机种子做确定性生成：
    token 刷新、进程重启都不改变设备身份。
    """
    rng = random.Random(str(user_id or ""))
    out: list[str] = []
    for i in range(36):
        if i in (8, 13, 18, 23):
            out.append("-")
        elif i == 14:
            out.append("4")
        else:
            rand = int(16 * rng.random())
            out.append(_CHARS[(rand & 0x3) | 0x8] if i == 19 else _CHARS[rand])
    return "".join(out) + "-" + str(user_id)


def generate_sign(t: str | int, token: str, data: str, app_key: str = MTOP_APP_KEY) -> str:
    """计算 mtop 接口签名。"""
    # 算法由闲鱼协议规定（md5(token&t&appKey&data)），不是我们挑的：
    # 它只用来向对方证明「请求是我们签的」，不承担任何本地安全职责。
    msg = f"{token}&{t}&{app_key}&{data}"
    return hashlib.md5(msg.encode("utf-8")).hexdigest()  # noqa: S324 - 协议要求


def now_ms() -> str:
    """当前毫秒时间戳（字符串）。"""
    return str(int(time.time() * 1000))


def parse_cookies(cookie_str: str) -> dict[str, str]:
    """把浏览器复制出来的 Cookie 字符串解析成字典。"""
    cookies: dict[str, str] = {}
    for chunk in (cookie_str or "").split(";"):
        chunk = chunk.strip()
        if not chunk or "=" not in chunk:
            continue
        key, value = chunk.split("=", 1)
        cookies[key.strip()] = value.strip()
    return cookies


#: Cookie 名允许的字符：RFC 6265 的 token 集合，一个不多。
_COOKIE_NAME_RE = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
#: 单个名字 / 值 / 整条 Cookie 串的上限。真实场景下单值都是几十字节，
#: 这里只是不给「上游塞一个 1 MB 的 Cookie」留下把整库撑爆的机会。
MAX_COOKIE_NAME_LEN = 64
MAX_COOKIE_VALUE_LEN = 4096
MAX_COOKIE_STRING_LEN = 8192


def clean_cookie_pair(name: str, value: str) -> tuple[str, str] | None:
    """把一对键值洗成「能安全拼进 Cookie 请求头」的样子，洗不干净就丢掉。

    为什么必须要洗：这些值最终会被原样写进下一次请求的 `Cookie:` 头，
    而且会落库、跨进程复用。名字里带非法字符、值里带 CR/LF/NUL 的，
    轻则整条连接开始报错（h11 拒绝非法头），重则把「响应头 → 请求头」
    变成一条注入通道。Cookie 是我们手里最敏感、又最容易被上游左右的字符串，
    这道口子不能靠「上游应该不会吧」。
    """
    key = str(name or "").strip()
    val = str(value or "").strip()
    if not key or len(key) > MAX_COOKIE_NAME_LEN or not _COOKIE_NAME_RE.match(key):
        return None
    if len(val) > MAX_COOKIE_VALUE_LEN:
        return None
    # 控制字符（含 \r \n \x00 \x1f \x7f）一律不要：合法 Cookie 值里不会有
    if any(ch < " " or ch == "\x7f" for ch in val):
        return None
    return key, val


#: 「看起来像密钥」的串：sk- 开头的，或者 32 位以上的纯字母数字。
_KEYLIKE_RE = re.compile(r"(?:sk-)?[A-Za-z0-9]{32,}")


def mask_keylike(text: str, limit: int = 32) -> str:
    """把日志里要出现的名字/片段先打码、再截断。

    为什么要这一步：这些日志是为了排查「Cookie 为什么没生效」加的，
    会写进 server.log —— 而日志是会被贴进工单、发群、截图转发的。
    键名本身不敏感，但万一手抖把一整串 Key 当名字粘进来，
    或者上游回了个奇怪的键名，就不该原样落盘。
    """
    snippet = str(text or "")[:limit]
    return _KEYLIKE_RE.sub("***", snippet)


def sanitize_cookie_string(cookie_str: str) -> tuple[str, list[str]]:
    """重建一条干净的 Cookie 字符串（给人粘贴用的路径）。

    返回 (新串, 被丢掉的键名)。丢掉的只记名字、不记值 —— 值可能是登录票据。

    比 clean_cookie_pair 宽容一点，但**只宽容格式、不宽容内容**：
      * 开头多粘的 Cookie 头名去掉；
      * 换行 / 制表符当分隔符（从 F12 复制、或者终端折行都会带进来）；
      * 然后逐条走 clean_cookie_pair，名字非法 / 值里还有控制字符 / 超长的一律丢。
    这样「粘错格式」能被修好，而「会把请求头搞坏的东西」一个都进不来。
    """
    text = str(cookie_str or "")
    text = re.sub(r"^\s*cookie\s*:\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"[\r\n\t]+", ";", text)
    kept: dict[str, str] = {}
    dropped: list[str] = []
    for chunk in text.split(";"):
        chunk = chunk.strip()
        if not chunk or "=" not in chunk:
            continue
        raw_name, raw_value = chunk.split("=", 1)
        pair = clean_cookie_pair(raw_name, raw_value)
        if pair is None:
            dropped.append(str(raw_name or "?").strip()[:MAX_COOKIE_NAME_LEN] or "?")
            continue
        kept[pair[0]] = pair[1]

    text = "; ".join(f"{k}={v}" for k, v in kept.items())
    while len(text) > MAX_COOKIE_STRING_LEN and kept:
        # 超长时从最后加进来的开始丢：真实场景下最不重要的是埋点类 Cookie
        name = next(reversed(kept))
        kept.pop(name)
        dropped.append(name)
        text = "; ".join(f"{k}={v}" for k, v in kept.items())
    return text, dropped


def h5_token(cookie_str: str) -> str:
    """从 Cookie 中提取 _m_h5_tk 的 token 段。"""
    raw = parse_cookies(cookie_str).get("_m_h5_tk", "")
    return raw.split("_")[0] if raw else ""
