"""闲鱼（Goofish）请求签名与设备指纹。

闲鱼 H5 接口的 sign 算法为 md5(token & t & appKey & data)，
其中 token 取自 Cookie _m_h5_tk 的下划线前一段，t 为毫秒时间戳。
"""

from __future__ import annotations

import hashlib
import random
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


def h5_token(cookie_str: str) -> str:
    """从 Cookie 中提取 _m_h5_tk 的 token 段。"""
    raw = parse_cookies(cookie_str).get("_m_h5_tk", "")
    return raw.split("_")[0] if raw else ""
