"""常驻进程的内存工具：RSS 采样 + 有界容器。

这个项目会在一台 512 MB 的机器上长期运行，内存不是「够不够」的问题，
而是「会不会慢慢涨到 OOM」的问题。所有按买家 / 会话 / 正则增长的
进程内缓存都必须有上限；这里放两个通用的小工具，避免每个模块各写一份。
"""

from __future__ import annotations

import resource
from collections.abc import MutableMapping, MutableSet
from typing import TypeVar

K = TypeVar("K")
V = TypeVar("V")


def rss_mb() -> float:
    """当前进程的常驻内存（MB）。读不到就退回 ru_maxrss。"""
    try:
        with open("/proc/self/status", encoding="ascii") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024.0
    except (OSError, ValueError, IndexError):
        pass
    try:
        # Linux 的 ru_maxrss 单位是 KB，macOS 是字节；这里只做日志展示，
        # 统一按 Linux 处理，非 Linux 上最多是显示值不准，不影响逻辑。
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
    except Exception:  # pragma: no cover - 极端平台
        return 0.0


def cap_mapping(mapping: MutableMapping[K, V], limit: int, *, keep_ratio: float = 0.8) -> None:
    """字典超过 limit 时丢掉最早插入的一批（dict 保持插入顺序）。

    一次丢到 limit * keep_ratio，而不是每插一条删一条：后者每次都要
    重建迭代器，热路径上反而更慢。
    """
    if limit <= 0 or len(mapping) <= limit:
        return
    target = max(1, int(limit * keep_ratio))
    for key in list(mapping)[: len(mapping) - target]:
        mapping.pop(key, None)


def cap_set(values: MutableSet[K], limit: int) -> None:
    """集合超过 limit 时直接清空。

    这些集合都只用于「别重复提示 / 别重复处理」的短期记忆，
    清空最坏只是多提示一次，不会造成业务错误。
    """
    if limit > 0 and len(values) > limit:
        values.clear()
