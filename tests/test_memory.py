"""内存工具：有界容器与 RSS 采样。"""

from __future__ import annotations

from app.memory import cap_mapping, cap_set, rss_mb


def test_cap_mapping_drops_oldest_and_keeps_recent():
    values = {index: index for index in range(100)}

    cap_mapping(values, 10, keep_ratio=0.5)

    assert len(values) == 5
    assert list(values) == [95, 96, 97, 98, 99]


def test_cap_mapping_noop_below_limit():
    values = {"a": 1, "b": 2}

    cap_mapping(values, 10)

    assert values == {"a": 1, "b": 2}


def test_cap_set_clears_when_oversized():
    values = set(range(10))

    cap_set(values, 5)

    assert values == set()


def test_rss_mb_is_positive():
    assert rss_mb() > 0
