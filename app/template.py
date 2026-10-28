"""极简占位符渲染。

只用一次正则扫描：插入的值里即使包含 {card} 这类文本，也不会被第二次替换。
发货 / 自动回复 / 评价三处模板都走这里，行为保持一致。
"""

from __future__ import annotations

import re

_PLACEHOLDER = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")


def render_placeholders(template: str, values: dict[str, object]) -> str:
    """把模板里的 {key} 替换成 values[key]；未知占位符原样保留。"""

    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in values:
            return match.group(0)
        return str(values[key])

    return _PLACEHOLDER.sub(replace, str(template or ""))
