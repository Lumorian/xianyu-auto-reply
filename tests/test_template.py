"""模板占位符的单次替换。"""

from __future__ import annotations

from app.engine import render
from app.template import render_placeholders


def test_render_placeholders_is_single_pass():
    out = render_placeholders("{buyer} / {card}", {"buyer": "{card}", "card": "SECRET"})
    assert out == "{card} / SECRET", "插入的值不能被再次展开"


def test_unknown_placeholders_are_left_alone():
    assert render_placeholders("{unknown} {buyer}", {"buyer": "小王"}) == "{unknown} 小王"


def test_delivery_render_does_not_expand_nickname():
    out = render("你好{buyer}，卡密：{card}", buyer_name="{card}", card="CARD-1")
    assert out == "你好{card}，卡密：CARD-1"
