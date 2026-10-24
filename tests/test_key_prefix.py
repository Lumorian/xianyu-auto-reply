"""API Key 的 sk- 前缀归一化。

踩过的坑：new-api 的 POST /api/token/:id/key 返回**裸 key**（48 位随机串），
但后台页面上显示的是 sk-xxx。发给买家的必须是后者 ——
他拿去填客户端时，教程里都是 sk- 开头，少了会被当成坏 Key。

（实测：两种形式 new-api 都能鉴权通过，因为它会 TrimPrefix(key, "sk-")。
  所以这纯粹是「看起来对不对」的问题，但足以让买家以为卡密是坏的。）
"""

from __future__ import annotations

from app.upstream import KEY_PREFIX, _mask, normalize_key

RAW = "ePD6LND2Pd2oHDjumQUprJJlRo1tjbDZ2mbjEcx7bApit9dw"


def test_adds_prefix_to_bare_key():
    assert normalize_key(RAW) == "sk-" + RAW


def test_keeps_existing_prefix():
    assert normalize_key("sk-" + RAW) == "sk-" + RAW


def test_does_not_double_prefix():
    assert normalize_key(normalize_key(RAW)) == "sk-" + RAW


def test_case_insensitive_prefix_detection():
    assert normalize_key("SK-" + RAW) == "SK-" + RAW


def test_strips_whitespace():
    assert normalize_key("  " + RAW + "\n") == "sk-" + RAW


def test_empty_stays_empty():
    assert normalize_key("") == ""
    assert normalize_key(None) == ""


def test_mask_keeps_prefix_and_tail():
    masked = _mask("sk-" + RAW)
    assert masked.startswith("sk-")
    assert masked.endswith(RAW[-4:])
    assert len(masked) < len(RAW)


def test_mask_for_short_key():
    assert _mask("abc") == "abc***"


def test_key_prefix_constant():
    assert KEY_PREFIX == "sk-"
