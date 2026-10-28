"""出站连接一律直连：不能跟着操作系统的代理跑。

踩过的坑：挂上代理后闲鱼的出口 IP 变了，服务端直接把长连接踢掉，
后台看起来就是「账号掉线」；代理对 WebSocket 的支持也经常不稳。
这个工具是常驻接单的 —— 操作者临时开代理上外网，不该把正在接单的账号一起带走。

所以：

* httpx 客户端全部 `trust_env=False`，`HTTP_PROXY` / `ALL_PROXY` 之类一概不读；
* websockets 显式 `proxy=None`（14+ 默认会读 `WS_PROXY` / `ALL_PROXY`）。
"""

from __future__ import annotations

import pytest

from app.client import XianyuClient
from app.config import proxy_env_vars
from app.qrlogin import QrLoginManager
from app.upstream import CardShopClient, NewApiClient

PROXY_VARS = (
    "http_proxy", "https_proxy", "all_proxy", "ws_proxy", "wss_proxy",
    "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "WS_PROXY", "WSS_PROXY",
)


def clear_proxy_env(monkeypatch) -> None:
    for key in PROXY_VARS:
        monkeypatch.delenv(key, raising=False)


def test_proxy_env_vars_reports_only_nonempty(monkeypatch):
    clear_proxy_env(monkeypatch)
    monkeypatch.setenv("https_proxy", "http://127.0.0.1:7890")
    monkeypatch.setenv("ALL_PROXY", "   ")   # 空值不算
    assert proxy_env_vars() == ["https_proxy"]


async def test_xianyu_http_client_ignores_proxy_env(monkeypatch):
    clear_proxy_env(monkeypatch)
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:7890")
    monkeypatch.setenv("ALL_PROXY", "socks5://127.0.0.1:7890")

    client = XianyuClient("unb=1; _m_h5_tk=tok_1")
    try:
        http = await client._client()
        assert http.trust_env is False
    finally:
        await client.close()


async def test_xianyu_websocket_disables_proxy(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_connect(uri, **kwargs):
        captured["uri"] = uri
        captured.update(kwargs)
        raise RuntimeError("stop before handshake")

    monkeypatch.setattr("app.client.websockets.connect", fake_connect)
    client = XianyuClient("unb=1; _m_h5_tk=tok_1")
    with pytest.raises(RuntimeError):
        await client.connect()
    assert captured["uri"].startswith("wss://")  # type: ignore[union-attr]
    # 关键：None = 明确关掉 websockets 的「跟随环境代理」默认行为
    assert captured["proxy"] is None


async def test_upstream_clients_ignore_proxy_env(monkeypatch):
    clear_proxy_env(monkeypatch)
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:7890")

    api = NewApiClient("https://api.example.com")
    shop = CardShopClient("https://shop.example.com")
    try:
        assert (await api._client()).trust_env is False
        assert (await shop._client()).trust_env is False
    finally:
        await api.close()
        await shop.close()


async def test_qrlogin_client_ignores_proxy_env(monkeypatch):
    clear_proxy_env(monkeypatch)
    monkeypatch.setenv("ALL_PROXY", "socks5://127.0.0.1:7890")

    client = QrLoginManager()._new_client()
    try:
        assert client.trust_env is False
    finally:
        await client.aclose()
