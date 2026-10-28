"""发货模式扩展：充值到已有 Key、规格匹配、买家 Key 绑定。"""

from __future__ import annotations

import asyncio
import base64
import json
from types import SimpleNamespace
from typing import Any

import httpx
import msgpack
import pytest

from app.codec import IncomingMessage
from app.config import settings
from app.db import Database
from app.engine import (
    DEFAULT_TOPUP_CONTENT,
    DeliveryEngine,
    amount_bounds,
    amount_matches,
    pick_rule,
    render,
    spec_matches,
    spec_text,
)
from app.runner import AccountWorker
from app.upstream import (
    NewApiClient,
    Provisioned,
    TopupUncertainError,
    UpstreamError,
)
from app.web import create_app

# ------------------------------------------------------------------ 测试替身


class FakeClient:
    """闲鱼客户端的替身：够发货链路用就行。"""

    user_id = "2000000000002"

    def __init__(self, order: dict | None = None) -> None:
        self.sent: list[tuple[str, str, str]] = []
        self.confirmed: list[str] = []
        self.websocket = object()
        self.order = order
        self.order_lookups: list[str] = []

    async def send_text_safe(self, chat_id: str, to_id: str, text: str) -> None:
        self.sent.append((chat_id, to_id, text))

    async def confirm_shipping(self, order_id: str) -> tuple[bool, str]:
        self.confirmed.append(order_id)
        return True, "SUCCESS"

    async def find_order(self, order_id: str) -> dict | None:
        self.order_lookups.append(order_id)
        return self.order

    async def close(self) -> None:
        pass


class FakeTopupProvisioner:
    """记录充值 / 开 Key 调用；可按需抛错。"""

    def __init__(self, fail: Exception | None = None,
                 open_key: bool = False) -> None:
        self.topups: list[tuple[float, str, str]] = []
        self.opens: list[dict[str, Any]] = []
        self.fail = fail
        self.open = open_key
        self.newapi = SimpleNamespace()
        #: 按订单号反查的令牌（模拟老订单没有 api_key_id 的情况）
        self.order_tokens: dict[str, dict[str, Any]] = {}
        #: 令牌 id -> 明文 Key（模拟按令牌读 Key）
        self.keys: dict[str, str] = {}
        #: 明文 Key -> 令牌记录（模拟全站按 Key 反查）
        self.site_tokens: dict[str, dict[str, Any]] = {}
        #: 本站令牌列表（真实站点里 key 字段是打码的，这里也一样）
        self.tokens: list[dict[str, Any]] = []
        self.key_reads: list[str] = []

    async def list_tokens(self, page_size: int = 100) -> list[dict[str, Any]]:
        return list(self.tokens)

    async def token_key(self, token_id: str) -> str:
        self.key_reads.append(token_id)
        return self.keys.get(str(token_id), "")

    async def find_token_by_key(self, api_key: str) -> dict[str, Any]:
        token = self.site_tokens.get(api_key)
        if token is None:
            raise UpstreamError("在 new-api 的令牌列表里找不到这个 Key。")
        return token

    async def find_tokens_for_orders(self, order_ids: list[str]) -> dict[str, dict[str, Any]]:
        return {
            order: self.order_tokens[order]
            for order in order_ids if order in self.order_tokens
        }

    async def topup(self, quota_usd: float, *, token_id: str = "",
                    api_key: str = "") -> Provisioned:
        self.topups.append((quota_usd, token_id, api_key))
        if self.fail is not None:
            raise self.fail
        return Provisioned(
            "", "sk-old…wxyz", quota_usd, token_id or "42",
            api_key="sk-old-1234567890abcdef", balance=quota_usd + 5.0,
            currency="USD", token_name="买家#订单",
        )

    async def open_key(self, quota_usd: float, label: str, username: str = "",
                       model_limits: str | None = None) -> Provisioned:
        self.opens.append({"quota": quota_usd, "label": label,
                           "username": username, "model_limits": model_limits})
        if self.fail is not None:
            raise self.fail
        return Provisioned(
            "", "sk-new…wxyz", quota_usd, "77",
            api_key="sk-new-1234567890abcdef", balance=quota_usd,
            token_name=label,
        )


ORDER = {"commonData": {"orderId": "2500000000000000001", "orderStatus": "待发货",
                        "itemTitle": "会员月卡 30 天"},
         "priceVO": {"actualPayFee": "10.00"}}


def paid_frame(order_id: str = "2500000000000000001",
               item_id: str = "900052644277") -> str:
    card = {
        "dxCard": {
            "item": {
                "main": {"targetUrl": "fleamarket://order_detail?id=" + order_id + "&role=seller"}
            }
        }
    }
    payload = {
        "1": {
            "2": "47983389096@goofish",
            "5": 1742458817389,
            "10": {
                "reminderContent": "[我已付款，等待你发货]",
                "senderUserId": "4100000001",
                "senderNick": "买家小王",
                "itemTitle": "会员月卡 30 天",
                "reminderUrl": (
                    "fleamarket://message_chat?itemId=" + item_id + "&peerUserId=4100000001"
                ),
            },
            "6": {"3": {"5": json.dumps(card)}},
        },
    }
    data = base64.b64encode(msgpack.packb(payload, use_bin_type=True)).decode()
    return json.dumps({
        "headers": {"mid": "m1"},
        "body": {"syncPushPackage": {"data": [{"data": data}]}},
    })


TOPUP_RULE = {
    "name": "充值到原 Key",
    "item_id": "*",
    "enabled": True,
    "mode": "topup",
    "content": "你好{buyer}，已充值 {quota} 到原 Key：{key}，余额 {balance}",
    "auto_confirm": True,
    "priority": 0,
    "remark": "",
    "usd_multiplier": 1.0,
    "fixed_usd": 0.0,
    "topup_missing": "create",
    "topup_key_source": "auto",
}


async def build_worker(db: Database, provisioner: Any,
                       order: dict | None = ORDER) -> tuple[AccountWorker, int]:
    account_id = await db.upsert_account("主号", "unb=2000000000002; _m_h5_tk=t_1")
    account = await db.get_account(account_id)
    worker = AccountWorker(db, account, DeliveryEngine(db), provisioner)
    worker.client = FakeClient(order)
    return worker, account_id


async def bind_key(db: Database, buyer_id: str = "4100000001", tid: str = "55",
                   api_key: str = "sk-bound-1234567890", source: str = "manual") -> int:
    return await db.save_buyer_key({
        "buyer_id": buyer_id,
        "buyer_name": "买家小王",
        "token_id": tid,
        "token_name": "买家小王#2500000000000000001",
        "api_key": api_key,
        "key_mask": "sk-bou…7890",
        "balance": 5.0,
        "remark": "",
    }) if source == "manual" else await db.auto_bind_buyer_key({
        "buyer_id": buyer_id,
        "buyer_name": "买家小王",
        "token_id": tid,
        "api_key": api_key,
        "key_mask": "sk-bou…7890",
        "source": source,
    })


# ------------------------------------------------------------------ 规格匹配


def test_spec_text_collects_sku_and_title():
    order = {
        "commonData": {"itemTitle": "AI 会员 季卡"},
        "skuInfo": {"skuName": "年卡", "attributes": ["自动发货", "官方直充"]},
        "priceVO": {"actualPayFee": "10.00"},
    }
    text = spec_text(order)
    assert "季卡" in text
    assert "年卡" in text
    assert "自动发货" in text
    # 金额字段不是规格
    assert "10.00" not in text


def test_spec_text_empty_when_no_fields():
    assert spec_text({"priceVO": {"actualPayFee": "10.00"}}) == ""
    assert spec_text(None) == ""


def test_spec_matches_only_when_rule_has_condition():
    assert spec_matches("", "月卡") is None
    assert spec_matches("月卡，季卡", "AI 会员季卡") is True
    assert spec_matches("年卡", "AI 会员季卡") is False
    # 订单拿不到规格文本：带规格条件的规则不命中（会退回商品级规则）
    assert spec_matches("月卡", "") is False


def test_pick_rule_prefers_spec_match_then_generic():
    rules = [
        {"id": 1, "item_id": "900", "priority": 0, "spec": ""},
        {"id": 2, "item_id": "900", "priority": 0, "spec": "季卡"},
    ]
    picked = pick_rule(rules, "900", "AI 会员季卡")
    assert picked["id"] == 2
    # 没有规格信息时，规格规则不参与，退回同商品无规格规则
    assert pick_rule(rules, "900", "")["id"] == 1
    # 规格不匹配也退回无规格规则
    assert pick_rule(rules, "900", "年卡")["id"] == 1


def test_pick_rule_item_scope_beats_wildcard_spec():
    rules = [
        {"id": 1, "item_id": "*", "priority": 9, "spec": "季卡"},
        {"id": 2, "item_id": "900", "priority": 0, "spec": ""},
    ]
    assert pick_rule(rules, "900", "AI 会员季卡")["id"] == 2


async def test_engine_topup_decision_and_spec_rule(db):
    await db.save_rule({
        "name": "季卡充值", "item_id": "*", "enabled": True, "mode": "topup",
        "content": "{key}", "auto_confirm": False, "priority": 0, "remark": "",
        "spec": "季卡", "topup_missing": "fail",
    })
    await db.save_rule({
        "name": "通用建 Key", "item_id": "*", "enabled": True, "mode": "upstream",
        "content": "{card}", "auto_confirm": False, "priority": 0, "remark": "",
    })
    engine = DeliveryEngine(db)
    msg = IncomingMessage(
        chat_id="c", sender_id="4100000001", sender_name="买家小王",
        content="[订单待发货]", item_id="900", order_id="2500000000000000001",
        raw={"itemTitle": "AI 会员季卡"}, order_id_trusted=True,
    )
    decision = await engine.decide_order(1, msg)
    assert decision.action == "topup"
    assert decision.rule["name"] == "季卡充值"

    msg.raw = {"itemTitle": "AI 会员月卡"}
    decision = await engine.decide_order(1, msg)
    assert decision.action == "provision"


def test_render_key_and_balance_placeholders():
    out = render("{key}|{balance}|{card}", key="sk-abc", balance="12.34", card="C")
    assert out == "sk-abc|12.34|C"


# ------------------------------------------------------------- runner 充值链路


async def test_topup_uses_bound_key_and_sends_it(db):
    await db.save_rule(dict(TOPUP_RULE))
    await bind_key(db)
    provisioner = FakeTopupProvisioner()
    worker, _ = await build_worker(db, provisioner)

    await worker._on_frame(paid_frame())

    assert provisioner.topups == [(10.0, "55", "sk-bound-1234567890")]
    assert provisioner.opens == []
    assert len(worker.client.sent) == 1
    text = worker.client.sent[0][2]
    assert "sk-old-1234567890abcdef" in text
    assert "15.00" in text  # balance = quota + 5
    assert worker.client.confirmed == ["2500000000000000001"]

    orders = await db.list_orders()
    assert orders[0]["status"] == "delivered"
    assert orders[0]["api_key_id"] == "55"
    assert orders[0]["card_code"] == "sk-old-1234567890abcdef"
    assert orders[0]["quota_usd"] == pytest.approx(10.0)

    binding = await db.get_buyer_key("4100000001")
    assert binding["topup_count"] == 1
    assert binding["total_usd"] == pytest.approx(10.0)
    assert binding["balance"] == pytest.approx(15.0)


async def test_topup_first_purchase_opens_and_binds(db):
    await db.save_rule(dict(TOPUP_RULE))
    provisioner = FakeTopupProvisioner()
    worker, _ = await build_worker(db, provisioner)

    await worker._on_frame(paid_frame())

    assert provisioner.topups == []
    assert len(provisioner.opens) == 1
    assert provisioner.opens[0]["quota"] == pytest.approx(10.0)
    assert "sk-new-1234567890abcdef" in worker.client.sent[0][2]

    binding = await db.get_buyer_key("4100000001")
    assert binding is not None
    assert binding["token_id"] == "77"
    assert binding["source"] == "order"
    assert binding["api_key"] == "sk-new-1234567890abcdef"


async def test_topup_first_key_uses_item_model_limits(db):
    from app.config import MODEL_LIMITS_KEY

    await db.save_rule(dict(TOPUP_RULE))
    await db.save_item_model({"item_id": "900052644277", "model_limits": "item-model",
                              "remark": ""})
    await db.set_config(MODEL_LIMITS_KEY, "global-model")
    provisioner = FakeTopupProvisioner()
    worker, _ = await build_worker(db, provisioner)

    await worker._on_frame(paid_frame())

    assert provisioner.opens[0]["model_limits"] == "item-model"


async def test_topup_missing_key_can_refuse_delivery(db):
    await db.save_rule({**TOPUP_RULE, "topup_missing": "fail"})
    provisioner = FakeTopupProvisioner()
    worker, _ = await build_worker(db, provisioner)

    await worker._on_frame(paid_frame())

    assert provisioner.topups == [] and provisioner.opens == []
    assert worker.client.sent == []
    orders = await db.list_orders()
    assert orders[0]["status"] == "topup_failed"
    assert "绑定" in orders[0]["detail"]


async def test_topup_fail_mode_retries_after_manual_binding(db):
    await db.save_rule({**TOPUP_RULE, "topup_missing": "fail"})
    provisioner = FakeTopupProvisioner()
    worker, _ = await build_worker(db, provisioner)

    await worker._on_frame(paid_frame())
    assert worker.client.sent == []
    assert (await db.list_orders())[0]["status"] == "topup_failed"

    # 运营在后台绑好 Key 之后，同一单轮询会重试
    await bind_key(db)
    await worker._on_frame(paid_frame())

    assert provisioner.topups == [(10.0, "55", "sk-bound-1234567890")]
    assert len(worker.client.sent) == 1
    assert (await db.list_orders())[0]["status"] == "delivered"


async def test_topup_falls_back_to_latest_delivered_order(db):
    await db.save_rule(dict(TOPUP_RULE))
    account_id = await db.upsert_account("主号", "unb=2000000000002; _m_h5_tk=t_1")
    await db.record_order(
        account_id=account_id, order_id="old-order", item_id="900052644277",
        buyer_id="4100000001", buyer_name="买家小王", rule_id=1, card_id=None,
        content="old", status="delivered", api_key_id="42",
    )
    provisioner = FakeTopupProvisioner()
    worker, _ = await build_worker(db, provisioner)

    await worker._on_frame(paid_frame())

    assert provisioner.topups == [(10.0, "42", "")]
    binding = await db.get_buyer_key("4100000001")
    assert binding["source"] == "order" or binding["source"] == "history"
    assert binding["token_id"] == "42"


async def test_topup_recovers_legacy_order_by_token_name(db):
    """早期订单没记令牌 id：靠「买家用户名#订单号」的令牌名把老客户认回来。"""
    await db.save_rule(dict(TOPUP_RULE))
    account_id = await db.upsert_account("主号", "unb=2000000000002; _m_h5_tk=t_1")
    await db.record_order(
        account_id=account_id, order_id="legacy-order", item_id="900052644277",
        buyer_id="4100000001", buyer_name="买家小王", rule_id=1, card_id=None,
        content="old", status="delivered",
    )
    provisioner = FakeTopupProvisioner()
    provisioner.order_tokens["legacy-order"] = {
        "id": "91", "name": "买家小王#legacy-order", "key": "legacy-key-1234567890",
    }
    worker, _ = await build_worker(db, provisioner)

    await worker._on_frame(paid_frame())

    assert provisioner.topups == [(10.0, "91", "sk-legacy-key-1234567890")]
    binding = await db.get_buyer_key("4100000001")
    assert binding["token_id"] == "91"
    # 充值成功后会用这次读到的 Key 刷新绑定（mask 来自充值结果）
    assert binding["api_key"] == "sk-old-1234567890abcdef"


async def test_worker_topup_end_to_end_with_real_provisioner(db):
    """真 Provisioner + MockTransport：验证 runner 与 new-api 充值的接线。"""
    from app.upstream import Provisioner

    captured: dict[str, Any] = {}

    def handler(request):
        path = request.url.path
        if path == "/api/user/self":
            return httpx.Response(200, json={"success": True, "data": {"id": 3}})
        if path == "/api/status":
            return httpx.Response(200, json={"success": True,
                                             "data": {"quota_per_unit": 500000}})
        if path == "/api/token/55" and request.method == "GET":
            return httpx.Response(200, json={"success": True, "data": token_payload(
                id=55, name="买家小王#2500000000000000001", remain_quota=1000,
                key="real-key-1234567890",
            )})
        if path == "/api/token/55/key":
            return httpx.Response(200, json={"success": True,
                                             "data": {"key": "real-key-1234567890"}})
        if path == "/api/token/" and request.method == "PUT":
            captured.update(json.loads(request.content))
            return httpx.Response(200, json={"success": True})
        raise AssertionError("没预料到的请求 " + request.method + " " + path)

    provisioner = Provisioner()
    provisioner.newapi.access_token = "jwt"
    provisioner.newapi.user_id = "3"
    provisioner.newapi._user_id_checked = True
    provisioner.newapi._http = httpx.AsyncClient(
        base_url="https://newapi.test",
        transport=httpx.MockTransport(handler),
        headers={"authorization": "Bearer jwt", "new-api-user": "3"},
    )
    try:
        await db.save_rule(dict(TOPUP_RULE))
        await bind_key(db)
        worker, _ = await build_worker(db, provisioner)
        await worker._on_frame(paid_frame())
    finally:
        await provisioner.close()

    assert captured["remain_quota"] == 1000 + 5_000_000
    assert "sk-real-key-1234567890" in worker.client.sent[0][2]
    orders = await db.list_orders()
    assert orders[0]["status"] == "delivered"
    assert orders[0]["api_key_id"] == "55"


async def record_delivered_key_order(db: Database, account_id: int, order_id: str,
                                     buyer_id: str = "4100000001",
                                     tid: str | None = "91",
                                     item_id: str = "900052644277") -> None:
    await db.record_order(
        account_id=account_id, order_id=order_id, item_id=item_id,
        buyer_id=buyer_id, buyer_name="买家小王", rule_id=1, card_id=None,
        content="old", status="delivered", api_key_id=tid or None,
    )


def buyer_text(content: str, *, order_id: str | None = None) -> IncomingMessage:
    return IncomingMessage(
        chat_id="47983389096",
        sender_id="4100000001",
        sender_name="买家小王",
        content=content,
        item_id="900052644277",
        order_id=order_id,
        is_system_card=False,
        order_id_trusted=False,
    )


async def test_topup_order_id_flow_prompts_then_tops_up(db):
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "order_id"})
    provisioner = FakeTopupProvisioner()
    worker, account_id = await build_worker(db, provisioner)
    await record_delivered_key_order(db, account_id, "2500000000000000099", tid="91")

    await worker._on_frame(paid_frame())

    # 还没提供旧订单号：只发提示，不充值
    assert provisioner.topups == []
    row = await db.get_order(account_id, "2500000000000000001")
    assert row["status"] == "awaiting_key"
    assert "订单号发给我" in worker.client.sent[-1][2]

    # 重复的付款卡片不能再催一遍
    sent_before = len(worker.client.sent)
    await worker._on_frame(paid_frame())
    assert len(worker.client.sent) == sent_before

    # 买家发来旧订单号 → 充到那一单的 Key 上
    await worker._process_incoming(buyer_text("旧订单 2500000000000000099"))

    assert provisioner.topups == [(10.0, "91", "")]
    row = await db.get_order(account_id, "2500000000000000001")
    assert row["status"] == "delivered"
    assert row["key_order_id"] == "2500000000000000099"
    assert "sk-old-1234567890abcdef" in worker.client.sent[-1][2]


async def test_topup_order_id_not_found_replies_and_waits(db):
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "order_id"})
    provisioner = FakeTopupProvisioner()
    worker, account_id = await build_worker(db, provisioner)
    await worker._on_frame(paid_frame())

    await worker._process_incoming(buyer_text("2500000000000000123"))

    assert provisioner.topups == []
    row = await db.get_order(account_id, "2500000000000000001")
    assert row["status"] == "awaiting_key"
    assert "没有找到" in worker.client.sent[-1][2]
    assert "2500000000000000123" in worker.client.sent[-1][2]


async def test_topup_order_id_must_belong_to_same_buyer(db):
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "order_id"})
    provisioner = FakeTopupProvisioner()
    worker, account_id = await build_worker(db, provisioner)
    await record_delivered_key_order(db, account_id, "2500000000000000099",
                                     buyer_id="other-buyer", tid="91")
    await worker._on_frame(paid_frame())

    await worker._process_incoming(buyer_text("2500000000000000099"))

    assert provisioner.topups == []
    assert "没有找到" in worker.client.sent[-1][2]
    assert (await db.get_order(account_id, "2500000000000000001"))["status"] == "awaiting_key"


async def test_topup_order_id_rejects_order_without_token(db):
    """本地卡密池的旧订单没有令牌，不能拿来当充值目标。"""
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "order_id"})
    provisioner = FakeTopupProvisioner()
    worker, account_id = await build_worker(db, provisioner)
    await record_delivered_key_order(db, account_id, "2500000000000000099", tid=None)
    await worker._on_frame(paid_frame())

    await worker._process_incoming(buyer_text("2500000000000000099"))

    assert provisioner.topups == []
    assert "没有找到" in worker.client.sent[-1][2]


async def test_topup_order_id_supports_legacy_order_without_token_id(db):
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "order_id"})
    provisioner = FakeTopupProvisioner()
    provisioner.order_tokens["2500000000000000099"] = {
        "id": "91", "name": "买家小王#2500000000000000099", "key": "legacy-key-1234567890",
    }
    worker, account_id = await build_worker(db, provisioner)
    await record_delivered_key_order(db, account_id, "2500000000000000099", tid=None)
    await worker._on_frame(paid_frame())

    await worker._process_incoming(buyer_text("2500000000000000099"))

    assert provisioner.topups == [(10.0, "91", "sk-legacy-key-1234567890")]
    assert (await db.get_order(account_id, "2500000000000000001"))["status"] == "delivered"


async def test_topup_order_id_handles_multiple_pending_orders(db):
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "order_id"})
    provisioner = FakeTopupProvisioner()
    worker, account_id = await build_worker(db, provisioner)
    await record_delivered_key_order(db, account_id, "2500000000000000099", tid="91")
    first, second = "2500000000000000001", "2500000000000000002"

    await worker._on_frame(paid_frame(first))
    await worker._on_frame(paid_frame(second))
    assert (await db.get_order(account_id, first))["status"] == "awaiting_key"
    assert (await db.get_order(account_id, second))["status"] == "awaiting_key"

    await worker._process_incoming(buyer_text("2500000000000000099"))
    assert (await db.get_order(account_id, first))["status"] == "delivered"
    assert (await db.get_order(account_id, second))["status"] == "awaiting_key"

    await worker._process_incoming(buyer_text("2500000000000000099"))
    assert (await db.get_order(account_id, second))["status"] == "delivered"
    assert len(provisioner.topups) == 2


async def test_topup_order_id_accepts_hint_sent_before_order(db):
    """买家比订单轮询先发旧订单号：订单出现时直接充值，不再催。"""
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "order_id"})
    provisioner = FakeTopupProvisioner()
    worker, account_id = await build_worker(db, provisioner)
    await record_delivered_key_order(db, account_id, "2500000000000000099", tid="91")

    await worker._process_incoming(buyer_text("2500000000000000099"))
    assert provisioner.topups == []

    await worker._on_frame(paid_frame())

    assert provisioner.topups == [(10.0, "91", "")]
    assert (await db.get_order(account_id, "2500000000000000001"))["status"] == "delivered"
    assert "订单号发给我" not in "".join(text for _, _, text in worker.client.sent)


async def test_topup_order_id_uses_quoted_amount_if_rule_changes(db):
    rule_id = await db.save_rule({**TOPUP_RULE, "topup_key_source": "order_id"})
    provisioner = FakeTopupProvisioner()
    worker, account_id = await build_worker(db, provisioner)
    await record_delivered_key_order(db, account_id, "2500000000000000099", tid="91")
    await worker._on_frame(paid_frame())

    # 买家还没回复期间，运营把倍率改了；已经拍下的这一单仍按原额度
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "order_id",
                        "usd_multiplier": 2.0}, rule_id)
    await worker._process_incoming(buyer_text("2500000000000000099"))

    assert provisioner.topups == [(10.0, "91", "")]


async def test_topup_order_id_retry_reuses_provided_order_id(db):
    from app.upstream import UpstreamError

    await db.save_rule({**TOPUP_RULE, "topup_key_source": "order_id"})
    provisioner = FakeTopupProvisioner(fail=UpstreamError("new-api 临时抖动"))
    worker, account_id = await build_worker(db, provisioner)
    await record_delivered_key_order(db, account_id, "2500000000000000099", tid="91")
    await worker._on_frame(paid_frame())
    await worker._process_incoming(buyer_text("2500000000000000099"))

    row = await db.get_order(account_id, "2500000000000000001")
    assert row["status"] == "topup_failed"
    assert row["key_order_id"] == "2500000000000000099"

    # 上游恢复后，轮询重试直接复用买家已经提供的旧订单号，不再催一遍
    worker.provisioner = FakeTopupProvisioner()
    sent_before = len(worker.client.sent)
    await worker._on_frame(paid_frame())

    row = await db.get_order(account_id, "2500000000000000001")
    assert row["status"] == "delivered"
    assert worker.provisioner.topups == [(10.0, "91", "")]
    assert len(worker.client.sent) == sent_before + 1  # 只有成功消息，没有再提示


async def test_topup_order_id_uses_custom_prompt_and_not_found(db):
    await db.save_rule({
        **TOPUP_RULE,
        "topup_key_source": "order_id",
        "topup_prompt": "请发旧单号，客服 {buyer} 在等（新单 {order_id}）",
        "topup_not_found": "查无此 Key：{key_order_id}",
    })
    provisioner = FakeTopupProvisioner()
    worker, _ = await build_worker(db, provisioner)
    await worker._on_frame(paid_frame())

    assert "请发旧单号" in worker.client.sent[-1][2]
    await worker._process_incoming(buyer_text("2500000000000000123"))
    assert "查无此 Key：2500000000000000123" in worker.client.sent[-1][2]


async def test_topup_order_id_second_reply_does_not_double_topup(db):
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "order_id"})
    provisioner = FakeTopupProvisioner()
    worker, account_id = await build_worker(db, provisioner)
    await record_delivered_key_order(db, account_id, "2500000000000000099", tid="91")
    await worker._on_frame(paid_frame())

    await worker._process_incoming(buyer_text("2500000000000000099"))
    assert len(provisioner.topups) == 1
    await worker._process_incoming(buyer_text("2500000000000000099"))
    assert len(provisioner.topups) == 1, "重复发同一单号不能重复充值"


#: 闲鱼商品 ID 是纯数字的；卡片里的 itemId 也是。
DEEPSEEK_ITEM = "9000000000001"
KIMI_ITEM = "9000000000002"


#: 带商品 ID 的订单详情：付款卡片只带 orderId，runner 会拿订单详情补商品。
def order_detail(item_id: str) -> dict:
    return {
        "commonData": {"orderId": "2500000000000000001", "orderStatus": "待发货",
                       "itemId": item_id, "itemTitle": "测试商品"},
        "priceVO": {"actualPayFee": "10.00"},
    }


async def test_topup_cross_item_order_still_works_by_default(db):
    """默认不限制商品：老行为不变，买家发哪一单就充哪一单的 Key。"""
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "order_id"})
    provisioner = FakeTopupProvisioner()
    worker, account_id = await build_worker(db, provisioner)
    await record_delivered_key_order(
        db, account_id, "2900000000000000001", tid="kimi-token", item_id=KIMI_ITEM)

    await worker._on_frame(paid_frame())
    await worker._process_incoming(buyer_text("2900000000000000001"))

    assert provisioner.topups == [(10.0, "kimi-token", "")]
    assert (await db.get_order(account_id, "2500000000000000001"))["status"] == "delivered"


async def test_topup_item_scope_rejects_other_item_order(db):
    """只认同商品：买家拿 kimi 那笔订单号来续 deepseek，钱不能进 kimi 的 Key。"""
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "order_id",
                        "topup_item_scope": "item"})
    provisioner = FakeTopupProvisioner()
    worker, account_id = await build_worker(db, provisioner)
    await record_delivered_key_order(
        db, account_id, "2900000000000000001", tid="kimi-token", item_id=KIMI_ITEM)

    await worker._on_frame(paid_frame(item_id=DEEPSEEK_ITEM))
    await worker._process_incoming(buyer_text("2900000000000000001"))

    assert provisioner.topups == [], "跨商品的订单号绝不能充到别的 Key 上"
    row = await db.get_order(account_id, "2500000000000000001")
    assert row["status"] == "awaiting_key"
    assert "不是这个商品的订单" in worker.client.sent[-1][2]
    assert "另一个商品" in row["detail"]


async def test_topup_item_scope_accepts_same_item_order(db):
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "order_id",
                        "topup_item_scope": "item"})
    provisioner = FakeTopupProvisioner()
    worker, account_id = await build_worker(db, provisioner)
    await record_delivered_key_order(
        db, account_id, "2900000000000000002", tid="deepseek-token",
        item_id=DEEPSEEK_ITEM)

    await worker._on_frame(paid_frame(item_id=DEEPSEEK_ITEM))
    await worker._process_incoming(buyer_text("2900000000000000002"))

    assert provisioner.topups == [(10.0, "deepseek-token", "")]
    assert (await db.get_order(account_id, "2500000000000000001"))["status"] == "delivered"


async def test_topup_item_scope_backfills_item_from_order_detail(db):
    """付款卡片常常不带 itemId：只认同商品时必须从订单详情补，否则保护形同虚设。"""
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "order_id",
                        "topup_item_scope": "item"})
    provisioner = FakeTopupProvisioner()
    worker, account_id = await build_worker(
        db, provisioner, order=order_detail(DEEPSEEK_ITEM))
    await record_delivered_key_order(
        db, account_id, "2900000000000000001", tid="kimi-token", item_id=KIMI_ITEM)

    await worker._on_frame(paid_frame(item_id=""))
    row = await db.get_order(account_id, "2500000000000000001")
    assert row["status"] == "awaiting_key"
    # 商品 ID 是补出来的，后面买家发错商品时才有得比
    assert row["item_id"] == DEEPSEEK_ITEM

    await worker._process_incoming(buyer_text("2900000000000000001"))
    assert provisioner.topups == []
    assert "不是这个商品的订单" in worker.client.sent[-1][2]


async def test_topup_item_scope_allows_legacy_order_without_item(db):
    """早期订单没记商品 ID：没有证据说它属于别的商品，按老行为放行。"""
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "order_id",
                        "topup_item_scope": "item"})
    provisioner = FakeTopupProvisioner()
    worker, account_id = await build_worker(db, provisioner)
    await record_delivered_key_order(
        db, account_id, "2900000000000000003", tid="old-token", item_id="")

    await worker._on_frame(paid_frame(item_id=DEEPSEEK_ITEM))
    await worker._process_incoming(buyer_text("2900000000000000003"))

    assert provisioner.topups == [(10.0, "old-token", "")]


async def test_topup_item_scope_auto_uses_same_item_history(db):
    """auto 策略：买家最近一单是别的商品，也不能拿它的 Key 来续这一单。"""
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "auto",
                        "topup_item_scope": "item"})
    provisioner = FakeTopupProvisioner()
    worker, account_id = await build_worker(db, provisioner)
    await record_delivered_key_order(
        db, account_id, "2900000000000000010", tid="deepseek-token",
        item_id=DEEPSEEK_ITEM)
    # 更近的一单是 kimi：不过滤的话 _resolve_buyer_key 会挑到它
    await record_delivered_key_order(
        db, account_id, "2900000000000000011", tid="kimi-token", item_id=KIMI_ITEM)

    await worker._on_frame(paid_frame(item_id=DEEPSEEK_ITEM))

    assert provisioner.topups == [(10.0, "deepseek-token", "")]


async def test_topup_item_scope_ignores_auto_binding_from_other_item(db):
    """绑定表按买家只存一行；自动绑定到别的商品的 Key 不能拿来充这一单。"""
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "auto",
                        "topup_item_scope": "item"})
    provisioner = FakeTopupProvisioner()
    worker, account_id = await build_worker(db, provisioner)
    await db.auto_bind_buyer_key({
        "buyer_id": "4100000001", "buyer_name": "买家小王", "account_id": account_id,
        "token_id": "kimi-token", "token_name": "买家小王#kimi", "api_key": "sk-kimi-key",
        "key_mask": "sk-kim-0000", "source": "order",
    })
    await record_delivered_key_order(
        db, account_id, "2900000000000000010", tid="deepseek-token",
        item_id=DEEPSEEK_ITEM)

    await worker._on_frame(paid_frame(item_id=DEEPSEEK_ITEM))

    assert provisioner.topups == [(10.0, "deepseek-token", "")]


async def test_topup_item_scope_keeps_manual_binding(db):
    """后台手工绑定的 Key 是运营的明确意图：只认同商品时也照用（打一条告警）。"""
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "auto",
                        "topup_item_scope": "item"})
    provisioner = FakeTopupProvisioner()
    worker, _ = await build_worker(db, provisioner)
    await bind_key(db)

    await worker._on_frame(paid_frame(item_id=DEEPSEEK_ITEM))

    assert provisioner.topups == [(10.0, "55", "sk-bound-1234567890")]

#: 48 位字母数字（sk- 后面 48 个），和 new-api 真实 Key 长度一致。
KEY_A = "sk-" + "a1b2c3d4e5f6" * 4
KEY_B = "sk-" + "f6e5d4c3b2a1" * 4
KEY_OTHER = "sk-" + "0123456789ab" * 4


async def test_topup_api_key_mode_prompts_then_tops_up(db):
    """买家直接发 Key：核对到「本人绑定」就充到那个令牌。"""
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "api_key"})
    provisioner = FakeTopupProvisioner()
    provisioner.keys["55"] = KEY_A
    worker, account_id = await build_worker(db, provisioner)
    await bind_key(db, tid="55", api_key=KEY_A)

    await worker._on_frame(paid_frame())
    row = await db.get_order(account_id, "2500000000000000001")
    assert row["status"] == "awaiting_key"
    assert "Key" in worker.client.sent[-1][2]
    assert provisioner.topups == []

    await worker._process_incoming(buyer_text(KEY_A))

    assert provisioner.topups == [(10.0, "55", KEY_A)]
    assert (await db.get_order(account_id, "2500000000000000001"))["status"] == "delivered"
    # 回复里给的是 new-api 现读到的 Key（充值结果里的那一串）
    assert "sk-old-1234567890abcdef" in worker.client.sent[-1][2]


async def test_topup_api_key_rejects_foreign_key(db):
    """不是本站的 Key：说清楚，别让人一直重发。"""
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "api_key"})
    provisioner = FakeTopupProvisioner()
    worker, account_id = await build_worker(db, provisioner)
    await bind_key(db, tid="55", api_key=KEY_A)
    await worker._on_frame(paid_frame())

    await worker._process_incoming(buyer_text(KEY_OTHER))

    assert provisioner.topups == []
    assert "不在本站" in worker.client.sent[-1][2]
    assert (await db.get_order(account_id, "2500000000000000001"))["status"] == "awaiting_key"


async def test_topup_api_key_rejects_key_of_another_buyer(db):
    """本站的 Key，但不是他买的：绝不往别人的 Key 上充钱。"""
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "api_key"})
    provisioner = FakeTopupProvisioner()
    provisioner.site_tokens[KEY_B] = {"id": "999", "name": "别人#订单"}
    worker, _ = await build_worker(db, provisioner)
    await worker._on_frame(paid_frame())

    await worker._process_incoming(buyer_text(KEY_B))

    assert provisioner.topups == []
    assert "不是你在本店购买" in worker.client.sent[-1][2]


async def test_topup_api_key_resolves_by_own_history(db):
    """没绑定也能认：他历史订单 card_code 里存的就是这串 Key。"""
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "api_key"})
    provisioner = FakeTopupProvisioner()
    provisioner.keys["88"] = KEY_A
    worker, account_id = await build_worker(db, provisioner)
    await db.record_order(
        account_id=account_id, order_id="2900000000000000010", item_id="900052644277",
        buyer_id="4100000001", buyer_name="买家小王", rule_id=1, card_id=None,
        content="old", status="delivered", api_key_id="88", card_code=KEY_A,
    )
    await worker._on_frame(paid_frame())

    await worker._process_incoming(buyer_text(KEY_A))

    assert provisioner.topups == [(10.0, "88", KEY_A)]


async def test_topup_api_key_scans_buyers_own_tokens(db):
    """绑定和 card_code 都没记 Key 时，退到「只读他自己的令牌」逐个比对。"""
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "api_key"})
    provisioner = FakeTopupProvisioner()
    provisioner.keys["77"] = KEY_A
    worker, account_id = await build_worker(db, provisioner)
    await db.record_order(
        account_id=account_id, order_id="2900000000000000011", item_id="900052644277",
        buyer_id="4100000001", buyer_name="买家小王", rule_id=1, card_id=None,
        content="old", status="delivered", api_key_id="77",
    )
    await worker._on_frame(paid_frame())

    await worker._process_incoming(buyer_text(KEY_A))

    assert provisioner.topups == [(10.0, "77", KEY_A)]
    assert "77" in provisioner.key_reads


async def test_topup_api_key_masked_gets_explicit_reply(db):
    """买家复制到打码的 Key：直接说清楚，别让他反复重发。"""
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "api_key"})
    provisioner = FakeTopupProvisioner()
    worker, account_id = await build_worker(db, provisioner)
    await worker._on_frame(paid_frame())

    await worker._process_incoming(buyer_text("sk-ab12…wxyz"))

    assert provisioner.topups == []
    assert "打码" in worker.client.sent[-1][2]
    assert (await db.get_order(account_id, "2500000000000000001"))["status"] == "awaiting_key"


async def test_topup_api_key_reports_deleted_token(db):
    """Key 认到了令牌，但令牌被删了：提示「读不到」，别套用「没有找到订单号」。"""
    from app.upstream import UpstreamError

    await db.save_rule({**TOPUP_RULE, "topup_key_source": "api_key"})
    provisioner = FakeTopupProvisioner()

    async def gone(token_id: str) -> dict[str, Any]:
        raise UpstreamError("读不到令牌 " + token_id)

    provisioner.newapi = SimpleNamespace(get_token=gone)
    worker, account_id = await build_worker(db, provisioner)
    await bind_key(db, tid="55", api_key=KEY_A)
    await worker._on_frame(paid_frame())

    await worker._process_incoming(buyer_text(KEY_A))

    assert provisioner.topups == []
    assert "读不到" in worker.client.sent[-1][2]
    assert (await db.get_order(account_id, "2500000000000000001"))["status"] == "awaiting_key"


async def test_topup_api_key_retry_reuses_resolved_token(db):
    """充值失败后轮询重试：目标令牌记在 api_key_id 上，不让买家重发一遍 Key。"""
    from app.upstream import UpstreamError

    await db.save_rule({**TOPUP_RULE, "topup_key_source": "api_key"})
    provisioner = FakeTopupProvisioner(fail=UpstreamError("new-api 抖了一下"))
    worker, account_id = await build_worker(db, provisioner)
    await bind_key(db, tid="55", api_key=KEY_A)
    await worker._on_frame(paid_frame())
    await worker._process_incoming(buyer_text(KEY_A))

    row = await db.get_order(account_id, "2500000000000000001")
    assert row["status"] == "topup_failed"
    assert row["api_key_id"] == "55"

    worker.provisioner = FakeTopupProvisioner()
    sent_before = len(worker.client.sent)
    await worker._on_frame(paid_frame())

    assert worker.provisioner.topups == [(10.0, "55", "")]
    assert (await db.get_order(account_id, "2500000000000000001"))["status"] == "delivered"
    assert len(worker.client.sent) == sent_before + 1  # 只有成功消息，没有再催 Key


async def test_topup_api_key_sent_before_order_is_remembered(db):
    """买家先发 Key、订单后到：核对通过后记 2 分钟，订单出现直接充值。"""
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "api_key"})
    provisioner = FakeTopupProvisioner()
    provisioner.keys["55"] = KEY_A
    worker, account_id = await build_worker(db, provisioner)
    await bind_key(db, tid="55", api_key=KEY_A)

    await worker._process_incoming(buyer_text(KEY_A))
    assert provisioner.topups == []

    await worker._on_frame(paid_frame())

    assert provisioner.topups == [(10.0, "55", KEY_A)]
    assert "请把要充值的 Key" not in "".join(t for _, _, t in worker.client.sent)
    assert (await db.get_order(account_id, "2500000000000000001"))["status"] == "delivered"


async def test_topup_api_key_found_when_site_masks_keys(db):
    """真实站点：令牌列表把 Key 打码，老订单也没记 api_key_id。

    这时只能靠「令牌名里的 #订单号」认出归属，再按令牌 id 读明文核对。
    """
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "api_key"})
    provisioner = FakeTopupProvisioner()
    provisioner.keys["19"] = KEY_A
    provisioner.tokens = [
        {"id": "19", "name": "买家小王#2900000000000000010", "key": "sk-***"},
        {"id": "20", "name": "别人#2900000000000000011", "key": "sk-***"},
    ]
    worker, account_id = await build_worker(db, provisioner)
    # 早期 upstream 订单：api_key_id / card_code 都不是 Key
    await record_delivered_key_order(db, account_id, "2900000000000000010",
                                     tid=None, item_id=KIMI_ITEM)
    await worker._on_frame(paid_frame())

    await worker._process_incoming(buyer_text(KEY_A))

    assert provisioner.topups == [(10.0, "19", KEY_A)]
    assert (await db.get_order(account_id, "2500000000000000001"))["status"] == "delivered"


async def test_topup_api_key_scan_rejects_other_buyers_token(db):
    """全站扫到了这个 Key，但令牌名里的订单不是他的：照样拒绝。"""
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "api_key"})
    provisioner = FakeTopupProvisioner()
    provisioner.keys["20"] = KEY_B
    provisioner.tokens = [
        {"id": "20", "name": "别人#2900000000000000011", "key": "sk-***"},
    ]
    worker, _ = await build_worker(db, provisioner)
    await worker._on_frame(paid_frame())

    await worker._process_incoming(buyer_text(KEY_B))

    assert provisioner.topups == []
    assert "不是你在本店购买" in worker.client.sent[-1][2]


def test_redact_keys_masks_key_in_logs():
    """日志里不能出现明文 Key：会被贴进工单、截图、发群。"""
    from app.runner import redact_keys

    text = "我的 Key 是 " + KEY_A + " ，帮我充 10 块"
    masked = redact_keys(text)
    assert KEY_A not in masked
    assert "…" in masked
    # 订单号不能被当成 Key 打码
    assert "2500000000000000001" in redact_keys("订单 2500000000000000001")


async def test_rule_api_accepts_api_key_source(web_client):
    client, _ = web_client
    res = await client.post("/api/rules", json={
        "name": "发 Key 充值", "item_id": "*", "mode": "topup", "content": "{key}",
        "topup_key_source": "api_key",
    })
    assert res.status_code == 200, res.text
    rows = (await client.get("/api/rules")).json()
    assert rows[0]["topup_key_source"] == "api_key"


async def test_topup_order_id_prompt_failure_is_retried(db):
    from app.client import XianyuError

    await db.save_rule({**TOPUP_RULE, "topup_key_source": "order_id"})
    provisioner = FakeTopupProvisioner()
    worker, account_id = await build_worker(db, provisioner)
    original = worker.client.send_text_safe

    async def broken(chat_id, to_id, text):
        raise XianyuError("websocket down")

    worker.client.send_text_safe = broken
    await worker._on_frame(paid_frame())
    row = await db.get_order(account_id, "2500000000000000001")
    assert row["status"] == "topup_failed", "提示没发出去不能停在 awaiting_key"
    assert "提示" in row["detail"]

    # 连接恢复后，轮询重试会重新提示
    worker.client.send_text_safe = original
    await worker._on_frame(paid_frame())
    row = await db.get_order(account_id, "2500000000000000001")
    assert row["status"] == "awaiting_key"
    assert "订单号发给我" in worker.client.sent[-1][2]


async def test_topup_order_id_real_client_uses_background_delivery(db, monkeypatch):
    """真客户端要等 ack：买家回复旧订单号后的充值必须扔后台任务，不能卡住收帧循环。"""
    import app.runner as runner_module

    class AckClient:
        user_id = "2000000000002"
        websocket = object()

        def __init__(self) -> None:
            self.notices: list[str] = []
            self.delivered: list[str] = []
            self.confirmed: list[str] = []

        async def send_text_safe(self, chat_id, to_id, text):
            self.notices.append(text)

        async def send_delivery_text(self, chat_id, to_id, text):
            self.delivered.append(text)
            return "mid", "sent"

        async def confirm_shipping(self, order_id):
            self.confirmed.append(order_id)
            return True, "SUCCESS"

        async def find_order(self, order_id):
            return ORDER

        async def close(self):
            pass

    monkeypatch.setattr(runner_module, "XianyuClient", AckClient)
    await db.save_rule({**TOPUP_RULE, "topup_key_source": "order_id"})
    provisioner = FakeTopupProvisioner()
    worker, account_id = await build_worker(db, provisioner)
    client = AckClient()
    worker.client = client  # type: ignore[assignment]
    await record_delivered_key_order(db, account_id, "2500000000000000099", tid="91")

    await worker._on_frame(paid_frame())
    for _ in range(5):
        tasks = list(worker._delivery_tasks)
        if not tasks:
            break
        await asyncio.gather(*tasks)
    assert (await db.get_order(account_id, "2500000000000000001"))["status"] == "awaiting_key"
    assert client.notices and "订单号发给我" in client.notices[-1]

    await worker._process_incoming(buyer_text("2500000000000000099"))
    for _ in range(5):
        tasks = list(worker._delivery_tasks)
        if not tasks:
            break
        await asyncio.gather(*tasks)

    rows = await db.list_orders()
    row = await db.get_order(account_id, "2500000000000000001")
    assert row["status"] == "delivered"
    assert provisioner.topups == [(10.0, "91", "")]
    assert client.delivered and "sk-old-1234567890abcdef" in client.delivered[-1]
    assert len(rows) >= 2


async def test_topup_send_failure_does_not_topup_twice(db):
    from app.client import XianyuError

    await db.save_rule(dict(TOPUP_RULE))
    await bind_key(db)
    provisioner = FakeTopupProvisioner()
    worker, _ = await build_worker(db, provisioner)

    original = worker.client.send_text_safe
    state = {"fail": True}

    async def flaky(chat_id, to_id, text):
        if state["fail"]:
            raise XianyuError("network down")
        await original(chat_id, to_id, text)

    worker.client.send_text_safe = flaky

    await worker._on_frame(paid_frame())
    assert len(provisioner.topups) == 1
    assert (await db.list_orders())[0]["status"] == "send_failed"
    first_content = (await db.list_orders())[0]["content"]
    assert "sk-old-1234567890abcdef" in first_content

    state["fail"] = False
    await worker._on_frame(paid_frame())

    assert len(provisioner.topups) == 1, "重试不能重复充值"
    assert len(worker.client.sent) == 1
    assert "sk-old-1234567890abcdef" in worker.client.sent[0][2]
    assert (await db.list_orders())[0]["status"] == "delivered"


async def test_topup_aborts_when_intent_record_cannot_be_written(db):
    await db.save_rule(dict(TOPUP_RULE))
    await bind_key(db)
    provisioner = FakeTopupProvisioner()
    worker, account_id = await build_worker(db, provisioner)

    async def no_mark(*args, **kwargs):
        return False

    worker._record_topup_pending = no_mark  # type: ignore[method-assign]
    await worker._on_frame(paid_frame())

    assert provisioner.topups == [], "没有落充值意图就不许真的充值"
    row = await db.get_order(account_id, "2500000000000000001")
    assert row["status"] == "topup_failed"
    assert "中止" in row["detail"]


async def test_topup_unknown_result_blocks_retry(db):
    await db.save_rule(dict(TOPUP_RULE))
    await bind_key(db)
    provisioner = FakeTopupProvisioner(
        fail=TopupUncertainError("充值请求发出后连接异常")
    )
    worker, _ = await build_worker(db, provisioner)

    await worker._on_frame(paid_frame())
    assert worker.client.sent == []
    orders = await db.list_orders()
    assert orders[0]["status"] == "topup_unknown"
    assert "结果未知" in orders[0]["detail"]

    # 再推一次也不能重新充：delivery_blocked 会挡住
    await worker._on_frame(paid_frame())
    assert len(provisioner.topups) == 1


async def test_topup_definite_failure_can_retry(db):
    await db.save_rule(dict(TOPUP_RULE))
    await bind_key(db)
    provisioner = FakeTopupProvisioner(fail=UpstreamError("令牌已过期"))
    worker, _ = await build_worker(db, provisioner)

    await worker._on_frame(paid_frame())
    assert worker.client.sent == []
    assert (await db.list_orders())[0]["status"] == "topup_failed"

    # 修好之后可以重试（这次令牌正常）
    worker.provisioner = FakeTopupProvisioner()
    await worker._on_frame(paid_frame())
    assert len(worker.client.sent) == 1
    assert (await db.list_orders())[0]["status"] == "delivered"


async def test_topup_empty_content_uses_default_template(db):
    await db.save_rule({**TOPUP_RULE, "content": ""})
    await bind_key(db)
    provisioner = FakeTopupProvisioner()
    worker, _ = await build_worker(db, provisioner)

    await worker._on_frame(paid_frame())

    text = worker.client.sent[0][2]
    assert "sk-old-1234567890abcdef" in text
    assert "充值" in text
    assert DEFAULT_TOPUP_CONTENT.split("{")[0].strip()[:4] in text


async def test_topup_dry_run_does_not_touch_upstream(db, monkeypatch):
    monkeypatch.setattr(settings, "dry_run", True)
    await db.save_rule(dict(TOPUP_RULE))
    await bind_key(db)
    provisioner = FakeTopupProvisioner()
    worker, _ = await build_worker(db, provisioner)

    await worker._on_frame(paid_frame())

    assert provisioner.topups == []
    assert worker.client.sent == []
    assert await db.list_orders() == []


async def test_upstream_delivery_binds_buyer_key_for_next_topup(db):
    class UpstreamFake:
        def __init__(self) -> None:
            self.newapi = SimpleNamespace()

        async def provision(self, quota_usd, label, username="", model_limits=None):
            return Provisioned(
                "CARD-1", "sk-up…wxyz", quota_usd, "88",
                api_key="sk-upstream-1234567890", token_name=label,
            )

    await db.save_rule({
        "name": "建 Key", "item_id": "*", "enabled": True, "mode": "upstream",
        "content": "卡密 {card}", "auto_confirm": False, "priority": 0,
        "remark": "", "usd_multiplier": 1.0, "fixed_usd": 0.0,
    })
    provisioner = UpstreamFake()
    worker, _ = await build_worker(db, provisioner)
    await worker._on_frame(paid_frame())

    binding = await db.get_buyer_key("4100000001")
    assert binding is not None
    assert binding["token_id"] == "88"
    assert binding["api_key"] == "sk-upstream-1234567890"
    assert binding["source"] == "order"


async def test_manual_binding_not_overwritten_by_auto(db):
    row_id = await bind_key(db, tid="55", source="manual")
    await db.auto_bind_buyer_key({
        "buyer_id": "4100000001", "token_id": "99",
        "api_key": "sk-other", "source": "order",
    })
    row = await db.get_buyer_key("4100000001")
    assert int(row["id"]) == int(row_id)
    assert row["token_id"] == "55", "手工绑定的 Key 不能被自动绑定覆盖"


async def test_buyer_key_token_conflict_raises(db):
    await bind_key(db, buyer_id="buyer-a", tid="55")
    with pytest.raises(ValueError):
        await db.save_buyer_key({
            "buyer_id": "buyer-b", "token_id": "55", "api_key": "sk-x",
        })


async def test_topup_unknown_is_not_overwritten_by_other_statuses(db):
    account_id = await db.upsert_account("主号", "unb=2000000000002; _m_h5_tk=t_1")
    await db.record_order(
        account_id=account_id, order_id="o1", item_id="i", buyer_id="b",
        buyer_name="", rule_id=1, card_id=None, content="", status="topping_up",
    )
    await db.record_order(
        account_id=account_id, order_id="o1", item_id="i", buyer_id="b",
        buyer_name="", rule_id=1, card_id=None, content="", status="topup_unknown",
        detail="结果未知",
    )
    await db.record_order(
        account_id=account_id, order_id="o1", item_id="i", buyer_id="b",
        buyer_name="", rule_id=1, card_id=None, content="", status="upstream_failed",
        detail="别覆盖我",
    )
    row = await db.get_order(account_id, "o1")
    assert row["status"] == "topup_unknown"
    assert "别覆盖我" in row["detail"]


async def test_recover_stale_awaiting_key_lets_polling_prompt_again(db):
    account_id = await db.upsert_account("主号", "unb=2000000000002; _m_h5_tk=t_1")
    await db.record_order(
        account_id=account_id, order_id="o-await", item_id="i", buyer_id="b",
        buyer_name="", rule_id=1, card_id=None, content="", status="awaiting_key",
        detail="等待买家提供对应 Key 的旧订单号",
    )
    assert await db.recover_stale_sending() == 1
    row = await db.get_order(account_id, "o-await")
    assert row["status"] == "topup_failed"
    assert "重新提示" in row["detail"]
    assert await db.delivery_blocked(account_id, "o-await") is False


async def test_recover_stale_topup_marks_unknown(db):
    account_id = await db.upsert_account("主号", "unb=2000000000002; _m_h5_tk=t_1")
    await db.record_order(
        account_id=account_id, order_id="o1", item_id="i", buyer_id="b",
        buyer_name="", rule_id=1, card_id=None, content="", status="topping_up",
    )
    assert await db.recover_stale_sending() == 1
    row = await db.get_order(account_id, "o1")
    assert row["status"] == "topup_unknown"
    assert await db.delivery_blocked(account_id, "o1") is True


# ------------------------------------------------------------- new-api 充值


def make_newapi(handler, **kwargs):
    client = NewApiClient("https://newapi.test", **kwargs)

    def dispatch(request):
        if request.url.path == "/api/user/self":
            return httpx.Response(200, json={"success": True, "data": {"id": 3}})
        return handler(request)

    headers = {}
    if client.access_token:
        headers["authorization"] = "Bearer " + client.access_token
    if client.user_id:
        headers["new-api-user"] = client.user_id
    client._http = httpx.AsyncClient(
        base_url=client.base_url,
        transport=httpx.MockTransport(dispatch),
        headers=headers,
    )
    return client


def token_payload(**overrides):
    data = {
        "id": 42,
        "name": "买家小王#123",
        "key": "sk-secret-value",
        "remain_quota": 1000,
        "unlimited_quota": False,
        "status": 1,
        "expired_time": -1,
        "model_limits_enabled": True,
        "model_limits": "deepseek-v4.1-flash",
        "group": "default",
        "created_time": 1,
        "used_quota": 0,
        "allow_ips": "",
    }
    data.update(overrides)
    return data


async def test_topup_token_adds_quota_and_preserves_record():
    captured: dict[str, Any] = {}

    def handler(request):
        if request.url.path == "/api/status":
            return httpx.Response(200, json={"success": True, "data": {"quota_per_unit": 500000}})
        if request.url.path == "/api/token/42" and request.method == "GET":
            return httpx.Response(200, json={"success": True, "data": token_payload()})
        if request.url.path == "/api/token/42/key":
            return httpx.Response(200, json={"success": True, "data": {"key": "sk-secret-value"}})
        if request.url.path == "/api/token/" and request.method == "PUT":
            captured.update(json.loads(request.content))
            return httpx.Response(200, json={"success": True})
        raise AssertionError("没预料到的请求 " + request.method + " " + request.url.path)

    client = make_newapi(handler, access_token="jwt", user_id="3")
    try:
        info = await client.topup_token("42", 10.0)
    finally:
        await client.close()

    assert info.api_key == "sk-secret-value"
    assert info.quota_usd == pytest.approx(10.0)
    assert info.balance == pytest.approx(10.0)  # 展示到分，超出部分四舍五入
    assert captured["remain_quota"] == 1000 + 5_000_000
    assert captured["name"] == "买家小王#123"
    assert captured["model_limits"] == "deepseek-v4.1-flash"
    assert captured["group"] == "default"
    assert "key" not in captured and "DeletedAt" not in captured
    assert captured.get("status") == 1


async def test_topup_token_reenables_exhausted_token():
    captured: dict[str, Any] = {}

    def handler(request):
        if request.url.path == "/api/status":
            return httpx.Response(200, json={"data": {"quota_per_unit": 500000}})
        if request.url.path == "/api/token/42":
            # 列表里有时只给打码值，这时必须回落到 /api/token/{id}/key 读明文
            return httpx.Response(200, json={"success": True,
                                             "data": token_payload(status=4, key="sk-***")})
        if request.url.path == "/api/token/42/key":
            return httpx.Response(200, json={"data": {"key": "sk-secret-value"}})
        if request.url.path == "/api/token/" and request.method == "PUT":
            captured.update(json.loads(request.content))
            return httpx.Response(200, json={"success": True})
        raise AssertionError(request.url.path)

    client = make_newapi(handler, access_token="jwt", user_id="3")
    try:
        await client.topup_token("42", 5.0)
    finally:
        await client.close()
    assert captured["status"] == 1


async def test_topup_token_rejects_disabled_and_expired():
    def handler(request):
        if request.url.path == "/api/status":
            return httpx.Response(200, json={"data": {"quota_per_unit": 500000}})
        if request.url.path == "/api/token/42":
            return httpx.Response(200, json={"success": True, "data": token_payload(status=2)})
        raise AssertionError(request.url.path)

    client = make_newapi(handler, access_token="jwt", user_id="3")
    try:
        with pytest.raises(UpstreamError, match="禁用"):
            await client.topup_token("42", 5.0)
    finally:
        await client.close()

    def expired_handler(request):
        if request.url.path == "/api/status":
            return httpx.Response(200, json={"data": {"quota_per_unit": 500000}})
        if request.url.path == "/api/token/42":
            return httpx.Response(200, json={
                "success": True, "data": token_payload(expired_time=1000),
            })
        raise AssertionError(request.url.path)

    client = make_newapi(expired_handler, access_token="jwt", user_id="3")
    try:
        with pytest.raises(UpstreamError, match="过期"):
            await client.topup_token("42", 5.0)
    finally:
        await client.close()


async def test_topup_token_unlimited_skips_put():
    calls: list[str] = []

    def handler(request):
        calls.append(request.method + " " + request.url.path)
        if request.url.path == "/api/status":
            return httpx.Response(200, json={"data": {"quota_per_unit": 500000}})
        if request.url.path == "/api/token/42":
            return httpx.Response(200, json={
                "success": True, "data": token_payload(unlimited_quota=True),
            })
        if request.url.path == "/api/token/42/key":
            return httpx.Response(200, json={"data": {"key": "sk-secret-value"}})
        raise AssertionError(request.url.path)

    client = make_newapi(handler, access_token="jwt", user_id="3")
    try:
        info = await client.topup_token("42", 5.0)
    finally:
        await client.close()
    assert info.topped_up is False
    assert info.balance is None
    assert not any(call.startswith("PUT") for call in calls)


async def test_topup_put_network_error_is_uncertain():
    def handler(request):
        if request.url.path == "/api/status":
            return httpx.Response(200, json={"data": {"quota_per_unit": 500000}})
        if request.url.path == "/api/token/42":
            return httpx.Response(200, json={"success": True, "data": token_payload()})
        if request.url.path == "/api/token/42/key":
            return httpx.Response(200, json={"data": {"key": "sk-secret-value"}})
        if request.url.path == "/api/token/" and request.method == "PUT":
            raise httpx.ConnectError("connection lost")
        raise AssertionError(request.url.path)

    client = make_newapi(handler, access_token="jwt", user_id="3")
    try:
        with pytest.raises(TopupUncertainError):
            await client.topup_token("42", 5.0)
    finally:
        await client.close()


async def test_topup_token_serializes_same_token():
    """同一个令牌并发充值不能丢额度（读-改-写必须串行）。"""
    state = {"remain": 1000}

    async def handler(request):
        if request.url.path == "/api/user/self":
            return httpx.Response(200, json={"success": True, "data": {"id": 3}})
        if request.url.path == "/api/status":
            return httpx.Response(200, json={"data": {"quota_per_unit": 500000}})
        if request.url.path == "/api/token/42" and request.method == "GET":
            await asyncio.sleep(0.01)
            return httpx.Response(200, json={
                "success": True, "data": token_payload(remain_quota=state["remain"]),
            })
        if request.url.path == "/api/token/" and request.method == "PUT":
            await asyncio.sleep(0.02)
            body = json.loads(request.content)
            state["remain"] = body["remain_quota"]
            return httpx.Response(200, json={"success": True})
        raise AssertionError(request.method + " " + request.url.path)

    client = NewApiClient("https://newapi.test", access_token="jwt", user_id="3")
    client._http = httpx.AsyncClient(
        base_url="https://newapi.test",
        transport=httpx.MockTransport(handler),
        headers={"authorization": "Bearer jwt", "new-api-user": "3"},
    )
    try:
        await asyncio.gather(
            client.topup_token("42", 5.0),
            client.topup_token("42", 5.0),
        )
    finally:
        await client.close()

    # 5 美元 = 2,500,000 quota，两单并发必须都加上
    assert state["remain"] == 1000 + 5_000_000


async def test_topup_5xx_json_is_uncertain():
    def handler(request):
        if request.url.path == "/api/status":
            return httpx.Response(200, json={"data": {"quota_per_unit": 500000}})
        if request.url.path == "/api/token/42":
            return httpx.Response(200, json={"success": True, "data": token_payload()})
        if request.url.path == "/api/token/" and request.method == "PUT":
            return httpx.Response(502, json={"success": False, "message": "bad gateway"})
        raise AssertionError(request.url.path)

    client = make_newapi(handler, access_token="jwt", user_id="3")
    try:
        with pytest.raises(TopupUncertainError):
            await client.topup_token("42", 5.0)
    finally:
        await client.close()


async def test_find_token_by_key_scans_list():
    pages = {
        1: [{"id": 7, "name": "a", "key": "plain-a"}, {"id": 8, "name": "b", "key": "plain-b"}],
    }

    def handler(request):
        if request.url.path == "/api/status":
            return httpx.Response(200, json={"data": {"quota_per_unit": 500000}})
        if request.url.path == "/api/token/":
            page = int(request.url.params.get("p", "1"))
            return httpx.Response(200, json={
                "success": True, "data": {"items": pages.get(page, [])},
            })
        raise AssertionError(request.url.path)

    client = make_newapi(handler, access_token="jwt", user_id="3")
    try:
        token = await client.find_token_by_key("sk-plain-b")
        assert token["id"] == 8
        with pytest.raises(UpstreamError, match="找不到"):
            await client.find_token_by_key("sk-nope")
    finally:
        await client.close()


async def test_find_tokens_for_orders_matches_name_suffix():
    def handler(request):
        if request.url.path == "/api/status":
            return httpx.Response(200, json={"data": {"quota_per_unit": 500000}})
        if request.url.path == "/api/token/":
            return httpx.Response(200, json={"success": True, "data": {"items": [
                {"id": 1, "name": "买家甲#111"},
                {"id": 2, "name": "买家乙#222"},
                {"id": 3, "name": "无关令牌"},
            ]}})
        raise AssertionError(request.url.path)

    client = make_newapi(handler, access_token="jwt", user_id="3")
    try:
        found = await client.find_tokens_for_orders(["111", "222", "333"])
    finally:
        await client.close()
    assert set(found) == {"111", "222"}
    assert found["111"]["id"] == 1
    assert found["222"]["id"] == 2


async def test_find_token_by_key_explains_when_list_hides_keys():
    def handler(request):
        if request.url.path == "/api/status":
            return httpx.Response(200, json={"data": {"quota_per_unit": 500000}})
        if request.url.path == "/api/token/":
            return httpx.Response(200, json={
                "success": True,
                "data": {"items": [{"id": 7, "name": "a", "key": "sk-***"}]},
            })
        raise AssertionError(request.url.path)

    client = make_newapi(handler, access_token="jwt", user_id="3")
    try:
        with pytest.raises(UpstreamError, match="令牌 ID"):
            await client.find_token_by_key("sk-abcdef")
    finally:
        await client.close()


# ------------------------------------------------------------------ Web 接口


class StubManager:
    def __init__(self, db: Database, newapi: Any = None) -> None:
        self.db = db
        self.provisioner = SimpleNamespace(newapi=newapi) if newapi is not None else SimpleNamespace()

    async def sync(self) -> None:
        pass

    async def stop_all(self) -> None:
        pass

    def status(self) -> list[dict[str, Any]]:
        return []


class FakeNewApi:
    async def get_token(self, token_id: str) -> dict[str, Any]:
        if token_id != "42":
            raise UpstreamError("读不到令牌 " + token_id)
        return token_payload()

    async def find_token_by_key(self, api_key: str) -> dict[str, Any]:
        if api_key.endswith("nope"):
            raise UpstreamError("找不到这个 Key")
        return token_payload()

    async def read_key_quiet(self, token_id: str) -> str:
        return "sk-secret-value"

    async def token_balance(self, token: dict[str, Any]) -> float:
        return 12.34

    async def list_tokens(self, page_size: int = 100) -> list[dict[str, Any]]:
        return [token_payload(), token_payload(id=43, name="另一个", key="sk-other")]


@pytest.fixture()
async def web_client(tmp_path, monkeypatch):
    import app.web as web_module

    monkeypatch.setattr(web_module, "newapi_ready", lambda: True)
    db = Database(tmp_path / "web.db")
    manager = StubManager(db, FakeNewApi())
    app = create_app(db=db, manager=manager)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1") as ac:
        async with app.router.lifespan_context(app):
            yield ac, db


async def test_web_bind_list_and_unbind_buyer_key(web_client):
    client, db = web_client
    res = await client.post("/api/buyer-keys", json={
        "buyer_id": "4100000001", "buyer_name": "买家小王",
        "token_id": "42", "remark": "老客户",
    })
    assert res.status_code == 200, res.text
    row_id = res.json()["id"]
    assert res.json()["key_mask"] == "sk-secr…alue"

    rows = (await client.get("/api/buyer-keys")).json()
    assert len(rows) == 1
    assert "api_key" not in rows[0], "接口不能把完整 Key 回给前端"
    assert rows[0]["has_key"] is True
    assert rows[0]["balance"] == pytest.approx(12.34)

    res = await client.post(f"/api/buyer-keys/{row_id}/refresh")
    assert res.status_code == 200
    assert res.json()["balance"] == pytest.approx(12.34)

    assert (await client.delete(f"/api/buyer-keys/{row_id}")).status_code == 200
    assert (await client.get("/api/buyer-keys")).json() == []
    assert await db.get_buyer_key("4100000001") is None


async def test_web_bind_by_api_key_and_conflicts(web_client):
    client, db = web_client
    res = await client.post("/api/buyer-keys", json={
        "buyer_id": "4100000002", "api_key": "sk-secret-value",
    })
    assert res.status_code == 200, res.text
    assert (await db.get_buyer_key("4100000002"))["token_id"] == "42"

    # 同一个买家重复保存同一个令牌是幂等的，不能报冲突
    res = await client.post("/api/buyer-keys", json={
        "buyer_id": "4100000002", "token_id": "42",
    })
    assert res.status_code == 200, res.text

    # 同一个令牌不能再绑给第二个买家
    res = await client.post("/api/buyer-keys", json={
        "buyer_id": "4100000003", "token_id": "42",
    })
    assert res.status_code == 409

    # 两个都没填
    res = await client.post("/api/buyer-keys", json={"buyer_id": "4100000004"})
    assert res.status_code == 409


async def test_web_newapi_token_list_masks_keys(web_client):
    client, _ = web_client
    rows = (await client.get("/api/newapi/tokens")).json()
    assert len(rows) == 2
    assert all("key" not in row for row in rows)
    assert rows[0]["key_mask"] == "sk-secr…alue"

    rows = (await client.get("/api/newapi/tokens?keyword=另一个")).json()
    assert len(rows) == 1
    assert rows[0]["id"] == "43"


async def test_web_buyer_key_requires_newapi(tmp_path, monkeypatch):
    import app.web as web_module

    monkeypatch.setattr(web_module, "newapi_ready", lambda: False)
    db = Database(tmp_path / "web2.db")
    app = create_app(db=db, manager=StubManager(db))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1") as ac:
        async with app.router.lifespan_context(app):
            res = await ac.post("/api/buyer-keys", json={"buyer_id": "b", "token_id": "42"})
    assert res.status_code == 400
    assert "new-api" in res.json()["detail"]


# ---------------------------------------------------------- 规则字段落库


async def test_legacy_database_gets_new_columns_and_buyer_keys_table(tmp_path):
    """老库升级：rules 补两列、buyer_keys 表自动建出来。"""
    import sqlite3

    path = tmp_path / "legacy.db"
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE rules (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL,"
        " item_id TEXT NOT NULL DEFAULT '*', enabled INTEGER NOT NULL DEFAULT 1,"
        " mode TEXT NOT NULL DEFAULT 'text', content TEXT NOT NULL DEFAULT '',"
        " auto_confirm INTEGER NOT NULL DEFAULT 0, priority INTEGER NOT NULL DEFAULT 0,"
        " remark TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL)"
    )
    conn.commit()
    conn.close()

    db = Database(path)
    await db.init()

    conn = sqlite3.connect(path)
    try:
        rule_cols = {row[1] for row in conn.execute("PRAGMA table_info(rules)")}
        order_cols = {row[1] for row in conn.execute("PRAGMA table_info(orders)")}
        tables = {row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )}
    finally:
        conn.close()
    assert {"spec", "topup_missing", "topup_key_source", "topup_item_scope",
            "topup_prompt", "topup_not_found", "min_amount", "max_amount"} <= rule_cols
    assert "key_order_id" in order_cols
    assert "buyer_keys" in tables


def test_pick_rule_matches_amount_range_and_defers_when_unknown():
    rules = [
        {"id": 1, "item_id": "900", "priority": 9},
        {"id": 2, "item_id": "900", "priority": 0, "min_amount": 4.5, "max_amount": 5.5},
    ]
    assert pick_rule(rules, "900", "", 5.0)["id"] == 2
    assert pick_rule(rules, "900", "", 10.0)["id"] == 1
    # 配了金额规则但还没拿到金额：先不选，避免用通用规则发错额度
    assert pick_rule(rules, "900", "", None) is None


def test_amount_bounds_tolerates_reversed_and_zero():
    assert amount_bounds({"min_amount": 0, "max_amount": 0}) is None
    assert amount_bounds({"min_amount": 10, "max_amount": 5}) == (5, 10)
    assert amount_matches({"min_amount": 4.5, "max_amount": 5.5}, 5.0) is True
    assert amount_matches({"min_amount": 4.5, "max_amount": 5.5}, 10.0) is False
    assert amount_matches({"min_amount": 4.5}, 10.0) is True
    assert amount_matches({}, 10.0) is None


async def test_worker_prefetches_amount_for_price_rule(db):
    """付款卡片不带金额时，配了金额区间的规则要先查订单金额再决策。"""
    await db.save_rule({
        "name": "通用", "item_id": "*", "enabled": True, "mode": "text",
        "content": "通用 10 元", "auto_confirm": False, "priority": 9, "remark": "",
    })
    await db.save_rule({
        "name": "5 元档", "item_id": "900052644277", "enabled": True, "mode": "text",
        "content": "五元档 {amount}", "auto_confirm": False, "priority": 0, "remark": "",
        "min_amount": 4.5, "max_amount": 5.5,
    })
    worker, _ = await build_worker(db, FakeTopupProvisioner(), order={
        "commonData": {"orderId": "2500000000000000001", "itemId": "900052644277",
                       "orderStatus": "待发货"},
        "priceVO": {"actualPayFee": "5.00"},
    })
    await worker._on_frame(paid_frame())
    assert worker.client.sent and "五元档 5.00" in worker.client.sent[0][2]
    assert worker.client.order_lookups, "金额是发货前查的"


async def test_item_price_tiers_from_orders(db):
    account_id = await db.upsert_account("主号", "unb=2000000000002; _m_h5_tk=t_1")
    for order_id, amount in [("a1", 1.0), ("a2", 5.0), ("a3", 5.0), ("a4", 10.0)]:
        await db.record_order(
            account_id=account_id, order_id=order_id, item_id="900052644277",
            buyer_id="b", buyer_name="", rule_id=1, card_id=None,
            content="x", status="delivered", amount_cny=amount,
        )
    tiers = await db.item_price_tiers("900052644277")
    assert [(row["amount"], row["count"]) for row in tiers] == [
        (1.0, 1), (5.0, 2), (10.0, 1),
    ]


async def test_rule_amount_fields_roundtrip_and_validation(web_client):
    client, _db = web_client
    res = await client.post("/api/rules", json={
        "name": "5 元档", "item_id": "900052644277", "mode": "text", "content": "hi",
        "min_amount": 4.5, "max_amount": 5.5,
    })
    assert res.status_code == 200, res.text
    rows = (await client.get("/api/rules")).json()
    assert rows[0]["min_amount"] == 4.5
    assert rows[0]["max_amount"] == 5.5

    res = await client.post("/api/rules", json={
        "name": "填反了", "item_id": "*", "mode": "text", "content": "hi",
        "min_amount": 10, "max_amount": 5,
    })
    assert res.status_code == 400
    assert "最低金额" in res.json()["detail"]


async def test_item_prices_api(web_client):
    client, db = web_client
    account_id = await db.upsert_account("主号", "unb=2000000000002; _m_h5_tk=t_1")
    for order_id, amount in [("p1", 1.0), ("p2", 5.0), ("p3", 5.0)]:
        await db.record_order(
            account_id=account_id, order_id=order_id, item_id="1094740913216",
            buyer_id="b", buyer_name="", rule_id=1, card_id=None,
            content="x", status="delivered", amount_cny=amount,
        )
    data = (await client.get("/api/item-prices?item_id=1094740913216")).json()
    assert data["item_id"] == "1094740913216"
    assert [(p["amount"], p["count"]) for p in data["prices"]] == [(1.0, 1), (5.0, 2)]
    assert (await client.get("/api/item-prices?item_id=not-exist")).json()["prices"] == []


async def test_rule_spec_and_topup_missing_roundtrip(db):
    rule_id = await db.save_rule({
        "name": "充值", "item_id": "*", "enabled": True, "mode": "topup",
        "content": "{key}", "auto_confirm": False, "priority": 0, "remark": "",
        "spec": "季卡", "topup_missing": "fail",
    })
    rule = await db.get_rule(rule_id)
    assert rule["spec"] == "季卡"
    assert rule["topup_missing"] == "fail"

    # 老调用方不传新字段时也要能用（默认 create）
    rule_id = await db.save_rule({
        "name": "老规则", "item_id": "*", "enabled": True, "mode": "text",
        "content": "hi", "auto_confirm": False, "priority": 0, "remark": "",
    })
    rule = await db.get_rule(rule_id)
    assert rule["spec"] == ""
    assert rule["topup_missing"] == "create"
    assert rule["topup_key_source"] == "order_id"
    # 新字段默认不限制商品：老库升级后行为不变
    assert rule["topup_item_scope"] == "any"


async def test_rule_api_accepts_topup_mode(web_client):
    client, _ = web_client
    res = await client.post("/api/rules", json={
        "name": "充值规则", "item_id": "*", "mode": "topup", "content": "{key}",
        "spec": "季卡", "topup_missing": "fail", "topup_key_source": "order_id",
        "topup_item_scope": "item",
        "topup_prompt": "请发旧订单号", "topup_not_found": "没有此 Key",
    })
    assert res.status_code == 200, res.text
    rows = (await client.get("/api/rules")).json()
    assert rows[0]["mode"] == "topup"
    assert rows[0]["spec"] == "季卡"
    assert rows[0]["topup_missing"] == "fail"
    assert rows[0]["topup_key_source"] == "order_id"
    assert rows[0]["topup_item_scope"] == "item"
    assert rows[0]["topup_prompt"] == "请发旧订单号"
    assert rows[0]["topup_not_found"] == "没有此 Key"

    # 只接受 any / item 两个值，写错要当场拒绝（不然会静默按 any 走，等于没配）
    bad = await client.post("/api/rules", json={
        "name": "写错了", "item_id": "*", "mode": "topup", "content": "{key}",
        "topup_item_scope": "sometimes",
    })
    assert bad.status_code == 422
