"""兑换码模式：金额 → new-api 兑换码 → 直接发码。"""

from __future__ import annotations

import base64
import json
from typing import Any

import msgpack
import pytest

from app.client import XianyuError
from app.codec import IncomingMessage
from app.db import Database
from app.engine import DeliveryEngine
from app.runner import AccountWorker
from app.upstream import Provisioner, UpstreamError, build_redeem_label

BUYER_ID = "4100000001"
ORDER_ID = "2500000000000000001"
ITEM_ID = "900052644277"

REDEEM_RULE = {
    "name": "兑换码 10 元",
    "item_id": "*",
    "enabled": True,
    "mode": "redeem",
    "content": "",
    "auto_confirm": True,
    "priority": 0,
    "remark": "",
    "usd_multiplier": 1.0,
    "fixed_usd": 0.0,
    "redeem_expiry_days": 0,
}


class FakeClient:
    """闲鱼客户端替身：够发货链路用就行。"""

    user_id = "2000000000002"

    def __init__(self) -> None:
        self.sent: list[tuple[str, str, str]] = []
        self.confirmed: list[str] = []
        self.websocket = object()

    async def send_text_safe(self, chat_id: str, to_id: str, text: str) -> None:
        self.sent.append((chat_id, to_id, text))

    async def confirm_shipping(self, order_id: str) -> tuple[bool, str]:
        self.confirmed.append(order_id)
        return True, "SUCCESS"

    async def find_order(self, order_id: str) -> dict | None:
        return {
            "commonData": {"orderId": order_id, "orderStatus": "待发货"},
            "priceVO": {"actualPayFee": "10.00"},
        }

    async def close(self) -> None:
        pass


class FakeRedeemProvisioner:
    """记录生成兑换码的调用；可以按需失败。"""

    def __init__(self, fail: Exception | None = None, codes: list[str] | None = None,
                 fail_card: Exception | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.fail = fail
        self.fail_card = fail_card
        self._codes = list(codes or [])
        self.generated = 0
        self.cards: list[tuple[str, float, str]] = []

    async def redeem_codes(self, quota_usd: float, label: str, *, count: int = 1,
                           valid_days: int = 0) -> list[str]:
        self.calls.append({"quota": quota_usd, "label": label, "valid_days": valid_days,
                           "count": count})
        if self.fail is not None:
            raise self.fail
        out = []
        for _ in range(count):
            self.generated += 1
            out.append(self._codes.pop(0) if self._codes else f"CODE{self.generated:018d}")
        return out

    async def issue_card(self, content: str, quota_usd: float, label: str) -> str:
        self.cards.append((content, quota_usd, label))
        if self.fail_card is not None:
            raise self.fail_card
        return "SHOPCARD-" + str(len(self.cards))


def paid_frame(order_id: str = ORDER_ID) -> str:
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
                "senderUserId": BUYER_ID,
                "senderNick": "买家小王",
                "itemTitle": "兑换码 10 元",
                "reminderUrl": (
                    "fleamarket://message_chat?itemId=" + ITEM_ID + "&peerUserId=" + BUYER_ID
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


async def build_worker(db: Database, provisioner: Any) -> tuple[AccountWorker, int]:
    account_id = await db.upsert_account("主号", "unb=2000000000002; _m_h5_tk=t_1")
    account = await db.get_account(account_id)
    worker = AccountWorker(db, account, DeliveryEngine(db), provisioner)
    worker.client = FakeClient()
    return worker, account_id


# ------------------------------------------------------------------ 名称


def test_redeem_label_fits_newapi_limit():
    """new-api 的兑换码名称最多 20 个字，别把 19 位订单号整个塞进去。"""
    label = build_redeem_label("买家小王的昵称很长很长", "2500000000000000001")
    assert len(label) <= 20
    assert label.endswith("#00000001")
    assert label.startswith("买家小王的")


def test_redeem_label_without_order():
    assert build_redeem_label("买家", "") == "买家"


# ------------------------------------------------------------------ 决策


async def test_engine_redeem_rule_becomes_redeem_action(db):
    await db.save_rule(dict(REDEEM_RULE))
    engine = DeliveryEngine(db)
    msg = IncomingMessage(
        chat_id="c", sender_id=BUYER_ID, sender_name="买家小王",
        content="[订单待发货]", item_id=ITEM_ID, order_id=ORDER_ID,
        order_id_trusted=True,
    )

    decision = await engine.decide_order(0, msg)

    assert decision.action == "redeem"
    assert decision.needs_redeem is True
    assert decision.needs_account is False


# ------------------------------------------------------------------ 发货


async def test_redeem_delivers_card_wrapping_the_code(db):
    await db.save_rule(dict(REDEEM_RULE))
    provisioner = FakeRedeemProvisioner(codes=["a" * 32])
    worker, account_id = await build_worker(db, provisioner)

    await worker._on_frame(paid_frame())

    assert len(provisioner.calls) == 1
    assert provisioner.calls[0]["quota"] == pytest.approx(10.0)
    assert provisioner.calls[0]["valid_days"] == 0

    # 卡密站只收兑换码本身（一个码一张卡），不能带自定义文本
    assert len(provisioner.cards) == 1
    card_content, card_quota, _ = provisioner.cards[0]
    assert card_content == "a" * 32
    assert card_quota == pytest.approx(10.0)

    text = worker.client.sent[-1][2]
    assert "SHOPCARD-1" in text
    assert "a" * 32 not in text, "兑换码本身不该出现在聊天里"

    row = await db.get_order(account_id, ORDER_ID)
    assert row["status"] == "delivered"
    assert row["card_code"] == "SHOPCARD-1"
    assert worker.client.confirmed == [ORDER_ID]


async def test_redeem_respects_expiry_days(db):
    await db.save_rule({**REDEEM_RULE, "redeem_expiry_days": 30})
    provisioner = FakeRedeemProvisioner()
    worker, _ = await build_worker(db, provisioner)

    await worker._on_frame(paid_frame())

    assert provisioner.calls[0]["valid_days"] == 30


async def test_redeem_failure_is_retryable_and_sends_nothing(db):
    await db.save_rule(dict(REDEEM_RULE))
    provisioner = FakeRedeemProvisioner(fail=UpstreamError("new-api 生成兑换码失败：boom"))
    worker, account_id = await build_worker(db, provisioner)

    await worker._on_frame(paid_frame())

    assert worker.client.sent == []
    row = await db.get_order(account_id, ORDER_ID)
    assert row["status"] == "redeem_failed"
    assert "生成兑换码失败" in row["detail"]


async def test_redeem_card_failure_reuses_same_code(db):
    """卡密站挂了：码已经生成，下一轮必须复用同一个码，不能每轮再生成一个。"""
    await db.save_rule(dict(REDEEM_RULE))
    provisioner = FakeRedeemProvisioner(codes=["d" * 32],
                                        fail_card=UpstreamError("卡密站 502"))
    worker, account_id = await build_worker(db, provisioner)

    await worker._on_frame(paid_frame())

    assert worker.client.sent == []
    row = await db.get_order(account_id, ORDER_ID)
    assert row["status"] == "redeem_card_failed"
    assert row["card_code"] == "d" * 32, "码要先落库，下一轮才知道复用什么"

    # 卡密站恢复 → 复用同一个码建卡密，不生成第二个
    provisioner.fail_card = None
    await worker._on_frame(paid_frame())

    assert provisioner.generated == 1
    assert len(provisioner.cards) == 2  # 第一次失败那次也记了一笔调用
    assert "d" * 32 in provisioner.cards[-1][0]
    assert "SHOPCARD-2" in worker.client.sent[-1][2]
    assert (await db.get_order(account_id, ORDER_ID))["status"] == "delivered"


async def test_redeem_send_failure_reuses_same_card(db):
    """卡密建好、消息没发出去：下一轮复用同一张卡密，不重新生成。"""
    await db.save_rule(dict(REDEEM_RULE))
    provisioner = FakeRedeemProvisioner(codes=["b" * 32])
    worker, account_id = await build_worker(db, provisioner)

    class Boom(FakeClient):
        async def send_text_safe(self, chat_id: str, to_id: str, text: str) -> None:
            raise XianyuError("ws closed")

    worker.client = Boom()
    await worker._on_frame(paid_frame())
    assert provisioner.generated == 1
    assert len(provisioner.cards) == 1
    assert (await db.get_order(account_id, ORDER_ID))["status"] == "send_failed"

    worker.client = FakeClient()
    await worker._on_frame(paid_frame())

    assert provisioner.generated == 1, "不能生成第二个兑换码"
    assert len(provisioner.cards) == 1, "不能建第二张卡密"
    assert "SHOPCARD-1" in worker.client.sent[-1][2]
    assert (await db.get_order(account_id, ORDER_ID))["status"] == "delivered"


async def test_redeem_refuses_when_cardshop_missing(db):
    """卡密站没配就别生成码：生成了也没地方放，只会每轮多一个孤儿码。"""
    await db.save_rule(dict(REDEEM_RULE))
    provisioner = FakeRedeemProvisioner()
    provisioner.can_issue_cards = False
    worker, account_id = await build_worker(db, provisioner)

    await worker._on_frame(paid_frame())

    assert provisioner.calls == []
    row = await db.get_order(account_id, ORDER_ID)
    assert row["status"] == "redeem_failed"
    assert "卡密站未配置" in row["detail"]


async def test_redeem_uses_custom_content(db):
    await db.save_rule({**REDEEM_RULE, "content": "卡密是 {card}（{quota}）"})
    provisioner = FakeRedeemProvisioner(codes=["c" * 32])
    worker, _ = await build_worker(db, provisioner)

    await worker._on_frame(paid_frame())

    assert worker.client.sent[-1][2] == "卡密是 SHOPCARD-1（10.00）"
    assert provisioner.cards[0][0] == "c" * 32


# ------------------------------------------------------------------ 客户端


async def test_redeem_codes_converts_amount_and_expiry():
    class StubNewApi:
        def __init__(self) -> None:
            self.seen: dict[str, Any] = {}

        async def quota_for_amount(self, amount: float) -> int:
            return int(amount * 500000)

        async def create_redemptions(self, quota: int, name: str, *, count: int = 1,
                                     expired_time: int = 0) -> list[str]:
            self.seen = {"quota": quota, "name": name, "count": count,
                         "expired_time": expired_time}
            return ["k" * 32]

    provisioner = Provisioner.__new__(Provisioner)
    provisioner.newapi = StubNewApi()

    codes = await provisioner.redeem_codes(10.0, "买家#00000001", valid_days=7)

    assert codes == ["k" * 32]
    assert provisioner.newapi.seen["quota"] == 5000000
    assert provisioner.newapi.seen["name"] == "买家#00000001"
    assert provisioner.newapi.seen["expired_time"] > 0
