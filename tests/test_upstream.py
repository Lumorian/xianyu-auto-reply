"""上游联动（new-api → 卡密站）的接线测试。"""

from __future__ import annotations

import base64
import json

import msgpack
import pytest

from app.db import Database
from app.engine import DeliveryEngine
from app.runner import AccountWorker
from app.upstream import Provisioned, UpstreamError


class FakeProvisioner:
    """替身：记录调用参数，按需返回卡密或抛错。"""

    def __init__(self, fail_with: str | None = None) -> None:
        self.calls: list[tuple[float, str]] = []
        self.usernames: list[str] = []
        self.fail_with = fail_with

    async def provision(self, quota_usd: float, label: str, username: str = "",
                        model_limits: str | None = None) -> Provisioned:
        self.calls.append((quota_usd, label))
        self.usernames.append(username)
        if self.fail_with:
            raise UpstreamError(self.fail_with)
        return Provisioned(card_code="CARD-ABCD-EFGH", key_mask="sk-abc…wxyz",
                           quota_usd=quota_usd, token_id="42")


class FakeClient:
    user_id = "2000000000002"

    def __init__(self, order: dict | None = None) -> None:
        self.sent: list[tuple[str, str, str]] = []
        self.confirmed: list[str] = []
        self.acked: list[dict] = []
        self.websocket = self
        self.order = order
        self.order_lookups: list[str] = []

    async def ack(self, ws, frame):
        self.acked.append(frame)

    async def send_text_safe(self, chat_id, to_id, text):
        self.sent.append((chat_id, to_id, text))

    async def confirm_shipping(self, order_id):
        self.confirmed.append(order_id)
        return True, "SUCCESS"

    async def find_order(self, order_id):
        self.order_lookups.append(order_id)
        return self.order

    async def close(self):
        pass


def paid_frame(order_id="2500000000000000001", item_id="900052644277"):
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
                "reminderUrl": (
                    "fleamarket://message_chat?itemId=" + item_id + "&peerUserId=4100000001"
                ),
            },
            "6": {"3": {"5": json.dumps(card)}},
        },
    }
    data = base64.b64encode(msgpack.packb(payload, use_bin_type=True)).decode()
    return json.dumps({"headers": {"mid": "m1"}, "body": {"syncPushPackage": {"data": [{"data": data}]}}})

UPSTREAM_RULE = {
    "name": "按金额建 Key", "item_id": "*", "enabled": True, "mode": "upstream",
    "content": "你好{buyer}，卡密：{card}\n额度 {quota} USD（订单 {amount} 元）",
    "auto_confirm": True, "priority": 0, "remark": "",
    "usd_multiplier": 1.0, "fixed_usd": 0.0,
}


ORDER = {"commonData": {"orderId": "2500000000000000001", "orderStatus": "待发货"},
         "priceVO": {"actualPayFee": "10.00"}}


async def build(db: Database, provisioner, order=ORDER):
    account_id = await db.upsert_account("主号", "unb=2000000000002; _m_h5_tk=t_1")
    account = await db.get_account(account_id)
    worker = AccountWorker(db, account, DeliveryEngine(db), provisioner)
    worker.client = FakeClient(order)
    return worker, account_id


async def test_upstream_delivery_creates_card_and_sends(db):
    await db.save_rule(dict(UPSTREAM_RULE))
    provisioner = FakeProvisioner()
    worker, _ = await build(db, provisioner)

    await worker._on_frame(paid_frame())

    # 1) 按订单实付金额建了 10 USD 额度
    assert len(provisioner.calls) == 1
    quota, label = provisioner.calls[0]
    assert quota == pytest.approx(10.0)
    # 卡密名称 = 买家用户名#订单号：不带价格、不带平台前缀
    assert label == "买家小王#2500000000000000001"
    assert "10.00" not in label and "元" not in label
    # 令牌名用买家用户名，不是「闲鱼-价格」
    assert provisioner.usernames == ["买家小王"]

    # 2) 卡密发给了买家，文案里带上额度与订单金额
    assert len(worker.client.sent) == 1
    text = worker.client.sent[0][2]
    assert "CARD-ABCD-EFGH" in text
    assert "10.00" in text

    # 3) 自动确认发货被调用，订单落库为 delivered
    assert worker.client.confirmed == ["2500000000000000001"]
    orders = await db.list_orders()
    assert orders[0]["status"] == "delivered"
    assert orders[0]["amount_cny"] == pytest.approx(10.0)
    assert orders[0]["quota_usd"] == pytest.approx(10.0)
    assert orders[0]["card_code"] == "CARD-ABCD-EFGH"


async def test_upstream_multiplier_and_fixed(db):
    await db.save_rule({**UPSTREAM_RULE, "usd_multiplier": 2.0})
    provisioner = FakeProvisioner()
    worker, _ = await build(db, provisioner)
    await worker._on_frame(paid_frame())
    assert provisioner.calls[0][0] == pytest.approx(20.0)


async def test_cny_site_does_not_apply_cny_per_usd_twice(db, monkeypatch):
    """CNY 展示站点的 quota 本身就是人民币，不能再按 XY_CNY_PER_USD 除一次。"""
    from app.config import settings

    class Site:
        currency = "CNY"
        quota_per_unit = 500000
        exchange_rate = 7.3

    class FakeNewApi:
        async def site_quota_model(self):
            return Site()

    monkeypatch.setattr(settings, "cny_per_usd", 7.3)
    await db.save_rule(dict(UPSTREAM_RULE))
    provisioner = FakeProvisioner()
    provisioner.newapi = FakeNewApi()  # type: ignore[attr-defined]
    worker, _ = await build(db, provisioner)

    await worker._on_frame(paid_frame())

    assert provisioner.calls[0][0] == pytest.approx(10.0), "CNY 站点不应双重收窄"
    assert (await db.list_orders())[0]["status"] == "delivered"


async def test_usd_site_still_applies_cny_per_usd(db, monkeypatch):
    from app.config import settings

    class Site:
        currency = "USD"
        quota_per_unit = 500000
        exchange_rate = 1.0

    class FakeNewApi:
        async def site_quota_model(self):
            return Site()

    monkeypatch.setattr(settings, "cny_per_usd", 7.3)
    await db.save_rule(dict(UPSTREAM_RULE))
    provisioner = FakeProvisioner()
    provisioner.newapi = FakeNewApi()  # type: ignore[attr-defined]
    worker, _ = await build(db, provisioner)

    await worker._on_frame(paid_frame())

    assert provisioner.calls[0][0] == pytest.approx(1.37)


async def test_upstream_failure_sends_nothing(db):
    await db.save_rule(dict(UPSTREAM_RULE))
    provisioner = FakeProvisioner(fail_with="new-api 建令牌失败：额度超上限")
    worker, _ = await build(db, provisioner)

    await worker._on_frame(paid_frame())

    assert worker.client.sent == []
    assert worker.client.confirmed == []
    orders = await db.list_orders()
    assert orders[0]["status"] == "upstream_failed"
    assert "建令牌失败" in orders[0]["detail"]
    # 金额是已经解析出来的，失败记录也要带上：否则页面上只显示「—」，
    # 事后看不出这单值多少钱、也就没法判断是不是金额算错导致的失败
    assert orders[0]["amount_cny"] == pytest.approx(10.0)
    assert orders[0]["quota_usd"] == pytest.approx(10.0)


async def test_upstream_requires_order_id(db):
    await db.save_rule(dict(UPSTREAM_RULE))
    provisioner = FakeProvisioner()
    worker, _ = await build(db, provisioner)

    payload = {
        "1": {
            "2": "47983389096@goofish",
            "10": {"reminderContent": "[我已付款，等待你发货]", "senderUserId": "4100000001"},
        }
    }
    data = base64.b64encode(msgpack.packb(payload, use_bin_type=True)).decode()
    frame = json.dumps({"headers": {}, "body": {"syncPushPackage": {"data": [{"data": data}]}}})
    await worker._on_frame(frame)

    # 没有订单号 → 鉴权直接拦下，既不建 Key 也不发消息
    assert provisioner.calls == []
    assert worker.client.sent == []
    # 也不落库：订单号为空时 ORDER 表没有去重键，落库等于让人刷屏写表
    assert await db.list_orders() == []


async def test_upstream_rejects_amount_over_limit(db, monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, "max_order_cny", 5.0)
    await db.save_rule(dict(UPSTREAM_RULE))
    provisioner = FakeProvisioner()
    worker, _ = await build(db, provisioner)
    await worker._on_frame(paid_frame())

    assert provisioner.calls == []
    orders = await db.list_orders()
    assert orders[0]["status"] == "upstream_failed"
    assert "上限" in orders[0]["detail"]


async def test_upstream_missing_order_data(db):
    await db.save_rule(dict(UPSTREAM_RULE))
    provisioner = FakeProvisioner()
    worker, _ = await build(db, provisioner, order=None)
    await worker._on_frame(paid_frame())

    assert provisioner.calls == []
    orders = await db.list_orders()
    assert orders[0]["status"] == "upstream_failed"
    assert "金额" in orders[0]["detail"]


async def test_same_order_not_provisioned_twice(db):
    await db.save_rule(dict(UPSTREAM_RULE))
    provisioner = FakeProvisioner()
    worker, _ = await build(db, provisioner)
    frame = paid_frame()
    await worker._on_frame(frame)
    await worker._on_frame(frame)
    assert len(provisioner.calls) == 1


async def test_send_failure_reuses_existing_card_instead_of_reprovisioning(db):
    """发送失败重试时必须复用上一轮的卡密，不能重新开 Key。

    旧实现每轮轮询都会重新 provision：一次网络抖动就留下一个孤儿令牌/卡密，
    持续失败时会不断烧额度。
    """
    from app.client import XianyuError

    await db.save_rule(dict(UPSTREAM_RULE))
    provisioner = FakeProvisioner()
    worker, _ = await build(db, provisioner)

    original = worker.client.send_text_safe
    state = {"fail": True}

    async def flaky(chat_id, to_id, text):
        if state["fail"]:
            raise XianyuError("network down")
        await original(chat_id, to_id, text)

    worker.client.send_text_safe = flaky

    await worker._on_frame(paid_frame())
    assert len(provisioner.calls) == 1
    assert (await db.list_orders())[0]["status"] == "send_failed"
    first_code = (await db.list_orders())[0]["card_code"]
    assert first_code

    state["fail"] = False
    await worker._on_frame(paid_frame())

    assert len(provisioner.calls) == 1, "重试不能重新建 Key/卡密"
    assert len(worker.client.sent) == 1
    assert first_code in worker.client.sent[0][2]
    orders = await db.list_orders()
    assert orders[0]["status"] == "delivered"
    assert orders[0]["card_code"] == first_code


async def test_dry_run_never_calls_upstream(db, monkeypatch):
    """DRY-RUN 下绝不能真的去 new-api 建 Key。"""
    from app.config import settings

    monkeypatch.setattr(settings, "dry_run", True)
    await db.save_rule(dict(UPSTREAM_RULE))
    provisioner = FakeProvisioner()
    worker, _ = await build(db, provisioner)

    await worker._on_frame(paid_frame())

    assert provisioner.calls == []
    assert worker.client.sent == []
    assert await db.list_orders() == []


async def test_fixed_usd_works_without_order_amount(db):
    """规则里填了固定额度时，查不到订单金额也能发货。"""
    await db.save_rule({**UPSTREAM_RULE, "fixed_usd": 5.0})
    provisioner = FakeProvisioner()
    worker, _ = await build(db, provisioner, order=None)

    await worker._on_frame(paid_frame())

    assert provisioner.calls and provisioner.calls[0][0] == pytest.approx(5.0)
    assert len(worker.client.sent) == 1
    orders = await db.list_orders()
    assert orders[0]["status"] == "delivered"
    assert orders[0]["quota_usd"] == pytest.approx(5.0)


async def test_failed_order_can_be_retried_after_config_fixed(db):
    """失败记录不能把订单永久锁死：修好配置后同一单还要能重发。"""
    await db.save_rule(dict(UPSTREAM_RULE))

    failing = FakeProvisioner(fail_with="卡密站挂了")
    worker, _ = await build(db, failing)
    await worker._on_frame(paid_frame())
    assert worker.client.sent == []
    assert (await db.list_orders())[0]["status"] == "upstream_failed"

    # 修好配置后，同一单再推一次应该能发出去（失败不会进冷却期，也不会占用记录）
    worker.engine = DeliveryEngine(db)
    worker.provisioner = FakeProvisioner()
    await worker._on_frame(paid_frame())

    assert len(worker.client.sent) == 1
    orders = await db.list_orders()
    # 同一订单只有一行，状态被更新成 delivered
    assert len(orders) == 1
    assert orders[0]["status"] == "delivered"


async def test_upstream_failure_does_not_consume_card_pool(db):
    """上游失败不能影响本地卡密池。"""
    await db.add_cards(["LOCAL-1"], "*")
    await db.save_rule(dict(UPSTREAM_RULE))
    worker, _ = await build(db, FakeProvisioner(fail_with="boom"))
    await worker._on_frame(paid_frame())
    cards = await db.list_cards()
    assert cards[0]["status"] == "available"
