"""账号模式：卡密换账号、老用户直接充值到账号。"""

from __future__ import annotations

import base64
import json
from typing import Any

import msgpack
import pytest

from app.client import XianyuError
from app.codec import IncomingMessage
from app.db import Database
from app.engine import DeliveryEngine, render
from app.runner import AccountWorker
from app.upstream import AccountInfo, UpstreamError

BUYER_ID = "4100000001"
ORDER_ID = "2500000000000000001"
ITEM_ID = "900052644277"

ACCOUNT_RULE = {
    "name": "账号发货",
    "item_id": "*",
    "enabled": True,
    "mode": "account",
    "content": "",
    "auto_confirm": True,
    "priority": 0,
    "remark": "",
    "usd_multiplier": 1.0,
    "fixed_usd": 0.0,
    "account_prompt": "",
    "account_confirm": "",
    "account_card": "",
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


class FakeAccountProvisioner:
    """记录建号 / 加额度 / 出卡密；可以按需让某一步失败。"""

    def __init__(self, users: dict[str, dict[str, Any]] | None = None,
                 fail_create: Exception | None = None,
                 fail_topup: Exception | None = None,
                 fail_card: Exception | None = None) -> None:
        #: 账号名 -> 用户记录（模拟 new-api 里已经存在的账号）
        self.users = dict(users or {})
        self.created: list[tuple[str, str]] = []
        self.topups: list[tuple[str, float]] = []
        self.cards: list[tuple[str, float, str]] = []
        self.fail_create = fail_create
        self.fail_topup = fail_topup
        self.fail_card = fail_card
        self._next_id = 100

    async def create_account(self, username: str, password: str) -> AccountInfo:
        self.created.append((username, password))
        if self.fail_create is not None:
            raise self.fail_create
        self._next_id += 1
        user_id = str(self._next_id)
        self.users[username] = {"id": int(user_id), "username": username, "quota": 0}
        return AccountInfo(user_id=user_id, username=username, password=password,
                           balance=0.0, created=True)

    async def topup_account(self, user_id: str, quota_usd: float) -> AccountInfo:
        self.topups.append((user_id, quota_usd))
        if self.fail_topup is not None:
            raise self.fail_topup
        user = next((u for u in self.users.values() if str(u.get("id")) == str(user_id)), {})
        user["quota"] = float(user.get("quota") or 0) + quota_usd
        return AccountInfo(
            user_id=str(user_id), username=str(user.get("username") or ""),
            quota_usd=quota_usd, balance=user["quota"], currency="USD",
        )

    async def find_account(self, username: str) -> dict[str, Any] | None:
        return self.users.get(str(username or "").strip())

    async def find_account_by_id(self, user_id: str) -> dict[str, Any]:
        for user in self.users.values():
            if str(user.get("id")) == str(user_id):
                return user
        raise UpstreamError("没有这个账号")

    async def account_balance(self, user: dict[str, Any]) -> tuple[float | None, str]:
        return float(user.get("quota") or 0), "USD"

    async def issue_card(self, content: str, quota_usd: float, label: str) -> str:
        self.cards.append((content, quota_usd, label))
        if self.fail_card is not None:
            raise self.fail_card
        return "CARD-" + str(len(self.cards))


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
                "itemTitle": "会员月卡 30 天",
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


def buyer_text(content: str, *, order_id: str | None = None) -> IncomingMessage:
    return IncomingMessage(
        chat_id="47983389096",
        sender_id=BUYER_ID,
        sender_name="买家小王",
        content=content,
        item_id=ITEM_ID,
        order_id=order_id,
        is_system_card=False,
        order_id_trusted=False,
    )


async def build_worker(db: Database, provisioner: Any) -> tuple[AccountWorker, int]:
    account_id = await db.upsert_account("主号", "unb=2000000000002; _m_h5_tk=t_1")
    account = await db.get_account(account_id)
    worker = AccountWorker(db, account, DeliveryEngine(db), provisioner)
    worker.client = FakeClient()
    return worker, account_id


# ------------------------------------------------------------------ 引擎


async def test_engine_account_rule_becomes_account_action(db):
    await db.save_rule(dict(ACCOUNT_RULE))
    engine = DeliveryEngine(db)
    msg = IncomingMessage(
        chat_id="c", sender_id=BUYER_ID, sender_name="买家小王",
        content="[订单待发货]", item_id=ITEM_ID, order_id=ORDER_ID,
        order_id_trusted=True,
    )
    decision = await engine.decide_order(0, msg)

    assert decision.action == "account"
    assert decision.needs_account is True
    assert decision.needs_topup is False


def test_render_account_placeholders():
    text = render(
        "{username}|{password}|{user_id}|{login_url}|{card}|{quota}|{balance}",
        username="abc", password="pw123456", user_id="7",
        login_url="https://api.test", card="CARD-1", quota_usd=10.0, balance="$10.00",
    )
    assert text == "abc|pw123456|7|https://api.test|CARD-1|10.00|$10.00"


# ------------------------------------------------------------------ 新用户


async def test_new_user_flow_asks_then_opens_account_with_card(db):
    await db.save_rule(dict(ACCOUNT_RULE))
    provisioner = FakeAccountProvisioner()
    worker, account_id = await build_worker(db, provisioner)

    await worker._on_frame(paid_frame())

    # 第一步只问「有没有账号」，不建号、不充值
    assert len(worker.client.sent) == 1
    assert "有没有账号" in worker.client.sent[0][2] or "注册过账号" in worker.client.sent[0][2]
    assert provisioner.created == [] and provisioner.topups == []
    row = await db.get_order(account_id, ORDER_ID)
    assert row["status"] == "awaiting_account"

    # 买家回「没有」→ 问想要的账号名
    await worker._process_incoming(buyer_text("没有"))
    assert "账号名" in worker.client.sent[-1][2]
    row = await db.get_order(account_id, ORDER_ID)
    assert row["status"] == "awaiting_username"

    # 买家给了账号名 → 建号 + 加额度 + 出卡密
    await worker._process_incoming(buyer_text("xinbuyer01"))

    assert provisioner.created and provisioner.created[0][0] == "xinbuyer01"
    assert len(provisioner.created[0][1]) >= 8
    assert provisioner.topups and provisioner.topups[0][1] == pytest.approx(10.0)
    assert provisioner.cards, "应该建一张卡密"
    card_text = provisioner.cards[0][0]
    assert "xinbuyer01" in card_text
    assert "CARD-1" in worker.client.sent[-1][2]

    row = await db.get_order(account_id, ORDER_ID)
    assert row["status"] == "delivered"
    assert row["account_username"] == "xinbuyer01"
    assert row["account_user_id"] == str(provisioner.users["xinbuyer01"]["id"])

    binding = await db.get_buyer_account(BUYER_ID)
    assert binding is not None
    assert binding["username"] == "xinbuyer01"
    assert binding["topup_count"] == 1
    assert binding["password"] == provisioner.created[0][1]


async def test_saying_have_account_asks_for_name(db):
    await db.save_rule(dict(ACCOUNT_RULE))
    worker, account_id = await build_worker(db, FakeAccountProvisioner())
    await worker._on_frame(paid_frame())

    await worker._process_incoming(buyer_text("有的"))

    assert "账号名发给我" in worker.client.sent[-1][2]
    assert (await db.get_order(account_id, ORDER_ID))["status"] == "awaiting_account"


async def test_thanks_is_not_registered_as_username(db):
    """买家回一句「thanks」不能被当成账号名去开号。"""
    await db.save_rule(dict(ACCOUNT_RULE))
    provisioner = FakeAccountProvisioner()
    worker, _ = await build_worker(db, provisioner)
    await worker._on_frame(paid_frame())
    await worker._process_incoming(buyer_text("没有"))

    await worker._process_incoming(buyer_text("thanks"))

    assert provisioner.created == []
    assert "账号名" in worker.client.sent[-1][2]


async def test_username_taken_asks_for_another(db):
    await db.save_rule(dict(ACCOUNT_RULE))
    provisioner = FakeAccountProvisioner(users={"taken01": {"id": 9, "username": "taken01",
                                                            "quota": 0}})
    worker, account_id = await build_worker(db, provisioner)
    await worker._on_frame(paid_frame())
    await worker._process_incoming(buyer_text("没有"))

    await worker._process_incoming(buyer_text("taken01"))

    assert provisioner.created == []
    assert "已经有人注册" in worker.client.sent[-1][2]
    row = await db.get_order(account_id, ORDER_ID)
    assert row["status"] == "awaiting_username"


async def test_invalid_username_gets_hint(db):
    await db.save_rule(dict(ACCOUNT_RULE))
    worker, account_id = await build_worker(db, FakeAccountProvisioner())
    await worker._on_frame(paid_frame())
    await worker._process_incoming(buyer_text("没有"))

    await worker._process_incoming(buyer_text("a!"))  # 太短又不合法

    assert "账号名不能用" in worker.client.sent[-1][2]
    assert (await db.get_order(account_id, ORDER_ID))["status"] == "awaiting_username"


async def test_card_failure_still_delivers_credentials(db):
    """卡密站挂了也不能把这一单记成失败：账号和额度都已经开出去了。"""
    await db.save_rule(dict(ACCOUNT_RULE))
    provisioner = FakeAccountProvisioner(fail_card=UpstreamError("卡密站 502"))
    worker, account_id = await build_worker(db, provisioner)
    await worker._on_frame(paid_frame())
    await worker._process_incoming(buyer_text("没有"))
    await worker._process_incoming(buyer_text("cardfail01"))

    text = worker.client.sent[-1][2]
    assert "cardfail01" in text and "密码" in text
    row = await db.get_order(account_id, ORDER_ID)
    assert row["status"] == "delivered"
    # 只充了一次
    assert len(provisioner.topups) == 1


# ------------------------------------------------------------------ 老用户


async def test_existing_user_needs_confirm_then_tops_up(db):
    await db.save_rule(dict(ACCOUNT_RULE))
    provisioner = FakeAccountProvisioner(users={"oldbuyer": {"id": 7, "username": "oldbuyer",
                                                             "quota": 3.5}})
    worker, account_id = await build_worker(db, provisioner)
    await worker._on_frame(paid_frame())

    await worker._process_incoming(buyer_text("oldbuyer"))

    confirm_text = worker.client.sent[-1][2]
    assert "oldbuyer" in confirm_text
    assert "3.50" in confirm_text, "确认前要回显余额"
    assert provisioner.topups == []
    row = await db.get_order(account_id, ORDER_ID)
    assert row["status"] == "awaiting_confirm"
    assert row["account_user_id"] == "7"

    await worker._process_incoming(buyer_text("确认"))

    assert provisioner.topups == [("7", pytest.approx(10.0))]
    row = await db.get_order(account_id, ORDER_ID)
    assert row["status"] == "delivered"
    assert "oldbuyer" in worker.client.sent[-1][2]
    binding = await db.get_buyer_account(BUYER_ID)
    assert binding["user_id"] == "7"
    assert binding["topup_count"] == 1


async def test_existing_account_not_found_keeps_waiting(db):
    await db.save_rule(dict(ACCOUNT_RULE))
    worker, account_id = await build_worker(db, FakeAccountProvisioner())
    await worker._on_frame(paid_frame())

    await worker._process_incoming(buyer_text("nobody99"))

    assert "没有找到账号" in worker.client.sent[-1][2]
    assert (await db.get_order(account_id, ORDER_ID))["status"] == "awaiting_account"


async def test_second_reply_does_not_double_topup(db):
    await db.save_rule(dict(ACCOUNT_RULE))
    provisioner = FakeAccountProvisioner(users={"oldbuyer": {"id": 7, "username": "oldbuyer",
                                                             "quota": 0}})
    worker, _ = await build_worker(db, provisioner)
    await worker._on_frame(paid_frame())
    await worker._process_incoming(buyer_text("oldbuyer"))
    await worker._process_incoming(buyer_text("确认"))

    assert len(provisioner.topups) == 1
    # 买家又发了一遍「确认」：已经发过货，不能再充一次
    await worker._process_incoming(buyer_text("确认"))
    assert len(provisioner.topups) == 1


async def test_bound_buyer_skips_questions_on_next_order(db):
    await db.save_rule(dict(ACCOUNT_RULE))
    provisioner = FakeAccountProvisioner(users={"oldbuyer": {"id": 7, "username": "oldbuyer",
                                                             "quota": 0}})
    worker, account_id = await build_worker(db, provisioner)
    await db.save_buyer_account({
        "buyer_id": BUYER_ID, "buyer_name": "买家小王", "user_id": "7",
        "username": "oldbuyer", "password": "pw12345678", "remark": "",
    }, None)

    await worker._on_frame(paid_frame("2500000000000000002"))

    assert provisioner.topups == [("7", pytest.approx(10.0))]
    assert len(worker.client.sent) == 1
    assert "oldbuyer" in worker.client.sent[0][2]
    row = await db.get_order(account_id, "2500000000000000002")
    assert row["status"] == "delivered"


async def test_admin_account_cannot_be_claimed(db):
    """买家把 "admin" 当账号名发过来时，绝不能给它充值。"""
    await db.save_rule(dict(ACCOUNT_RULE))
    provisioner = FakeAccountProvisioner(users={
        "admin": {"id": 1, "username": "admin", "quota": 0, "role": 10},
    })
    worker, account_id = await build_worker(db, provisioner)
    await worker._on_frame(paid_frame())

    await worker._process_incoming(buyer_text("admin"))

    assert provisioner.topups == []
    assert "不能自动充值" in worker.client.sent[-1][2]
    assert (await db.get_order(account_id, ORDER_ID))["status"] == "awaiting_account"


async def test_confirm_targets_the_right_pending_order(db):
    """买家有老单在等「有没有账号」、新单在等确认时，「确认」要作用在新单上。"""
    await db.save_rule(dict(ACCOUNT_RULE))
    provisioner = FakeAccountProvisioner(users={"oldbuyer": {"id": 7, "username": "oldbuyer",
                                                             "quota": 0}})
    worker, account_id = await build_worker(db, provisioner)
    # 老单：还在问有没有账号
    await db.record_order(
        account_id=account_id, order_id="2500000000000000001", item_id=ITEM_ID,
        buyer_id=BUYER_ID, buyer_name="买家小王", rule_id=1, card_id=None, content="",
        status="awaiting_account", detail="", amount_cny=10.0, quota_usd=10.0,
    )
    # 新单：已经回显完账号，等确认
    await db.record_order(
        account_id=account_id, order_id="2500000000000000002", item_id=ITEM_ID,
        buyer_id=BUYER_ID, buyer_name="买家小王", rule_id=1, card_id=None, content="",
        status="awaiting_confirm", detail="", amount_cny=10.0, quota_usd=10.0,
        account_user_id="7", account_username="oldbuyer",
    )

    await worker._process_incoming(buyer_text("确认"))

    assert provisioner.topups == [("7", pytest.approx(10.0))]
    assert (await db.get_order(account_id, "2500000000000000002"))["status"] == "delivered"
    # 老单没被顺手做了，仍在等买家回「有没有账号」
    assert (await db.get_order(account_id, "2500000000000000001"))["status"] == "awaiting_account"


async def test_unrelated_message_is_not_treated_as_username(db):
    await db.save_rule(dict(ACCOUNT_RULE))
    worker, account_id = await build_worker(db, FakeAccountProvisioner())
    await worker._on_frame(paid_frame())
    sent_before = len(worker.client.sent)

    await worker._process_incoming(buyer_text("什么时候发货？"))

    assert len(worker.client.sent) == sent_before
    assert (await db.get_order(account_id, ORDER_ID))["status"] == "awaiting_account"


async def test_account_retry_is_idempotent_when_send_fails(db):
    """账号已经开好、额度已经加上，只是消息没发出去 —— 重试不能再充一次。"""
    await db.save_rule(dict(ACCOUNT_RULE))
    provisioner = FakeAccountProvisioner(users={"oldbuyer": {"id": 7, "username": "oldbuyer",
                                                             "quota": 0}})
    worker, _ = await build_worker(db, provisioner)
    await db.save_buyer_account({
        "buyer_id": BUYER_ID, "buyer_name": "买家小王", "user_id": "7",
        "username": "oldbuyer", "password": "", "remark": "",
    }, None)

    class Boom(FakeClient):
        async def send_text_safe(self, chat_id: str, to_id: str, text: str) -> None:
            raise XianyuError("ws closed")

    worker.client = Boom()
    await worker._on_frame(paid_frame())
    assert len(provisioner.topups) == 1

    # 换成正常客户端，同一单再来一次：应该复用上次准备好的内容
    worker.client = FakeClient()
    await worker._on_frame(paid_frame())
    assert len(provisioner.topups) == 1
    assert len(worker.client.sent) == 1
