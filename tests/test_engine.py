"""发货决策引擎的单元测试。"""

from __future__ import annotations

from app.codec import IncomingMessage
from app.engine import DeliveryEngine, pick_rule, render


def make_message(**overrides):
    """默认造一条「系统交易卡片」：真实链路上付款信息就是这么来的。

    要造「买家自己在聊天框打的字」，传 is_system_card=False。
    要造「卡片形态但订单号是从正文里捞出来的」，传 order_id_trusted=False
    —— 那种必须去核实订单，不能直接发。
    """
    data = {
        "chat_id": "47983389096",
        "sender_id": "4100000001",
        "sender_name": "买家小王",
        "content": "[我已付款，等待你发货]",
        "item_id": "900052644277",
        "order_id": "2500000000000000001",
        "is_system_card": True,
        # 真卡片的订单号来自 dxCard / reminderUrl 这类服务端字段
        "order_id_trusted": True,
    }
    data.update(overrides)
    return IncomingMessage(**data)


def test_pick_rule_prefers_item_specific():
    rules = [
        {"id": 1, "item_id": "*", "priority": 5},
        {"id": 2, "item_id": "900052644277", "priority": 0},
    ]
    assert pick_rule(rules, "900052644277")["id"] == 2
    assert pick_rule(rules, "123")["id"] == 1
    assert pick_rule([], "123") is None


def test_render_substitutes_variables():
    out = render("卡密：{card}\n订单：{order_id}\n买家：{buyer}",
                 buyer_name="小王", order_id="123", card="ABC")
    assert "ABC" in out and "123" in out and "小王" in out


async def test_decide_skips_non_trigger(db):
    engine = DeliveryEngine(db)
    decision = await engine.decide(1, make_message(content="在吗？"))
    assert not decision.should_deliver
    assert "非发货触发" in decision.reason


async def test_decide_skips_without_rules(db):
    engine = DeliveryEngine(db)
    decision = await engine.decide(1, make_message())
    assert not decision.should_deliver
    assert "规则" in decision.reason


async def test_decide_delivers_card(db):
    await db.save_rule({
        "name": "月卡", "item_id": "900052644277", "enabled": True, "mode": "card",
        "content": "你好{buyer}，卡密：{card}", "auto_confirm": True, "priority": 0, "remark": "",
    })
    await db.add_cards(["CARD-0001", "CARD-0002"], "900052644277")

    engine = DeliveryEngine(db)
    decision = await engine.decide(1, make_message())
    assert decision.should_deliver
    assert "CARD-0001" in decision.content
    assert decision.auto_confirm is True

    # 卡密出库后应被标记为已售，再次判断不会再发同一张
    cards = {c["content"]: c for c in await db.list_cards()}
    assert cards["CARD-0001"]["status"] == "sold"
    assert cards["CARD-0001"]["order_id"] == "2500000000000000001"
    assert cards["CARD-0002"]["status"] == "available"

    again = await engine.decide(1, make_message(order_id="999"))
    assert "CARD-0002" in again.content


async def test_decide_skips_when_card_stock_empty(db):
    await db.save_rule({
        "name": "月卡", "item_id": "*", "enabled": True, "mode": "card",
        "content": "卡密：{card}", "auto_confirm": False, "priority": 0, "remark": "",
    })
    engine = DeliveryEngine(db)
    decision = await engine.decide(1, make_message())
    assert not decision.should_deliver
    assert "库存" in decision.reason


async def test_decide_deduplicates_by_order(db):
    await db.save_rule({
        "name": "文案", "item_id": "*", "enabled": True, "mode": "text",
        "content": "已发货", "auto_confirm": False, "priority": 0, "remark": "",
    })
    await db.record_order(
        account_id=1, order_id="2500000000000000001", item_id="900052644277",
        buyer_id="4100000001", buyer_name="小王", rule_id=1, card_id=None,
        content="已发货", status="delivered", detail="",
    )
    engine = DeliveryEngine(db)
    decision = await engine.decide(1, make_message())
    assert not decision.should_deliver
    assert "已发过货" in decision.reason


async def test_typed_paid_text_from_buyer_does_not_deliver(db):
    """买家自己在聊天框打下「我已付款」不能触发发货。

    这是最要命的一条：正文匹配是任何人都能伪造的，
    不核订单等于把卡密池和上游 Key 白送。
    """
    await db.save_rule({
        "name": "月卡", "item_id": "*", "enabled": True, "mode": "card",
        "content": "卡密：{card}", "auto_confirm": False, "priority": 0, "remark": "",
    })
    await db.add_cards(["CARD-0001"], "*")

    engine = DeliveryEngine(db)
    # 带订单号但没核实过：被鉴权条文拦下
    typed = await engine.decide(1, make_message(is_system_card=False, order_id="2500000000000000001"))
    assert not typed.should_deliver
    assert "核实" in typed.reason
    # 连订单号都没有：更早一步就被拦
    no_order = await engine.decide(1, make_message(is_system_card=False, order_id=None))
    assert not no_order.should_deliver
    assert "订单号" in no_order.reason

    # 卡密必须一张都没动
    cards = await db.list_cards()
    assert cards[0]["status"] == "available"


async def test_verified_text_message_can_deliver(db):
    """真人文本 + runner 已经核实过订单 → 允许发货。"""
    await db.save_rule({
        "name": "月卡", "item_id": "*", "enabled": True, "mode": "card",
        "content": "卡密：{card}", "auto_confirm": False, "priority": 0, "remark": "",
    })
    await db.add_cards(["CARD-0001"], "*")

    engine = DeliveryEngine(db)
    decision = await engine.decide(1, make_message(is_system_card=False, order_verified=True))
    assert decision.should_deliver
    assert "CARD-0001" in decision.content


async def test_card_without_order_id_does_not_deliver(db):
    """连系统卡片也要有订单号：没法去重、没法核归属的单宁可漏发。"""
    await db.save_rule({
        "name": "月卡", "item_id": "*", "enabled": True, "mode": "card",
        "content": "卡密：{card}", "auto_confirm": False, "priority": 0, "remark": "",
    })
    await db.add_cards(["CARD-0001"], "*")

    engine = DeliveryEngine(db)
    decision = await engine.decide(1, make_message(order_id=None))
    assert not decision.should_deliver
    assert "订单号" in decision.reason
    assert (await db.list_cards())[0]["status"] == "available"


async def test_ignored_system_message(db):
    engine = DeliveryEngine(db)
    decision = await engine.decide(1, make_message(content="[我已拍下，待付款]"))
    assert not decision.should_deliver
