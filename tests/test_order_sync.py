"""从闲鱼同步历史订单：分页解析、入库去重、Worker 与 API。"""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from app.client import XianyuClient, XianyuError
from app.db import Database
from app.engine import DeliveryEngine
from app.runner import AccountWorker
from app.web import create_app


def sold_item(
    order_id: str,
    *,
    status: str = "交易成功",
    title: str = "deepseek 月卡",
    amount: str = "12.34",
    paid_at: str = "2026-10-29 10:00:00",
) -> dict[str, Any]:
    """构造一条和线上 mtop 回包同形状的卖家订单。"""
    return {
        "commonData": {
            "orderId": int(order_id),
            "itemId": 900052644277,
            "orderStatus": status,
            "sellerRateStatus": 5,
            "createTime": "2026-10-29 09:59:58",
            "paySuccessTime": paid_at,
            "consignTime": "2026-10-29 10:01:00",
            "finishTime": None,
            "inRefund": False,
        },
        "buyerInfoVO": {
            "buyerId": 4100000001,
            "userNick": "买家小王",
            "name": "买家小王",
            "phone": "13800000000",
            "address": "不该落库的地址",
        },
        "priceVO": {"totalPrice": amount},
        "itemVO": {"title": title},
        "rightVO": {},
    }


def flat_item(order_id: str, **kwargs: Any) -> dict[str, Any]:
    """Worker 入口消费的是 `_parse_sold_order` 摊平后的订单，不是原始回包。"""
    item = sold_item(order_id, **kwargs)
    common = item["commonData"]
    buyer = item["buyerInfoVO"]
    return {
        "order_id": str(common["orderId"]),
        "item_id": str(common["itemId"]),
        "order_status": common["orderStatus"],
        "buyer_id": str(buyer["buyerId"]),
        "buyer_name": buyer["userNick"],
        "item_title": item["itemVO"]["title"],
        "paid_at": common["paySuccessTime"],
        "created_at": common["createTime"],
        "in_refund": common["inRefund"],
        "raw": item,
    }


async def test_sold_orders_paginate_and_parse_fields():
    """pageNumber 1 起、nextPage 为 true 时继续翻；金额/标题/时间都要摊平。"""
    client = XianyuClient("unb=2000000000002; _m_h5_tk=token_1")
    calls: list[tuple[int, int]] = []

    async def fake(api: str, data: dict[str, Any], version: str = "1.0") -> dict[str, Any]:
        assert api == "mtop.taobao.idle.trade.merchant.sold.get"
        calls.append((data["pageNumber"], data["rowsPerPage"]))
        if data["pageNumber"] == 1:
            return {
                "ret": ["SUCCESS::调用成功"],
                "data": {"module": {
                    "items": [
                        sold_item("3316547750028090561"),
                        sold_item("3316547750028090562", status="已发货", amount="1.00"),
                    ],
                    "nextPage": "true",
                    "totalCount": 3,
                }},
            }
        return {
            "ret": ["SUCCESS::调用成功"],
            "data": {"module": {
                "items": [sold_item("3316547750028090563", title="deepseek v4.1 flash")],
                "nextPage": 0,
                "totalCount": 3,
            }},
        }

    client.mtop = fake  # type: ignore[method-assign]
    orders = await client.list_all_sold_orders(max_orders=10, page_size=50)

    assert calls == [(1, 50), (2, 50)]
    assert [o["order_id"] for o in orders] == [
        "3316547750028090561",
        "3316547750028090562",
        "3316547750028090563",
    ]
    first = orders[0]
    assert first["item_id"] == "900052644277"
    assert first["buyer_id"] == "4100000001"
    assert first["buyer_name"] == "买家小王"
    assert first["item_title"] == "deepseek 月卡"
    assert first["amount"] == "12.34"
    assert first["paid_at"] == "2026-10-29 10:00:00"
    assert first["created_at"] == "2026-10-29 09:59:58"
    assert first["in_refund"] is False
    assert first["raw"]["commonData"]["orderStatus"] == "交易成功"


async def test_sold_orders_page_wraps_network_error_as_xianyu_error():
    """网络超时要变成业务异常，API 才能返回可读的 502，而不是裸 500。"""
    client = XianyuClient("unb=2000000000002; _m_h5_tk=token_1")

    async def boom(api: str, data: dict[str, Any], version: str = "1.0") -> dict[str, Any]:
        raise httpx.ConnectTimeout("连接超时")

    client.mtop = boom  # type: ignore[method-assign]
    with pytest.raises(XianyuError, match="网络异常"):
        await client.list_sold_orders_page(1, 50)


async def test_recent_sold_orders_still_swallows_generic_error():
    """评价轮询用的旧入口不能因为列表接口抖动就把后台循环打挂。"""
    client = XianyuClient("unb=2000000000002; _m_h5_tk=token_1")

    async def boom(api: str, data: dict[str, Any], version: str = "1.0") -> dict[str, Any]:
        raise XianyuError("列表接口暂时不可用")

    client.mtop = boom  # type: ignore[method-assign]
    assert await client.list_sold_orders() == []


async def test_import_orders_only_inserts_and_never_overwrites(db):
    """历史同步只能补缺，不能把本机 delivered 的明细改写成 synced。"""
    first_id = await db.record_order(
        account_id=1,
        order_id="3316547750028090561",
        item_id="old-item",
        buyer_name="老买家",
        amount_cny=1.0,
        status="delivered",
        detail="本机发货明细",
        created_at="2026-01-01 00:00:00",
    )

    inserted, skipped = await db.import_orders([
        {
            "account_id": 1,
            "order_id": "3316547750028090561",
            "item_id": "remote-item",
            "buyer_name": "远端买家",
            "status": "synced",
            "detail": "远端详情",
            "created_at": "2026-02-02 00:00:00",
        },
        {
            "account_id": 1,
            "order_id": "3316547750028090562",
            "item_id": "new-item",
            "buyer_name": "新买家",
            "amount_cny": 12.34,
            "status": "synced",
            "detail": "闲鱼状态：已发货；仅同步订单记录",
            "created_at": "2026-03-03 00:00:00",
        },
        {"account_id": 1, "order_id": "", "status": "synced"},
        {"account_id": 1, "status": "synced"},
        "not a dict",
    ])

    assert (inserted, skipped) == (1, 4)
    rows = {row["order_id"]: row for row in await db.list_orders()}
    old = rows["3316547750028090561"]
    assert old["id"] == first_id
    assert old["status"] == "delivered"
    assert old["item_id"] == "old-item"
    assert old["buyer_name"] == "老买家"
    assert old["detail"] == "本机发货明细"
    assert old["created_at"] == "2026-01-01 00:00:00"
    new = rows["3316547750028090562"]
    assert new["status"] == "synced"
    assert new["amount_cny"] == 12.34
    assert new["created_at"] == "2026-03-03 00:00:00"


class HistoryClient:
    user_id = "2000000000002"
    websocket = object()

    def __init__(self, orders: list[dict[str, Any]]) -> None:
        self.orders = orders
        self.max_orders = 0

    async def list_all_sold_orders(self, *, max_orders: int = 2000):
        self.max_orders = max_orders
        return self.orders


async def test_worker_sync_imports_history_and_keeps_local_delivery(db):
    account_id = await db.upsert_account("主号", "unb=2000000000002; _m_h5_tk=token_1")
    account = await db.get_account(account_id)
    worker = AccountWorker(db, account, DeliveryEngine(db))

    local_id = await db.record_order(
        account_id=account_id,
        order_id="3316547750028090561",
        status="delivered",
        detail="本机发货成功",
    )
    client = HistoryClient([
        flat_item("3316547750028090561", title="远端标题", amount="99.00"),
        flat_item(
            "3316547750028090562",
            status="已发货",
            title="deepseek v4.1 flash",
            amount="12.34",
            paid_at="2026-10-29 11:37:00",
        ),
    ])
    worker.client = client  # type: ignore[assignment]

    result = await worker.sync_order_history(max_orders=50)

    assert client.max_orders == 50
    assert result == {
        "account_id": account_id,
        "account_name": "主号",
        "fetched": 2,
        "imported": 1,
        "skipped": 1,
        "ignored": 0,
    }
    rows = {row["order_id"]: row for row in await db.list_orders()}
    local = rows["3316547750028090561"]
    assert local["id"] == local_id
    assert local["status"] == "delivered"
    assert local["detail"] == "本机发货成功"
    synced = rows["3316547750028090562"]
    assert synced["status"] == "synced"
    assert synced["amount_cny"] == 12.34
    assert synced["created_at"] == "2026-10-29 11:37:00"
    assert "闲鱼状态：已发货" in synced["detail"]
    assert "商品：deepseek v4.1 flash" in synced["detail"]
    assert "仅同步订单记录，本机无发货明细" in synced["detail"]



async def test_worker_sync_skips_unpaid_and_closed_orders(db):
    """待付款/交易关闭不是成交，不能导入成 synced 把销售额撑大。"""
    account_id = await db.upsert_account("主号", "unb=2000000000002; _m_h5_tk=token_1")
    account = await db.get_account(account_id)
    worker = AccountWorker(db, account, DeliveryEngine(db))
    worker.client = HistoryClient([  # type: ignore[assignment]
        flat_item("3316547750028090571", status="待付款", title="未付款商品", paid_at=""),
        flat_item("3316547750028090572", status="交易关闭", title="已关闭商品"),
        flat_item("3316547750028090573", status="已发货", title="正常商品", amount="12.34"),
    ])

    result = await worker.sync_order_history(max_orders=50)

    assert result == {
        "account_id": account_id,
        "account_name": "主号",
        "fetched": 3,
        "imported": 1,
        "skipped": 0,
        "ignored": 2,
    }
    rows = {row["order_id"]: row for row in await db.list_orders()}
    assert set(rows) == {"3316547750028090573"}
    assert rows["3316547750028090573"]["status"] == "synced"


class SyncStub:
    def __init__(self, result: dict[str, Any] | None = None, error: Exception | None = None) -> None:
        self.result = result or {
            "accounts": [], "fetched": 0, "imported": 0, "skipped": 0,
            "ignored": 0, "errors": [],
        }
        self.error = error
        self.account_ids: list[int | None] = []

    async def sync(self) -> None:
        pass

    async def stop_all(self) -> None:
        pass

    def status(self) -> list[dict[str, Any]]:
        return []

    async def sync_order_history(self, account_id: int | None = None) -> dict[str, Any]:
        self.account_ids.append(account_id)
        if self.error is not None:
            raise self.error
        return self.result


@pytest.fixture()
async def sync_api(tmp_path):
    db = Database(tmp_path / "sync-web.db")
    stub = SyncStub({
        "accounts": [{"account_id": 7, "account_name": "主号", "fetched": 3, "imported": 2, "skipped": 1, "ignored": 0}],
        "fetched": 3,
        "imported": 2,
        "skipped": 1,
        "ignored": 0,
        "errors": [],
    })
    app = create_app(db=db, manager=stub)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1") as ac:
        async with app.router.lifespan_context(app):
            yield ac, stub


async def test_api_orders_sync_returns_counts(sync_api):
    ac, stub = sync_api
    res = await ac.post("/api/orders/sync?account_id=7")
    assert res.status_code == 200
    assert res.json()["imported"] == 2
    assert res.json()["ignored"] == 0
    assert res.json()["accounts"][0]["account_id"] == 7
    assert stub.account_ids == [7]


async def test_api_orders_sync_maps_not_online_to_409(sync_api):
    ac, stub = sync_api
    stub.error = LookupError("没有在线账号，先等账号连接成功再同步")
    res = await ac.post("/api/orders/sync")
    assert res.status_code == 409
    assert "在线账号" in res.json()["detail"]
