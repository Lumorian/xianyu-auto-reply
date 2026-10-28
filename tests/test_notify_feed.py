"""新消息 / 新订单提示依赖的两个约定。

1. `/api/messages` 会给每条流水标 `system` —— 系统提示、交易事件不是买家发言，
   前端弹「新消息」时要跳过它们。判断留在后端，免得那份关键词表在 Python 和
   JS 里各维护一份、迟早对不上。
2. 前端靠自增 id 判断「哪些是这一轮新出现的」，所以流水接口必须原样带 id。
"""

from __future__ import annotations

import httpx
import pytest

from app.db import Database
from app.web import create_app


class StubManager:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def sync(self) -> None:
        pass

    async def stop_all(self) -> None:
        pass

    def status(self) -> list[dict[str, object]]:
        return []


@pytest.fixture()
async def feed_client(tmp_path):
    db = Database(tmp_path / "notify.db")
    app = create_app(db=db, manager=StubManager(db))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1") as ac:
        async with app.router.lifespan_context(app):
            yield ac, db


async def test_messages_api_marks_system_messages(feed_client):
    client, db = feed_client
    account_id = await db.upsert_account("主号", "cookie")
    for content, message_type in (
        ("在吗？", "text"),
        ("[你已发货]", "text"),
        ("快给ta一个评价吧~", "text"),
        ("[图片]", "image"),
    ):
        await db.log_message(
            account_id=account_id, chat_id="c1", sender_id="b1",
            sender_name="买家小王", content=content, kind="incoming",
            message_type=message_type,
        )

    rows = (await client.get("/api/messages?limit=10")).json()
    by_content = {row["content"]: row for row in rows}

    # 买家真人发言 + 图片占位都要提示；系统卡片 / 交易事件不提示
    assert by_content["在吗？"]["system"] is False
    assert by_content["[图片]"]["system"] is False
    assert by_content["[你已发货]"]["system"] is True
    assert by_content["快给ta一个评价吧~"]["system"] is True
    # 前端靠 id 前后对比找「新消息」，不能被顺手抹掉
    assert all(isinstance(row["id"], int) for row in rows)
