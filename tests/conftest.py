"""测试公共夹具。"""

from __future__ import annotations

import asyncio

import pytest

from app.db import Database


@pytest.fixture()
def db(tmp_path):
    """临时 SQLite 数据库，初始化好表结构后交给测试。"""
    database = Database(tmp_path / "test.db")
    asyncio.run(database.init())
    return database
