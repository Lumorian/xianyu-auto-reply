"""日志配置：轮转与重复初始化。

踩过的坑：之前用 nohup 重定向 + basicConfig，日志只往一个文件里灌。
闲鱼一次连接就推上百条会话引导，排查时还会开 XY_DEBUG_PAYLOAD，
几小时就能涨到几十 MB，打开都卡。现在改成按大小轮转。
"""

from __future__ import annotations

import logging

import pytest

import main as entry
from app.config import settings


@pytest.fixture()
def temp_log(tmp_path, monkeypatch):
    log_path = tmp_path / "server.log"
    monkeypatch.setattr(settings, "log_file", str(log_path))
    monkeypatch.setattr(settings, "log_max_bytes", 2000)
    monkeypatch.setattr(settings, "log_backup_count", 3)
    yield log_path
    # 清掉测试装的 handler，避免影响其它用例
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
        try:
            handler.close()
        except Exception:  # noqa: S110 - 这里就是要忽略 handler 的异常
            pass


def test_log_file_is_created(temp_log):
    entry.setup_logging()
    logging.getLogger("测试").info("hello")
    assert temp_log.exists()
    assert "hello" in temp_log.read_text(encoding="utf-8")


def test_log_rotates_and_caps_backups(temp_log):
    entry.setup_logging()
    log = logging.getLogger("刷日志")
    for i in range(300):
        log.info("第 %03d 条，填充内容 abcdefghijklmnopqrstuvwxyz", i)

    files = sorted(p.name for p in temp_log.parent.glob("server.log*"))
    assert len(files) > 1, "应该发生轮转"
    # backupCount=3 → 当前文件 + 3 份历史
    assert len(files) <= 4, f"份数应受控，实际 {files}"


def test_setup_is_idempotent(temp_log):
    """重复调用不能叠加 handler，否则每条日志写两遍。"""
    entry.setup_logging()
    entry.setup_logging()
    entry.setup_logging()

    root = logging.getLogger()
    file_handlers = [h for h in root.handlers if isinstance(h, logging.FileHandler)]
    assert len(file_handlers) == 1


def test_httpx_is_quiet(temp_log):
    """httpx 每个请求一行 INFO，会淹没有用信息。"""
    entry.setup_logging()
    assert logging.getLogger("httpx").level >= logging.WARNING


def test_empty_log_file_means_console_only(monkeypatch):
    monkeypatch.setattr(settings, "log_file", "")
    entry.setup_logging()
    root = logging.getLogger()
    file_handlers = [h for h in root.handlers if isinstance(h, logging.FileHandler)]
    assert file_handlers == []
    for handler in list(root.handlers):
        root.removeHandler(handler)



def test_debug_payload_redacts_buyer_pii():
    """debug dump 只保留结构：昵称、头像、买家/订单 ID 都要打码。"""
    from app.runner import redact_payload

    redacted = redact_payload({
        "senderUserId": "4100000001",
        "buyerInfoVO": {
            "userNick": "买家小王",
            "avatar": "https://img.example/a.jpg",
        },
        "orderId": "2500000000000000001",
        "content": "我的 Key 是 sk-abcdefghijklmnopqrstuvwxyz",
    })

    assert redacted["senderUserId"] == "***"
    assert redacted["buyerInfoVO"]["userNick"] == "***"
    assert redacted["buyerInfoVO"]["avatar"] == "***"
    assert redacted["orderId"] == "***"
    assert "abcdefghijklmnopqrstuvwxyz" not in redacted["content"]
