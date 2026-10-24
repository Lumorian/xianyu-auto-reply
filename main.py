"""入口：启动 Web 管理后台，并让所有已启用账号开始监听私信。"""

from __future__ import annotations

import logging
import logging.handlers
import os
from pathlib import Path

import uvicorn

from app.config import BASE_DIR, settings
from app.web import create_app


def setup_logging(*, console: bool = True) -> None:
    """配置日志：同时输出到控制台和按大小轮转的文件。

    为什么要轮转：
      闲鱼每次连接会推上百条「会话引导」消息，排查期还会打开 XY_DEBUG_PAYLOAD，
      日志几小时就能涨到几十 MB。不做轮转的话，要么磁盘被吃满，
      要么你打开文件时它已经大到没法看。

    策略：单文件 10 MB，保留 5 份历史（server.log.1 ... server.log.5），
    加起来最多约 60 MB，超出自动丢弃最旧的。

    console=False 用于后台模式（./start.sh）：那时进程的 stdout/stderr 本身
    就被重定向进了同一个日志文件，再挂一个控制台 handler 会把每行写两遍。
    """
    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s | %(message)s")

    root = logging.getLogger()
    root.setLevel(level)
    # 重复调用时不叠加 handler（测试会多次 create_app）
    for handler in list(root.handlers):
        root.removeHandler(handler)

    if console:
        stream = logging.StreamHandler()
        stream.setFormatter(formatter)
        root.addHandler(stream)

    if settings.log_file:
        log_path = Path(settings.log_file)
        if not log_path.is_absolute():
            log_path = BASE_DIR / log_path
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            log_path,
            maxBytes=settings.log_max_bytes,
            backupCount=settings.log_backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    # httpx 每个请求都打一行 INFO，太吵，只在排查时开 DEBUG
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def main() -> None:
    # ./start.sh 会把这个置为 1：它的 stdout 已经指向日志文件了
    background = os.getenv("XY_BACKGROUND") == "1"
    setup_logging(console=not background)
    logger = logging.getLogger("app")
    # 用 logger 而不是 print：重定向到文件时 print 是块缓冲，进程被 kill 就丢了
    logger.info("管理后台: http://%s:%s/", settings.host, settings.port)
    if settings.log_file:
        logger.info(
            "日志: %s（%s MB 轮转，保留 %s 份）",
            settings.log_file,
            settings.log_max_bytes // 1024 // 1024,
            settings.log_backup_count,
        )
    uvicorn.run(
        create_app(),
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
        log_config=None,  # 让 uvicorn 用我们配好的 root logger，别自己再建一套
    )


if __name__ == "__main__":
    main()
