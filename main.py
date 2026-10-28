"""入口：启动 Web 管理后台，并让所有已启用账号开始监听私信。"""

from __future__ import annotations

import logging
import logging.handlers
import os
from pathlib import Path

import uvicorn

from app.config import BASE_DIR, proxy_env_vars, settings
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
        try:
            # ctl.py/start.sh 可能在父进程里先创建了日志文件（umask 644）；
            # 日志里有买家消息，收一下权限。轮转后的新文件会继承本进程 umask。
            log_path.chmod(0o600)
        except OSError:
            pass
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
    # 后台页面每 5 秒轮询一次 /api/status 等接口，access log 能占满整个文件：
    # 实测 10 MB 只够跑两小时，真正要看的「保活/过期」全被挤进轮转文件里。
    # 只留 4xx/5xx（uvicorn 在 WARNING 级别记录异常请求）。
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)


#: 只监听这些地址时，管理接口的「无口令本机兜底」才成立。
LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1", "[::1]"})


def validate_admin_binding(host: str, token: str) -> str | None:
    """监听非回环地址却没配管理口令时，返回拒绝启动的理由。

    同机反向代理会让所有远程请求的 peer 都变成 127.0.0.1，从而被
    _is_loopback() 当成「本机访问」。如果这里再没有口令，后台等于对公网敞开。
    与其在日志里警告一句，不如直接不启动 —— 这类配置错误没人会主动去查日志。
    """
    if token:
        return None
    if host in LOOPBACK_HOSTS or host.startswith("127."):
        return None
    return (
        f"XY_HOST={host} 监听非回环地址，但没有配置 XY_ADMIN_TOKEN。"
        "同机反向代理会把远程请求变成「本机访问」，管理后台将没有口令保护。"
        "请设置 XY_ADMIN_TOKEN，或把 XY_HOST 改回 127.0.0.1。"
    )


def uvicorn_run_kwargs() -> dict[str, object]:
    """uvicorn.run 的参数。

    单独拎出来是为了能被测试盯住：proxy_headers 一旦回到默认的 True，
    「没配口令只允许本机」这道闸就能被一个 X-Forwarded-For 头绕过。
    """
    return {
        "host": settings.host,
        "port": settings.port,
        "log_level": settings.log_level.lower(),
        "log_config": None,  # 让 uvicorn 用我们配好的 root logger，别自己再建一套
        # 不能相信 X-Forwarded-For：默认 True 时，只要前面挂一台同机反代，
        # 客户端自己发一个 X-Forwarded-For: 127.0.0.1 就会被当成「本机访问」。
        # 管理接口的鉴权不能建立在一个客户端可写的请求头上面。
        "proxy_headers": False,
    }


def main() -> None:
    # data/ 下有 Cookie、卡密、Key，logs/ 里有买家消息：新文件默认只给属主读。
    # 已经在跑的旧文件仍建议手工 chmod 600（DB 的 init 会顺带修正自己）。
    os.umask(0o077)
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
    # 闲鱼对登录 IP 很敏感：挂上代理后出口 IP 一变，服务端就会把长连接踢掉，
    # 后台看起来就是「账号掉线」。所以所有出站请求都固定直连，不跟随代理环境变量；
    # 操作者临时开代理上外网时，不该把正在接单的机器人一起带走。
    logger.info("出站连接：一律直连（忽略 HTTP_PROXY / HTTPS_PROXY / ALL_PROXY / WS_PROXY）")
    proxy_vars = proxy_env_vars()
    if proxy_vars:
        logger.warning(
            "检测到代理环境变量 %s：已忽略，全部直连。若账号仍然掉线，"
            "多半是代理客户端的 TUN / 全局模式在接管流量，见 README「代理 / TUN」一节",
            ", ".join(proxy_vars),
        )

    if settings.admin_allow_remote and not settings.admin_token:
        # 这个开关当前不会放宽「无口令访问」：没配 token 时远程请求仍会 401。
        # 保留提示是怕有人以为自己已经放开了，实际远程根本进不来。
        logger.warning(
            "XY_ADMIN_ALLOW_REMOTE=1 但没配 XY_ADMIN_TOKEN：该开关不会放宽无口令访问，"
            "远程请求仍会被 401。请设置 XY_ADMIN_TOKEN。"
        )

    binding_error = validate_admin_binding(settings.host, settings.admin_token)
    if binding_error:
        logger.error(binding_error)
        raise SystemExit(1)

    uvicorn.run(create_app(), **uvicorn_run_kwargs())  # type: ignore[arg-type]


if __name__ == "__main__":
    main()
