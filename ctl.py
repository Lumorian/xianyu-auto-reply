#!/usr/bin/env python3
"""启停逻辑的内部实现 —— 平时请用 ./start.sh，别直接调这个文件。

    ./start.sh            # 后台启动，等到端口通了才返回
    ./start.sh stop       # 停掉（先 SIGTERM，15 秒不退就 SIGKILL）
    ./start.sh restart    # = stop + start
    ./start.sh status     # 在不在跑、pid、端口、地址
    ./start.sh logs -f    # 跟着看日志，Ctrl-C 只是退出查看

拆成两层是有意的：start.sh 负责「找 Python、建虚拟环境、装依赖」这些 shell
更顺手的事，ctl.py 负责 pid / 端口 / 信号这些需要精确控制、也值得写测试的事。
直接跑 ctl.py 也能用，但要求 .venv 已经就绪。
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from app.config import BASE_DIR, settings
from app.daemon import ControlError, ProcessController

PID_FILE = BASE_DIR / "data" / "app.pid"


def _controller() -> ProcessController:
    log_file = None
    if settings.log_file:
        log_file = Path(settings.log_file)
        if not log_file.is_absolute():
            log_file = BASE_DIR / log_file
    return ProcessController(
        PID_FILE,
        cwd=BASE_DIR,
        host=settings.host,
        port=settings.port,
        log_file=log_file,
    )


def _url(host: str, port: int) -> str:
    # 0.0.0.0 不能直接当访问地址
    shown = "127.0.0.1" if host in ("", "0.0.0.0", "::") else host  # noqa: S104 - 只是拼给人看的地址
    return f"http://{shown}:{port}/"


def _fmt_uptime(started_at: float) -> str:
    if not started_at:
        return "未知"
    seconds = max(0, int(time.time() - started_at))
    days, rest = divmod(seconds, 86400)
    hours, rest = divmod(rest, 3600)
    minutes, secs = divmod(rest, 60)
    if days:
        return f"{days} 天 {hours} 小时"
    if hours:
        return f"{hours} 小时 {minutes} 分"
    if minutes:
        return f"{minutes} 分 {secs} 秒"
    return f"{secs} 秒"


def cmd_start(args) -> int:
    ctl = _controller()
    if args.foreground:
        # 前台模式就是原来的 main.py，别在这里再包一层
        from main import main as run_main

        run_main()
        return 0
    try:
        info = ctl.start()
    except ControlError as exc:
        print(f"✗ {exc}", file=sys.stderr)
        return 1
    print(f"✓ 已启动 pid {info.pid}")
    print(f"  后台地址 {_url(info.host, info.port)}")
    if info.log_file:
        print(f"  日志     {info.log_file}")
    print("  停止用   ./start.sh stop")
    return 0


def cmd_stop(args) -> int:
    ctl = _controller()
    try:
        print(ctl.stop(force=args.force))
    except ControlError as exc:
        print(f"✗ {exc}", file=sys.stderr)
        return 1
    return 0


def cmd_restart(args) -> int:
    ctl = _controller()
    try:
        print(ctl.stop(force=args.force))
        info = ctl.start()
    except ControlError as exc:
        print(f"✗ {exc}", file=sys.stderr)
        return 1
    print(f"✓ 已重启 pid {info.pid} —— {_url(info.host, info.port)}")
    return 0


def cmd_status(args) -> int:
    ctl = _controller()
    health = ctl.inspect()
    if health.running and health.info:
        info = health.info
        print(f"● 运行中  pid {info.pid}")
        print(f"  地址    {_url(info.host, info.port)}")
        print(f"  已运行  {_fmt_uptime(info.started_at)}")
        print(f"  pid 文件 {PID_FILE}")
        if info.log_file:
            print(f"  日志    {info.log_file}")
        return 0

    print("○ 未运行")
    if health.problem:
        print(f"  提示：{health.problem}")
    if health.stale:
        print(f"  （遗留的 pid 文件会自动清理：{PID_FILE}）")
    print("  启动用 ./start.sh")
    return 3  # 和 systemd / LSB 的「未运行」保持一致


def cmd_logs(args) -> int:
    ctl = _controller()
    if ctl.log_file is None or not ctl.log_file.exists():
        print("还没有日志文件（检查 XY_LOG_FILE）", file=sys.stderr)
        return 1
    if args.follow:
        # -f 自己实现，省掉对 tail 二进制的依赖（精简镜像上不一定有）
        return _follow(ctl.log_file, args.lines)
    text = ctl.log_file.read_text(encoding="utf-8", errors="replace")
    for line in text.splitlines()[-args.lines:]:
        print(line)
    return 0


def _follow(path: Path, lines: int) -> int:
    with open(path, encoding="utf-8", errors="replace") as handle:
        for line in handle.readlines()[-lines:]:
            sys.stdout.write(line)
        sys.stdout.flush()
        print("— 按 Ctrl-C 退出查看（服务继续运行）—", file=sys.stderr)
        try:
            while True:
                chunk = handle.readline()
                if chunk:
                    sys.stdout.write(chunk)
                    sys.stdout.flush()
                else:
                    time.sleep(0.2)
        except KeyboardInterrupt:
            return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ctl.py",
        description="闲鱼自动发货工具 —— 启停管理",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_start = sub.add_parser("start", help="后台启动（等到端口通了才返回）")
    p_start.add_argument("-f", "--foreground", action="store_true",
                         help="前台运行，日志直接打在终端（等同 python main.py）")
    p_start.set_defaults(func=cmd_start)

    p_stop = sub.add_parser("stop", help="停止服务")
    p_stop.add_argument("--force", action="store_true", help="直接 SIGKILL，不走优雅收尾")
    p_stop.set_defaults(func=cmd_stop)

    p_restart = sub.add_parser("restart", help="重启服务")
    p_restart.add_argument("--force", action="store_true", help="停止时直接 SIGKILL")
    p_restart.set_defaults(func=cmd_restart)

    sub.add_parser("status", help="查看运行状态").set_defaults(func=cmd_status)

    p_logs = sub.add_parser("logs", help="查看日志")
    p_logs.add_argument("-f", "--follow", action="store_true", help="持续输出新日志")
    p_logs.add_argument("-n", "--lines", type=int, default=40, help="先打印最后几行（默认 40）")
    p_logs.set_defaults(func=cmd_logs)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args) or 0)
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
