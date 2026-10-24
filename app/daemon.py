"""进程控制：让「启动 / 停止 / 重启 / 看状态」变成一条命令。

以前启动就是裸跑的 `python main.py`：

    $ python main.py &          # 或者 nohup … &
    $ ps aux | grep main.py     # 忘记 pid 了就得翻一遍
    $ kill 12345                # 杀掉之后还不敢确定它真的死了

问题不只是麻烦：

* **pid 会撒谎**。机器重启、进程被 OOM 杀掉以后，pid 数字会被别的进程复用，
  照着旧 pid `kill` 很可能杀掉一个完全无关的进程（比如你的编辑器）。
* **杀掉不等于停干净**。主进程没了，它拉起的子进程 / 监听端口可能还在。
* **重复启动没人拦**。跑了两份实例就会有两个 Worker 抢同一个账号 Cookie，
  闲鱼侧表现为「被挤爆啦」，两边都发不出货。

所以这里管三件事：

1. **有且只有一个实例**：启动前先拿 `data/app.pid` 上的锁，拿不到就报错退出，
   而不是默默再起一份。
2. **停止要确认**：先 SIGTERM 让 uvicorn 体面收尾，超时再 SIGKILL，
   然后**等端口真的释放**才返回。
3. **pid 必须可验证**：光看 pid 存在不够，还要核对这个 pid 的命令行里确实是本项目的
   `main.py`，对不上就当作「没有在跑」，顺手把脏 pid 文件清掉。

设计上刻意把「判断」和「发信号」分开：inspect_health / decide_stop 是纯函数，
好测；ProcessController 才碰 os.kill 和子进程。
"""

from __future__ import annotations

import errno
import json
import os
import signal
import socket
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:  # Windows 没有 fcntl：退化成「不加锁」，其余逻辑照旧
    import fcntl
except ImportError:  # pragma: no cover - 非 POSIX 平台
    fcntl = None  # type: ignore[assignment]

#: 正常停止时给 uvicorn 的收尾时间（秒）。它退出时要关闭长连接、等 DB 落盘。
STOP_TIMEOUT = 15.0
#: 端口释放等待上限（秒）。SIGTERM 之后内核回收 socket 通常瞬间完成。
PORT_RELEASE_TIMEOUT = 5.0
#: 轮询间隔（秒）
POLL_INTERVAL = 0.1
#: 入口脚本名，用来核对 pid 是不是我们自己
ENTRY_NAME = "main.py"


class ControlError(RuntimeError):
    """进程操作失败（已经在跑、没在跑、停不掉……）。"""


@dataclass(slots=True)
class ProcessInfo:
    """一个正在运行的实例。"""

    pid: int
    argv: list[str] = field(default_factory=list)
    started_at: float = 0.0
    port: int = 0
    host: str = ""
    log_file: str = ""

    @property
    def command(self) -> str:
        return " ".join(self.argv)


@dataclass(slots=True)
class Health:
    """「这个实例现在活着吗」的结论。"""

    running: bool
    info: ProcessInfo | None = None
    #: 有问题时给用户看的一句话
    problem: str = ""
    #: pid 文件存在但对应进程已不在（机器重启 / 被 kill -9），调用方应当清理
    stale: bool = False


# ------------------------------------------------------------------ 纯判断


def is_pid_alive(pid: int) -> bool:
    """这个 pid 现在是否存在（僵尸进程算不存在）。

    `os.kill(pid, 0)` 只是探测权限/存在性，不会真的发信号。
    /proc 里状态是 Z 的进程已经退出、只是还没被父进程回收，
    不能当成「服务在跑」——否则会出现「停不掉」的假象。
    """
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # 进程存在，只是不归我们管
        return True
    except OSError:
        return False

    if sys.platform.startswith("linux"):
        try:
            stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8", errors="replace")
            fields = stat.rsplit(")", 1)[-1].split()
            if fields and fields[0] == "Z":
                return False
        except OSError:
            pass
    return True


def read_cmdline(pid: int) -> list[str]:
    """读 /proc/<pid>/cmdline；读不到返回空列表（macOS 上就没有 /proc）。"""
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return []
    return [part.decode("utf-8", "replace") for part in raw.split(b"\0") if part]


def looks_like_ours(argv: list[str], entry: str = ENTRY_NAME) -> bool:
    """命令行里有没有本项目的入口脚本。

    只比对 pid 是否存在是不够的：pid 会被复用，机器重启后旧 pid 文件
    完全可能指向一个毫不相干的进程。宁可认为「没在跑」，也不能误杀。
    """
    return any(Path(arg).name == entry for arg in argv)


def decide_health(
    pid_file: Path,
    *,
    pid_alive: Callable[[int], bool] = is_pid_alive,
    cmdline: Callable[[int], list[str]] = read_cmdline,
    entry: str = ENTRY_NAME,
) -> Health:
    """根据 pid 文件判断实例状态。读不到文件 = 没在跑。"""
    if not pid_file.exists():
        return Health(running=False)

    try:
        data: Any = json.loads(pid_file.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return Health(running=False, problem="pid 文件内容损坏", stale=True)

    try:
        pid = int(data["pid"])
    except (TypeError, KeyError, ValueError):
        return Health(running=False, problem="pid 文件里没有合法的 pid", stale=True)

    if not pid_alive(pid):
        return Health(running=False, problem=f"pid {pid} 已不存在（进程已退出）", stale=True)

    argv = cmdline(pid)
    if argv and not looks_like_ours(argv, entry):
        return Health(
            running=False,
            problem=f"pid {pid} 被别的进程占用了：{' '.join(argv)[:80]}",
            stale=True,
        )

    return Health(
        running=True,
        info=ProcessInfo(
            pid=pid,
            argv=argv or list(data.get("argv") or []),
            started_at=float(data.get("started_at") or 0.0),
            port=int(data.get("port") or 0),
            host=str(data.get("host") or ""),
            log_file=str(data.get("log_file") or ""),
        ),
    )


def decide_stop(health: Health, *, force: bool = False) -> tuple[str, str]:
    """返回 (action, reason)。action ∈ {none, term, kill}。

    不 force 时最多 SIGTERM；force 时如果第一步没弄死它，才升级到 SIGKILL。
    """
    if not health.running or health.info is None:
        return "none", health.problem or "没有正在运行的实例"
    if force:
        return "kill", "强制停止"
    return "term", "正在停止"


# ------------------------------------------------------------------ 网络小工具


def port_in_use(host: str, port: int) -> bool:
    """端口上有没有人在监听。

    注意地址族：XY_HOST=::1 / :: 时必须用 AF_INET6 去探测，否则 connect_ex
    永远返回「连不上」—— 那样「已经在跑」会判成「没在跑」，起出第二个实例。
    """
    # 0.0.0.0 / :: 是「监听所有网卡」的写法，探测时得换成对应族的回环地址
    if host in ("", "0.0.0.0"):  # noqa: S104 - 只是判断用户是不是填了通配地址，并不真的绑定
        probe = "127.0.0.1"
    elif host == "::":
        probe = "::1"
    else:
        probe = host
    family = socket.AF_INET6 if ":" in probe else socket.AF_INET
    try:
        sock = socket.socket(family, socket.SOCK_STREAM)
    except OSError:  # 系统没开 IPv6
        return False
    with sock:
        sock.settimeout(0.3)
        try:
            return sock.connect_ex((probe, port)) == 0
        except OSError:
            return False


def wait_until(predicate: Callable[[], bool], timeout: float,
               interval: float = POLL_INTERVAL) -> bool:
    """轮询到 predicate 为真或超时。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()

# ------------------------------------------------------------------ 控制器


class ProcessController:
    """在后台启停本项目的主进程。

    pid 文件里有 pid、命令行、端口和日志路径 —— 只为「下次能核对是谁」，
    不放任何凭据。
    """

    def __init__(
        self,
        pid_file: Path,
        *,
        cwd: Path,
        host: str,
        port: int,
        python: str | None = None,
        log_file: Path | None = None,
        stop_timeout: float = STOP_TIMEOUT,
    ) -> None:
        self.pid_file = Path(pid_file)
        self.cwd = Path(cwd)
        self.host = host
        self.port = port
        #: 用当前解释器启动，保证走的是同一个 .venv
        self.python = python or sys.executable
        self.log_file = Path(log_file) if log_file else None
        self.stop_timeout = stop_timeout

    @property
    def lock_file(self) -> Path:
        return self.pid_file.with_suffix(".lock")

    @contextmanager
    def run_lock(self) -> Iterator[None]:
        """给「启动 / 停止」加互斥锁（跨进程）。

        只靠 pid 文件判断「有没有在跑」是典型的 TOCTOU：两个 start 同时看到
        「没有 pid 文件」就会都去 Popen。后一个因端口占用退出时还会
        clear_stale_pid()，把前一个**正在运行**的实例的 pid 文件删掉 ——
        之后 status/stop 都不认它，下一次 start 又能起第三个实例。
        """
        self.lock_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.lock_file, "a+") as handle:
            if fcntl is not None:
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError as exc:
                    raise ControlError("另一个 start/stop 正在执行，请稍后再试") from exc
            try:
                yield
            finally:
                if fcntl is not None:
                    try:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                    except OSError:
                        pass

    # ---------------------------------------------------------- 查询

    def inspect(self) -> Health:
        return decide_health(
            self.pid_file,
            pid_alive=is_pid_alive,
            cmdline=read_cmdline,
            entry=ENTRY_NAME,
        )

    def clear_stale_pid(self) -> None:
        """进程确认不在时删掉 pid 文件，避免下次误判。"""
        try:
            self.pid_file.unlink()
        except OSError:
            pass

    def _write_pid(self, pid: int, argv: list[str]) -> None:
        self.pid_file.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "pid": pid,
            "argv": argv,
            "started_at": time.time(),
            "host": self.host,
            "port": self.port,
            "log_file": str(self.log_file or ""),
        }
        # 先写临时文件再 rename：中途断电不会留下半个 JSON
        tmp = self.pid_file.with_suffix(".pid.tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.pid_file)

    # ---------------------------------------------------------- 启动

    def start(self, *, timeout: float = 20.0) -> ProcessInfo:
        """后台启动一个实例（带跨进程锁）。"""
        with self.run_lock():
            return self._start_locked(timeout=timeout)

    def _start_locked(self, *, timeout: float = 20.0) -> ProcessInfo:
        """后台启动一个实例，等它真正能响应再返回。

        这里**不做 double-fork**：直接 Popen 之后放进新会话（start_new_session），
        它就和当前终端脱钩了，关掉终端也不会被 SIGHUP 带走。
        返回时进程一定已经拿到 pid 文件，且端口在监听。
        """
        health = self.inspect()
        if health.running and health.info:
            raise ControlError(f"已经在运行了（pid {health.info.pid}），先 stop 再 start")
        if health.stale:
            # 旧 pid 已经不在了，清掉脏文件继续启动
            self.clear_stale_pid()

        if port_in_use(self.host, self.port):
            raise ControlError(
                f"端口 {self.port} 已被占用（不是本项目：pid 文件里没有记录）。\n"
                f"要么换个端口（XY_PORT），要么先停掉占用的进程。"
            )

        argv = [self.python, str(self.cwd / ENTRY_NAME)]
        # 后台模式：stdout 就是日志文件，让子进程别再往控制台写一份（否则每行都重复）
        child_env = {**os.environ, "XY_BACKGROUND": "1"} if self.log_file else None
        out = self._open_log()
        try:
            proc = subprocess.Popen(  # noqa: S603 - 固定 argv，无 shell
                argv,
                cwd=str(self.cwd),
                env=child_env,
                stdout=out,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                start_new_session=True,  # 脱离终端，Ctrl-C / 关窗口都不影响它
            )
        except OSError as exc:
            raise ControlError(f"启动失败：{exc}") from exc
        finally:
            if out is not None:
                out.close()

        self._write_pid(proc.pid, argv)

        # 等两个信号都到位：端口开始监听，而且进程还没死
        ok = wait_until(
            lambda: (not is_pid_alive(proc.pid)) or port_in_use(self.host, self.port),
            timeout,
        )
        if not is_pid_alive(proc.pid):
            # 起崩了（端口冲突 / .env 写错 / import 失败），把日志尾巴给他看
            self.clear_stale_pid()
            raise ControlError(
                "启动后进程立刻退出了，最近的日志：\n" + self.tail_log(15)
            )
        if not ok:
            # 进程还活着但端口一直没起来，别留个半死不活的在后面。
            # 这里必须调不带锁的版本：锁已经握在 start 手里了。
            self._stop_locked(force=True)
            raise ControlError(f"启动超时：{timeout:g} 秒内端口 {self.port} 没有开始监听")

        return ProcessInfo(
            pid=proc.pid, argv=argv, started_at=time.time(),
            host=self.host, port=self.port, log_file=str(self.log_file or ""),
        )

    def _open_log(self):
        if self.log_file is None:
            return None
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        # 追加而不是覆盖：重启后历史日志还在，排查「上次为什么挂」才有得看
        return open(self.log_file, "ab")  # Popen 继承这个 fd 后我们立刻关闭自己那份

    def tail_log(self, lines: int = 15) -> str:
        if self.log_file is None or not self.log_file.exists():
            return "（没有日志文件，看看 XY_LOG_FILE 配置）"
        try:
            text = self.log_file.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            return f"（读日志失败：{exc}）"
        tail = text.splitlines()[-lines:]
        return "\n".join(tail) if tail else "（日志是空的）"

    # ---------------------------------------------------------- 停止

    def stop(self, *, force: bool = False, timeout: float | None = None) -> str:
        """停掉实例（带跨进程锁）。"""
        with self.run_lock():
            return self._stop_locked(force=force, timeout=timeout)

    def _stop_locked(self, *, force: bool = False, timeout: float | None = None) -> str:
        """停掉实例。返回一句给用户看的结果。

        流程：SIGTERM → 等进程消失 → （必要时）SIGKILL → 等端口释放 → 删 pid 文件。
        只有端口真的空了才算成功，避免「以为停了其实还在」。
        """
        health = self.inspect()
        action, _reason = decide_stop(health, force=force)
        if action == "none":
            if health.stale:
                self.clear_stale_pid()
            return "没有在运行的实例"

        assert health.info is not None
        pid = health.info.pid
        wait_for = self.stop_timeout if timeout is None else timeout

        sig = signal.SIGKILL if action == "kill" else signal.SIGTERM
        self._signal(pid, sig)

        graceful = action == "term" and wait_until(
            lambda: not is_pid_alive(pid), wait_for
        )
        killed = False
        if not graceful and is_pid_alive(pid):
            # 没体面退出（或者本来就要求强制）：直接 SIGKILL
            self._signal(pid, signal.SIGKILL)
            killed = True
            wait_until(lambda: not is_pid_alive(pid), 5.0)

        if is_pid_alive(pid):
            raise ControlError(f"停不掉 pid {pid}（可能不是你启动的，没权限）。试试 sudo。")

        # 进程没了不代表端口立刻空：内核回收 + TIME_WAIT 需要一点时间
        wait_until(lambda: not port_in_use(self.host, self.port), PORT_RELEASE_TIMEOUT)
        self.clear_stale_pid()

        if port_in_use(self.host, self.port):
            return f"已停止 pid {pid}，但端口 {self.port} 仍被其他进程占用。"
        if killed:
            return f"已强制停止 pid {pid}"
        return f"已停止 pid {pid}"

    def _signal(self, pid: int, sig: int) -> None:
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            return  # 刚好自己退出了，不算错
        except PermissionError as exc:
            raise ControlError(f"没有权限给 pid {pid} 发信号。") from exc
        except OSError as exc:
            if exc.errno == errno.ESRCH:
                return
            raise ControlError(f"给 pid {pid} 发信号失败：{exc}") from exc

    # ---------------------------------------------------------- 重启

    def restart(self, *, timeout: float = 20.0) -> tuple[str, ProcessInfo]:
        stopped = self.stop()
        return stopped, self.start(timeout=timeout)
