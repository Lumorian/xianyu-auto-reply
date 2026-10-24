"""进程控制（app/daemon.py）的单元测试。

这里刻意只测「判断逻辑」：真实启停子进程那部分走集成验证，
否则测试会互相抢端口，跑得还慢。
"""

from __future__ import annotations

import json
import os
import socket

import pytest

from app.daemon import (
    ControlError,
    Health,
    ProcessController,
    ProcessInfo,
    decide_health,
    decide_stop,
    is_pid_alive,
    looks_like_ours,
    port_in_use,
    wait_until,
)


def _write_pid(path, payload):
    path.write_text(json.dumps(payload), encoding="utf-8")


# ---------------------------------------------------------- looks_like_ours


def test_looks_like_ours_matches_entry_by_basename():
    """用相对路径、绝对路径、venv 解释器启动都算「是我们的」。"""
    assert looks_like_ours(["/x/.venv/bin/python", "/proj/main.py"])
    assert looks_like_ours(["python", "main.py"])
    assert looks_like_ours([".venv/bin/python", "main.py", "--port", "1"])


def test_looks_like_ours_rejects_unrelated_process():
    """pid 被复用时命令行对不上，绝不能当成自己的进程去杀。"""
    assert not looks_like_ours(["/usr/bin/nginx"])
    assert not looks_like_ours([])


# ---------------------------------------------------------- decide_health


def test_health_without_pid_file_is_not_running(tmp_path):
    health = decide_health(tmp_path / "app.pid")
    assert health.running is False
    assert health.stale is False
    assert health.info is None


def test_health_detects_running_process(tmp_path):
    pid_file = tmp_path / "app.pid"
    _write_pid(pid_file, {"pid": 4242, "host": "127.0.0.1", "port": 8000, "started_at": 123.0})

    health = decide_health(
        pid_file,
        pid_alive=lambda pid: pid == 4242,
        cmdline=lambda pid: ["/venv/bin/python", "/proj/main.py"],
    )
    assert health.running is True
    assert health.info is not None
    assert health.info.pid == 4242
    assert health.info.port == 8000
    assert health.info.started_at == 123.0


def test_health_flags_dead_pid_as_stale(tmp_path):
    pid_file = tmp_path / "app.pid"
    _write_pid(pid_file, {"pid": 999999})

    health = decide_health(pid_file, pid_alive=lambda pid: False, cmdline=lambda pid: [])
    assert health.running is False
    assert health.stale is True
    assert "999999" in health.problem


def test_health_rejects_reused_pid(tmp_path):
    """机器重启后旧 pid 可能被别的进程复用 —— 必须认成「没在跑」。"""
    pid_file = tmp_path / "app.pid"
    _write_pid(pid_file, {"pid": 31337})

    health = decide_health(
        pid_file,
        pid_alive=lambda pid: True,
        cmdline=lambda pid: ["/usr/bin/postgres"],
    )
    assert health.running is False
    assert health.stale is True
    assert "postgres" in health.problem


def test_health_tolerates_unreadable_cmdline(tmp_path):
    """非 Linux 没有 /proc，读不到命令行时退回到「信 pid 文件」。"""
    pid_file = tmp_path / "app.pid"
    _write_pid(pid_file, {"pid": 77, "argv": ["python", "main.py"]})

    health = decide_health(pid_file, pid_alive=lambda pid: True, cmdline=lambda pid: [])
    assert health.running is True
    assert health.info is not None
    assert health.info.argv == ["python", "main.py"]


def test_health_handles_corrupt_pid_file(tmp_path):
    pid_file = tmp_path / "app.pid"
    pid_file.write_text("not json{", encoding="utf-8")

    health = decide_health(pid_file)
    assert health.running is False
    assert health.stale is True


def test_health_handles_pid_file_without_pid(tmp_path):
    pid_file = tmp_path / "app.pid"
    _write_pid(pid_file, {"port": 8000})

    health = decide_health(pid_file)
    assert health.running is False
    assert health.stale is True


# ---------------------------------------------------------- decide_stop


def test_stop_action_when_nothing_running():
    action, reason = decide_stop(Health(running=False))
    assert action == "none"
    assert "没有" in reason


def test_stop_action_prefers_graceful_term():
    health = Health(running=True, info=ProcessInfo(pid=1))
    assert decide_stop(health)[0] == "term"
    assert decide_stop(health, force=True)[0] == "kill"


# ---------------------------------------------------------- 其它


def test_is_pid_alive_for_current_process():
    assert is_pid_alive(os.getpid()) is True


def test_is_pid_alive_for_bogus_pid():
    assert is_pid_alive(0) is False
    assert is_pid_alive(-1) is False


def test_wait_until_returns_immediately_when_true():
    assert wait_until(lambda: True, timeout=1.0) is True


# ------------------------------------------------- 端口探测 / 启动互斥


def _controller(tmp_path) -> ProcessController:
    return ProcessController(
        tmp_path / "app.pid", cwd=tmp_path, host="127.0.0.1", port=1,
    )


def test_port_in_use_detects_ipv6_listener():
    """XY_HOST=::1 / :: 时必须用 AF_INET6 探测，否则会把「在跑」判成「没在跑」。"""
    try:
        sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    except OSError:
        pytest.skip("系统没开 IPv6")
    with sock:
        try:
            sock.bind(("::1", 0))
            sock.listen(1)
        except OSError:
            pytest.skip("::1 不可用")
        port = sock.getsockname()[1]
        assert port_in_use("::1", port) is True
        assert port_in_use("::", port) is True


def test_port_in_use_false_for_closed_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    # 已经关闭，应该探测不到
    assert port_in_use("127.0.0.1", port) is False


def test_run_lock_is_exclusive(tmp_path):
    """两个 start 同时判断「没在跑」→ 必须只有一个能进临界区。"""
    ctl = _controller(tmp_path)
    with ctl.run_lock():
        with pytest.raises(ControlError):
            with ctl.run_lock():
                pass
    # 释放之后还能再拿（第二次启动不会永远卡住）
    with ctl.run_lock():
        pass


def test_wait_until_times_out():
    assert wait_until(lambda: False, timeout=0.05, interval=0.01) is False
