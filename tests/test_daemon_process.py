"""ProcessController 的真实启停集成测试（会真的拉起子进程）。

用一个临时端口 + 临时 DB，跑完一定清干净；起不来就跳过，
免得在没网/受限沙箱里把 CI 弄红。
"""

from __future__ import annotations

import socket

import pytest

from app.daemon import ControlError, ProcessController, port_in_use


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture()
def ctl(tmp_path, monkeypatch):
    """指向临时目录的控制器：pid 文件、DB、日志都在 tmp 里。"""
    from app.config import BASE_DIR

    port = _free_port()
    monkeypatch.setenv("XY_PORT", str(port))
    monkeypatch.setenv("XY_DB_PATH", str(tmp_path / "app.db"))
    monkeypatch.setenv("XY_LOG_FILE", str(tmp_path / "server.log"))

    controller = ProcessController(
        tmp_path / "app.pid",
        cwd=BASE_DIR,
        host="127.0.0.1",
        port=port,
        log_file=tmp_path / "server.log",
        stop_timeout=10.0,
    )
    yield controller
    # 无论测试怎么结束，都别留下后台进程
    try:
        controller.stop(force=True)
    except ControlError:
        pass


def test_real_start_status_stop(ctl):
    info = ctl.start(timeout=30)
    assert info.pid > 0
    assert port_in_use("127.0.0.1", ctl.port)

    health = ctl.inspect()
    assert health.running is True
    assert health.info is not None
    assert health.info.pid == info.pid

    assert "已停止" in ctl.stop()
    assert ctl.inspect().running is False
    assert not ctl.pid_file.exists()
    assert not port_in_use("127.0.0.1", ctl.port)


def test_start_twice_is_rejected(ctl):
    ctl.start(timeout=30)
    with pytest.raises(ControlError, match="已经在运行"):
        ctl.start(timeout=10)


def test_restart_replaces_the_process(ctl):
    first = ctl.start(timeout=30)
    _, second = ctl.restart(timeout=30)
    assert second.pid != first.pid
    assert ctl.inspect().running is True


def test_stop_when_not_running_is_harmless(ctl):
    assert "没有" in ctl.stop()


def test_stale_pid_file_is_cleaned_on_start(ctl):
    """机器重启后留下的旧 pid 文件不能挡住重新启动。"""
    ctl.pid_file.parent.mkdir(parents=True, exist_ok=True)
    ctl.pid_file.write_text(
        '{"pid": 999999, "argv": ["python", "main.py"]}', encoding="utf-8"
    )
    assert ctl.inspect().stale is True

    info = ctl.start(timeout=30)
    assert info.pid > 0
    assert ctl.inspect().running is True
