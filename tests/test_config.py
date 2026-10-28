"""小内存配置的联动测试：显式开关必须优先于自动判断。"""

from __future__ import annotations

import app.config as config


def test_low_memory_default_tightens_upload_concurrency(monkeypatch):
    monkeypatch.setattr(config, "_total_ram_mb", lambda: 512)
    monkeypatch.delenv("XY_LOW_MEMORY", raising=False)
    monkeypatch.delenv("XY_MEDIA_CONCURRENCY", raising=False)

    settings = config.Settings()

    assert settings.low_memory is True
    assert settings.media_concurrency == 1


def test_explicit_low_memory_forces_upload_concurrency(monkeypatch):
    monkeypatch.setattr(config, "_total_ram_mb", lambda: 8192)
    monkeypatch.setenv("XY_LOW_MEMORY", "1")
    monkeypatch.delenv("XY_MEDIA_CONCURRENCY", raising=False)

    settings = config.Settings()

    assert settings.low_memory is True
    assert settings.media_concurrency == 1


def test_explicit_low_memory_off_restores_default_concurrency(monkeypatch):
    monkeypatch.setattr(config, "_total_ram_mb", lambda: 512)
    monkeypatch.setenv("XY_LOW_MEMORY", "0")
    monkeypatch.delenv("XY_MEDIA_CONCURRENCY", raising=False)

    settings = config.Settings()

    assert settings.low_memory is False
    assert settings.media_concurrency == 2
