"""数据迁移包：导出 / 检查 / 恢复。"""

from __future__ import annotations

import io
import json
import sqlite3
import tarfile
from pathlib import Path

import pytest

from app.backup import BackupError, export_archive, inspect_archive, restore_archive
from app.db import Database
from app.secrets import CookieVault


def order_fields(**overrides):
    data = {
        "account_id": 1,
        "order_id": "2500000000000000001",
        "item_id": "900052644277",
        "buyer_id": "4100000001",
        "buyer_name": "买家小王",
        "rule_id": None,
        "card_id": None,
        "content": "卡密：CARD-1",
        "amount_cny": 9.9,
        "status": "delivered",
        "detail": "",
    }
    data.update(overrides)
    return data


def _make_tar(path: Path, manifest: dict | None, extra: dict[str, bytes] | None = None) -> None:
    with tarfile.open(path, "w:gz") as archive:
        if manifest is not None:
            payload = json.dumps(manifest, ensure_ascii=False).encode("utf-8")
            info = tarfile.TarInfo("manifest.json")
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))
        for name, data in (extra or {}).items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))


async def test_export_and_restore_roundtrip(tmp_path):
    db = Database(tmp_path / "app.db")
    await db.init()
    await db.record_order(**order_fields())

    archive = tmp_path / "backup.tar.gz"
    manifest = export_archive(db.path, archive, include_media=False)

    assert manifest["db"]["counts"]["orders"] == 1
    assert manifest["includes_key"] is False
    assert inspect_archive(archive)["format"] == "xianyu-auto-reply-backup"

    target = tmp_path / "new-device" / "app.db"
    result = restore_archive(archive, target, restore_media=False)

    assert result["counts"]["orders"] == 1
    assert result["pre_restore"] == ""
    conn = sqlite3.connect(target)
    try:
        row = conn.execute("SELECT order_id, buyer_name FROM orders").fetchone()
    finally:
        conn.close()
    assert row == ("2500000000000000001", "买家小王")


async def test_restore_snapshots_existing_db_before_replacing(tmp_path):
    old_db = Database(tmp_path / "app.db")
    await old_db.init()
    await old_db.record_order(**order_fields(order_id="old-order"))

    source = Database(tmp_path / "source.db")
    await source.init()
    await source.record_order(**order_fields(order_id="new-order"))
    archive = tmp_path / "backup.tar.gz"
    export_archive(source.path, archive, include_media=False)

    result = restore_archive(archive, old_db.path, restore_media=False)

    assert result["pre_restore"]
    snapshot = Path(result["pre_restore"])
    conn = sqlite3.connect(snapshot)
    try:
        assert conn.execute("SELECT order_id FROM orders").fetchone() == ("old-order",)
    finally:
        conn.close()

    conn = sqlite3.connect(old_db.path)
    try:
        assert conn.execute("SELECT order_id FROM orders").fetchone() == ("new-order",)
    finally:
        conn.close()


async def test_export_refuses_to_overwrite_database(tmp_path):
    db = Database(tmp_path / "app.db")
    await db.init()
    with pytest.raises(BackupError, match="不能直接覆盖数据库"):
        export_archive(db.path, db.path, include_media=False)


async def test_bad_archive_does_not_touch_existing_db(tmp_path):
    db = Database(tmp_path / "app.db")
    await db.init()
    await db.record_order(**order_fields(order_id="keep-me"))

    archive = tmp_path / "broken.tar.gz"
    _make_tar(
        archive,
        {"format": "xianyu-auto-reply-backup", "version": 1},
        {"app.db": b"this is not sqlite"},
    )

    with pytest.raises(BackupError):
        restore_archive(archive, db.path, restore_media=False)

    conn = sqlite3.connect(db.path)
    try:
        assert conn.execute("SELECT order_id FROM orders").fetchone() == ("keep-me",)
    finally:
        conn.close()


def test_rejects_path_traversal_in_archive(tmp_path):
    archive = tmp_path / "evil.tar.gz"
    _make_tar(
        archive,
        {"format": "xianyu-auto-reply-backup", "version": 1},
        {"../outside.txt": b"x"},
    )
    with pytest.raises(BackupError, match="不安全"):
        inspect_archive(archive)


def test_rejects_archive_without_database(tmp_path):
    archive = tmp_path / "manifest-only.tar.gz"
    _make_tar(archive, {"format": "xianyu-auto-reply-backup", "version": 1})
    with pytest.raises(BackupError, match=r"没有 app\.db"):
        inspect_archive(archive)


def test_rejects_unknown_archive(tmp_path):
    archive = tmp_path / "other.tar.gz"
    _make_tar(archive, {"format": "something-else", "version": 1})
    with pytest.raises(BackupError, match="不是本工具"):
        inspect_archive(archive)


async def test_key_can_travel_with_archive(tmp_path):
    key = tmp_path / "cookie.key"
    db = Database(tmp_path / "app.db", vault=CookieVault(key))
    await db.init()
    await db.upsert_account("主账号", "session=abc")

    archive = tmp_path / "backup.tar.gz"
    manifest = export_archive(
        db.path, archive, key_path=key, include_key=True, include_media=False
    )
    assert manifest["includes_key"] is True

    target = tmp_path / "new-device"
    result = restore_archive(
        archive,
        target / "app.db",
        key_path=target / "cookie.key",
        restore_media=False,
    )
    assert result["key_restored"] is True
    assert result["key_status"] == "ok"

    restored = Database(target / "app.db", vault=CookieVault(target / "cookie.key"))
    account = await restored.get_account(1)
    assert account is not None
    assert account["cookie"] == "session=abc"
