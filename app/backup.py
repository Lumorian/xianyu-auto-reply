"""数据迁移包：把 SQLite、Cookie 密钥和媒体文件打成一个归档。

为什么需要它：这套工具是单机 SQLite 部署，data/ 整个目录都在 .gitignore 里。
换一台机器重新 clone 之后，代码是新的、库是空的 —— 看起来就像「数据没有同步」，
其实只是数据从来没有离开过旧机器。手工搬库还要记得带 cookie.key（不带的话
所有账号 Cookie 都解不开），以及 data/media（内置图片库会全部变成裂图）。

这个模块只做三件事：
  * export_archive() 用 SQLite 的在线备份 API 取一份一致快照，连同可选的
    密钥、媒体文件打成 tar.gz；
  * inspect_archive() 只读地读 manifest / 成员列表，不落盘、不执行任何东西；
  * restore_archive() 先校验（归档路径、SQLite integrity_check、必需表、
    Cookie 密钥是否匹配），再把当前库快照成 *.pre-restore-* 后原子替换。

安全边界：
  * 归档默认不含 cookie.key；要无缝迁移登录态才显式 include_key=True。
  * 带密钥的归档文件固定 0600；密钥只写进归档，日志里不打印内容。
  * 恢复时拒绝绝对路径 / .. / 符号链接等 tar 常见攻击面；
    坏包不会覆盖现有数据。
"""

from __future__ import annotations

import io
import json
import os
import shutil
import sqlite3
import tarfile
import tempfile
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any

from .secrets import CookieVault

FORMAT = "xianyu-auto-reply-backup"
VERSION = 1
DB_NAME = "app.db"
KEY_NAME = "cookie.key"
MANIFEST_NAME = "manifest.json"
MEDIA_DIR_NAME = "media"

#: manifest.json 的读取上限。正常包只有几 KB；1 MB 已经非常宽裕。
MAX_MANIFEST_BYTES = 1024 * 1024
#: 单个迁移包解压后的总字节上限，防止 gzip 炸弹把磁盘打满。
MAX_UNCOMPRESSED_BYTES = 8 * 1024 * 1024 * 1024

#: 统计行数用；老库可能没有某张表，缺的就跳过。
_COUNT_TABLES = (
    "accounts",
    "rules",
    "cards",
    "orders",
    "reply_rules",
    "reply_rule_entries",
    "buyer_keys",
    "messages",
    "app_config",
)

#: 一个可用的业务库至少要有这几张表。少了说明拿错文件了（比如拿成别的项目的库）。
_REQUIRED_TABLES = frozenset({"accounts", "orders", "messages"})


class BackupError(RuntimeError):
    """归档不存在、格式不对、校验失败，或恢复目标不安全。"""


def _stamp() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _snapshot_sqlite(source: Path, dest: Path) -> None:
    """用 SQLite 在线备份 API 取一致快照（WAL 里没 checkpoint 的数据也会带上）。"""
    if not source.is_file():
        raise BackupError(f"数据库不存在：{source}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    src_conn = sqlite3.connect(str(source), timeout=30)
    dst_conn = sqlite3.connect(str(dest))
    try:
        src_conn.execute("PRAGMA busy_timeout = 30000")
        src_conn.backup(dst_conn)
    except sqlite3.Error as exc:
        raise BackupError(f"备份数据库失败：{exc}") from exc
    finally:
        dst_conn.close()
        src_conn.close()


def _inspect_db(path: Path) -> dict[str, Any]:
    """校验 SQLite 文件并返回行数统计。只读，不修改文件。"""
    if not path.is_file():
        raise BackupError(f"数据库不存在：{path}")
    try:
        conn = sqlite3.connect(str(path))
    except sqlite3.Error as exc:
        raise BackupError(f"打不开数据库：{exc}") from exc
    try:
        result = conn.execute("PRAGMA integrity_check").fetchone()
        if not result or str(result[0]).lower() != "ok":
            raise BackupError(f"数据库完整性校验失败：{result[0] if result else '无返回'}")
        tables = {
            str(row[0])
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        missing = sorted(_REQUIRED_TABLES - tables)
        if missing:
            raise BackupError("这不是本工具的数据库（缺少表：" + "、".join(missing) + "）")
        counts: dict[str, int] = {}
        for name in _COUNT_TABLES:
            if name not in tables:
                continue
            row = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()  # noqa: S608 - 表名来自白名单
            counts[name] = int(row[0]) if row else 0
        encrypted = conn.execute(
            "SELECT COUNT(*) FROM accounts WHERE cookie LIKE 'enc:%'"
        ).fetchone()
        return {
            "counts": counts,
            "encrypted_accounts": int(encrypted[0]) if encrypted else 0,
        }
    except sqlite3.Error as exc:
        raise BackupError(f"数据库校验失败：{exc}") from exc
    finally:
        conn.close()


def _check_key(db_path: Path, key_path: Path) -> tuple[bool, str]:
    """检查密钥能不能解开库里第一条加密 Cookie。"""
    info = _inspect_db(db_path)
    if not info["encrypted_accounts"]:
        return True, "no_encrypted_cookies"
    if not key_path.is_file():
        return False, "missing_key"
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT cookie FROM accounts WHERE cookie LIKE 'enc:%' LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return True, "no_encrypted_cookies"
    vault = CookieVault(key_path)
    plain = vault.decrypt(str(row[0]))
    if not plain:
        return False, vault.last_error or "密钥不匹配"
    return True, "ok"


def _validate_manifest(manifest: Any) -> dict[str, Any]:
    if not isinstance(manifest, dict):
        raise BackupError("manifest 不是 JSON 对象")
    if manifest.get("format") != FORMAT:
        raise BackupError("不是本工具的迁移包（manifest.format 不匹配）")
    try:
        version = int(manifest.get("version", 0))
    except (TypeError, ValueError) as exc:
        raise BackupError("manifest.version 不是整数") from exc
    if version != VERSION:
        raise BackupError(f"迁移包版本不支持：{version}（当前支持 {VERSION}）")
    return manifest


def _member_parts(name: str) -> tuple[str, ...]:
    path = PurePosixPath(name)
    if path.is_absolute() or not path.parts or ".." in path.parts:
        raise BackupError(f"归档里有不安全的路径：{name}")
    return path.parts


def _validate_member(member: tarfile.TarInfo) -> None:
    parts = _member_parts(member.name)
    if member.issym() or member.islnk() or member.isdev():
        raise BackupError(f"归档里不允许链接/设备文件：{member.name}")
    if not (member.isfile() or member.isdir()):
        raise BackupError(f"归档里有不支持的文件类型：{member.name}")
    if parts[0] == MEDIA_DIR_NAME:
        return
    if len(parts) == 1 and parts[0] in {DB_NAME, KEY_NAME, MANIFEST_NAME}:
        return
    raise BackupError(f"归档里有不认识的文件：{member.name}")


def _check_archive_size(members: list[tarfile.TarInfo], *, dest: Path | None = None) -> None:
    """拒绝声明体积过大的归档；同时按目标磁盘余量再收紧一次。

    tar 头里的 size 是权威值，解包只会写这么多字节；在解包前累加就能
    挡住「几十 KB 的 gzip 解出几十 GB」的压缩炸弹。dest 给出时还要求
    解压总量不超过可用空间的 90%，避免恢复过程把系统盘写满。
    """
    total = 0
    for member in members:
        if not member.isfile():
            continue
        if member.size < 0:
            raise BackupError(f"归档成员大小非法：{member.name}")
        total += member.size
        if total > MAX_UNCOMPRESSED_BYTES:
            raise BackupError(
                f"迁移包解压后超过 {MAX_UNCOMPRESSED_BYTES // (1024 ** 3)} GB，拒绝解包"
            )
    if dest is not None:
        free = shutil.disk_usage(dest).free
        if total > int(free * 0.9):
            raise BackupError(
                f"迁移包解压需要 {total} 字节，目标磁盘只剩 {free} 字节，拒绝解包"
            )


def export_archive(
    db_path: str | Path,
    dest: str | Path,
    *,
    key_path: str | Path | None = None,
    media_dir: str | Path | None = None,
    include_key: bool = False,
    include_media: bool = True,
) -> dict[str, Any]:
    """导出一个迁移包，返回 manifest + 实际文件大小。

    默认只带业务库和媒体文件；include_key=True 才会把 Cookie 密钥一起打包。
    数据库通过在线备份 API 取快照，服务运行中也可以导出。
    """
    db_path = Path(db_path)
    dest = Path(dest)
    if not db_path.is_file():
        raise BackupError(f"数据库不存在：{db_path}")
    dest.parent.mkdir(parents=True, exist_ok=True)

    key_file = Path(key_path) if key_path else None
    media_path = Path(media_dir) if media_dir else None
    if include_key and (key_file is None or not key_file.is_file()):
        raise BackupError(
            "要打包 cookie.key，但密钥文件不存在："
            f"{key_file or '(未指定)'}。请先确认账号已登录过，"
            "或去掉 --with-key 只备份业务数据"
        )
    key_used = bool(include_key)
    media_used = bool(include_media and media_path and media_path.is_dir())

    if dest.resolve() == db_path.resolve():
        raise BackupError("迁移包不能直接覆盖数据库文件，请换一个输出路径")
    if media_used and media_path is not None:
        try:
            dest.resolve().relative_to(media_path.resolve())
        except ValueError:
            pass
        else:
            raise BackupError("迁移包不能输出到 media 目录里，否则会把归档自身也打进去")

    with tempfile.TemporaryDirectory(dir=dest.parent, prefix=".backup-") as tmp:
        tmp_dir = Path(tmp)
        snapshot = tmp_dir / DB_NAME
        _snapshot_sqlite(db_path, snapshot)
        db_info = _inspect_db(snapshot)
        manifest: dict[str, Any] = {
            "format": FORMAT,
            "version": VERSION,
            "created_at": _now_iso(),
            "app": "xianyu-auto-reply",
            "db": {
                "name": DB_NAME,
                "bytes": snapshot.stat().st_size,
                "counts": db_info["counts"],
                "encrypted_accounts": db_info["encrypted_accounts"],
            },
            "includes_key": key_used,
            "includes_media": media_used,
        }
        payload = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")
        try:
            with tarfile.open(dest, "w:gz") as archive:
                info = tarfile.TarInfo(MANIFEST_NAME)
                info.size = len(payload)
                info.mode = 0o600
                info.mtime = int(datetime.now().timestamp())
                archive.addfile(info, io.BytesIO(payload))
                archive.add(snapshot, arcname=DB_NAME)
                if key_used and key_file is not None:
                    archive.add(key_file, arcname=KEY_NAME)
                if media_used and media_path is not None:
                    archive.add(media_path, arcname=MEDIA_DIR_NAME)
        except (OSError, tarfile.TarError) as exc:
            dest.unlink(missing_ok=True)
            raise BackupError(f"写迁移包失败：{exc}") from exc

    try:
        os.chmod(dest, 0o600)
    except OSError:
        pass
    manifest["path"] = str(dest)
    manifest["bytes"] = dest.stat().st_size
    return manifest


def inspect_archive(archive_path: str | Path) -> dict[str, Any]:
    """只读检查迁移包：manifest 是否合法、成员路径是否安全。"""
    archive_path = Path(archive_path)
    if not archive_path.is_file():
        raise BackupError(f"迁移包不存在：{archive_path}")
    try:
        with tarfile.open(archive_path, "r:gz") as archive:
            members = archive.getmembers()
            for member in members:
                _validate_member(member)
            names = [member.name for member in members]
            if len(names) != len(set(names)):
                raise BackupError("迁移包里有重复成员，拒绝解包")
            manifest_member = next((m for m in members if m.name == MANIFEST_NAME), None)
            if manifest_member is None:
                raise BackupError("迁移包里没有 manifest.json")
            if manifest_member.size > MAX_MANIFEST_BYTES:
                raise BackupError(
                    f"manifest.json 超过 {MAX_MANIFEST_BYTES} 字节，拒绝读取"
                )
            _check_archive_size(members)
            extracted = archive.extractfile(manifest_member)
            if extracted is None:
                raise BackupError("读不到 manifest.json")
            raw = extracted.read(MAX_MANIFEST_BYTES + 1)
    except BackupError:
        raise
    except (OSError, tarfile.TarError) as exc:
        raise BackupError(f"迁移包损坏或不是 tar.gz：{exc}") from exc
    try:
        manifest = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BackupError(f"manifest.json 不是合法 JSON：{exc}") from exc
    manifest = _validate_manifest(manifest)
    if DB_NAME not in names:
        raise BackupError("迁移包里没有 app.db")
    return manifest


def _extract_safe(archive_path: Path, dest: Path) -> None:
    """按白名单解包；拒绝绝对路径、..、链接和设备文件。"""
    with tarfile.open(archive_path, "r:gz") as archive:
        members = archive.getmembers()
        for member in members:
            _validate_member(member)
        _check_archive_size(members, dest=dest)
        for member in members:
            target = dest.joinpath(*_member_parts(member.name))
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            source = archive.extractfile(member)
            if source is None:
                raise BackupError(f"归档成员读取失败：{member.name}")
            with source, target.open("wb") as handle:
                shutil.copyfileobj(source, handle)
            if member.name == KEY_NAME:
                try:
                    target.chmod(0o600)
                except OSError:
                    pass


def restore_archive(
    archive_path: str | Path,
    db_path: str | Path,
    *,
    key_path: str | Path | None = None,
    media_dir: str | Path | None = None,
    restore_key: bool = True,
    restore_media: bool = True,
    keep_current: bool = True,
) -> dict[str, Any]:
    """校验并恢复迁移包。

    调用前必须先停掉服务：SQLite 文件被运行中的进程打开时替换，旧进程还会继续
    往旧 inode 写，轻则丢数据、重则两边各写一半。
    """
    archive_path = Path(archive_path)
    db_path = Path(db_path)
    key_target = Path(key_path) if key_path else None
    media_target = Path(media_dir) if media_dir else None
    manifest = inspect_archive(archive_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(dir=db_path.parent, prefix=".restore-") as tmp:
        tmp_dir = Path(tmp)
        _extract_safe(archive_path, tmp_dir)
        new_db = tmp_dir / DB_NAME
        db_info = _inspect_db(new_db)

        wants_key = bool(restore_key and (tmp_dir / KEY_NAME).is_file())
        if wants_key and key_target is None:
            key_target = db_path.with_name(KEY_NAME)
        if wants_key and key_target is not None:
            ok, reason = _check_key(new_db, tmp_dir / KEY_NAME)
            if not ok:
                raise BackupError(
                    "迁移包里的 cookie.key 与账号 Cookie 不匹配，"
                    "导入后所有账号都会解不开。请换一份完整的迁移包，"
                    f"或先用 --without-key 只恢复业务数据。原因：{reason}"
                )
        else:
            # 不带密钥：如果目标机器已有密钥，顺手检查它能不能解开新库；
            # 解不开不算致命错误（可以重新扫码），但要让调用方看到。
            ok = False
            reason = "missing_key"
            if key_target is not None and key_target.is_file():
                ok, reason = _check_key(new_db, key_target)

        pre_restore: Path | None = None
        if keep_current and db_path.exists():
            pre_restore = db_path.with_name(
                f"{db_path.stem}.pre-restore-{_stamp()}{db_path.suffix}"
            )
            _snapshot_sqlite(db_path, pre_restore)
            try:
                os.chmod(pre_restore, 0o600)
            except OSError:
                pass

        for suffix in ("-wal", "-shm"):
            Path(str(db_path) + suffix).unlink(missing_ok=True)
        os.replace(new_db, db_path)
        try:
            os.chmod(db_path, 0o600)
        except OSError:
            pass

        key_restored = False
        if wants_key and key_target is not None:
            key_target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(tmp_dir / KEY_NAME, key_target)
            try:
                os.chmod(key_target, 0o600)
            except OSError:
                pass
            key_restored = True

        media_restored = False
        extracted_media = tmp_dir / MEDIA_DIR_NAME
        if restore_media and media_target is not None and extracted_media.is_dir():
            media_target.mkdir(parents=True, exist_ok=True)
            shutil.copytree(extracted_media, media_target, dirs_exist_ok=True)
            media_restored = True

    return {
        "manifest": manifest,
        "db": str(db_path),
        "bytes": db_path.stat().st_size,
        "counts": db_info["counts"],
        "encrypted_accounts": db_info["encrypted_accounts"],
        "key_restored": key_restored,
        "key_status": "ok" if (key_restored or ok) else reason,
        "media_restored": media_restored,
        "pre_restore": str(pre_restore) if pre_restore else "",
    }


__all__ = [
    "FORMAT",
    "VERSION",
    "BackupError",
    "export_archive",
    "inspect_archive",
    "restore_archive",
]
