"""后台聊天的图片 / 视频 / 撤回链路。"""

from __future__ import annotations

import os
import subprocess
import time
from io import BytesIO
from pathlib import Path
from typing import Any

import httpx
import pytest
from PIL import Image

from app import media
from app.config import settings
from app.db import Database, now_iso
from app.engine import DeliveryEngine
from app.runner import AccountWorker, WorkerManager
from app.web import create_app


def png_bytes(size=(80, 60), color="red") -> bytes:
    image = Image.new("RGB", size, color)
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


def make_video(tmp_path: Path) -> bytes:
    if not media.has_ffmpeg():
        pytest.skip("没有 ffmpeg")
    path = tmp_path / "sample.mp4"
    subprocess.run(
        [
            "ffmpeg", "-v", "error", "-f", "lavfi", "-i",
            "color=c=blue:s=64x48:d=1", "-pix_fmt", "yuv420p", str(path),
        ],
        check=True,
    )
    return path.read_bytes()


class MediaFakeClient:
    user_id = "2000000000002"

    def __init__(self) -> None:
        self.websocket = self
        self.texts: list[tuple[str, str, str]] = []
        self.images: list[dict[str, Any]] = []
        self.videos: list[dict[str, Any]] = []
        self.recalled: list[str] = []

    async def send_text_with_id(self, chat_id, to_id, text):
        self.texts.append((chat_id, to_id, text))
        return "m-text"

    async def upload_image_to_cdn(self, data):
        return {"url": "https://img.alicdn.com/demo.jpg", "width": 80, "height": 60}

    async def send_image_with_id(self, chat_id, to_id, url, width, height):
        self.images.append({
            "chat_id": chat_id, "to_id": to_id, "url": url,
            "width": width, "height": height,
        })
        return "m-image"

    async def upload_media_lwp(self, chat_id, data, *, file_name, file_type, width, height):
        return "auth-" + file_name

    async def send_video_with_id(self, chat_id, to_id, **kwargs):
        self.videos.append({"chat_id": chat_id, "to_id": to_id, **kwargs})
        return "m-video"

    async def recall_message(self, message_id):
        self.recalled.append(message_id)


async def build_worker(db: Database) -> tuple[AccountWorker, int, MediaFakeClient]:
    account_id = await db.upsert_account("主号", "unb=2000000000002; _m_h5_tk=token_1")
    account = await db.get_account(account_id)
    worker = AccountWorker(db, account, DeliveryEngine(db))
    client = MediaFakeClient()
    worker.client = client  # type: ignore[assignment]
    return worker, account_id, client


async def test_manual_text_records_server_message_id(db):
    worker, _account_id, client = await build_worker(db)
    message = await worker.send_manual_text("c1", "b1", "你好")
    assert client.texts == [("c1", "b1", "你好")]
    assert message["external_id"] == "m-text"
    row = await db.get_message(int(message["id"]))
    assert row is not None
    assert row["external_id"] == "m-text"
    assert row["message_type"] == "text"


async def test_manual_image_logs_media_and_url(db):
    worker, _account_id, client = await build_worker(db)
    message = await worker.send_manual_image("c1", "b1", png_bytes())
    assert client.images[0]["url"] == "https://img.alicdn.com/demo.jpg"
    row = await db.get_message(int(message["id"]))
    assert row is not None
    assert row["message_type"] == "image"
    assert row["media_url"] == "https://img.alicdn.com/demo.jpg"
    assert row["media_width"] == 80
    assert row["media_height"] == 60
    assert row["external_id"] == "m-image"


async def test_manual_video_uploads_poster_and_keeps_local_preview(db, tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "media_dir", tmp_path / "media")
    video = make_video(tmp_path)
    worker, _account_id, client = await build_worker(db)
    message = await worker.send_manual_video("c1", "b1", video, "demo.mp4")

    assert len(client.videos) == 1
    sent = client.videos[0]
    assert sent["file_name"] == "demo.mp4"
    assert sent["duration_ms"] >= 800
    assert sent["pic_media_id"] == "auth-poster.jpg"

    row = await db.get_message(int(message["id"]))
    assert row is not None
    assert row["message_type"] == "video"
    assert row["media_duration"] >= 800
    assert row["external_id"] == "m-video"
    assert row["local_media"]
    assert row["local_poster"]
    assert (tmp_path / "media" / "chat" / row["local_media"]).exists()
    assert (tmp_path / "media" / "chat" / row["local_poster"]).exists()


async def test_manual_video_forces_video_suffix_on_disk(db, tmp_path, monkeypatch):
    """文件名可以是任意文本，但落盘后缀必须是已知视频格式。"""
    monkeypatch.setattr(settings, "media_dir", tmp_path / "media")
    video = make_video(tmp_path)
    worker, _account_id, _client = await build_worker(db)
    message = await worker.send_manual_video("c1", "b1", video, "evil.html")

    row = await db.get_message(int(message["id"]))
    assert row is not None
    assert row["local_media"].endswith(".mp4")
    assert not row["local_media"].endswith(".html")
    assert (tmp_path / "media" / "chat" / row["local_media"]).exists()


async def test_manager_recalls_recent_outgoing_message(db):
    worker, account_id, client = await build_worker(db)
    message_id = await db.log_message(
        account_id=account_id, chat_id="c1", sender_id="b1", sender_name="买家小王",
        content="发错了", kind="outgoing", message_type="text",
        external_id="m-recall", created_at=now_iso(),
    )
    manager = WorkerManager(db, provisioner=object())  # type: ignore[arg-type]
    manager.workers[account_id] = worker

    await manager.recall_chat_message(account_id, "c1", message_id)
    assert client.recalled == ["m-recall"]
    row = await db.get_message(message_id)
    assert row is not None and row["recalled_at"]


async def test_manager_refuses_old_or_missing_id(db):
    worker, account_id, _ = await build_worker(db)
    old_id = await db.log_message(
        account_id=account_id, chat_id="c1", sender_id="b1", content="旧消息",
        kind="outgoing", external_id="m-old", created_at="2020-01-01 00:00:00",
    )
    no_id = await db.log_message(
        account_id=account_id, chat_id="c1", sender_id="b1", content="没有消息 ID",
        kind="outgoing", created_at=now_iso(),
    )
    manager = WorkerManager(db, provisioner=object())  # type: ignore[arg-type]
    manager.workers[account_id] = worker

    with pytest.raises(ValueError, match="两分钟"):
        await manager.recall_chat_message(account_id, "c1", old_id)
    with pytest.raises(ValueError, match="消息 ID"):
        await manager.recall_chat_message(account_id, "c1", no_id)
    with pytest.raises(LookupError):
        await manager.recall_chat_message(account_id, "c2", no_id)


class MediaManager:
    def __init__(self, db: Database) -> None:
        self.db = db
        self.images: list[dict[str, Any]] = []
        self.videos: list[dict[str, Any]] = []
        self.recalls: list[tuple[int, str, int]] = []

    async def sync(self) -> None:
        pass

    async def stop_all(self) -> None:
        pass

    def status(self) -> list[dict[str, Any]]:
        return []

    async def send_chat_image(self, account_id, chat_id, to_id, data, **kwargs):
        self.images.append({"account_id": account_id, "chat_id": chat_id, "size": len(data), **kwargs})
        return {
            "id": 101, "account_id": account_id, "chat_id": chat_id,
            "content": "[图片]", "kind": "outgoing", "message_type": "image",
            "media_url": "https://img.alicdn.com/demo.jpg", "created_at": now_iso(),
        }

    async def send_chat_video(self, account_id, chat_id, to_id, data, file_name, **kwargs):
        self.videos.append({"account_id": account_id, "chat_id": chat_id, "size": len(data), "file_name": file_name})
        return {
            "id": 102, "account_id": account_id, "chat_id": chat_id,
            "content": "[视频]", "kind": "outgoing", "message_type": "video",
            "created_at": now_iso(),
        }

    async def recall_chat_message(self, account_id, chat_id, message_id):
        self.recalls.append((account_id, chat_id, message_id))
        return {"ok": True, "message_id": message_id}


@pytest.fixture()
async def media_client(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "media_dir", tmp_path / "media")
    db = Database(tmp_path / "web.db")
    manager = MediaManager(db)
    app = create_app(db=db, manager=manager)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1") as ac:
        async with app.router.lifespan_context(app):
            yield ac, manager, db


async def test_builtin_image_upload_list_content_delete(media_client):
    client, _, _ = media_client
    res = await client.post(
        "/api/media/images",
        files={"file": ("demo.png", png_bytes(), "image/png")},
        data={"name": "发货说明"},
    )
    assert res.status_code == 200
    image = res.json()["image"]
    assert image["name"] == "发货说明"
    assert image["width"] == 80

    rows = (await client.get("/api/media/images")).json()
    assert len(rows) == 1
    content = await client.get(rows[0]["content_url"])
    assert content.status_code == 200
    assert content.content == png_bytes()

    assert (await client.delete(f"/api/media/images/{image['id']}")).status_code == 200
    assert (await client.get("/api/media/images")).json() == []


async def test_builtin_image_content_ignores_db_mime(media_client):
    """内置图片按磁盘后缀发 MIME，旧库里的 text/html 不能生效。"""
    client, _, db = media_client
    folder = settings.media_dir / "builtin"
    folder.mkdir(parents=True, exist_ok=True)
    name = "legacy.html"
    payload = b"<script>alert(1)</script>"
    (folder / name).write_bytes(payload)
    image_id = await db.save_builtin_image(
        name="旧图", filename=name, mime="text/html",
        size=len(payload), width=1, height=1,
    )

    got = await client.get(f"/api/media/images/{image_id}/content")
    assert got.status_code == 200
    assert got.content == payload
    assert got.headers["content-type"] == "application/octet-stream"
    assert got.headers["content-disposition"] == "attachment"


async def test_prune_media_dir_removes_old_chat_copies(tmp_path):
    """聊天视频的本地副本要跟消息保留期一起清理；新文件不能误删。"""
    root = tmp_path / "media"
    chat = root / "chat"
    chat.mkdir(parents=True)
    old = chat / "old.mp4"
    new = chat / "new.mp4"
    old.write_bytes(b"old")
    new.write_bytes(b"new")
    old_time = time.time() - 40 * 86400
    os.utime(old, (old_time, old_time))

    assert media.prune_media_dir(root, "chat", 30) == 1
    assert not old.exists()
    assert new.exists()
    assert media.prune_media_dir(root, "chat", 0) == 0


async def test_upload_over_limit_is_rejected(media_client, monkeypatch):
    """超限上传要尽早 413，不能先把整个文件读进内存。"""
    client, _, _ = media_client
    monkeypatch.setattr(settings, "max_image_mb", 0)
    res = await client.post(
        "/api/media/images",
        files={"file": ("big.png", png_bytes(), "image/png")},
    )
    assert res.status_code == 413


async def test_send_builtin_image_and_local_file(media_client):
    client, manager, _ = media_client
    uploaded = (await client.post(
        "/api/media/images",
        files={"file": ("demo.png", png_bytes(), "image/png")},
        data={"name": "图"},
    )).json()["image"]

    res = await client.post("/api/chats/c1/send-image", json={
        "account_id": 1, "image_id": uploaded["id"], "to_id": "b1", "buyer_name": "买家小王",
    })
    assert res.status_code == 200
    assert res.json()["message"]["message_type"] == "image"
    assert manager.images[0]["size"] == len(png_bytes())

    res = await client.post(
        "/api/chats/c1/send-image-file",
        files={"file": ("one.png", png_bytes(), "image/png")},
        data={"account_id": "1", "to_id": "b1", "buyer_name": "买家小王"},
    )
    assert res.status_code == 200
    assert len(manager.images) == 2


async def test_send_video_endpoint(media_client, tmp_path):
    client, manager, _ = media_client
    video = make_video(tmp_path)
    res = await client.post(
        "/api/chats/c1/send-video",
        files={"file": ("demo.mp4", video, "video/mp4")},
        data={"account_id": "1", "to_id": "b1"},
    )
    assert res.status_code == 200
    assert manager.videos[0]["file_name"] == "demo.mp4"


async def test_recall_endpoint_and_local_media(media_client, tmp_path):
    client, manager, db = media_client
    account_id = await db.upsert_account("主号", "cookie")
    media_name = media.save_media_file(settings.media_dir, "chat", b"video-bytes", ".mp4")
    message_id = await db.log_message(
        account_id=account_id, chat_id="c1", sender_id="b1", content="[视频]",
        kind="outgoing", message_type="video", local_media=media_name,
        external_id="m-video", created_at=now_iso(),
    )

    res = await client.post("/api/chats/c1/recall", json={
        "account_id": account_id, "message_id": message_id,
    })
    assert res.status_code == 200
    assert manager.recalls == [(account_id, "c1", message_id)]

    got = await client.get(f"/api/messages/{message_id}/media?account_id={account_id}&kind=media")
    assert got.status_code == 200
    assert got.content == b"video-bytes"
    assert got.headers["content-type"] == "video/mp4"
    assert "attachment" not in got.headers.get("content-disposition", "")
    assert (await client.get(f"/api/messages/{message_id}/media?account_id=999&kind=media")).status_code == 404


async def test_local_media_never_served_as_html(media_client):
    """旧数据里即使有 .html 本地文件，也只能当附件下载，不能同源渲染。"""
    client, _, db = media_client
    account_id = await db.upsert_account("主号", "cookie")
    payload = b"<script>alert(1)</script>"
    # 模拟旧版本/手工改库留下的 .html 文件：新 save_media_file 已不会产生这种后缀
    folder = settings.media_dir / "chat"
    folder.mkdir(parents=True, exist_ok=True)
    media_name = "legacy-evil.html"
    (folder / media_name).write_bytes(payload)
    message_id = await db.log_message(
        account_id=account_id, chat_id="c1", sender_id="b1", content="[视频]",
        kind="outgoing", message_type="video", local_media=media_name,
        external_id="m-evil", created_at=now_iso(),
    )

    got = await client.get(f"/api/messages/{message_id}/media?account_id={account_id}&kind=media")
    assert got.status_code == 200
    assert got.content == payload
    assert got.headers["content-type"] == "application/octet-stream"
    assert got.headers["content-disposition"] == "attachment"
    assert "text/html" not in got.headers["content-type"]
