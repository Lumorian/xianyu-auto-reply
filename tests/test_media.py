"""媒体处理：图片压缩、上传响应解析、视频探测/封面。"""

from __future__ import annotations

import subprocess
from io import BytesIO
from pathlib import Path

import pytest
from PIL import Image

from app import media


def png_bytes(size: tuple[int, int] = (80, 60), color: str = "red") -> bytes:
    image = Image.new("RGBA", size, color)
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


def test_image_info_and_compress_to_jpeg():
    data = png_bytes()
    fmt, mime, width, height = media.image_info(data)
    assert (fmt, mime, width, height) == ("PNG", "image/png", 80, 60)

    asset = media.compress_image(data, max_side=40)
    assert asset.mime == "image/jpeg"
    assert asset.ext == ".jpg"
    assert max(asset.width, asset.height) <= 40
    assert asset.data.startswith(b"\xff\xd8")


def test_image_info_rejects_non_image():
    with pytest.raises(media.MediaError):
        media.image_info(b"not an image")


def test_parse_image_upload_response_variants():
    assert media.parse_image_upload_response({
        "success": True,
        "object": {"url": "https://img.alicdn.com/a.jpg", "pix": "800x600"},
    }) == ("https://img.alicdn.com/a.jpg", 800, 600)

    assert media.parse_image_upload_response({
        "data": {"url": "https://img.alicdn.com/b.png", "width": 12, "height": 34},
    }) == ("https://img.alicdn.com/b.png", 12, 34)

    with pytest.raises(media.MediaError):
        media.parse_image_upload_response({"success": True, "object": {"url": ""}})
    with pytest.raises(media.MediaError):
        media.parse_image_upload_response({
            "success": True,
            "object": {"url": "https://evil.example/a.jpg", "pix": "1x1"},
        })


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


def test_probe_video_and_poster(tmp_path):
    data = make_video(tmp_path)
    info = media.probe_video(data)
    assert info.width == 64
    assert info.height == 48
    assert 800 <= info.duration_ms <= 1200

    poster = media.video_poster(data)
    fmt, mime, width, height = media.image_info(poster)
    assert fmt == "JPEG"
    assert mime == "image/jpeg"
    assert (width, height) == (64, 48)


def test_video_pixel_bomb_rejected(monkeypatch):
    monkeypatch.setattr(media, "has_ffmpeg", lambda: True)
    payload = b'{"streams":[{"width":16000,"height":16000,"duration":"1"}]}'
    monkeypatch.setattr(media, "_run", lambda *args, **kwargs: payload)
    with pytest.raises(media.MediaError, match="分辨率太大"):
        media.probe_video(b"fake")
    with pytest.raises(media.MediaError, match="分辨率太大"):
        media.video_poster(b"fake", media.VideoInfo(width=16000, height=16000, duration_ms=1000))


def test_video_extension_whitelist():
    assert media.video_extension("clip.mp4") == ".mp4"
    assert media.video_extension("clip.MOV") == ".mov"
    assert media.video_extension("evil.html") == ".mp4"
    assert media.video_extension("no-extension") == ".mp4"
    assert media.video_mime("clip.mp4") == "video/mp4"
    assert media.video_mime("evil.html") is None


def test_save_media_file_and_path_traversal(tmp_path):
    name = media.save_media_file(tmp_path, "chat", b"abc", ".mp4")
    assert (tmp_path / "chat" / name).read_bytes() == b"abc"
    with pytest.raises(media.MediaError):
        media.local_media_path(tmp_path, "chat", "..", "..", "etc", "passwd")
    with pytest.raises(media.MediaError):
        media.local_media_path(tmp_path, "/etc/passwd")


def test_save_media_file_only_allows_known_media_suffixes(tmp_path):
    assert media.save_media_file(tmp_path, "chat", b"x", ".mp4").endswith(".mp4")
    assert media.save_media_file(tmp_path, "chat", b"x", "png").endswith(".png")
    assert media.save_media_file(tmp_path, "chat", b"x", ".html").endswith(".bin")
    assert media.image_mime("photo.PNG") == "image/png"
    assert media.image_mime("evil.html") is None
