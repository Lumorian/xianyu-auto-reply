"""媒体处理：图片压缩/校验、视频探测与封面、闲鱼上传响应解析。

这里只做纯本地处理，不碰网络；WebSocket / HTTP 上传分别在 client.py 里。
"""

from __future__ import annotations

import json
import logging
import re
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from PIL import Image, UnidentifiedImageError

logger = logging.getLogger(__name__)

#: 闲鱼 IM 图片上传入口（来自网页端 / goofish-cli 的稳定实现）
IMAGE_UPLOAD_URL = "https://stream-upload.goofish.com/api/upload.api"
IMAGE_UPLOAD_PARAMS = {"floderId": "0", "appkey": "xy_chat", "_input_charset": "utf-8"}

#: 允许的图片格式 → 给 CDN 用的扩展名 / MIME
IMAGE_TYPES: dict[str, tuple[str, str]] = {
    "JPEG": (".jpg", "image/jpeg"),
    "PNG": (".png", "image/png"),
    "WEBP": (".webp", "image/webp"),
    "GIF": (".gif", "image/gif"),
    "BMP": (".bmp", "image/bmp"),
}

#: 视频文件类型 ID（闲鱼 ContentModel 的 fileType 字段）
VIDEO_TYPE_IDS = {
    "mp4": 6,
    "avi": 21,
    "rmvb": 22,
    "rm": 23,
    "mpg": 24,
    "mpeg": 24,
    "wmv": 25,
    "mkv": 26,
    "vob": 27,
    "mov": 6,
    "webm": 6,
}

_ALICDN_RE = re.compile(r'https://[^"\'\s,]+(?:alicdn\.com|goofish\.com)[^"\'\s,]+')


class MediaError(RuntimeError):
    """媒体处理失败（格式不支持、ffmpeg 不可用、上传响应不可解析等）。"""


@dataclass(slots=True)
class ImageAsset:
    data: bytes
    mime: str
    ext: str
    width: int
    height: int


@dataclass(slots=True)
class VideoInfo:
    width: int
    height: int
    duration_ms: int


def extension_for_mime(mime: str, default: str = ".jpg") -> str:
    """MIME → 常用扩展名（内置图库落盘用）。"""
    for ext, candidate in (
        (".jpg", "image/jpeg"),
        (".png", "image/png"),
        (".webp", "image/webp"),
        (".gif", "image/gif"),
        (".bmp", "image/bmp"),
    ):
        if mime == candidate:
            return ext
    return default


def image_info(data: bytes) -> tuple[str, str, int, int]:
    """校验图片并返回 (format, mime, width, height)。"""
    try:
        with Image.open(BytesIO(data)) as image:
            fmt = (image.format or "").upper()
            width, height = image.size
    except (UnidentifiedImageError, OSError) as exc:
        raise MediaError("无法识别图片格式，请上传 jpg / png / webp / gif") from exc
    if fmt not in IMAGE_TYPES:
        raise MediaError(f"暂不支持 {fmt or '该'} 格式的图片")
    if width <= 0 or height <= 0:
        raise MediaError("图片尺寸无效")
    _, mime = IMAGE_TYPES[fmt]
    return fmt, mime, width, height


def compress_image(data: bytes, *, max_side: int = 1920, target_bytes: int = 500 * 1024) -> ImageAsset:
    """把图片统一成 JPEG（透明区域铺白底），供闲鱼 CDN 上传。

    闲鱼网页端上传图片也是先压缩再传；原图几十 MB 会明显拖慢发送。
    GIF 只保留第一帧（当前只发静态图）。
    """
    image_info(data)  # 先校验格式，后面统一转 JPEG
    with Image.open(BytesIO(data)) as image:
        if image.mode in ("RGBA", "LA", "P"):
            image = image.convert("RGBA")
            background = Image.new("RGB", image.size, (255, 255, 255))
            background.paste(image, mask=image.split()[-1])
            image = background
        elif image.mode != "RGB":
            image = image.convert("RGB")

        width, height = image.size
        if max(width, height) > max_side:
            if width >= height:
                new_size = (max_side, max(1, int(height * max_side / width)))
            else:
                new_size = (max(1, int(width * max_side / height)), max_side)
            resample = getattr(Image, "Resampling", Image).LANCZOS
            image = image.resize(new_size, resample)

        for quality in range(85, 19, -5):
            buffer = BytesIO()
            image.save(buffer, format="JPEG", quality=quality, optimize=True)
            output = buffer.getvalue()
            if len(output) <= target_bytes:
                return ImageAsset(output, "image/jpeg", ".jpg", image.width, image.height)

        buffer = BytesIO()
        image.save(buffer, format="JPEG", quality=20, optimize=True)
        output = buffer.getvalue()
        return ImageAsset(output, "image/jpeg", ".jpg", image.width, image.height)


def parse_image_upload_response(payload: dict[str, Any]) -> tuple[str, int, int]:
    """解析 stream-upload 的响应，返回 (url, width, height)。

    成功响应形如：
        {"success": true, "object": {"url": "https://img.alicdn.com/...",
                                      "pix": "800x600", "size": 12345}}
    少数版本是 data / result / 数组，全部兜底。
    """
    if not isinstance(payload, dict):
        raise MediaError("图片上传响应格式异常")
    if payload.get("success") is False:
        marker = str(payload.get("status") or payload.get("message") or "")
        raise MediaError("图片上传被拒绝" + (f"：{marker}" if marker else ""))

    candidates: list[Any] = [payload.get("object"), payload.get("data"), payload.get("result")]
    candidates.extend(value for value in payload.values() if isinstance(value, list))
    url = ""
    width = height = 0
    for candidate in candidates:
        if isinstance(candidate, list) and candidate:
            candidate = candidate[0]
        if not isinstance(candidate, dict):
            continue
        url = str(candidate.get("url") or candidate.get("fileUrl") or candidate.get("cdnUrl") or "")
        pix = str(candidate.get("pix") or "")
        match = re.fullmatch(r"(\d+)x(\d+)", pix)
        if match:
            width, height = int(match.group(1)), int(match.group(2))
        else:
            try:
                width = int(candidate.get("width") or 0)
                height = int(candidate.get("height") or 0)
            except (TypeError, ValueError):
                width = height = 0
        if url:
            break

    if not url:
        # 有些版本把 URL 放在顶层 / 深处，用正则兜一次
        text = json.dumps(payload, ensure_ascii=False)
        match = _ALICDN_RE.search(text)
        if match:
            url = match.group(0)
    if not url:
        raise MediaError("图片上传成功但响应里没有图片地址")

    parsed = urlsplit(url)
    host = parsed.hostname or ""
    if parsed.scheme != "https" or not (
        host == "alicdn.com" or host.endswith(".alicdn.com")
        or host == "goofish.com" or host.endswith(".goofish.com")
    ):
        raise MediaError("图片上传返回的地址不是闲鱼 CDN，已拒绝使用")
    if width <= 0 or height <= 0:
        raise MediaError("图片上传响应缺少有效尺寸")
    return url, width, height


def has_ffmpeg() -> bool:
    return bool(shutil.which("ffmpeg") and shutil.which("ffprobe"))


def _run(cmd: list[str], data: bytes, *, timeout: float = 60.0) -> bytes:
    try:
        result = subprocess.run(  # noqa: S603 - 命令由本模块写死，输入只从 stdin 走
            cmd, input=data, capture_output=True, timeout=timeout, check=False
        )
    except FileNotFoundError as exc:
        raise MediaError("服务器没有 ffmpeg/ffprobe，暂时无法处理视频") from exc
    except subprocess.TimeoutExpired as exc:
        raise MediaError("视频处理超时，请换一个更短/更小的文件") from exc
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip().splitlines()[-1:]
        raise MediaError("视频处理失败" + (f"：{detail[0]}" if detail else ""))
    return result.stdout


def probe_video(data: bytes) -> VideoInfo:
    """用 ffprobe 读取视频宽高与时长（毫秒）。"""
    if not has_ffmpeg():
        raise MediaError("服务器没有 ffmpeg/ffprobe，暂时无法处理视频")
    with tempfile.NamedTemporaryFile(suffix=".mp4") as handle:
        handle.write(data)
        handle.flush()
        result = _run(
            [
                "ffprobe", "-v", "error", "-select_streams", "v:0",
                "-show_entries", "stream=width,height,duration:format=duration",
                "-of", "json", handle.name,
            ],
            b"",
            timeout=30.0,
        )
    try:
        payload = json.loads(result.decode("utf-8", errors="replace"))
    except ValueError as exc:
        raise MediaError("ffprobe 返回了无法解析的结果") from exc
    streams = payload.get("streams") or []
    stream = streams[0] if streams and isinstance(streams[0], dict) else {}
    try:
        width = int(stream.get("width") or 0)
        height = int(stream.get("height") or 0)
    except (TypeError, ValueError):
        width = height = 0
    if width <= 0 or height <= 0:
        raise MediaError("读不到视频尺寸，可能不是有效的视频文件")

    duration_text = stream.get("duration") or (payload.get("format") or {}).get("duration") or "0"
    try:
        duration_ms = max(0, int(float(duration_text) * 1000))
    except (TypeError, ValueError):
        duration_ms = 0
    return VideoInfo(width=width, height=height, duration_ms=duration_ms)


def video_poster(data: bytes) -> bytes:
    """截取视频第一帧作为封面（JPEG）。"""
    if not has_ffmpeg():
        raise MediaError("服务器没有 ffmpeg/ffprobe，暂时无法处理视频")
    with tempfile.NamedTemporaryFile(suffix=".mp4") as handle:
        handle.write(data)
        handle.flush()
        output = _run(
            [
                "ffmpeg", "-v", "error", "-ss", "0.1", "-i", handle.name,
                "-frames:v", "1", "-vf", "scale='min(1280,iw)':-2",
                "-f", "image2pipe", "-vcodec", "mjpeg", "-q:v", "3", "pipe:1",
            ],
            b"",
            timeout=60.0,
        )
    if not output:
        raise MediaError("截取视频封面失败")
    return compress_image(output, max_side=1280).data


def video_file_type(file_name: str) -> int:
    suffix = Path(str(file_name or "")).suffix.lower().lstrip(".")
    return VIDEO_TYPE_IDS.get(suffix, VIDEO_TYPE_IDS["mp4"])


def extension_of(file_name: str, default: str = ".mp4") -> str:
    suffix = Path(str(file_name or "")).suffix.lower()
    if not suffix or not re.fullmatch(r"\.[a-z0-9]{1,8}", suffix):
        return default
    return suffix


def save_media_file(root: Path, folder: str, data: bytes, suffix: str) -> str:
    """把媒体写到 media/<folder>/<uuid><suffix>，返回相对文件名。"""
    import uuid

    directory = local_media_path(root, folder)
    directory.mkdir(parents=True, exist_ok=True)
    name = uuid.uuid4().hex + (suffix if suffix.startswith(".") else "." + suffix)
    target = local_media_path(root, folder, name)
    target.write_bytes(data)
    return name


def prune_media_dir(root: Path, folder: str, days: int) -> int:
    """删掉 folder 下超过 days 天的本地媒体副本，返回删除数量。

    只给聊天视频的本地副本用：图库里的内置图片不能按消息保留期删。
    days <= 0 表示不清理。
    """
    if days <= 0:
        return 0
    directory = local_media_path(root, folder)
    if not directory.is_dir():
        return 0
    cutoff = time.time() - days * 86400
    removed = 0
    for path in directory.iterdir():
        try:
            if not path.is_file() or path.stat().st_mtime >= cutoff:
                continue
            path.unlink()
            removed += 1
        except OSError:
            logger.warning("清理媒体文件失败：%s", path, exc_info=True)
    return removed


def local_media_path(root: Path, *parts: str) -> Path:
    """在 media 根目录下拼一个安全路径（拒绝 .. / 绝对路径）。"""
    root = Path(root).resolve()
    target = (root.joinpath(*parts)).resolve()
    if root != target and root not in target.parents:
        raise MediaError("非法的媒体路径")
    return target
