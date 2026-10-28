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

logger = logging.getLogger(__name__)

#: 单张图片允许的最大像素数（约 4000 万，和常见手机原图上限相当）。
#: 超过就直接拒绝：一张 100 MP 的图在 Pillow 里解码要 300 MB 内存，
#: 512 MB 的机器会直接 OOM，而闲鱼聊天图片根本不需要这么大。
MAX_IMAGE_PIXELS = 40_000_000

#: 单段视频允许的最大像素数。视频封面要解码一帧，16K 视频一帧就要几百 MB，
#: 512 MB 的机器会被一个 20 MB 的文件打爆；先按尺寸拒绝，再交给 ffmpeg。
MAX_VIDEO_PIXELS = 40_000_000

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

#: 内置图片扩展名 → MIME。服务端只按白名单后缀发 Content-Type，
#: 避免旧库/手改库把 .html 当成图片以 text/html 渲染。
IMAGE_MIMES = {ext.lstrip("."): mime for ext, mime in IMAGE_TYPES.values()}
IMAGE_MIMES["jpeg"] = "image/jpeg"

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

#: save_media_file 允许落盘的媒体后缀。调用方传进来的文件名不可信，
#: 未知后缀统一落成 .bin，交给下载而不是浏览器渲染。
_ALLOWED_MEDIA_SUFFIXES = (
    frozenset(ext for ext, _ in IMAGE_TYPES.values())
    | frozenset(f".{suffix}" for suffix in VIDEO_TYPE_IDS)
    | {".jpeg"}
)

#: 视频扩展名对应的 MIME。只用于把本机预览发回后台；未知后缀一律按附件下载，
#: 避免旧数据里的 .html 之类被浏览器当成网页渲染。
VIDEO_MIMES = {
    "mp4": "video/mp4",
    "avi": "video/x-msvideo",
    "rmvb": "application/vnd.rn-realmedia-vbr",
    "rm": "application/vnd.rn-realmedia",
    "mpg": "video/mpeg",
    "mpeg": "video/mpeg",
    "wmv": "video/x-ms-wmv",
    "mkv": "video/x-matroska",
    "vob": "video/mpeg",
    "mov": "video/quicktime",
    "webm": "video/webm",
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


def _pillow():
    """延迟导入 Pillow。

    Pillow 是一个纯 C 扩展的大包，导入就要 10 MB 以上的常驻内存；
    自动回复的绝大多数请求根本不碰图片。把导入放到真正需要的地方，
    可以让 512 MB 的小机器在空闲时少背这 10 MB。
    """
    try:
        from PIL import Image, UnidentifiedImageError
    except ImportError as exc:  # pragma: no cover - 依赖缺失
        raise MediaError("图片处理组件未安装") from exc
    # 自己检查像素数；把 Pillow 的阈值抬高，避免它在报错前先发 Warning。
    Image.MAX_IMAGE_PIXELS = MAX_IMAGE_PIXELS * 2
    return Image, UnidentifiedImageError


def _open_image(data: bytes):
    Image, unidentified = _pillow()
    try:
        return Image.open(BytesIO(data))
    except (unidentified, OSError) as exc:
        raise MediaError("无法识别图片格式，请上传 jpg / png / webp / gif") from exc
    except Exception as exc:
        # Pillow 对超大图抛 DecompressionBombError；它继承自 Exception 而非
        # OSError，这里按类名兜底，避免为了一个异常类型把 Pillow 再导一次。
        if exc.__class__.__name__ != "DecompressionBombError":
            raise
        raise MediaError("图片像素太大，请压缩后再上传") from exc


def _check_image(image) -> tuple[str, int, int]:
    fmt = (image.format or "").upper()
    width, height = image.size
    if fmt not in IMAGE_TYPES:
        raise MediaError(f"暂不支持 {fmt or '该'} 格式的图片")
    if width <= 0 or height <= 0:
        raise MediaError("图片尺寸无效")
    if width * height > MAX_IMAGE_PIXELS:
        raise MediaError("图片像素太大，请压缩后再上传")
    return fmt, width, height


def image_info(data: bytes) -> tuple[str, str, int, int]:
    """校验图片并返回 (format, mime, width, height)。"""
    with _open_image(data) as image:
        fmt, width, height = _check_image(image)
    _, mime = IMAGE_TYPES[fmt]
    return fmt, mime, width, height


def compress_image(data: bytes, *, max_side: int = 1920, target_bytes: int = 500 * 1024) -> ImageAsset:
    """把图片统一成 JPEG（透明区域铺白底），供闲鱼 CDN 上传。

    闲鱼网页端上传图片也是先压缩再传；原图几十 MB 会明显拖慢发送。
    GIF 只保留第一帧（当前只发静态图）。
    """
    with _open_image(data) as source:
        fmt, width, height = _check_image(source)
        Image, _ = _pillow()

        # JPEG 可以只按缩小尺寸解码（draft），一张 4000 万像素的原图
        # 解码像素能从 4000 万降到 1000 万；其他格式没有这个能力。
        # 注意 draft 的 size 是「解码后不要超过这个框」，而且 Pillow 只支持
        # 1/2、1/4、1/8 的缩放。传 max_side*2 对 4:3/16:9 的图往往不触发
        # （短边的一半仍然大于框），所以直接按原图一半要，强制走 1/2 解码。
        if fmt == "JPEG" and max(width, height) > max_side * 2:
            try:
                source.draft("RGB", (max(1, width // 2), max(1, height // 2)))
            except (ValueError, OSError):
                pass

        if source.mode in ("RGBA", "LA", "P"):
            image = source.convert("RGBA")
            background = Image.new("RGB", image.size, (255, 255, 255))
            background.paste(image, mask=image.split()[-1])
            image.close()
            image = background
        elif source.mode != "RGB":
            image = source.convert("RGB")
        else:
            image = source

        width, height = image.size
        if max(width, height) > max_side:
            if width >= height:
                new_size = (max_side, max(1, int(height * max_side / width)))
            else:
                new_size = (max(1, int(width * max_side / height)), max_side)
            resample = getattr(Image, "Resampling", Image).LANCZOS
            resized = image.resize(new_size, resample)
            if resized is not image:
                image.close()
            image = resized

        try:
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
        finally:
            image.close()


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


def _check_video_pixels(width: int, height: int) -> None:
    """拒绝分辨率大到足以在解码时打爆内存的视频。"""
    if width > 0 and height > 0 and width * height > MAX_VIDEO_PIXELS:
        raise MediaError(
            f"视频分辨率太大（{width}x{height}），最多支持 {MAX_VIDEO_PIXELS // 1_000_000} 百万像素"
        )


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
    _check_video_pixels(width, height)

    duration_text = stream.get("duration") or (payload.get("format") or {}).get("duration") or "0"
    try:
        duration_ms = max(0, int(float(duration_text) * 1000))
    except (TypeError, ValueError):
        duration_ms = 0
    return VideoInfo(width=width, height=height, duration_ms=duration_ms)


def video_poster(data: bytes, info: VideoInfo | None = None) -> bytes:
    """截取视频第一帧作为封面（JPEG）。

    info 可由调用方传入 probe_video 的结果，避免同一段视频探测两次；
    不传时这里会先探测并做像素上限检查，单独调用也不会被像素炸弹打爆。
    """
    if not has_ffmpeg():
        raise MediaError("服务器没有 ffmpeg/ffprobe，暂时无法处理视频")
    if info is None:
        info = probe_video(data)
    _check_video_pixels(info.width, info.height)
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


def _video_suffix(file_name: str) -> str:
    return Path(str(file_name or "")).suffix.lower().lstrip(".")


def video_file_type(file_name: str) -> int:
    return VIDEO_TYPE_IDS.get(_video_suffix(file_name), VIDEO_TYPE_IDS["mp4"])


def video_extension(file_name: str, default: str = ".mp4") -> str:
    """只接受已知视频扩展名。

    上传接口的原始文件名不可信；如果直接拿后缀拼本地文件名，
    把合法视频命名成 evil.html 就会在媒体目录里落下一个同源 HTML 文件。
    未知后缀统一回退成 .mp4，内容本身仍由 ffprobe 校验。
    """
    suffix = _video_suffix(file_name)
    return f".{suffix}" if suffix in VIDEO_TYPE_IDS else default


def video_mime(file_name: str) -> str | None:
    """视频本地预览的 MIME；未知/非视频后缀返回 None。"""
    return VIDEO_MIMES.get(_video_suffix(file_name))


def image_mime(file_name: str) -> str | None:
    """内置图片按后缀给出受控 MIME；未知后缀返回 None（走附件下载）。"""
    return IMAGE_MIMES.get(_video_suffix(file_name))


def save_media_file(root: Path, folder: str, data: bytes, suffix: str) -> str:
    """把媒体写到 media/<folder>/<uuid><suffix>，返回相对文件名。

    suffix 只允许图片/视频白名单；未知后缀落成 .bin，防止把上传文件名
    直接拼进媒体目录后留下可被浏览器渲染的 .html/.svg。
    """
    import uuid

    directory = local_media_path(root, folder)
    directory.mkdir(parents=True, exist_ok=True)
    suffix = str(suffix or "").lower()
    if not suffix.startswith("."):
        suffix = "." + suffix
    if suffix not in _ALLOWED_MEDIA_SUFFIXES:
        suffix = ".bin"
    name = uuid.uuid4().hex + suffix
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
