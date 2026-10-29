"""账号 Cookie 的落库加密（AES-256-GCM + 独立密钥文件）。

为什么需要它：accounts.cookie 是这套系统里最高价值的凭据 —— 拿到它等于
拿到闲鱼账号（能发消息、能点发货、能改价）。以前它是明文躺在 SQLite 里的：
data/app.db 权限虽然是 0600，但备份、scp、docker cp、给人看的工单附件里
带一份，就等于把账号一起送出去了。

设计取舍：
  * 只加密需要长期保密的字段（目前只有 accounts.cookie）。卡密和买家 Key
    本来就要发给买家，加密它们只是把明文从库里挪到内存，收益小得多。
  * 密文带版本前缀 enc:v1:，老数据（明文）在启动时自动就地迁移，不用手工操作。
  * 密钥默认在 data/cookie.key（0600）。**密钥丢了 = Cookie 全都读不出来 =
    重新扫码**。这是有意的：能绕过密钥的解密，等于没有加密。
  * 密钥文件必须和数据库分开备份（或者干脆不备份密钥）。README 里有说明。
"""

from __future__ import annotations

import base64
import logging
import os
import secrets
from pathlib import Path

logger = logging.getLogger(__name__)

#: 密文前缀。带版本号，以后换算法/换密钥时能平滑升级。
ENCRYPTED_PREFIX = "enc:v1:"
#: 任何以 enc: 开头的都当密文看待（不认识的版本宁可报错，也不能当明文发出去）
_ENCRYPTED_MARK = "enc:"
#: AES-256-GCM：32 字节密钥 + 12 字节 nonce
KEY_BYTES = 32
NONCE_BYTES = 12
#: 绑进 AEAD 的附加数据：这份密文只能当 Cookie 用，搬到别处解密会失败
_AAD = b"xianyu-auto-reply:cookie:v1"

try:  # pragma: no cover - 只有没装 cryptography 的环境才会走到 else
    from cryptography.exceptions import InvalidTag
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    CRYPTO_OK = True
except Exception:
    CRYPTO_OK = False

    class InvalidTag(Exception):  # type: ignore[no-redef]
        """占位异常：没装 cryptography 时保持接口可 import。"""


class SecretError(RuntimeError):
    """密钥不可用：没装依赖 / 密钥文件坏了 / 密钥对不上。"""


def is_encrypted(value: str) -> bool:
    """这串值是不是密文（含不认识的版本）。"""
    return str(value or "").startswith(_ENCRYPTED_MARK)


def _b64e(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _b64d(text: str) -> bytes:
    return base64.urlsafe_b64decode(text.encode("ascii"))


class CookieVault:
    """一个密钥文件 + 一组加解密方法。

    线程/协程都安全：密钥只在 ensure_key() 里读一次，之后只读不写；
    os.urandom 每次给新 nonce，所以同样的明文每次密文都不同。
    """

    def __init__(self, key_path: str | Path, *, enabled: bool = True) -> None:
        self.key_path = Path(key_path)
        self.enabled = bool(enabled)
        self._key: bytes | None = None
        #: 最近一次 decrypt 失败的原因（给上层做提示用）
        self.last_error = ""

    # ------------------------------------------------------------ 状态

    @property
    def mode(self) -> str:
        """给日志/后台看的一句话状态。"""
        if not self.enabled:
            return "plaintext（已关闭加密）"
        if not CRYPTO_OK:
            return "unavailable（缺少 cryptography）"
        return "aes-gcm"

    # ------------------------------------------------------------ 密钥

    def ensure_key(self) -> bytes:
        """读/建密钥文件，返回 32 字节密钥；不可用时抛 SecretError。"""
        if self._key is not None:
            return self._key
        if not self.enabled:
            raise SecretError("Cookie 加密已被关闭（XY_COOKIE_ENCRYPTION=0）")
        if not CRYPTO_OK:
            raise SecretError(
                "缺少 cryptography，账号 Cookie 无法加密/解密。"
                "请重跑 ./start.sh 安装依赖（requirements.lock 里已锁定），"
                "或显式设置 XY_COOKIE_ENCRYPTION=0 关闭加密（不建议）"
            )

        self.key_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.key_path.parent.chmod(0o700)
        except OSError:
            pass  # 不是自己的目录（比如挂载进来的 volume）就随它

        if not self.key_path.exists():
            key = secrets.token_bytes(KEY_BYTES)
            # O_EXCL：并发启动两个实例时只有一个能建，另一个会走到「已存在」分支
            try:
                fd = os.open(self.key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            except FileExistsError:
                pass
            else:
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    handle.write(key.hex() + "\n")
                logger.warning(
                    "已生成 Cookie 加密密钥 %s（0600）。请单独保管："
                    "密钥丢了库里所有 Cookie 都读不出来，只能重新扫码。",
                    self.key_path,
                )
                self._key = key
                return key
        else:
            # 已有密钥：先把权限收回来（有人可能 cp 过来带了 644）
            try:
                mode = self.key_path.stat().st_mode & 0o777
                if mode & 0o077:
                    logger.warning(
                        "Cookie 密钥文件 %s 权限过宽（%o），已收紧到 600", self.key_path, mode
                    )
                    self.key_path.chmod(0o600)
            except OSError:
                pass

        text = self.key_path.read_text(encoding="utf-8").strip()
        try:
            key = bytes.fromhex(text)
        except ValueError as exc:
            raise SecretError(
                f"Cookie 密钥文件 {self.key_path} 不是合法的十六进制，无法解密已存的 Cookie"
            ) from exc
        if len(key) != KEY_BYTES:
            raise SecretError(
                f"Cookie 密钥文件 {self.key_path} 长度不对（{len(key)} 字节，应为 {KEY_BYTES}）"
            )
        self._key = key
        return key

    # ------------------------------------------------------------ 加解密

    def encrypt(self, value: str) -> str:
        """明文 -> enc:v1:... ；空值/已加密/未启用时原样返回。"""
        text = str(value or "")
        if not text or not self.enabled or is_encrypted(text):
            return text
        key = self.ensure_key()
        nonce = os.urandom(NONCE_BYTES)
        blob = AESGCM(key).encrypt(nonce, text.encode("utf-8"), _AAD)
        return ENCRYPTED_PREFIX + _b64e(nonce + blob)

    def decrypt(self, value: str) -> str:
        """enc:v1:... -> 明文；老数据的明文原样返回；坏了就返回空串并置 last_error。

        返回空串而不是抛异常，是为了「一条坏数据不要拖垮整个后台列表」；
        调用方（db 层）会把它翻译成 cookie_error 展示出来。
        """
        text = str(value or "")
        if not text:
            return ""
        if not is_encrypted(text):
            return text  # 老数据：明文，交给启动迁移去加密
        if not text.startswith(ENCRYPTED_PREFIX):
            self.last_error = "密文版本不认识（可能是更新版本写的），请升级程序后再读"
            return ""
        if not self.enabled:
            self.last_error = "读到密文，但当前已关闭 Cookie 加密（XY_COOKIE_ENCRYPTION=0）"
            return ""
        try:
            key = self.ensure_key()
        except SecretError as exc:
            self.last_error = str(exc)
            return ""
        try:
            raw = _b64d(text[len(ENCRYPTED_PREFIX):])
        except Exception:
            self.last_error = "密文不是合法的 base64，无法解密"
            return ""
        if len(raw) <= NONCE_BYTES:
            self.last_error = "密文长度不对，无法解密"
            return ""
        nonce, blob = raw[:NONCE_BYTES], raw[NONCE_BYTES:]
        try:
            plain = AESGCM(key).decrypt(nonce, blob, _AAD)
        except InvalidTag:
            # 密钥换过 / 数据被改过 / 从别处拷来的密文：都属于「解不开」
            self.last_error = "Cookie 解密失败：密钥不匹配或数据被改过"
            return ""
        self.last_error = ""
        return plain.decode("utf-8", "replace")
