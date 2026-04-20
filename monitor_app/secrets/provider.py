from __future__ import annotations

import base64
import json
import os
import tempfile
from pathlib import Path
from threading import RLock
from typing import Any

_STORE_VERSION = 1
_STORE_ALGORITHM = "AES-256-GCM+PBKDF2-SHA256"
_SALT_LENGTH = 16
_NONCE_LENGTH = 12
_DERIVED_KEY_LENGTH = 32
_PBKDF2_ITERATIONS = 390_000


def _b64encode(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _b64decode(data: str, *, field_name: str) -> bytes:
    try:
        return base64.b64decode(data.encode("ascii"), validate=True)
    except Exception as exc:  # pragma: no cover - invalid encodings are environment-specific
        raise ValueError(f"Invalid base64 value for {field_name}") from exc


def create_master_key() -> str:
    return _b64encode(os.urandom(_DERIVED_KEY_LENGTH))


def _load_crypto() -> tuple[type, type, type]:
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        from cryptography.hazmat.primitives.hashes import SHA256
        from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    except ModuleNotFoundError as exc:  # pragma: no cover - depends on local environment
        raise RuntimeError("cryptography is required for encrypted secret storage") from exc
    return AESGCM, SHA256, PBKDF2HMAC


def _derive_key(master_key: str, *, salt: bytes) -> bytes:
    _, sha256_cls, pbkdf2_cls = _load_crypto()
    material = _b64decode(master_key.strip(), field_name="master_key")
    if len(material) < _DERIVED_KEY_LENGTH:
        raise ValueError("Master key must decode to at least 32 bytes")
    kdf = pbkdf2_cls(
        algorithm=sha256_cls(),
        length=_DERIVED_KEY_LENGTH,
        salt=salt,
        iterations=_PBKDF2_ITERATIONS,
    )
    return kdf.derive(material)


class EncryptedFileSecretProvider:
    def __init__(self, path: Path, *, master_key: str) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._master_key = str(master_key or "").strip()
        if not self._master_key:
            raise ValueError("Secret store master key is required")
        self._lock = RLock()

    def get_secret(self, ref: str) -> str:
        normalized = str(ref or "").strip()
        if not normalized:
            raise ValueError("Secret ref is required")
        store = self._load_store()
        if normalized not in store:
            raise KeyError(f"Secret ref not found: {normalized}")
        return str(store[normalized])

    @property
    def path(self) -> Path:
        return self._path

    def set_secret(self, ref: str, value: str) -> None:
        normalized = str(ref or "").strip()
        if not normalized:
            raise ValueError("Secret ref is required")
        stored_value = str(value or "")
        with self._lock:
            store = self._load_store_locked()
            store[normalized] = stored_value
            self._write_store_locked(store)

    def delete_secret(self, ref: str) -> None:
        normalized = str(ref or "").strip()
        if not normalized:
            raise ValueError("Secret ref is required")
        with self._lock:
            store = self._load_store_locked()
            if normalized in store:
                del store[normalized]
                self._write_store_locked(store)

    def has_secret(self, ref: str) -> bool:
        normalized = str(ref or "").strip()
        if not normalized:
            return False
        return normalized in self._load_store()

    def list_secret_refs(self) -> list[str]:
        return sorted(self._load_store().keys())

    def dump_store(self) -> dict[str, str]:
        return dict(self._load_store())

    def replace_store(self, store: dict[str, str]) -> None:
        normalized_store = {str(key): str(value) for key, value in dict(store).items()}
        with self._lock:
            self._write_store_locked(normalized_store)

    def _load_store(self) -> dict[str, str]:
        with self._lock:
            return self._load_store_locked()

    def _load_store_locked(self) -> dict[str, str]:
        if not self._path.exists():
            return {}
        try:
            envelope = json.loads(self._path.read_text(encoding="utf-8"))
        except Exception as exc:  # pragma: no cover - filesystem/json failures vary
            raise ValueError("Failed to read secret store") from exc
        if not isinstance(envelope, dict):
            raise ValueError("Secret store must be an object")
        if int(envelope.get("version") or 0) != _STORE_VERSION:
            raise ValueError("Unsupported secret store version")
        if str(envelope.get("algorithm") or "") != _STORE_ALGORITHM:
            raise ValueError("Unsupported secret store algorithm")
        salt = _b64decode(str(envelope.get("salt") or ""), field_name="salt")
        nonce = _b64decode(str(envelope.get("nonce") or ""), field_name="nonce")
        ciphertext = _b64decode(str(envelope.get("ciphertext") or ""), field_name="ciphertext")
        try:
            aesgcm_cls, _, _ = _load_crypto()
            key = _derive_key(self._master_key, salt=salt)
            plaintext = aesgcm_cls(key).decrypt(nonce, ciphertext, None)
        except Exception as exc:
            raise ValueError("Failed to decrypt secret store") from exc
        try:
            payload = json.loads(plaintext.decode("utf-8"))
        except Exception as exc:
            raise ValueError("Secret store payload is invalid") from exc
        if not isinstance(payload, dict):
            raise ValueError("Secret store payload must be an object")
        return {str(key): str(value) for key, value in payload.items()}

    def _write_store_locked(self, store: dict[str, str]) -> None:
        aesgcm_cls, _, _ = _load_crypto()
        salt = os.urandom(_SALT_LENGTH)
        nonce = os.urandom(_NONCE_LENGTH)
        key = _derive_key(self._master_key, salt=salt)
        plaintext = json.dumps(store, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ciphertext = aesgcm_cls(key).encrypt(nonce, plaintext, None)
        envelope: dict[str, Any] = {
            "version": _STORE_VERSION,
            "algorithm": _STORE_ALGORITHM,
            "salt": _b64encode(salt),
            "nonce": _b64encode(nonce),
            "ciphertext": _b64encode(ciphertext),
        }
        serialized = json.dumps(envelope, ensure_ascii=False, indent=2) + "\n"
        temp_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self._path.parent,
                delete=False,
                prefix=f"{self._path.stem}-",
                suffix=".tmp",
            ) as handle:
                handle.write(serialized)
                temp_path = Path(handle.name)
            os.replace(temp_path, self._path)
        except OSError:
            if temp_path is not None and temp_path.exists():
                temp_path.unlink(missing_ok=True)
            raise
