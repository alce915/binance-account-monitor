from __future__ import annotations

import ipaddress
import json
from dataclasses import dataclass
from pathlib import Path

from monitor_app.config import Settings


@dataclass(frozen=True, slots=True)
class AccessControlConfig:
    enabled: bool
    whitelist_ips: tuple[str, ...]
    allow_plaintext_secrets: bool
    cookie_secure_mode: str
    guest_password: str
    admin_password: str
    session_secret: str


def default_access_control_payload() -> dict:
    return {
        "enabled": False,
        "whitelist_ips": [],
        "allow_plaintext_secrets": False,
        "cookie_secure_mode": "auto",
        "guest_password_secret_ref": "",
        "admin_password_secret_ref": "",
        "session_secret_secret_ref": "",
    }


def ensure_access_control_template(path: Path) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        return
    target.write_text(json.dumps(default_access_control_payload(), ensure_ascii=False, indent=2), encoding="utf-8")


def load_access_control_config(path: Path, *, settings: Settings | None = None) -> AccessControlConfig:
    raw_payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if not isinstance(raw_payload, dict):
        raise ValueError("Access control config must be an object")

    whitelist_ips = raw_payload.get("whitelist_ips") or []
    if not isinstance(whitelist_ips, list):
        raise ValueError("whitelist_ips must be an array")
    normalized_whitelist_ips = tuple(_normalize_ip_entry(item) for item in whitelist_ips if str(item or "").strip())

    cookie_secure_mode = str(raw_payload.get("cookie_secure_mode") or "auto").strip().lower()
    if cookie_secure_mode not in {"auto", "always", "never"}:
        raise ValueError("cookie_secure_mode must be one of: auto, always, never")

    allow_plaintext_secrets = bool(raw_payload.get("allow_plaintext_secrets", False))

    resolver = settings.resolve_secret_value if settings is not None else _resolve_plaintext_compatible_secret

    return AccessControlConfig(
        enabled=bool(raw_payload.get("enabled", False)),
        whitelist_ips=normalized_whitelist_ips,
        allow_plaintext_secrets=allow_plaintext_secrets,
        cookie_secure_mode=cookie_secure_mode,
        guest_password=resolver(
            secret_ref=str(raw_payload.get("guest_password_secret_ref") or "").strip(),
            plaintext_value=str(raw_payload.get("guest_password") or "").strip(),
            allow_plaintext=allow_plaintext_secrets,
            required=False,
            field_name="guest_password",
        ),
        admin_password=resolver(
            secret_ref=str(raw_payload.get("admin_password_secret_ref") or "").strip(),
            plaintext_value=str(raw_payload.get("admin_password") or "").strip(),
            allow_plaintext=allow_plaintext_secrets,
            required=False,
            field_name="admin_password",
        ),
        session_secret=resolver(
            secret_ref=str(raw_payload.get("session_secret_secret_ref") or "").strip(),
            plaintext_value=str(raw_payload.get("session_secret") or "").strip(),
            allow_plaintext=allow_plaintext_secrets,
            required=False,
            field_name="session_secret",
        ),
    )


def _normalize_ip_entry(value: object) -> str:
    normalized = str(value or "").strip()
    try:
        return str(ipaddress.ip_address(normalized))
    except ValueError as exc:
        raise ValueError(f"Invalid whitelist IP: {normalized}") from exc


def _resolve_plaintext_compatible_secret(
    *,
    secret_ref: str = "",
    plaintext_value: str = "",
    allow_plaintext: bool = False,
    required: bool = False,
    field_name: str = "secret",
) -> str:
    if secret_ref:
        raise ValueError(f"Secret provider is required for {field_name}")
    direct_value = str(plaintext_value or "").strip()
    if direct_value and allow_plaintext:
        return direct_value
    if direct_value and not allow_plaintext:
        raise ValueError(f"{field_name} plaintext value is not allowed in refs-only mode")
    if required:
        raise ValueError(f"{field_name} is required")
    return ""
