from __future__ import annotations

import json
import re
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from monitor_app.secrets import EncryptedFileSecretProvider

ID_PATTERN = re.compile(r"^[a-z0-9_-]+$")


@dataclass(frozen=True, slots=True)
class MonitorAccountConfig:
    account_id: str
    child_account_id: str
    child_account_name: str
    main_account_id: str
    main_account_name: str
    api_key: str
    api_secret: str
    uid: str = ""
    use_testnet: bool = False
    rest_base_url: str = ""
    ws_base_url: str = ""

    @property
    def name(self) -> str:
        return self.child_account_name

    @property
    def display_name(self) -> str:
        return f"{self.main_account_name} / {self.child_account_name}"

    @property
    def effective_rest_base_url(self) -> str:
        if self.rest_base_url:
            return self.rest_base_url
        if self.use_testnet:
            return "https://testnet.binancefuture.com"
        return "https://fapi.binance.com"

    @property
    def effective_websocket_base_url(self) -> str:
        if self.ws_base_url:
            return self.ws_base_url
        if self.use_testnet:
            return "wss://stream.binancefuture.com/ws"
        return "wss://fstream.binance.com/ws"


@dataclass(frozen=True, slots=True)
class MainAccountConfig:
    main_id: str
    name: str
    children: tuple[MonitorAccountConfig, ...]
    transfer_api_key: str = ""
    transfer_api_secret: str = ""
    transfer_uid: str = ""

    @property
    def has_transfer_credentials(self) -> bool:
        return bool(self.transfer_api_key and self.transfer_api_secret and self.transfer_uid)


def normalize_monitor_id(value: object, *, field_name: str) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    if ID_PATTERN.fullmatch(normalized) is None:
        raise ValueError(f"{field_name} must match {ID_PATTERN.pattern}")
    return normalized


def parse_monitor_bool(value: str | bool | None, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def parse_monitor_accounts_payload(
    payload: dict[str, Any],
    *,
    secret_provider: EncryptedFileSecretProvider | None = None,
    allow_plaintext_secrets: bool = True,
) -> tuple[dict[str, MainAccountConfig], dict[str, MonitorAccountConfig]]:
    raw_main_accounts = payload.get("main_accounts") if isinstance(payload, dict) else None
    if not isinstance(raw_main_accounts, list):
        raise ValueError("Monitor accounts file must contain a main_accounts array")

    main_accounts: dict[str, MainAccountConfig] = {}
    monitor_accounts: dict[str, MonitorAccountConfig] = {}
    for raw_main_account in raw_main_accounts:
        if not isinstance(raw_main_account, dict):
            raise ValueError("Each main account entry must be an object")
        main_id = normalize_monitor_id(raw_main_account.get("main_id"), field_name="main_id")
        if main_id in main_accounts:
            raise ValueError(f"Duplicate main_id: {main_id}")
        main_name = str(raw_main_account.get("name") or "").strip()
        if not main_name:
            raise ValueError(f"Main account {main_id} must define name")
        transfer_api_key = _resolve_secret_value(
            raw_main_account,
            plaintext_field="transfer_api_key",
            ref_field="transfer_api_key_secret_ref",
            secret_provider=secret_provider,
            allow_plaintext=allow_plaintext_secrets,
            required=False,
        )
        transfer_api_secret = _resolve_secret_value(
            raw_main_account,
            plaintext_field="transfer_api_secret",
            ref_field="transfer_api_secret_secret_ref",
            secret_provider=secret_provider,
            allow_plaintext=allow_plaintext_secrets,
            required=False,
        )
        transfer_uid = str(raw_main_account.get("transfer_uid") or "").strip()
        raw_children = raw_main_account.get("children")
        if not isinstance(raw_children, list) or not raw_children:
            raise ValueError(f"Main account {main_id} must define a non-empty children array")

        child_accounts: list[MonitorAccountConfig] = []
        seen_child_ids: set[str] = set()
        for raw_child in raw_children:
            if not isinstance(raw_child, dict):
                raise ValueError(f"Children for {main_id} must be objects")
            child_account_id = normalize_monitor_id(raw_child.get("account_id"), field_name="account_id")
            if child_account_id in seen_child_ids:
                raise ValueError(f"Duplicate child account_id under {main_id}: {child_account_id}")
            child_name = str(raw_child.get("name") or "").strip()
            if not child_name:
                raise ValueError(f"Child account {main_id}.{child_account_id} must define name")
            api_key = _resolve_secret_value(
                raw_child,
                plaintext_field="api_key",
                ref_field="api_key_secret_ref",
                secret_provider=secret_provider,
                allow_plaintext=allow_plaintext_secrets,
                required=True,
            )
            api_secret = _resolve_secret_value(
                raw_child,
                plaintext_field="api_secret",
                ref_field="api_secret_secret_ref",
                secret_provider=secret_provider,
                allow_plaintext=allow_plaintext_secrets,
                required=True,
            )
            if not api_key or not api_secret:
                raise ValueError(f"Child account {main_id}.{child_account_id} must define api_key and api_secret")
            composite_account_id = f"{main_id}.{child_account_id}"
            if composite_account_id in monitor_accounts:
                raise ValueError(f"Duplicate composite account id: {composite_account_id}")
            account = MonitorAccountConfig(
                account_id=composite_account_id,
                child_account_id=child_account_id,
                child_account_name=child_name,
                main_account_id=main_id,
                main_account_name=main_name,
                api_key=api_key,
                api_secret=api_secret,
                uid=str(raw_child.get("uid") or "").strip(),
                use_testnet=parse_monitor_bool(raw_child.get("use_testnet"), False),
                rest_base_url=str(raw_child.get("rest_base_url") or "").strip(),
                ws_base_url=str(raw_child.get("ws_base_url") or "").strip(),
            )
            child_accounts.append(account)
            monitor_accounts[composite_account_id] = account
            seen_child_ids.add(child_account_id)

        main_accounts[main_id] = MainAccountConfig(
            main_id=main_id,
            name=main_name,
            transfer_api_key=transfer_api_key,
            transfer_api_secret=transfer_api_secret,
            transfer_uid=transfer_uid,
            children=tuple(child_accounts),
        )

    return main_accounts, monitor_accounts


def _resolve_secret_value(
    payload: dict[str, Any],
    *,
    plaintext_field: str,
    ref_field: str,
    secret_provider: EncryptedFileSecretProvider | None,
    allow_plaintext: bool,
    required: bool,
) -> str:
    plaintext_value = str(payload.get(plaintext_field) or "").strip()
    ref_value = str(payload.get(ref_field) or "").strip()
    if ref_value:
        if secret_provider is None:
            raise ValueError(f"Secret provider is required for {ref_field}")
        try:
            return secret_provider.get_secret(ref_value)
        except KeyError as exc:
            raise ValueError(f"Secret ref not found: {ref_value}") from exc
    if plaintext_value and allow_plaintext:
        return plaintext_value
    if plaintext_value and not allow_plaintext:
        raise ValueError(f"{plaintext_field} plaintext value is not allowed in refs-only mode")
    if required:
        raise ValueError(f"{plaintext_field} is required")
    return ""


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(".env",),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        populate_by_name=True,
        validate_by_name=True,
        validate_by_alias=True,
    )

    monitor_app_name: str = "Binance Account Monitor"
    monitor_api_host: str = "127.0.0.1"
    monitor_api_port: int = 8010
    access_control_config_file: Path = Path("config/access_control.json")
    monitor_accounts_file: Path = Path("config/binance_monitor_accounts.json")
    monitor_history_db_path: Path = Path("data/monitor_history.db")
    monitor_refresh_interval_ms: int = 600_000
    monitor_refresh_timeout_ms: int = 60_000
    monitor_history_window_days: int = 7
    monitor_account_concurrency: int = 5
    monitor_history_max_rows: int = 300
    monitor_runtime_log_path: Path = Path("monitor.runtime.log")
    monitor_runtime_log_max_lines: int = 300
    monitor_runtime_log_trim_interval_s: int = 60
    binance_recv_window_ms: int = 5_000
    binance_core_timeout_ms: int = 4_000
    binance_secondary_timeout_ms: int = 2_500
    binance_core_retry_attempts: int = 5
    binance_secondary_retry_attempts: int = 3
    funding_transfer_write_enabled: bool = True
    funding_max_accounts_per_operation: int = 20
    funding_max_total_amount_per_operation: Decimal = Decimal("10000")
    funding_idempotency_ttl_seconds: int = 600
    funding_audit_max_rows: int = 2_000
    tg_enabled: bool = False
    tg_bot_token: str = ""
    tg_chat_id: str = ""
    tg_bot_token_secret_ref: str = ""
    tg_chat_id_secret_ref: str = ""
    tg_proxy_url: str = ""
    tg_max_queue_size: int = 50
    tg_dry_run: bool = False
    allow_plaintext_secrets: bool = False
    secrets_file: Path = Path("config/secrets.enc.json")
    monitor_master_key: str = ""
    monitor_master_key_file: str = ""
    env_file_path: Path = Path(".env")
    admin_idle_timeout_minutes: int = 120
    guest_idle_timeout_minutes: int = 120
    unimmr_alerts_enabled: bool = Field(
        default=False,
        validation_alias=AliasChoices("unimmr_alerts_enabled", "UNI_MMR_ALERTS_ENABLED", "UNIMMR_ALERTS_ENABLED"),
    )
    unimmr_alert_event_max_rows: int = 2_000
    auth_audit_max_rows: int = 2_000

    monitor_accounts: dict[str, MonitorAccountConfig] = Field(default_factory=dict)
    monitor_main_accounts: dict[str, MainAccountConfig] = Field(default_factory=dict)

    def load_monitor_accounts(self) -> None:
        path = self.monitor_accounts_file
        if not path.exists():
            self.monitor_accounts = {}
            self.monitor_main_accounts = {}
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"Invalid monitor accounts file: {path}") from exc
        provider = self.build_secret_provider(required=False)
        self.monitor_main_accounts, self.monitor_accounts = parse_monitor_accounts_payload(
            payload,
            secret_provider=provider,
            allow_plaintext_secrets=self.allow_plaintext_secrets,
        )

    def _normalize_id(self, value: object, *, field_name: str) -> str:
        return normalize_monitor_id(value, field_name=field_name)

    def _as_bool(self, value: str | bool | None, default: bool = False) -> bool:
        return parse_monitor_bool(value, default)

    def build_secret_provider(self, *, required: bool = False) -> EncryptedFileSecretProvider | None:
        master_key_file = str(self.monitor_master_key_file or "").strip()
        master_key = str(self.monitor_master_key or "").strip()
        if master_key_file:
            key_path = Path(master_key_file)
            if not key_path.exists():
                raise ValueError(f"Secret master key file not found: {key_path}")
            master_key = key_path.read_text(encoding="utf-8").strip()
        if not master_key:
            if required:
                raise ValueError("MONITOR_MASTER_KEY_FILE or MONITOR_MASTER_KEY is required")
            return None
        return EncryptedFileSecretProvider(self.secrets_file, master_key=master_key)

    def resolve_secret_value(
        self,
        *,
        secret_ref: str = "",
        plaintext_value: str = "",
        allow_plaintext: bool | None = None,
        required: bool = False,
        field_name: str = "secret",
    ) -> str:
        allow_plaintext_value = self.allow_plaintext_secrets if allow_plaintext is None else bool(allow_plaintext)
        ref_value = str(secret_ref or "").strip()
        if ref_value:
            provider = self.build_secret_provider(required=True)
            assert provider is not None
            try:
                return provider.get_secret(ref_value)
            except KeyError as exc:
                raise ValueError(f"Secret ref not found for {field_name}: {ref_value}") from exc
        direct_value = str(plaintext_value or "").strip()
        if direct_value and allow_plaintext_value:
            return direct_value
        if direct_value and not allow_plaintext_value:
            raise ValueError(f"{field_name} plaintext value is not allowed in refs-only mode")
        if required:
            raise ValueError(f"{field_name} is required")
        return ""

    def resolved_tg_bot_token(self) -> str:
        return self.resolve_secret_value(
            secret_ref=self.tg_bot_token_secret_ref,
            plaintext_value=self.tg_bot_token,
            allow_plaintext=self.allow_plaintext_secrets,
            required=False,
            field_name="tg_bot_token",
        )

    def resolved_tg_chat_id(self) -> str:
        return self.resolve_secret_value(
            secret_ref=self.tg_chat_id_secret_ref,
            plaintext_value=self.tg_chat_id,
            allow_plaintext=self.allow_plaintext_secrets,
            required=False,
            field_name="tg_chat_id",
        )

    def capture_runtime_env_overrides_snapshot(self) -> dict[str, str]:
        return {
            "monitor_master_key": str(self.monitor_master_key or "").strip(),
            "monitor_master_key_file": str(self.monitor_master_key_file or "").strip(),
            "tg_bot_token": str(self.tg_bot_token or "").strip(),
            "tg_chat_id": str(self.tg_chat_id or "").strip(),
            "tg_bot_token_secret_ref": str(self.tg_bot_token_secret_ref or "").strip(),
            "tg_chat_id_secret_ref": str(self.tg_chat_id_secret_ref or "").strip(),
        }

    def restore_runtime_env_overrides_snapshot(self, snapshot: dict[str, str] | None) -> None:
        snapshot = snapshot or {}
        self.monitor_master_key = str(snapshot.get("monitor_master_key") or "").strip()
        self.monitor_master_key_file = str(snapshot.get("monitor_master_key_file") or "").strip()
        self.tg_bot_token = str(snapshot.get("tg_bot_token") or "").strip()
        self.tg_chat_id = str(snapshot.get("tg_chat_id") or "").strip()
        self.tg_bot_token_secret_ref = str(snapshot.get("tg_bot_token_secret_ref") or "").strip()
        self.tg_chat_id_secret_ref = str(snapshot.get("tg_chat_id_secret_ref") or "").strip()

    def reload_runtime_env_overrides(self, *, env_content: str | None = None) -> None:
        if env_content is None:
            env_path = Path(self.env_file_path)
            if not env_path.exists():
                return
            env_content = env_path.read_text(encoding="utf-8")

        env_values: dict[str, str] = {}
        for raw_line in str(env_content or "").splitlines():
            if not raw_line or raw_line.lstrip().startswith("#") or "=" not in raw_line:
                continue
            key, value = raw_line.split("=", 1)
            env_values[key.strip()] = value.strip()

        if "MONITOR_MASTER_KEY" in env_values:
            self.monitor_master_key = str(env_values.get("MONITOR_MASTER_KEY") or "").strip()
        if "MONITOR_MASTER_KEY_FILE" in env_values:
            self.monitor_master_key_file = str(env_values.get("MONITOR_MASTER_KEY_FILE") or "").strip()
        if "TG_BOT_TOKEN" in env_values:
            self.tg_bot_token = str(env_values.get("TG_BOT_TOKEN") or "").strip()
        if "TG_CHAT_ID" in env_values:
            self.tg_chat_id = str(env_values.get("TG_CHAT_ID") or "").strip()
        if "TG_BOT_TOKEN_SECRET_REF" in env_values:
            self.tg_bot_token_secret_ref = str(env_values.get("TG_BOT_TOKEN_SECRET_REF") or "").strip()
        if "TG_CHAT_ID_SECRET_REF" in env_values:
            self.tg_chat_id_secret_ref = str(env_values.get("TG_CHAT_ID_SECRET_REF") or "").strip()


settings = Settings()
settings.load_monitor_accounts()
