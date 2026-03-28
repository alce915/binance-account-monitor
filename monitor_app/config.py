from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


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
        transfer_api_key = str(raw_main_account.get("transfer_api_key") or "").strip()
        transfer_api_secret = str(raw_main_account.get("transfer_api_secret") or "").strip()
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
            api_key = str(raw_child.get("api_key") or "").strip()
            api_secret = str(raw_child.get("api_secret") or "").strip()
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


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(".env",),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    monitor_app_name: str = "Binance Account Monitor"
    monitor_api_host: str = "127.0.0.1"
    monitor_api_port: int = 8010
    monitor_accounts_file: Path = Path("config/binance_monitor_accounts.json")
    monitor_history_db_path: Path = Path("data/monitor_history.db")
    monitor_refresh_interval_ms: int = 600_000
    monitor_refresh_timeout_ms: int = 60_000
    monitor_history_window_days: int = 7
    monitor_account_concurrency: int = 5
    binance_recv_window_ms: int = 5_000
    binance_core_timeout_ms: int = 4_000
    binance_secondary_timeout_ms: int = 2_500
    binance_core_retry_attempts: int = 5
    binance_secondary_retry_attempts: int = 3

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
        self.monitor_main_accounts, self.monitor_accounts = parse_monitor_accounts_payload(payload)

    def _normalize_id(self, value: object, *, field_name: str) -> str:
        return normalize_monitor_id(value, field_name=field_name)

    def _as_bool(self, value: str | bool | None, default: bool = False) -> bool:
        return parse_monitor_bool(value, default)


settings = Settings()
settings.load_monitor_accounts()
