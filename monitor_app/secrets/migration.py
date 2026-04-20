from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Iterable

from monitor_app.secrets.provider import EncryptedFileSecretProvider
from monitor_app.secrets.refs import (
    access_control_secret_ref,
    child_account_secret_ref,
    main_account_secret_ref,
    telegram_secret_ref,
)


def _write_text_atomic(path: Path, content: str) -> None:
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            delete=False,
            prefix=f"{path.stem}-",
            suffix=".tmp",
        ) as handle:
            handle.write(content)
            temp_path = Path(handle.name)
        os.replace(temp_path, path)
    except OSError:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink(missing_ok=True)
        raise


def _render_json(payload: dict) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2) + "\n"


def transform_access_control_payload(payload: dict, store: dict[str, str]) -> tuple[dict, dict[str, str]]:
    if not isinstance(payload, dict):
        raise ValueError("Access control config must be an object")

    next_payload = dict(payload)
    next_store = dict(store)
    next_payload["allow_plaintext_secrets"] = False
    next_payload.setdefault("cookie_secure_mode", "auto")

    for field_name in ("guest_password", "admin_password", "session_secret"):
        secret_ref_field = f"{field_name}_secret_ref"
        secret_ref = str(next_payload.get(secret_ref_field) or "").strip() or access_control_secret_ref(field_name)
        secret_value = str(next_payload.get(field_name) or "").strip()
        if secret_value:
            next_store[secret_ref] = secret_value
        elif secret_ref not in next_store:
            raise ValueError(f"Missing source value for {field_name}")
        next_payload[secret_ref_field] = secret_ref
        next_payload.pop(field_name, None)

    return next_payload, next_store


def transform_monitor_accounts_payload(payload: dict, store: dict[str, str]) -> tuple[dict, dict[str, str]]:
    if not isinstance(payload, dict):
        raise ValueError("Monitor accounts config must be an object")
    main_accounts = payload.get("main_accounts")
    if not isinstance(main_accounts, list):
        raise ValueError("Monitor accounts file must contain a main_accounts array")

    next_store = dict(store)
    next_payload = {"main_accounts": []}
    for main_account in main_accounts:
        if not isinstance(main_account, dict):
            raise ValueError("Each main account entry must be an object")
        main_id = str(main_account.get("main_id") or "").strip().lower()
        if not main_id:
            raise ValueError("main_id is required")

        rendered_main_account = dict(main_account)
        for field_name in ("transfer_api_key", "transfer_api_secret"):
            secret_ref_field = f"{field_name}_secret_ref"
            secret_ref = str(rendered_main_account.get(secret_ref_field) or "").strip() or main_account_secret_ref(main_id, field_name)
            secret_value = str(rendered_main_account.get(field_name) or "").strip()
            if secret_value:
                next_store[secret_ref] = secret_value
                rendered_main_account[secret_ref_field] = secret_ref
            elif str(rendered_main_account.get(secret_ref_field) or "").strip():
                if secret_ref not in next_store:
                    raise ValueError(f"Missing source value for {main_id}.{field_name}")
                rendered_main_account[secret_ref_field] = secret_ref
            else:
                rendered_main_account.pop(secret_ref_field, None)
            rendered_main_account.pop(field_name, None)

        children = rendered_main_account.get("children")
        if not isinstance(children, list):
            raise ValueError(f"Main account {main_id} must define children")

        rendered_children: list[dict] = []
        for child in children:
            if not isinstance(child, dict):
                raise ValueError(f"Child account under {main_id} must be an object")
            account_id = str(child.get("account_id") or "").strip().lower()
            if not account_id:
                raise ValueError(f"Child account under {main_id} must define account_id")
            rendered_child = dict(child)
            for field_name in ("api_key", "api_secret"):
                secret_ref_field = f"{field_name}_secret_ref"
                secret_ref = str(rendered_child.get(secret_ref_field) or "").strip() or child_account_secret_ref(main_id, account_id, field_name)
                secret_value = str(rendered_child.get(field_name) or "").strip()
                if secret_value:
                    next_store[secret_ref] = secret_value
                elif str(rendered_child.get(secret_ref_field) or "").strip():
                    if secret_ref not in next_store:
                        raise ValueError(f"Missing source value for {main_id}.{account_id}.{field_name}")
                else:
                    raise ValueError(f"Missing source value for {main_id}.{account_id}.{field_name}")
                rendered_child[secret_ref_field] = secret_ref
                rendered_child.pop(field_name, None)
            rendered_children.append(rendered_child)

        rendered_main_account["children"] = rendered_children
        next_payload["main_accounts"].append(rendered_main_account)

    return next_payload, next_store


def transform_env_content(content: str, store: dict[str, str], *, master_key_file: str = "") -> tuple[str, dict[str, str]]:
    lines = content.splitlines()
    values: dict[str, str] = {}
    for raw_line in lines:
        if not raw_line or raw_line.lstrip().startswith("#") or "=" not in raw_line:
            continue
        key, value = raw_line.split("=", 1)
        values[key.strip()] = value

    next_store = dict(store)
    replacements = {
        "TG_BOT_TOKEN": ("TG_BOT_TOKEN_SECRET_REF", telegram_secret_ref("bot_token")),
        "TG_CHAT_ID": ("TG_CHAT_ID_SECRET_REF", telegram_secret_ref("chat_id")),
    }
    for source_key, (target_key, secret_ref) in replacements.items():
        source_value = str(values.get(source_key) or "").strip()
        existing_ref = str(values.get(target_key) or "").strip()
        if source_value:
            next_store[secret_ref] = source_value
            values.pop(source_key, None)
            values[target_key] = secret_ref
        elif existing_ref:
            if existing_ref not in next_store:
                raise ValueError(f"Secret ref declared but not present in store: {existing_ref}")

    normalized_master_key_file = str(master_key_file or "").strip()
    if normalized_master_key_file:
        values["MONITOR_MASTER_KEY_FILE"] = normalized_master_key_file

    rendered_lines: list[str] = []
    seen_keys: set[str] = set()
    for raw_line in lines:
        if not raw_line or raw_line.lstrip().startswith("#") or "=" not in raw_line:
            rendered_lines.append(raw_line)
            continue
        key, _ = raw_line.split("=", 1)
        normalized = key.strip()
        if normalized in replacements:
            seen_keys.add(normalized)
            continue
        if normalized in {target for target, _ in replacements.values()}:
            rendered_lines.append(f"{normalized}={values[normalized]}")
            seen_keys.add(normalized)
            continue
        if normalized == "MONITOR_MASTER_KEY_FILE" and normalized in values:
            rendered_lines.append(f"{normalized}={values[normalized]}")
            seen_keys.add(normalized)
            continue
        rendered_lines.append(raw_line)
        seen_keys.add(normalized)

    for target_key, _ in replacements.values():
        if target_key not in seen_keys and target_key in values:
            rendered_lines.append(f"{target_key}={values[target_key]}")
    if "MONITOR_MASTER_KEY_FILE" not in seen_keys and "MONITOR_MASTER_KEY_FILE" in values:
        rendered_lines.append(f"MONITOR_MASTER_KEY_FILE={values['MONITOR_MASTER_KEY_FILE']}")

    return "\n".join(rendered_lines).rstrip() + "\n", next_store


def migrate_access_control_config(path: Path, provider: EncryptedFileSecretProvider) -> None:
    config_path = Path(path)
    original_text = config_path.read_text(encoding="utf-8-sig")
    original_store = provider.dump_store()
    next_payload, next_store = transform_access_control_payload(
        json.loads(original_text),
        original_store,
    )
    try:
        provider.replace_store(next_store)
        _write_text_atomic(config_path, _render_json(next_payload))
    except Exception:
        provider.replace_store(original_store)
        _write_text_atomic(config_path, original_text if original_text.endswith("\n") else f"{original_text}\n")
        raise


def migrate_monitor_accounts_config(path: Path, provider: EncryptedFileSecretProvider) -> None:
    config_path = Path(path)
    original_text = config_path.read_text(encoding="utf-8-sig")
    original_store = provider.dump_store()
    next_payload, next_store = transform_monitor_accounts_payload(
        json.loads(original_text),
        original_store,
    )
    try:
        provider.replace_store(next_store)
        _write_text_atomic(config_path, _render_json(next_payload))
    except Exception:
        provider.replace_store(original_store)
        _write_text_atomic(config_path, original_text if original_text.endswith("\n") else f"{original_text}\n")
        raise


def migrate_env_file(path: Path, provider: EncryptedFileSecretProvider, *, master_key_file: str = "") -> None:
    env_path = Path(path)
    original_text = env_path.read_text(encoding="utf-8")
    original_store = provider.dump_store()
    next_content, next_store = transform_env_content(
        original_text,
        original_store,
        master_key_file=master_key_file,
    )
    try:
        provider.replace_store(next_store)
        _write_text_atomic(env_path, next_content)
    except Exception:
        provider.replace_store(original_store)
        _write_text_atomic(env_path, original_text if original_text.endswith("\n") else f"{original_text}\n")
        raise


def migrate_all_configs(
    *,
    access_control_path: Path,
    monitor_accounts_path: Path,
    env_path: Path,
    provider: EncryptedFileSecretProvider,
    master_key_file: str = "",
) -> None:
    access_control_path = Path(access_control_path)
    monitor_accounts_path = Path(monitor_accounts_path)
    env_path = Path(env_path)
    original_access_control = access_control_path.read_text(encoding="utf-8-sig")
    original_monitor_accounts = monitor_accounts_path.read_text(encoding="utf-8-sig")
    original_env = env_path.read_text(encoding="utf-8")
    original_store = provider.dump_store()
    store = original_store
    next_access_control_payload, store = transform_access_control_payload(
        json.loads(original_access_control),
        store,
    )
    next_monitor_payload, store = transform_monitor_accounts_payload(
        json.loads(original_monitor_accounts),
        store,
    )
    next_env_content, store = transform_env_content(
        original_env,
        store,
        master_key_file=master_key_file,
    )

    try:
        provider.replace_store(store)
        _write_text_atomic(access_control_path, _render_json(next_access_control_payload))
        _write_text_atomic(monitor_accounts_path, _render_json(next_monitor_payload))
        _write_text_atomic(env_path, next_env_content)
    except Exception:
        provider.replace_store(original_store)
        _write_text_atomic(access_control_path, original_access_control if original_access_control.endswith("\n") else f"{original_access_control}\n")
        _write_text_atomic(monitor_accounts_path, original_monitor_accounts if original_monitor_accounts.endswith("\n") else f"{original_monitor_accounts}\n")
        _write_text_atomic(env_path, original_env if original_env.endswith("\n") else f"{original_env}\n")
        raise


def expected_secret_refs_from_project(
    *,
    access_control_payload: dict | None = None,
    monitor_accounts_payload: dict | None = None,
    env_content: str | None = None,
) -> set[str]:
    refs: set[str] = set()
    if isinstance(access_control_payload, dict):
        for field_name in ("guest_password_secret_ref", "admin_password_secret_ref", "session_secret_secret_ref"):
            value = str(access_control_payload.get(field_name) or "").strip()
            if value:
                refs.add(value)
    if isinstance(monitor_accounts_payload, dict):
        main_accounts = monitor_accounts_payload.get("main_accounts")
        if isinstance(main_accounts, list):
            for main_account in main_accounts:
                if not isinstance(main_account, dict):
                    continue
                for field_name in ("transfer_api_key_secret_ref", "transfer_api_secret_secret_ref"):
                    value = str(main_account.get(field_name) or "").strip()
                    if value:
                        refs.add(value)
                children = main_account.get("children")
                if not isinstance(children, list):
                    continue
                for child in children:
                    if not isinstance(child, dict):
                        continue
                    for field_name in ("api_key_secret_ref", "api_secret_secret_ref"):
                        value = str(child.get(field_name) or "").strip()
                        if value:
                            refs.add(value)
    if env_content:
        for raw_line in env_content.splitlines():
            if not raw_line or raw_line.lstrip().startswith("#") or "=" not in raw_line:
                continue
            key, value = raw_line.split("=", 1)
            if key.strip() in {"TG_BOT_TOKEN_SECRET_REF", "TG_CHAT_ID_SECRET_REF"}:
                normalized = value.strip()
                if normalized:
                    refs.add(normalized)
    return refs


def verify_secret_store_consistency(
    *,
    access_control_path: Path | None,
    monitor_accounts_path: Path | None,
    env_path: Path | None,
    provider: EncryptedFileSecretProvider,
) -> dict[str, list[str]]:
    access_control_payload = None
    monitor_accounts_payload = None
    env_content = None
    if access_control_path is not None and Path(access_control_path).exists():
        access_control_payload = json.loads(Path(access_control_path).read_text(encoding="utf-8-sig"))
    if monitor_accounts_path is not None and Path(monitor_accounts_path).exists():
        monitor_accounts_payload = json.loads(Path(monitor_accounts_path).read_text(encoding="utf-8-sig"))
    if env_path is not None and Path(env_path).exists():
        env_content = Path(env_path).read_text(encoding="utf-8")
    expected_refs = expected_secret_refs_from_project(
        access_control_payload=access_control_payload,
        monitor_accounts_payload=monitor_accounts_payload,
        env_content=env_content,
    )
    store_refs = set(provider.list_secret_refs())
    missing_refs = sorted(expected_refs - store_refs)
    unused_refs = sorted(store_refs - expected_refs)
    return {
        "missing_refs": missing_refs,
        "unused_refs": unused_refs,
        "expected_refs": sorted(expected_refs),
        "stored_refs": sorted(store_refs),
    }
