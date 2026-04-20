from __future__ import annotations

import json
from pathlib import Path

import pytest

from monitor_app.config import Settings
from monitor_app.secrets import EncryptedFileSecretProvider, create_master_key


def test_load_monitor_accounts_from_hierarchical_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "binance_monitor_accounts.json").write_text(
        json.dumps(
            {
                "main_accounts": [
                    {
                        "main_id": "group_a",
                        "name": "Group A",
                        "transfer_api_key": "main-k",
                        "transfer_api_secret": "main-s",
                        "transfer_uid": "123456789",
                        "children": [
                            {
                                "account_id": "sub1",
                                "name": "Sub One",
                                "api_key": "k1",
                                "api_secret": "s1",
                                "uid": "223456789",
                            },
                            {
                                "account_id": "sub2",
                                "name": "Sub Two",
                                "api_key": "k2",
                                "api_secret": "s2",
                                "uid": "323456789",
                                "use_testnet": True,
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    settings = Settings(_env_file=None, allow_plaintext_secrets=True)
    settings.load_monitor_accounts()

    assert set(settings.monitor_accounts) == {"group_a.sub1", "group_a.sub2"}
    assert settings.monitor_accounts["group_a.sub1"].display_name == "Group A / Sub One"
    assert settings.monitor_accounts["group_a.sub1"].uid == "223456789"
    assert settings.monitor_accounts["group_a.sub2"].use_testnet is True
    assert settings.monitor_main_accounts["group_a"].name == "Group A"
    assert settings.monitor_main_accounts["group_a"].has_transfer_credentials is True


def test_load_monitor_accounts_rejects_duplicate_main_id(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "binance_monitor_accounts.json").write_text(
        json.dumps(
            {
                "main_accounts": [
                    {"main_id": "group_a", "name": "A", "children": [{"account_id": "sub1", "name": "One", "api_key": "k1", "api_secret": "s1"}]},
                    {"main_id": "group_a", "name": "B", "children": [{"account_id": "sub2", "name": "Two", "api_key": "k2", "api_secret": "s2"}]},
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    settings = Settings(_env_file=None, allow_plaintext_secrets=True)

    with pytest.raises(ValueError, match="Duplicate main_id"):
        settings.load_monitor_accounts()


def test_load_monitor_accounts_from_secret_refs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    secrets_path = config_dir / "secrets.enc.json"
    master_key = create_master_key()
    provider = EncryptedFileSecretProvider(secrets_path, master_key=master_key)
    provider.set_secret("main_accounts.group_a.transfer_api_key", "main-k")
    provider.set_secret("main_accounts.group_a.transfer_api_secret", "main-s")
    provider.set_secret("accounts.group_a.sub1.api_key", "k1")
    provider.set_secret("accounts.group_a.sub1.api_secret", "s1")
    (config_dir / "binance_monitor_accounts.json").write_text(
        json.dumps(
            {
                "main_accounts": [
                    {
                        "main_id": "group_a",
                        "name": "Group A",
                        "transfer_api_key_secret_ref": "main_accounts.group_a.transfer_api_key",
                        "transfer_api_secret_secret_ref": "main_accounts.group_a.transfer_api_secret",
                        "transfer_uid": "123456789",
                        "children": [
                            {
                                "account_id": "sub1",
                                "name": "Sub One",
                                "api_key_secret_ref": "accounts.group_a.sub1.api_key",
                                "api_secret_secret_ref": "accounts.group_a.sub1.api_secret",
                                "uid": "223456789",
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    settings = Settings(_env_file=None, monitor_master_key=master_key, secrets_file=secrets_path)
    settings.load_monitor_accounts()

    assert settings.monitor_accounts["group_a.sub1"].api_key == "k1"
    assert settings.monitor_accounts["group_a.sub1"].api_secret == "s1"
    assert settings.monitor_main_accounts["group_a"].transfer_api_key == "main-k"
    assert settings.monitor_main_accounts["group_a"].transfer_api_secret == "main-s"


def test_load_monitor_accounts_rejects_plaintext_when_refs_only_mode_is_enabled(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "binance_monitor_accounts.json").write_text(
        json.dumps(
            {
                "main_accounts": [
                    {
                        "main_id": "group_a",
                        "name": "Group A",
                        "children": [
                            {
                                "account_id": "sub1",
                                "name": "Sub One",
                                "api_key": "k1",
                                "api_secret": "s1",
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    settings = Settings(_env_file=None)

    with pytest.raises(ValueError, match="plaintext value is not allowed in refs-only mode"):
        settings.load_monitor_accounts()


def test_reload_runtime_env_overrides_only_updates_present_keys() -> None:
    settings = Settings(
        _env_file=None,
        monitor_master_key="example-master-key",
        monitor_master_key_file="D:/secrets/example-original.key",
        tg_bot_token="example-bot-token",
        tg_chat_id="example-chat-id",
        tg_bot_token_secret_ref="telegram.bot_token.example",
        tg_chat_id_secret_ref="telegram.chat_id.example",
    )

    settings.reload_runtime_env_overrides(env_content="MONITOR_MASTER_KEY_FILE=D:/secrets/example-rotated.key\n")

    assert settings.monitor_master_key == "example-master-key"
    assert settings.monitor_master_key_file == "D:/secrets/example-rotated.key"
    assert settings.tg_bot_token == "example-bot-token"
    assert settings.tg_chat_id == "example-chat-id"
    assert settings.tg_bot_token_secret_ref == "telegram.bot_token.example"
    assert settings.tg_chat_id_secret_ref == "telegram.chat_id.example"


def test_restore_runtime_env_overrides_snapshot_restores_previous_runtime_values() -> None:
    settings = Settings(
        _env_file=None,
        monitor_master_key="example-master-key",
        monitor_master_key_file="D:/secrets/example-original.key",
        tg_bot_token="example-bot-token",
        tg_chat_id="example-chat-id",
    )
    snapshot = settings.capture_runtime_env_overrides_snapshot()

    settings.reload_runtime_env_overrides(
        env_content=(
            "MONITOR_MASTER_KEY_FILE=D:/secrets/example-imported.key\n"
            "TG_BOT_TOKEN_SECRET_REF=telegram.bot_token.example\n"
            "TG_CHAT_ID_SECRET_REF=telegram.chat_id.example\n"
        )
    )

    assert settings.monitor_master_key_file == "D:/secrets/example-imported.key"
    assert settings.tg_bot_token_secret_ref == "telegram.bot_token.example"
    assert settings.tg_chat_id_secret_ref == "telegram.chat_id.example"

    settings.restore_runtime_env_overrides_snapshot(snapshot)

    assert settings.monitor_master_key == "example-master-key"
    assert settings.monitor_master_key_file == "D:/secrets/example-original.key"
    assert settings.tg_bot_token == "example-bot-token"
    assert settings.tg_chat_id == "example-chat-id"
    assert settings.tg_bot_token_secret_ref == ""
    assert settings.tg_chat_id_secret_ref == ""
