from __future__ import annotations

import json
from pathlib import Path

import pytest

from monitor_app.secrets import EncryptedFileSecretProvider, create_master_key
from monitor_app.secrets.migration import (
    migrate_access_control_config,
    migrate_all_configs,
    migrate_env_file,
    migrate_monitor_accounts_config,
    verify_secret_store_consistency,
)


def test_encrypted_file_secret_provider_round_trip(tmp_path: Path) -> None:
    provider = EncryptedFileSecretProvider(
        tmp_path / "secrets.enc.json",
        master_key=create_master_key(),
    )

    provider.set_secret("accounts.group_a.sub1.api_key", "k1")
    provider.set_secret("accounts.group_a.sub1.api_secret", "s1")

    assert provider.get_secret("accounts.group_a.sub1.api_key") == "k1"
    assert provider.get_secret("accounts.group_a.sub1.api_secret") == "s1"
    assert provider.has_secret("accounts.group_a.sub1.api_key") is True
    assert sorted(provider.list_secret_refs()) == [
        "accounts.group_a.sub1.api_key",
        "accounts.group_a.sub1.api_secret",
    ]


def test_encrypted_file_secret_provider_rejects_wrong_master_key(tmp_path: Path) -> None:
    secrets_path = tmp_path / "secrets.enc.json"
    provider = EncryptedFileSecretProvider(secrets_path, master_key=create_master_key())
    provider.set_secret("access_control.admin_password", "admin-pass")

    with pytest.raises(ValueError, match="Failed to decrypt secret store"):
        EncryptedFileSecretProvider(secrets_path, master_key=create_master_key()).get_secret(
            "access_control.admin_password"
        )


def test_migrate_access_control_config_rewrites_plaintext_to_secret_refs(tmp_path: Path) -> None:
    config_path = tmp_path / "access_control.json"
    config_path.write_text(
        json.dumps(
            {
                "enabled": True,
                "whitelist_ips": ["127.0.0.1"],
                "guest_password": "guest-pass",
                "admin_password": "admin-pass",
                "session_secret": "session-secret",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    provider = EncryptedFileSecretProvider(tmp_path / "secrets.enc.json", master_key=create_master_key())

    migrate_access_control_config(config_path, provider)

    migrated = json.loads(config_path.read_text(encoding="utf-8"))
    assert "guest_password" not in migrated
    assert "admin_password" not in migrated
    assert "session_secret" not in migrated
    assert migrated["allow_plaintext_secrets"] is False
    assert migrated["cookie_secure_mode"] == "auto"
    assert migrated["guest_password_secret_ref"] == "access_control.guest_password"
    assert migrated["admin_password_secret_ref"] == "access_control.admin_password"
    assert migrated["session_secret_secret_ref"] == "access_control.session_secret"
    assert provider.get_secret("access_control.guest_password") == "guest-pass"
    assert provider.get_secret("access_control.admin_password") == "admin-pass"
    assert provider.get_secret("access_control.session_secret") == "session-secret"


def test_migrate_monitor_accounts_config_rewrites_plaintext_to_secret_refs(tmp_path: Path) -> None:
    config_path = tmp_path / "binance_monitor_accounts.json"
    config_path.write_text(
        json.dumps(
            {
                "main_accounts": [
                    {
                        "main_id": "group_a",
                        "name": "Group A",
                        "transfer_api_key": "mk1",
                        "transfer_api_secret": "ms1",
                        "transfer_uid": "123456789",
                        "children": [
                            {
                                "account_id": "sub1",
                                "name": "Sub One",
                                "api_key": "k1",
                                "api_secret": "s1",
                                "uid": "223456789",
                            }
                        ],
                    }
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    provider = EncryptedFileSecretProvider(tmp_path / "secrets.enc.json", master_key=create_master_key())

    migrate_monitor_accounts_config(config_path, provider)

    migrated = json.loads(config_path.read_text(encoding="utf-8"))
    main_account = migrated["main_accounts"][0]
    child = main_account["children"][0]
    assert "transfer_api_key" not in main_account
    assert "transfer_api_secret" not in main_account
    assert "api_key" not in child
    assert "api_secret" not in child
    assert main_account["transfer_api_key_secret_ref"] == "main_accounts.group_a.transfer_api_key"
    assert main_account["transfer_api_secret_secret_ref"] == "main_accounts.group_a.transfer_api_secret"
    assert child["api_key_secret_ref"] == "accounts.group_a.sub1.api_key"
    assert child["api_secret_secret_ref"] == "accounts.group_a.sub1.api_secret"
    assert provider.get_secret("main_accounts.group_a.transfer_api_key") == "mk1"
    assert provider.get_secret("main_accounts.group_a.transfer_api_secret") == "ms1"
    assert provider.get_secret("accounts.group_a.sub1.api_key") == "k1"
    assert provider.get_secret("accounts.group_a.sub1.api_secret") == "s1"


def test_migrate_monitor_accounts_config_omits_empty_optional_transfer_refs(tmp_path: Path) -> None:
    config_path = tmp_path / "binance_monitor_accounts.json"
    config_path.write_text(
        json.dumps(
            {
                "main_accounts": [
                    {
                        "main_id": "group_b",
                        "name": "Group B",
                        "transfer_api_key": "",
                        "transfer_api_secret": "",
                        "transfer_uid": "",
                        "children": [
                            {
                                "account_id": "sub1",
                                "name": "Sub One",
                                "api_key": "k1",
                                "api_secret": "s1",
                                "uid": "223456789",
                            }
                        ],
                    }
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    provider = EncryptedFileSecretProvider(tmp_path / "secrets.enc.json", master_key=create_master_key())

    migrate_monitor_accounts_config(config_path, provider)

    migrated = json.loads(config_path.read_text(encoding="utf-8"))
    main_account = migrated["main_accounts"][0]
    assert "transfer_api_key_secret_ref" not in main_account
    assert "transfer_api_secret_secret_ref" not in main_account
    assert provider.get_secret("accounts.group_b.sub1.api_key") == "k1"
    assert provider.get_secret("accounts.group_b.sub1.api_secret") == "s1"


def test_migrate_env_file_rewrites_telegram_plaintext_to_secret_refs(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "TG_ENABLED=true",
                "TG_BOT_TOKEN=bot-token",
                "TG_CHAT_ID=chat-id",
                "",
            ]
        ),
        encoding="utf-8",
    )
    provider = EncryptedFileSecretProvider(tmp_path / "secrets.enc.json", master_key=create_master_key())

    migrate_env_file(env_path, provider)

    content = env_path.read_text(encoding="utf-8")
    assert "TG_BOT_TOKEN=" not in content
    assert "TG_CHAT_ID=" not in content
    assert "TG_BOT_TOKEN_SECRET_REF=telegram.bot_token" in content
    assert "TG_CHAT_ID_SECRET_REF=telegram.chat_id" in content
    assert provider.get_secret("telegram.bot_token") == "bot-token"
    assert provider.get_secret("telegram.chat_id") == "chat-id"


def test_migrate_env_file_writes_master_key_file_reference(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text("TG_ENABLED=true\n", encoding="utf-8")
    provider = EncryptedFileSecretProvider(tmp_path / "secrets.enc.json", master_key=create_master_key())

    migrate_env_file(env_path, provider, master_key_file=".local-secrets/monitor-master-key")

    content = env_path.read_text(encoding="utf-8")
    assert "MONITOR_MASTER_KEY_FILE=.local-secrets/monitor-master-key" in content


def test_migrate_all_configs_is_consistent_after_atomic_rewrite(tmp_path: Path) -> None:
    access_control_path = tmp_path / "access_control.json"
    monitor_accounts_path = tmp_path / "binance_monitor_accounts.json"
    env_path = tmp_path / ".env"
    access_control_path.write_text(
        json.dumps(
            {
                "enabled": True,
                "whitelist_ips": ["127.0.0.1"],
                "guest_password": "guest-pass",
                "admin_password": "admin-pass",
                "session_secret": "session-secret",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    monitor_accounts_path.write_text(
        json.dumps(
            {
                "main_accounts": [
                    {
                        "main_id": "group_a",
                        "name": "Group A",
                        "transfer_api_key": "mk1",
                        "transfer_api_secret": "ms1",
                        "transfer_uid": "123456789",
                        "children": [
                            {
                                "account_id": "sub1",
                                "name": "Sub One",
                                "api_key": "k1",
                                "api_secret": "s1",
                                "uid": "223456789",
                            }
                        ],
                    }
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    env_path.write_text("TG_BOT_TOKEN=bot-token\nTG_CHAT_ID=chat-id\n", encoding="utf-8")
    provider = EncryptedFileSecretProvider(tmp_path / "secrets.enc.json", master_key=create_master_key())

    migrate_all_configs(
        access_control_path=access_control_path,
        monitor_accounts_path=monitor_accounts_path,
        env_path=env_path,
        provider=provider,
        master_key_file=".local-secrets/monitor-master-key",
    )

    verification = verify_secret_store_consistency(
        access_control_path=access_control_path,
        monitor_accounts_path=monitor_accounts_path,
        env_path=env_path,
        provider=provider,
    )
    assert verification["missing_refs"] == []
    assert verification["unused_refs"] == []
    assert "MONITOR_MASTER_KEY_FILE=.local-secrets/monitor-master-key" in env_path.read_text(encoding="utf-8")


def test_verify_secret_store_consistency_reports_missing_and_unused_refs(tmp_path: Path) -> None:
    access_control_path = tmp_path / "access_control.json"
    monitor_accounts_path = tmp_path / "binance_monitor_accounts.json"
    env_path = tmp_path / ".env"
    access_control_path.write_text(
        json.dumps(
            {
                "enabled": True,
                "whitelist_ips": [],
                "allow_plaintext_secrets": False,
                "cookie_secure_mode": "auto",
                "guest_password_secret_ref": "access_control.guest_password",
                "admin_password_secret_ref": "access_control.admin_password",
                "session_secret_secret_ref": "access_control.session_secret",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    monitor_accounts_path.write_text(json.dumps({"main_accounts": []}, ensure_ascii=False, indent=2), encoding="utf-8")
    env_path.write_text("TG_BOT_TOKEN_SECRET_REF=telegram.bot_token\n", encoding="utf-8")
    provider = EncryptedFileSecretProvider(tmp_path / "secrets.enc.json", master_key=create_master_key())
    provider.set_secret("access_control.guest_password", "guest-pass")
    provider.set_secret("unused.secret", "unused")

    result = verify_secret_store_consistency(
        access_control_path=access_control_path,
        monitor_accounts_path=monitor_accounts_path,
        env_path=env_path,
        provider=provider,
    )

    assert result["missing_refs"] == [
        "access_control.admin_password",
        "access_control.session_secret",
        "telegram.bot_token",
    ]
    assert result["unused_refs"] == ["unused.secret"]


def test_verify_secret_store_consistency_skips_optional_missing_files(tmp_path: Path) -> None:
    monitor_accounts_path = tmp_path / "binance_monitor_accounts.json"
    monitor_accounts_path.write_text(
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
                                "api_key_secret_ref": "accounts.group_a.sub1.api_key",
                                "api_secret_secret_ref": "accounts.group_a.sub1.api_secret",
                            }
                        ],
                    }
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    provider = EncryptedFileSecretProvider(tmp_path / "secrets.enc.json", master_key=create_master_key())
    provider.set_secret("accounts.group_a.sub1.api_key", "k1")
    provider.set_secret("accounts.group_a.sub1.api_secret", "s1")

    result = verify_secret_store_consistency(
        access_control_path=None,
        monitor_accounts_path=monitor_accounts_path,
        env_path=None,
        provider=provider,
    )

    assert result["missing_refs"] == []
    assert result["unused_refs"] == []
    assert result["expected_refs"] == [
        "accounts.group_a.sub1.api_key",
        "accounts.group_a.sub1.api_secret",
    ]
