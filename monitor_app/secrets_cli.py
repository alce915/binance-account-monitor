from __future__ import annotations

import argparse
import getpass
import os
from pathlib import Path

from monitor_app.config import settings
from monitor_app.secrets import (
    EncryptedFileSecretProvider,
    create_master_key,
    migrate_all_configs,
    migrate_access_control_config,
    migrate_env_file,
    migrate_monitor_accounts_config,
    verify_secret_store_consistency,
)


def _default_master_key_file() -> Path:
    return Path(".local-secrets/monitor-master-key")


def _resolve_master_key_file(args: argparse.Namespace) -> Path:
    explicit = str(args.master_key_file or "").strip()
    if explicit:
        return Path(explicit)
    configured = str(settings.monitor_master_key_file or "").strip()
    if configured:
        return Path(configured)
    return _default_master_key_file()


def _write_master_key_file(path: Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        target.write_text(create_master_key() + "\n", encoding="utf-8")
    return target


def _resolve_master_key(args: argparse.Namespace) -> str:
    key_file = _resolve_master_key_file(args)
    if key_file:
        return key_file.read_text(encoding="utf-8").strip()
    if settings.monitor_master_key:
        return settings.monitor_master_key.strip()
    raise ValueError("MONITOR_MASTER_KEY_FILE or MONITOR_MASTER_KEY is required")


def _build_provider(args: argparse.Namespace) -> EncryptedFileSecretProvider:
    secrets_file = Path(args.secrets_file or settings.secrets_file)
    return EncryptedFileSecretProvider(secrets_file, master_key=_resolve_master_key(args))


def command_init(args: argparse.Namespace) -> int:
    key_file = _write_master_key_file(_resolve_master_key_file(args))
    print(f"Master key file ready: {key_file}")
    return 0


def command_migrate(args: argparse.Namespace) -> int:
    provider = _build_provider(args)
    key_file = _resolve_master_key_file(args)
    migrate_all_configs(
        access_control_path=Path(args.access_control_config or settings.access_control_config_file),
        monitor_accounts_path=Path(args.monitor_accounts_file or settings.monitor_accounts_file),
        env_path=Path(args.env_file or ".env"),
        provider=provider,
        master_key_file=str(key_file),
    )
    print(f"Secrets migrated into {provider.path}")
    return 0


def _resolve_secret_value_argument(args: argparse.Namespace) -> str:
    direct_value = str(args.value or "")
    if direct_value:
        return direct_value
    prompted_value = getpass.getpass(f"Enter value for {args.ref}: ")
    if not str(prompted_value or ""):
        raise ValueError("Secret value cannot be empty")
    return prompted_value


def command_set(args: argparse.Namespace) -> int:
    provider = _build_provider(args)
    value = _resolve_secret_value_argument(args)
    provider.set_secret(args.ref, value)
    print(f"Secret updated: {args.ref}")
    return 0


def command_delete(args: argparse.Namespace) -> int:
    provider = _build_provider(args)
    provider.delete_secret(args.ref)
    print(f"Secret deleted: {args.ref}")
    return 0


def command_list(args: argparse.Namespace) -> int:
    provider = _build_provider(args)
    refs = provider.list_secret_refs()
    if not refs:
        print("No secret refs found.")
        return 0
    for ref in refs:
        print(ref)
    return 0


def _doctor_report_lines(result: dict[str, list[str]]) -> list[str]:
    lines = [
        f"expected_refs={len(result['expected_refs'])}",
        f"stored_refs={len(result['stored_refs'])}",
        f"missing_refs={len(result['missing_refs'])}",
        f"unused_refs={len(result['unused_refs'])}",
    ]
    if result["missing_refs"]:
        lines.append("Missing refs:")
        lines.extend(f"  - {ref}" for ref in result["missing_refs"])
    if result["unused_refs"]:
        lines.append("Unused refs:")
        lines.extend(f"  - {ref}" for ref in result["unused_refs"])
    return lines


def command_doctor(args: argparse.Namespace) -> int:
    provider = _build_provider(args)
    result = verify_secret_store_consistency(
        access_control_path=Path(args.access_control_config or settings.access_control_config_file),
        monitor_accounts_path=Path(args.monitor_accounts_file or settings.monitor_accounts_file),
        env_path=Path(args.env_file or ".env"),
        provider=provider,
    )
    for line in _doctor_report_lines(result):
        print(line)
    return 1 if result["missing_refs"] else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Local secret store management")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="Create a local master key file")
    init_parser.add_argument("--master-key-file", default=str(_default_master_key_file()))
    init_parser.set_defaults(func=command_init)

    migrate_parser = subparsers.add_parser("migrate", help="Migrate plaintext secrets into encrypted store")
    migrate_parser.add_argument("--secrets-file", default="")
    migrate_parser.add_argument("--master-key-file", default=os.getenv("MONITOR_MASTER_KEY_FILE", ""))
    migrate_parser.add_argument("--access-control-config", default="")
    migrate_parser.add_argument("--monitor-accounts-file", default="")
    migrate_parser.add_argument("--env-file", default=str(settings.env_file_path))
    migrate_parser.add_argument("--write-config", action="store_true", help="Compatibility flag; config files are always rewritten")
    migrate_parser.set_defaults(func=command_migrate)

    set_parser = subparsers.add_parser("set", help="Create or update a secret value")
    set_parser.add_argument("ref")
    set_parser.add_argument("--value", default="")
    set_parser.add_argument("--secrets-file", default="")
    set_parser.add_argument("--master-key-file", default=os.getenv("MONITOR_MASTER_KEY_FILE", ""))
    set_parser.set_defaults(func=command_set)

    delete_parser = subparsers.add_parser("delete", help="Delete a secret ref")
    delete_parser.add_argument("ref")
    delete_parser.add_argument("--secrets-file", default="")
    delete_parser.add_argument("--master-key-file", default=os.getenv("MONITOR_MASTER_KEY_FILE", ""))
    delete_parser.set_defaults(func=command_delete)

    list_parser = subparsers.add_parser("list", help="List stored secret refs")
    list_parser.add_argument("--secrets-file", default="")
    list_parser.add_argument("--master-key-file", default=os.getenv("MONITOR_MASTER_KEY_FILE", ""))
    list_parser.set_defaults(func=command_list)

    doctor_parser = subparsers.add_parser("doctor", help="Verify refs and encrypted secret store consistency")
    doctor_parser.add_argument("--secrets-file", default="")
    doctor_parser.add_argument("--master-key-file", default=os.getenv("MONITOR_MASTER_KEY_FILE", ""))
    doctor_parser.add_argument("--access-control-config", default="")
    doctor_parser.add_argument("--monitor-accounts-file", default="")
    doctor_parser.add_argument("--env-file", default=str(settings.env_file_path))
    doctor_parser.set_defaults(func=command_doctor)

    verify_parser = subparsers.add_parser("verify", help="Alias for doctor")
    verify_parser.add_argument("--secrets-file", default="")
    verify_parser.add_argument("--master-key-file", default=os.getenv("MONITOR_MASTER_KEY_FILE", ""))
    verify_parser.add_argument("--access-control-config", default="")
    verify_parser.add_argument("--monitor-accounts-file", default="")
    verify_parser.add_argument("--env-file", default=str(settings.env_file_path))
    verify_parser.set_defaults(func=command_doctor)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
