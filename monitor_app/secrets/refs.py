from __future__ import annotations


def access_control_secret_ref(name: str) -> str:
    return f"access_control.{name}"


def telegram_secret_ref(name: str) -> str:
    return f"telegram.{name}"


def main_account_secret_ref(main_id: str, field_name: str) -> str:
    return f"main_accounts.{main_id}.{field_name}"


def child_account_secret_ref(main_id: str, account_id: str, field_name: str) -> str:
    return f"accounts.{main_id}.{account_id}.{field_name}"
