from __future__ import annotations

from io import BytesIO

import pytest
from openpyxl import Workbook

from monitor_app.account_import import AccountImportError, parse_accounts_excel


def test_parse_accounts_excel_builds_hierarchical_payload() -> None:
    content = _build_workbook_bytes(
        [
            ["main_id", "main_name", "account_id", "name", "api_key", "api_secret", "use_testnet"],
            ["group_a", "Group A", "sub1", "Sub One", "k1", "s1", "true"],
            ["group_a", "Group A", "sub2", "Sub Two", "k2", "s2", ""],
            ["group_b", "Group B", "sub1", "Sub Three", "k3", "s3", "false"],
        ]
    )

    payload, result = parse_accounts_excel(content, filename="accounts.xlsx")

    assert result.file_name == "accounts.xlsx"
    assert result.main_account_count == 2
    assert result.account_count == 3
    assert payload == {
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
                        "use_testnet": True,
                        "rest_base_url": "",
                        "ws_base_url": "",
                    },
                    {
                        "account_id": "sub2",
                        "name": "Sub Two",
                        "api_key": "k2",
                        "api_secret": "s2",
                        "use_testnet": False,
                        "rest_base_url": "",
                        "ws_base_url": "",
                    },
                ],
            },
            {
                "main_id": "group_b",
                "name": "Group B",
                "children": [
                    {
                        "account_id": "sub1",
                        "name": "Sub Three",
                        "api_key": "k3",
                        "api_secret": "s3",
                        "use_testnet": False,
                        "rest_base_url": "",
                        "ws_base_url": "",
                    }
                ],
            },
        ]
    }


def test_parse_accounts_excel_rejects_missing_required_headers() -> None:
    content = _build_workbook_bytes(
        [
            ["main_id", "main_name", "account_id", "name", "api_key"],
            ["group_a", "Group A", "sub1", "Sub One", "k1"],
        ]
    )

    with pytest.raises(AccountImportError, match="Missing required columns: api_secret"):
        parse_accounts_excel(content, filename="accounts.xlsx")


def test_parse_accounts_excel_rejects_blank_required_value() -> None:
    content = _build_workbook_bytes(
        [
            ["main_id", "main_name", "account_id", "name", "api_key", "api_secret"],
            ["group_a", "Group A", "sub1", "Sub One", "", "s1"],
        ]
    )

    with pytest.raises(AccountImportError, match=r"Row 2: api_key is required"):
        parse_accounts_excel(content, filename="accounts.xlsx")


def test_parse_accounts_excel_rejects_invalid_account_id() -> None:
    content = _build_workbook_bytes(
        [
            ["main_id", "main_name", "account_id", "name", "api_key", "api_secret"],
            ["group_a", "Group A", "Sub 1", "Sub One", "k1", "s1"],
        ]
    )

    with pytest.raises(AccountImportError, match=r"Row 2: account_id must match"):
        parse_accounts_excel(content, filename="accounts.xlsx")


def test_parse_accounts_excel_rejects_duplicate_account_id_within_group() -> None:
    content = _build_workbook_bytes(
        [
            ["main_id", "main_name", "account_id", "name", "api_key", "api_secret"],
            ["group_a", "Group A", "sub1", "Sub One", "k1", "s1"],
            ["group_a", "Group A", "sub1", "Sub Two", "k2", "s2"],
        ]
    )

    with pytest.raises(AccountImportError, match=r"Row 3: duplicate account_id sub1 under main_id group_a"):
        parse_accounts_excel(content, filename="accounts.xlsx")


def test_parse_accounts_excel_rejects_inconsistent_group_name() -> None:
    content = _build_workbook_bytes(
        [
            ["main_id", "main_name", "account_id", "name", "api_key", "api_secret"],
            ["group_a", "Group A", "sub1", "Sub One", "k1", "s1"],
            ["group_a", "Group X", "sub2", "Sub Two", "k2", "s2"],
        ]
    )

    with pytest.raises(AccountImportError, match=r"Row 3: main_name must stay consistent for main_id group_a"):
        parse_accounts_excel(content, filename="accounts.xlsx")


def test_parse_accounts_excel_defaults_optional_columns() -> None:
    content = _build_workbook_bytes(
        [
            ["main_id", "main_name", "account_id", "name", "api_key", "api_secret"],
            ["group_a", "Group A", "sub1", "Sub One", "k1", "s1"],
            [None, None, None, None, None, None],
        ]
    )

    payload, _ = parse_accounts_excel(content, filename="accounts.xlsx")

    child = payload["main_accounts"][0]["children"][0]
    assert child["use_testnet"] is False
    assert child["rest_base_url"] == ""
    assert child["ws_base_url"] == ""


def _build_workbook_bytes(rows: list[list[object]]) -> bytes:
    workbook = Workbook()
    worksheet = workbook.active
    for row in rows:
        worksheet.append(row)
    buffer = BytesIO()
    workbook.save(buffer)
    workbook.close()
    return buffer.getvalue()
