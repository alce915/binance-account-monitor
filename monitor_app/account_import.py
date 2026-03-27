from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from io import BytesIO
from pathlib import Path
from typing import Any

from openpyxl import Workbook, load_workbook

from monitor_app.config import normalize_monitor_id, parse_monitor_accounts_payload


REQUIRED_COLUMNS = ("main_id", "main_name", "account_id", "name", "api_key", "api_secret")
OPTIONAL_COLUMNS = ("use_testnet", "rest_base_url", "ws_base_url")
SUPPORTED_COLUMNS = REQUIRED_COLUMNS + OPTIONAL_COLUMNS
TRUE_VALUES = {"1", "true", "yes", "on"}
FALSE_VALUES = {"0", "false", "no", "off"}


class AccountImportError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ImportResult:
    file_name: str
    main_account_count: int
    account_count: int
    mode: str = "replace_all"

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def parse_accounts_excel(
    content: bytes,
    *,
    filename: str,
) -> tuple[dict[str, Any], ImportResult]:
    if not content:
        raise AccountImportError("Excel file is empty")

    try:
        workbook = load_workbook(filename=BytesIO(content), read_only=True, data_only=True)
    except Exception as exc:  # pragma: no cover - library-specific failures vary
        raise AccountImportError("Failed to read Excel workbook") from exc

    try:
        if not workbook.worksheets:
            raise AccountImportError("Excel workbook must contain at least one worksheet")
        worksheet = workbook.worksheets[0]
        rows = worksheet.iter_rows(values_only=True)
        header_row = next(rows, None)
        if header_row is None:
            raise AccountImportError("Excel file must contain a header row")

        header_map = _build_header_map(header_row)
        grouped_rows: dict[str, dict[str, Any]] = {}

        for row_index, row in enumerate(rows, start=2):
            row_data = _extract_row_data(row, header_map)
            if _is_blank_row(row_data):
                continue

            main_id = _parse_required_id(row_data["main_id"], field_name="main_id", row_index=row_index)
            main_name = _parse_required_text(row_data["main_name"], field_name="main_name", row_index=row_index)
            child_account_id = _parse_required_id(row_data["account_id"], field_name="account_id", row_index=row_index)
            child_name = _parse_required_text(row_data["name"], field_name="name", row_index=row_index)
            api_key = _parse_required_text(row_data["api_key"], field_name="api_key", row_index=row_index)
            api_secret = _parse_required_text(row_data["api_secret"], field_name="api_secret", row_index=row_index)
            use_testnet = _parse_optional_bool(row_data["use_testnet"], field_name="use_testnet", row_index=row_index)
            rest_base_url = _parse_optional_text(row_data["rest_base_url"])
            ws_base_url = _parse_optional_text(row_data["ws_base_url"])

            grouped = grouped_rows.setdefault(
                main_id,
                {
                    "main_id": main_id,
                    "name": main_name,
                    "children": [],
                    "_child_ids": set(),
                },
            )
            if grouped["name"] != main_name:
                raise AccountImportError(
                    f"Row {row_index}: main_name must stay consistent for main_id {main_id}"
                )
            if child_account_id in grouped["_child_ids"]:
                raise AccountImportError(
                    f"Row {row_index}: duplicate account_id {child_account_id} under main_id {main_id}"
                )

            grouped["children"].append(
                {
                    "account_id": child_account_id,
                    "name": child_name,
                    "api_key": api_key,
                    "api_secret": api_secret,
                    "use_testnet": use_testnet,
                    "rest_base_url": rest_base_url,
                    "ws_base_url": ws_base_url,
                }
            )
            grouped["_child_ids"].add(child_account_id)

        payload = {
            "main_accounts": [
                {
                    "main_id": grouped["main_id"],
                    "name": grouped["name"],
                    "children": grouped["children"],
                }
                for _, grouped in sorted(grouped_rows.items(), key=lambda item: item[0])
            ]
        }
        parse_monitor_accounts_payload(payload)
        result = ImportResult(
            file_name=filename,
            main_account_count=len(payload["main_accounts"]),
            account_count=sum(len(main_account["children"]) for main_account in payload["main_accounts"]),
        )
        return payload, result
    finally:
        workbook.close()


def write_monitor_accounts_payload(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"

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
            handle.write(serialized)
            temp_path = Path(handle.name)
        os.replace(temp_path, path)
    except OSError:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink(missing_ok=True)
        raise


def build_accounts_excel_template() -> bytes:
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "accounts"
    worksheet.append(list(SUPPORTED_COLUMNS))
    worksheet.append(["group_a", "A组", "sub1", "张三", "replace-with-api-key-1", "replace-with-api-secret-1", "false", "", ""])
    worksheet.append(["group_a", "A组", "sub2", "李四", "replace-with-api-key-2", "replace-with-api-secret-2", "true", "", ""])
    worksheet.append(["group_b", "B组", "sub1", "王五", "replace-with-api-key-3", "replace-with-api-secret-3", "false", "", ""])

    buffer = BytesIO()
    workbook.save(buffer)
    workbook.close()
    return buffer.getvalue()


def _build_header_map(header_row: tuple[Any, ...]) -> dict[str, int]:
    header_map: dict[str, int] = {}
    for index, value in enumerate(header_row):
        name = _parse_optional_text(value).lower()
        if not name:
            continue
        if name in header_map:
            raise AccountImportError(f"Duplicate header: {name}")
        header_map[name] = index

    missing = [column for column in REQUIRED_COLUMNS if column not in header_map]
    if missing:
        raise AccountImportError(f"Missing required columns: {', '.join(missing)}")
    return header_map


def _extract_row_data(row: tuple[Any, ...], header_map: dict[str, int]) -> dict[str, Any]:
    data: dict[str, Any] = {}
    for column in SUPPORTED_COLUMNS:
        index = header_map.get(column)
        data[column] = row[index] if index is not None and index < len(row) else None
    return data


def _is_blank_row(row_data: dict[str, Any]) -> bool:
    return all(_parse_optional_text(value) == "" for value in row_data.values())


def _parse_required_id(value: Any, *, field_name: str, row_index: int) -> str:
    try:
        return normalize_monitor_id(value, field_name=field_name)
    except ValueError as exc:
        raise AccountImportError(f"Row {row_index}: {exc}") from exc


def _parse_required_text(value: Any, *, field_name: str, row_index: int) -> str:
    normalized = _parse_optional_text(value)
    if not normalized:
        raise AccountImportError(f"Row {row_index}: {field_name} is required")
    return normalized


def _parse_optional_text(value: Any) -> str:
    return str(value or "").strip()


def _parse_optional_bool(value: Any, *, field_name: str, row_index: int) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if not normalized:
        return False
    if normalized in TRUE_VALUES:
        return True
    if normalized in FALSE_VALUES:
        return False
    raise AccountImportError(
        f"Row {row_index}: {field_name} must be one of true/false/1/0/yes/no/on/off"
    )
