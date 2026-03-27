from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from io import BytesIO
from pathlib import Path

from fastapi.testclient import TestClient
from openpyxl import Workbook

import monitor_app.api as api_module
from monitor_app.account_monitor import AccountMonitorController
from monitor_app.config import MonitorAccountConfig, Settings


class FakeImportGateway:
    def __init__(self, account: MonitorAccountConfig) -> None:
        self.account = account

    async def get_unified_account_snapshot(
        self,
        *,
        history_window_days: int = 7,
        income_limit: int = 100,
        interest_limit: int = 100,
        previous_snapshot: dict | None = None,
        mark_price_provider=None,
        refresh_id: str | None = None,
    ) -> dict:
        return {
            "status": "ok",
            "source": "papi",
            "account_id": self.account.account_id,
            "account_name": self.account.display_name,
            "main_account_id": self.account.main_account_id,
            "main_account_name": self.account.main_account_name,
            "child_account_id": self.account.child_account_id,
            "child_account_name": self.account.child_account_name,
            "account_status": "NORMAL",
            "updated_at": "2026-03-27T00:00:00+00:00",
            "message": "ok",
            "totals": {
                "equity": Decimal("100"),
                "margin": Decimal("10"),
                "available_balance": Decimal("90"),
                "unrealized_pnl": Decimal("5"),
                "total_income": Decimal("0"),
                "total_commission": Decimal("0"),
                "total_distribution": Decimal("0"),
                "distribution_apy_7d": Decimal("0"),
                "total_interest": Decimal("0"),
            },
            "positions": [],
            "assets": [],
            "income_summary": {
                "window_days": history_window_days,
                "records": 0,
                "total_income": Decimal("0"),
                "total_commission": Decimal("0"),
                "by_type": {},
                "by_asset": {},
            },
            "distribution_summary": {
                "window_days": history_window_days,
                "records": 0,
                "total_distribution": Decimal("0"),
                "by_type": {},
                "by_asset": {},
            },
            "distribution_profit_summary": {
                "today": {"label": "today", "amount": Decimal("0"), "rate": Decimal("0"), "start_at": None, "complete": True},
                "week": {"label": "week", "amount": Decimal("0"), "rate": Decimal("0"), "start_at": None, "complete": True},
                "month": {"label": "month", "amount": Decimal("0"), "rate": Decimal("0"), "start_at": None, "complete": True},
                "year": {"label": "year", "amount": Decimal("0"), "rate": Decimal("0"), "start_at": None, "complete": True},
                "all": {"label": "all", "amount": Decimal("0"), "rate": Decimal("0"), "start_at": None, "complete": True},
                "backfill_complete": True,
            },
            "interest_summary": {
                "window_days": history_window_days,
                "records": 0,
                "margin_interest_total": Decimal("0"),
                "negative_balance_interest_total": Decimal("0"),
                "total_interest": Decimal("0"),
            },
            "section_errors": {},
            "diagnostics": {
                "refresh_id": refresh_id,
                "timings": {"gateway_total_ms": 1},
                "fallback_sections": [],
            },
        }

    async def close(self) -> None:
        return None


class FailingImportGateway(FakeImportGateway):
    async def get_unified_account_snapshot(
        self,
        *,
        history_window_days: int = 7,
        income_limit: int = 100,
        interest_limit: int = 100,
        previous_snapshot: dict | None = None,
        mark_price_provider=None,
        refresh_id: str | None = None,
    ) -> dict:
        raise RuntimeError("refresh failed")


def test_import_excel_endpoint_replaces_accounts_and_refreshes(monkeypatch, tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        monitor_accounts_file=tmp_path / "config" / "binance_monitor_accounts.json",
        monitor_refresh_interval_ms=999999,
        monitor_history_window_days=3,
        monitor_history_db_path=tmp_path / "history.db",
    )
    monkeypatch.setattr(api_module, "settings", settings)

    with TestClient(api_module.app) as client:
        controller = AccountMonitorController(settings, gateway_factory=lambda account: FakeImportGateway(account))
        client.app.state.monitor = controller
        try:
            response = client.post(
                "/api/config/import/excel",
                files={"file": ("accounts.xlsx", _build_workbook_bytes(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            )
        finally:
            asyncio.run(controller.close())

    assert response.status_code == 200
    payload = response.json()
    assert payload["message"] == "Excel 导入成功，数据已刷新"
    assert payload["import_result"] == {
        "file_name": "accounts.xlsx",
        "main_account_count": 2,
        "account_count": 3,
        "mode": "replace_all",
    }
    assert payload["refresh_result"]["success"] is True
    assert payload["service"]["account_ids"] == ["group_a.sub1", "group_a.sub2", "group_b.sub1"]
    assert payload["service"]["main_account_ids"] == ["group_a", "group_b"]
    persisted = json.loads(settings.monitor_accounts_file.read_text(encoding="utf-8"))
    assert len(persisted["main_accounts"]) == 2
    assert persisted["main_accounts"][0]["main_id"] == "group_a"


def test_import_excel_endpoint_rejects_non_xlsx(monkeypatch, tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        monitor_accounts_file=tmp_path / "config" / "binance_monitor_accounts.json",
        monitor_history_db_path=tmp_path / "history.db",
    )
    monkeypatch.setattr(api_module, "settings", settings)

    with TestClient(api_module.app) as client:
        response = client.post(
            "/api/config/import/excel",
            files={"file": ("accounts.csv", b"not-used", "text/csv")},
        )

    assert response.status_code == 400
    assert response.json()["detail"] == "Only .xlsx files are supported"


def test_import_excel_validation_failure_does_not_overwrite_existing_file(monkeypatch, tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        monitor_accounts_file=tmp_path / "config" / "binance_monitor_accounts.json",
        monitor_history_db_path=tmp_path / "history.db",
    )
    settings.monitor_accounts_file.parent.mkdir(parents=True, exist_ok=True)
    existing_payload = {
        "main_accounts": [
            {
                "main_id": "existing",
                "name": "Existing",
                "children": [
                    {"account_id": "sub1", "name": "Old", "api_key": "k1", "api_secret": "s1"}
                ],
            }
        ]
    }
    settings.monitor_accounts_file.write_text(json.dumps(existing_payload), encoding="utf-8")
    monkeypatch.setattr(api_module, "settings", settings)

    with TestClient(api_module.app) as client:
        response = client.post(
            "/api/config/import/excel",
            files={"file": ("accounts.xlsx", _build_invalid_workbook_bytes(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        )

    assert response.status_code == 400
    assert response.json()["detail"] == "Missing required columns: api_secret"
    persisted = json.loads(settings.monitor_accounts_file.read_text(encoding="utf-8"))
    assert persisted == existing_payload


def test_import_excel_endpoint_reports_refresh_failure_after_successful_write(monkeypatch, tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        monitor_accounts_file=tmp_path / "config" / "binance_monitor_accounts.json",
        monitor_refresh_interval_ms=999999,
        monitor_history_window_days=3,
        monitor_history_db_path=tmp_path / "history.db",
    )
    monkeypatch.setattr(api_module, "settings", settings)

    with TestClient(api_module.app) as client:
        controller = AccountMonitorController(settings, gateway_factory=lambda account: FailingImportGateway(account))
        client.app.state.monitor = controller
        try:
            response = client.post(
                "/api/config/import/excel",
                files={"file": ("accounts.xlsx", _build_workbook_bytes(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            )
        finally:
            asyncio.run(controller.close())

    assert response.status_code == 200
    payload = response.json()
    assert payload["message"] == "Excel 导入成功，但刷新失败"
    assert payload["import_result"]["main_account_count"] == 2
    assert payload["refresh_result"]["success"] is False
    assert payload["service"]["account_ids"] == ["group_a.sub1", "group_a.sub2", "group_b.sub1"]
    persisted = json.loads(settings.monitor_accounts_file.read_text(encoding="utf-8"))
    assert len(persisted["main_accounts"]) == 2


def test_download_excel_template_endpoint_returns_sample_workbook() -> None:
    with TestClient(api_module.app) as client:
        response = client.get("/api/config/import/excel-template")

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    assert response.headers["content-disposition"] == 'attachment; filename="monitor_accounts_template.xlsx"'

    loaded = _load_workbook_bytes(response.content)
    try:
        worksheet = loaded.worksheets[0]
        rows = list(worksheet.iter_rows(values_only=True))
    finally:
        loaded.close()

    assert rows[0] == (
        "main_id",
        "main_name",
        "account_id",
        "name",
        "api_key",
        "api_secret",
        "use_testnet",
        "rest_base_url",
        "ws_base_url",
    )
    assert rows[1][:4] == ("group_a", "A组", "sub1", "张三")
    assert rows[3][:4] == ("group_b", "B组", "sub1", "王五")


def _build_workbook_bytes() -> bytes:
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.append(["main_id", "main_name", "account_id", "name", "api_key", "api_secret", "use_testnet"])
    worksheet.append(["group_a", "Group A", "sub1", "Sub One", "k1", "s1", "true"])
    worksheet.append(["group_a", "Group A", "sub2", "Sub Two", "k2", "s2", ""])
    worksheet.append(["group_b", "Group B", "sub1", "Sub Three", "k3", "s3", "false"])
    buffer = BytesIO()
    workbook.save(buffer)
    workbook.close()
    return buffer.getvalue()


def _build_invalid_workbook_bytes() -> bytes:
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.append(["main_id", "main_name", "account_id", "name", "api_key"])
    worksheet.append(["group_a", "Group A", "sub1", "Sub One", "k1"])
    buffer = BytesIO()
    workbook.save(buffer)
    workbook.close()
    return buffer.getvalue()


def _load_workbook_bytes(content: bytes):
    from openpyxl import load_workbook

    return load_workbook(filename=BytesIO(content), read_only=True, data_only=True)
