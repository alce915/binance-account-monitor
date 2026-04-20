from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

import monitor_app.api as api_module
from monitor_app.access_control.service import AccessControlService
from monitor_app.config import settings
from monitor_app.funding_transfer import FundingTransferRequestRejected


class FakeFundingService:
    async def get_group_overview(self, main_id: str) -> dict:
        return {
            "main_account_id": main_id,
            "main_account_name": "Group A",
            "available": True,
            "reason": "Main UID 123456789 email main@example.com token=abc",
            "write_enabled": True,
            "write_disabled_reason": "",
            "assets": ["USDT"],
            "request_id": "req-123",
            "timings": {"total_ms": 12},
            "error_context": {"main_account_query": {"message": "email=main@example.com"}},
            "main_account": {
                "uid": "123456789",
                "transfer_ready": True,
                "reason": "UID 123456789 email main@example.com",
                "spot_assets": [{"asset": "USDT", "free": "100", "locked": "0", "total": "100"}],
                "spot_available": {"USDT": "100"},
                "funding_assets": [{"asset": "USDT", "free": "100", "locked": "0", "total": "100"}],
                "funding_available": {"USDT": "100"},
            },
            "children": [
                {
                    "account_id": "group_a.sub1",
                    "child_account_id": "sub1",
                    "name": "Sub One",
                    "uid": "223456789",
                    "eligible": True,
                    "reason": "UID 223456789 email child@example.com",
                    "can_distribute": True,
                    "can_collect": True,
                    "reason_distribute": "UID 223456789 email child@example.com token=child",
                    "reason_collect": "UID 223456789 email child@example.com token=child",
                    "spot_assets": [{"asset": "USDT", "free": "8", "locked": "0", "total": "8"}],
                    "spot_available": {"USDT": "8"},
                    "funding_assets": [{"asset": "USDT", "free": "8", "locked": "0", "total": "8"}],
                    "funding_available": {"USDT": "8"},
                }
            ],
        }

    async def get_audit_entries(self, main_id: str, *, limit: int = 50) -> dict:
        return {
            "main_account_id": main_id,
            "entries": [
                {
                    "created_at": "2026-04-18T12:00:00+08:00",
                    "updated_at": "2026-04-18T12:00:10+08:00",
                    "operation_id": "op-12345678",
                    "direction": "distribute",
                    "execution_stage": "completed",
                    "operation_status": "operation_fully_succeeded",
                    "asset": "USDT",
                    "message": "completed",
                    "account_count": 1,
                    "success_count": 1,
                    "failure_count": 0,
                    "confirmed_count": 1,
                    "pending_confirmation_count": 0,
                }
            ],
        }

    async def get_audit_entry_detail(self, main_id: str, operation_id: str, *, direction: str | None = None) -> dict:
        return {
            "operation_id": operation_id,
            "direction": direction or "distribute",
            "execution_stage": "completed",
            "operation_status": "operation_fully_succeeded",
            "asset": "USDT",
            "message": "completed",
            "operation_summary": {
                "asset": "USDT",
                "requested_total_amount": "5",
                "attempted_count": 1,
                "success_count": 1,
                "failure_count": 0,
                "confirmed_count": 1,
                "pending_confirmation_count": 0,
                "main_before_available_amount": "100",
                "main_after_available_amount": "95",
                "expected_main_direction": "decrease",
                "unconfirmed_account_ids": [],
            },
            "precheck": {
                "asset": "USDT",
                "requested_total_amount": "5",
                "validated_account_count": 1,
                "main_available_amount": "100",
                "accounts": [{"account_id": "group_a.sub1", "precheck_available_amount": "8"}],
            },
            "results": [{"account_id": "group_a.sub1", "uid": "223456789", "message": "ok"}],
            "overview_refresh": {"success": True, "message": "ok"},
            "reconciliation": {
                "status": "confirmed",
                "confirmed_count": 1,
                "failed_count": 0,
                "results": [
                    {
                        "account_id": "group_a.sub1",
                        "before_available_amount": "8",
                        "after_available_amount": "13",
                        "expected_direction": "increase",
                        "confirmed": True,
                        "message": "ok",
                    }
                ],
            },
        }

    async def distribute(self, main_id: str, *, asset: str, operation_id: str, transfers: list[dict]) -> dict:
        return {
            "direction": "distribute",
            "asset": asset,
            "operation_id": operation_id,
            "execution_stage": "completed",
            "operation_status": "operation_fully_succeeded",
            "operation_summary": {
                "asset": asset,
                "requested_total_amount": transfers[0]["amount"],
                "attempted_count": 1,
                "success_count": 1,
                "failure_count": 0,
                "confirmed_count": 1,
                "pending_confirmation_count": 0,
                "main_before_available_amount": "100",
                "main_after_available_amount": "95",
                "expected_main_direction": "decrease",
                "unconfirmed_account_ids": [],
            },
            "precheck": {
                "asset": asset,
                "requested_total_amount": transfers[0]["amount"],
                "validated_account_count": 1,
                "main_available_amount": "100",
                "accounts": [],
            },
            "results": [{
                "account_id": transfers[0]["account_id"],
                "success": True,
                "amount": transfers[0]["amount"],
                "requested_amount": transfers[0]["amount"],
                "normalized_amount": transfers[0]["amount"],
                "precheck_available_amount": "-",
                "executed_amount": transfers[0]["amount"],
                "transfer_attempted": True,
                "message": "ok",
            }],
            "overview_refresh": {"success": True, "message": "ok"},
            "reconciliation": {"status": "confirmed", "confirmed_count": 1, "failed_count": 0, "results": []},
            "overview": await self.get_group_overview(main_id),
            "message": "Distribute succeeded for 1 sub-accounts",
        }

    async def collect(self, main_id: str, *, asset: str, operation_id: str, transfers: list[dict], account_ids: list[str]) -> dict:
        account_id = transfers[0]["account_id"] if transfers else account_ids[0]
        amount = transfers[0]["amount"] if transfers else "8"
        return {
            "direction": "collect",
            "asset": asset,
            "operation_id": operation_id,
            "execution_stage": "completed",
            "operation_status": "operation_fully_succeeded",
            "operation_summary": {
                "asset": asset,
                "requested_total_amount": amount,
                "attempted_count": 1,
                "success_count": 1,
                "failure_count": 0,
                "confirmed_count": 1,
                "pending_confirmation_count": 0,
                "main_before_available_amount": "100",
                "main_after_available_amount": "108",
                "expected_main_direction": "increase",
                "unconfirmed_account_ids": [],
            },
            "precheck": {
                "asset": asset,
                "requested_total_amount": amount,
                "validated_account_count": 1,
                "main_available_amount": "100",
                "accounts": [],
            },
            "results": [{
                "account_id": account_id,
                "success": True,
                "amount": amount,
                "requested_amount": amount,
                "normalized_amount": amount,
                "precheck_available_amount": "8",
                "executed_amount": amount,
                "transfer_attempted": True,
                "message": "ok",
            }],
            "overview_refresh": {"success": True, "message": "ok"},
            "reconciliation": {"status": "confirmed", "confirmed_count": 1, "failed_count": 0, "results": []},
            "overview": await self.get_group_overview(main_id),
            "message": "Collect succeeded for 1 sub-accounts",
        }


def _build_disabled_access_control(tmp_path: Path) -> AccessControlService:
    config_path = tmp_path / "access_control.json"
    audit_db_path = tmp_path / "access_audit.db"
    config_path.write_text(
        json.dumps(
            {
                "enabled": False,
                "whitelist_ips": [],
                "allow_plaintext_secrets": True,
                "cookie_secure_mode": "auto",
                "guest_password": "guest-pass",
                "admin_password": "admin-pass",
                "session_secret": "dev-session-secret-1234567890",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return AccessControlService(settings, config_path=config_path, audit_db_path=audit_db_path)


def test_get_funding_group_endpoint_returns_overview(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        client.app.state.access_control = _build_disabled_access_control(tmp_path)
        client.app.state.funding_transfer = FakeFundingService()
        response = client.get("/api/funding/groups/group_a")

    assert response.status_code == 200
    payload = response.json()
    assert payload["main_account_id"] == "group_a"
    assert payload["available"] is True
    assert payload["assets"] == ["USDT"]
    assert "request_id" not in payload
    assert "timings" not in payload
    assert "error_context" not in payload
    assert payload["main_account"]["uid"] == "1234***89"
    assert payload["children"][0]["uid"] == "2234***89"
    assert "main@example.com" not in str(payload)
    assert "child@example.com" not in str(payload)
    assert "123456789" not in str(payload)
    assert "223456789" not in str(payload)
    assert "[redacted-email]" in payload["reason"]
    assert "[redacted]" in payload["children"][0]["reason_distribute"]


def test_distribute_group_funding_endpoint_returns_operation_result_and_header(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        client.app.state.access_control = _build_disabled_access_control(tmp_path)
        client.app.state.funding_transfer = FakeFundingService()
        response = client.post(
            "/api/funding/groups/group_a/distribute",
            json={"asset": "USDT", "operation_id": "op-distribute", "transfers": [{"account_id": "group_a.sub1", "amount": "5"}]},
        )

    assert response.status_code == 200
    assert response.headers["X-Funding-Operation-Id"] == "op-distribute"
    payload = response.json()
    assert payload["direction"] == "distribute"
    assert payload["operation_status"] == "operation_fully_succeeded"
    assert payload["execution_stage"] == "completed"
    assert payload["operation_id"] == "op-distribute"
    assert payload["results"][0]["success"] is True
    assert payload["overview"]["main_account"]["uid"] == "1234***89"
    assert "request_id" not in payload
    assert "timings" not in payload
    assert "error_context" not in payload


def test_collect_group_funding_endpoint_returns_operation_result(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        client.app.state.access_control = _build_disabled_access_control(tmp_path)
        client.app.state.funding_transfer = FakeFundingService()
        response = client.post(
            "/api/funding/groups/group_a/collect",
            json={"asset": "USDT", "operation_id": "op-collect", "transfers": [{"account_id": "group_a.sub1", "amount": "6.5"}]},
        )

    assert response.status_code == 200
    assert response.headers["X-Funding-Operation-Id"] == "op-collect"
    payload = response.json()
    assert payload["direction"] == "collect"
    assert payload["operation_status"] == "operation_fully_succeeded"
    assert payload["operation_id"] == "op-collect"
    assert payload["results"][0]["account_id"] == "group_a.sub1"
    assert payload["results"][0]["amount"] == "6.5"
    assert payload["overview"]["children"][0]["uid"] == "2234***89"


def test_collect_group_funding_endpoint_accepts_legacy_account_ids_payload(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        client.app.state.access_control = _build_disabled_access_control(tmp_path)
        client.app.state.funding_transfer = FakeFundingService()
        response = client.post(
            "/api/funding/groups/group_a/collect",
            json={"asset": "USDT", "operation_id": "op-legacy", "account_ids": ["group_a.sub1"]},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["direction"] == "collect"
    assert payload["results"][0]["account_id"] == "group_a.sub1"


def test_get_funding_group_audit_endpoint_returns_sanitized_summary_entries(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        client.app.state.access_control = _build_disabled_access_control(tmp_path)
        client.app.state.funding_transfer = FakeFundingService()
        response = client.get("/api/funding/groups/group_a/audit")

    assert response.status_code == 200
    payload = response.json()
    assert payload["entries"][0]["operation_id"] == "op-12345678"
    assert payload["entries"][0]["execution_stage"] == "completed"
    assert "results" not in payload["entries"][0]


def test_get_funding_group_audit_detail_endpoint_returns_sanitized_detail(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        client.app.state.access_control = _build_disabled_access_control(tmp_path)
        client.app.state.funding_transfer = FakeFundingService()
        response = client.get("/api/funding/groups/group_a/audit/op-12345678?direction=distribute")

    assert response.status_code == 200
    payload = response.json()
    assert payload["operation_id"] == "op-12345678"
    assert payload["direction"] == "distribute"
    assert payload["results"][0]["uid"] == "2234***89"
    assert "223456789" not in str(payload)


def test_get_funding_group_audit_detail_endpoint_requires_direction(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        client.app.state.access_control = _build_disabled_access_control(tmp_path)
        client.app.state.funding_transfer = FakeFundingService()
        response = client.get("/api/funding/groups/group_a/audit/op-12345678")

    assert response.status_code == 422


def test_funding_write_endpoint_returns_structured_error_for_missing_operation_id(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        client.app.state.access_control = _build_disabled_access_control(tmp_path)
        client.app.state.funding_transfer = FakeFundingService()
        response = client.post(
            "/api/funding/groups/group_a/distribute",
            json={"asset": "USDT", "transfers": [{"account_id": "group_a.sub1", "amount": "5"}]},
        )

    assert response.status_code == 400
    payload = response.json()
    assert payload["detail"] == "operation_id is required"
    assert payload["error"]["code"] == "OPERATION_ID_REQUIRED"


def test_distribute_group_funding_endpoint_preserves_main_email_map_error_code(tmp_path: Path) -> None:
    class EmailMapFailureService(FakeFundingService):
        async def distribute(self, main_id: str, *, asset: str, operation_id: str, transfers: list[dict]) -> dict:
            raise FundingTransferRequestRejected(
                "Failed to query sub-account mapping",
                code="SUB_ACCOUNT_MAPPING_QUERY_FAILED",
                operation_id=operation_id,
            )

    with TestClient(api_module.app) as client:
        client.app.state.access_control = _build_disabled_access_control(tmp_path)
        client.app.state.funding_transfer = EmailMapFailureService()
        response = client.post(
            "/api/funding/groups/group_a/distribute",
            json={"asset": "USDT", "operation_id": "op-email-map-fail", "transfers": [{"account_id": "group_a.sub1", "amount": "5"}]},
        )

    assert response.status_code == 400
    assert response.headers["X-Funding-Operation-Id"] == "op-email-map-fail"
    payload = response.json()
    assert payload["detail"] == "Failed to query sub-account mapping"
    assert payload["error"]["code"] == "SUB_ACCOUNT_MAPPING_QUERY_FAILED"
    assert payload["error"]["operation_id"] == "op-email-map-fail"


def test_distribute_group_funding_endpoint_preserves_main_spot_query_error_code(tmp_path: Path) -> None:
    class MainSpotFailureService(FakeFundingService):
        async def distribute(self, main_id: str, *, asset: str, operation_id: str, transfers: list[dict]) -> dict:
            raise FundingTransferRequestRejected(
                "Failed to query main account spot balances",
                code="MAIN_BALANCE_QUERY_FAILED",
                operation_id=operation_id,
            )

    with TestClient(api_module.app) as client:
        client.app.state.access_control = _build_disabled_access_control(tmp_path)
        client.app.state.funding_transfer = MainSpotFailureService()
        response = client.post(
            "/api/funding/groups/group_a/distribute",
            json={"asset": "USDT", "operation_id": "op-main-spot-fail", "transfers": [{"account_id": "group_a.sub1", "amount": "5"}]},
        )

    assert response.status_code == 400
    assert response.headers["X-Funding-Operation-Id"] == "op-main-spot-fail"
    payload = response.json()
    assert payload["detail"] == "Failed to query main account spot balances"
    assert payload["error"]["code"] == "MAIN_BALANCE_QUERY_FAILED"
    assert payload["error"]["operation_id"] == "op-main-spot-fail"
