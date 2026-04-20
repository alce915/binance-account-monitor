from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

import monitor_app.api as api_module
from monitor_app.access_control.service import AccessControlService
from monitor_app.config import settings


class FakeMonitor:
    def current_groups(self, account_ids=None) -> dict:
        return {
            "status": "ok",
            "message": "uid 123456789 email main@example.com token=abc",
            "service": {
                "monitor_enabled": True,
                "account_ids": ["group_a.sub1"],
                "main_account_ids": ["group_a"],
            },
            "groups": [
                {
                    "main_account_id": "group_a",
                    "main_account_name": "Group A",
                    "accounts": [
                        {
                            "account_id": "group_a.sub1",
                            "account_name": "Sub One",
                            "message": "uid 223456789 email child@example.com token=child",
                            "diagnostics": {"timings": {"gateway_total_ms": 17}},
                            "section_errors": {"spot_account": {"message": "secret"}},
                        }
                    ],
                }
            ],
        }

    async def refresh_now(self) -> dict:
        return {
            "status": "ok",
            "message": "uid 123456789 email main@example.com token=abc",
            "service": {
                "monitor_enabled": True,
                "account_ids": ["group_a.sub1"],
                "main_account_ids": ["group_a"],
            },
            "groups": self.current_groups()["groups"],
            "summary": {"account_count": 1, "success_count": 1, "error_count": 0},
            "refresh_result": {
                "success": True,
                "message": "uid 223456789 email child@example.com token=child",
                "refresh_id": "refresh-1",
                "failed_accounts": ["group_a.sub1"],
                "slow_accounts": [{"account_id": "group_a.sub1", "duration_ms": 17}],
                "timings": {"total_ms": 17, "broadcast_ms": 4},
            },
            "refresh_meta": {"slow_accounts": [{"account_id": "group_a.sub1"}]},
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


def test_docs_endpoints_are_disabled(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        access_control = _build_disabled_access_control(tmp_path)
        api_module.app.state.access_control = access_control
        client.app.state.access_control = access_control
        assert client.get("/docs", follow_redirects=False).status_code == 404
        assert client.get("/redoc", follow_redirects=False).status_code == 404
        assert client.get("/openapi.json", follow_redirects=False).status_code == 404


def test_monitor_groups_response_is_sanitized_for_public_clients(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        access_control = _build_disabled_access_control(tmp_path)
        monitor = FakeMonitor()
        api_module.app.state.access_control = access_control
        api_module.app.state.monitor = monitor
        client.app.state.access_control = access_control
        client.app.state.monitor = monitor
        response = client.get("/api/monitor/groups")

    assert response.status_code == 200
    payload = response.json()
    assert payload["service"] == {
        "monitor_enabled": True,
        "account_ids": ["group_a.sub1"],
        "main_account_ids": ["group_a"],
    }
    assert "[redacted-email]" in payload["message"]
    assert "123456789" not in str(payload)
    assert "223456789" not in str(payload)
    account = payload["groups"][0]["accounts"][0]
    assert "diagnostics" not in account
    assert "section_errors" not in account
    assert "[redacted-email]" in account["message"]


def test_monitor_refresh_response_strips_internal_diagnostics(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        access_control = _build_disabled_access_control(tmp_path)
        monitor = FakeMonitor()
        api_module.app.state.access_control = access_control
        api_module.app.state.monitor = monitor
        client.app.state.access_control = access_control
        client.app.state.monitor = monitor
        response = client.post("/api/monitor/refresh")

    assert response.status_code == 200
    payload = response.json()
    assert "refresh_meta" not in payload
    assert payload["refresh_result"]["timings"] == {"total_ms": 17}
    assert "refresh_id" not in payload["refresh_result"]
    assert "failed_accounts" not in payload["refresh_result"]
    assert "slow_accounts" not in payload["refresh_result"]
