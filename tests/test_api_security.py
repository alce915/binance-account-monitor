from __future__ import annotations

import httpx
import pytest
from fastapi.testclient import TestClient

import monitor_app.api as api_module


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


def test_docs_endpoints_are_disabled() -> None:
    with TestClient(api_module.app) as client:
        client.app.state.allow_test_non_loopback = True
        assert client.get("/docs").status_code == 404
        assert client.get("/redoc").status_code == 404
        assert client.get("/openapi.json").status_code == 404


@pytest.mark.asyncio
async def test_loopback_guard_rejects_non_loopback_client() -> None:
    transport = httpx.ASGITransport(app=api_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://localhost") as client:
        response = await client.get("/healthz", headers={"host": "198.51.100.10"})

    assert response.status_code in {400, 403}


def test_monitor_groups_response_is_sanitized_for_public_clients() -> None:
    with TestClient(api_module.app) as client:
        client.app.state.allow_test_non_loopback = True
        client.app.state.monitor = FakeMonitor()
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


def test_monitor_refresh_response_strips_internal_diagnostics() -> None:
    with TestClient(api_module.app) as client:
        client.app.state.allow_test_non_loopback = True
        client.app.state.monitor = FakeMonitor()
        response = client.post("/api/monitor/refresh")

    assert response.status_code == 200
    payload = response.json()
    assert "refresh_meta" not in payload
    assert payload["refresh_result"]["timings"] == {"total_ms": 17}
    assert "refresh_id" not in payload["refresh_result"]
    assert "failed_accounts" not in payload["refresh_result"]
    assert "slow_accounts" not in payload["refresh_result"]
