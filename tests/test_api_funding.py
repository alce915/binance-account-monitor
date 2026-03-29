from __future__ import annotations

from fastapi.testclient import TestClient

import monitor_app.api as api_module


class FakeFundingService:
    async def get_group_overview(self, main_id: str) -> dict:
        return {
            "main_account_id": main_id,
            "main_account_name": "Group A",
            "available": True,
            "reason": "Main UID 123456789 email main@example.com token=abc",
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

    async def distribute(self, main_id: str, *, asset: str, transfers: list[dict]) -> dict:
        return {
            "direction": "distribute",
            "asset": asset,
            "results": [{"account_id": transfers[0]["account_id"], "success": True, "amount": transfers[0]["amount"], "message": "ok"}],
            "overview": await self.get_group_overview(main_id),
            "message": "分发成功，共处理 1 个子账号",
        }

    async def collect(self, main_id: str, *, asset: str, transfers: list[dict], account_ids: list[str]) -> dict:
        account_id = transfers[0]["account_id"] if transfers else account_ids[0]
        amount = transfers[0]["amount"] if transfers else "8"
        return {
            "direction": "collect",
            "asset": asset,
            "results": [{"account_id": account_id, "success": True, "amount": amount, "message": "ok"}],
            "overview": await self.get_group_overview(main_id),
            "message": "归集成功，共处理 1 个子账号",
        }


def test_get_funding_group_endpoint_returns_overview() -> None:
    with TestClient(api_module.app) as client:
        client.app.state.allow_test_non_loopback = True
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


def test_distribute_group_funding_endpoint_returns_operation_result() -> None:
    with TestClient(api_module.app) as client:
        client.app.state.allow_test_non_loopback = True
        client.app.state.funding_transfer = FakeFundingService()
        response = client.post(
            "/api/funding/groups/group_a/distribute",
            json={"asset": "USDT", "transfers": [{"account_id": "group_a.sub1", "amount": "5"}]},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["direction"] == "distribute"
    assert payload["results"][0]["success"] is True
    assert payload["overview"]["main_account"]["uid"] == "1234***89"
    assert "request_id" not in payload
    assert "timings" not in payload
    assert "error_context" not in payload


def test_collect_group_funding_endpoint_returns_operation_result() -> None:
    with TestClient(api_module.app) as client:
        client.app.state.allow_test_non_loopback = True
        client.app.state.funding_transfer = FakeFundingService()
        response = client.post(
            "/api/funding/groups/group_a/collect",
            json={"asset": "USDT", "transfers": [{"account_id": "group_a.sub1", "amount": "6.5"}]},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["direction"] == "collect"
    assert payload["results"][0]["account_id"] == "group_a.sub1"
    assert payload["results"][0]["amount"] == "6.5"
    assert payload["overview"]["children"][0]["uid"] == "2234***89"


def test_collect_group_funding_endpoint_accepts_legacy_account_ids_payload() -> None:
    with TestClient(api_module.app) as client:
        client.app.state.allow_test_non_loopback = True
        client.app.state.funding_transfer = FakeFundingService()
        response = client.post(
            "/api/funding/groups/group_a/collect",
            json={"asset": "USDT", "account_ids": ["group_a.sub1"]},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["direction"] == "collect"
    assert payload["results"][0]["account_id"] == "group_a.sub1"
