from __future__ import annotations

from fastapi.testclient import TestClient

import monitor_app.api as api_module


class FakeAlertMonitor:
    async def send_test_telegram_notification(self, message: str | None = None) -> dict:
        return {"status": "queued", "echo": message or "UniMMR Telegram test notification"}

    async def unimmr_alert_status(self) -> dict:
        return {
            "enabled": True,
            "telegram": {"queued": 1, "sent": 1, "dropped": 0, "failed": 0, "last_error": "", "last_sent_at": None},
            "accounts": [
                {
                    "account_id": "group_a.sub1",
                    "current_band": "warning",
                    "last_value": "1.45",
                    "last_reason": "warning_entry",
                }
            ],
        }

    async def simulate_unimmr_alerts(self, updates: list[dict[str, str]]) -> dict:
        return {"triggered": len(updates), "updates": updates}


def test_post_telegram_test_endpoint_returns_queue_result_and_stats() -> None:
    with TestClient(api_module.app) as client:
        client.app.state.allow_test_non_loopback = True
        client.app.state.monitor = FakeAlertMonitor()
        response = client.post("/api/alerts/telegram/test", json={"message": "hello tg"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["result"]["status"] == "queued"
    assert payload["result"]["echo"] == "hello tg"
    assert payload["stats"]["enabled"] is True


def test_get_unimmr_alert_status_returns_summary() -> None:
    with TestClient(api_module.app) as client:
        client.app.state.allow_test_non_loopback = True
        client.app.state.monitor = FakeAlertMonitor()
        response = client.get("/api/alerts/unimmr/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["enabled"] is True
    assert payload["accounts"][0]["account_id"] == "group_a.sub1"


def test_post_unimmr_simulate_endpoint_returns_result_and_stats() -> None:
    with TestClient(api_module.app) as client:
        client.app.state.allow_test_non_loopback = True
        client.app.state.monitor = FakeAlertMonitor()
        response = client.post(
            "/api/alerts/unimmr/simulate",
            json={"updates": [{"account_id": "group_a.sub1", "uni_mmr": "1.18"}]},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["result"]["triggered"] == 1
    assert payload["result"]["updates"][0]["account_id"] == "group_a.sub1"
    assert payload["stats"]["enabled"] is True
