from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import monitor_app.api as api_module
from monitor_app.access_control.service import AUTH_COOKIE_NAME, AccessControlService, route_capability_snapshot
from monitor_app.config import settings


class FakeMonitor:
    def current_summary(self, account_ids=None) -> dict:
        return {
            "status": "ok",
            "message": "summary",
            "updated_at": "2026-04-19T00:00:00+08:00",
            "service": {"monitor_enabled": True},
            "summary": {},
        }

    def current_groups(self, account_ids=None) -> dict:
        return {
            "status": "ok",
            "message": "groups",
            "updated_at": "2026-04-19T00:00:00+08:00",
            "service": {"monitor_enabled": True},
            "summary": {},
            "profit_summary": {},
            "groups": [],
        }

    def current_accounts(self, account_ids=None) -> dict:
        return {
            "status": "ok",
            "message": "accounts",
            "updated_at": "2026-04-19T00:00:00+08:00",
            "service": {"monitor_enabled": True},
            "summary": {},
            "profit_summary": {},
            "accounts": [],
        }

    async def set_monitor_enabled(self, enabled: bool) -> dict:
        return {
            "status": "disabled" if not enabled else "ok",
            "message": "monitor toggled",
            "service": {"monitor_enabled": enabled},
        }

    async def refresh_now(self) -> dict:
        return {
            "status": "ok",
            "message": "\u6240\u6709\u8d26\u53f7\u72b6\u6001\u6b63\u5e38",
            "updated_at": "2026-04-19T00:00:00+08:00",
            "service": {"monitor_enabled": True},
            "summary": {},
            "profit_summary": {},
            "groups": [],
            "refresh_result": {"success": True, "message": "\u5237\u65b0\u5b8c\u6210"},
        }

    async def send_test_telegram_notification(self, message: str | None = None) -> dict:
        return {"status": "queued", "echo": message or "hello"}

    async def unimmr_alert_status(self) -> dict:
        return {"enabled": True, "monitor_enabled": True, "accounts": [], "telegram": {}}

    async def simulate_unimmr_alerts(self, updates: list[dict[str, str]]) -> dict:
        return {"triggered": len(updates), "simulated": True}

    async def subscribe(self, account_ids=None):
        raise RuntimeError("not used in auth tests")

    def unsubscribe(self, queue) -> None:
        return None


def write_auth_config(path: Path, *, enabled: bool, whitelist_ips: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "enabled": enabled,
                "whitelist_ips": whitelist_ips or [],
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


def build_auth_service(tmp_path: Path, *, enabled: bool, whitelist_ips: list[str] | None = None) -> AccessControlService:
    config_path = tmp_path / "access_control.json"
    audit_db_path = tmp_path / "access_audit.db"
    write_auth_config(config_path, enabled=enabled, whitelist_ips=whitelist_ips)
    return AccessControlService(settings, config_path=config_path, audit_db_path=audit_db_path)


def test_auth_disabled_allows_non_whitelisted_write_access(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        client.app.state.monitor = FakeMonitor()
        client.app.state.access_control = build_auth_service(tmp_path, enabled=False)
        client.app.state.test_client_ip = "198.51.100.10"

        response = client.post("/api/monitor/refresh")

    assert response.status_code == 200
    assert response.json()["refresh_result"]["success"] is True


def test_enabled_auth_redirects_non_whitelisted_page_requests_to_login(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        client.app.state.monitor = FakeMonitor()
        client.app.state.access_control = build_auth_service(tmp_path, enabled=True)
        client.app.state.test_client_ip = "198.51.100.10"

        response = client.get("/", follow_redirects=False)

    assert response.status_code in {302, 307}
    assert response.headers["location"].startswith("/login?")


def test_enabled_auth_rejects_non_whitelisted_api_requests(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        client.app.state.monitor = FakeMonitor()
        client.app.state.access_control = build_auth_service(tmp_path, enabled=True)
        client.app.state.test_client_ip = "198.51.100.10"

        response = client.post("/api/monitor/refresh")

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "AUTH_REQUIRED"


def test_unmapped_protected_api_route_returns_policy_missing(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        client.app.state.monitor = FakeMonitor()
        client.app.state.access_control = build_auth_service(tmp_path, enabled=True)
        client.app.state.test_client_ip = "198.51.100.10"

        response = client.get("/api/not-declared")

    assert response.status_code == 500
    assert response.json()["error"]["code"] == "AUTH_POLICY_MISSING"


def test_public_login_assets_prefix_is_not_blocked_by_auth(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        client.app.state.monitor = FakeMonitor()
        client.app.state.access_control = build_auth_service(tmp_path, enabled=True)
        client.app.state.test_client_ip = "198.51.100.10"

        response = client.get("/public/login/example.css")

    assert response.status_code == 404


def test_guest_login_can_read_but_cannot_refresh(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        client.app.state.monitor = FakeMonitor()
        client.app.state.access_control = build_auth_service(tmp_path, enabled=True)
        client.app.state.test_client_ip = "198.51.100.10"

        login = client.post("/api/auth/login", json={"password": "guest-pass"})
        assert login.status_code == 200
        assert login.json()["role"] == "guest"

        read_response = client.get("/api/monitor/groups")
        write_response = client.post("/api/monitor/refresh")

    assert read_response.status_code == 200
    assert write_response.status_code == 403
    assert write_response.json()["error"]["code"] == "AUTH_ROLE_FORBIDDEN"


def test_guest_high_risk_write_routes_return_json_auth_errors(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        client.app.state.monitor = FakeMonitor()
        client.app.state.access_control = build_auth_service(tmp_path, enabled=True)
        client.app.state.test_client_ip = "198.51.100.10"

        login = client.post("/api/auth/login", json={"password": "guest-pass"})
        assert login.status_code == 200

        responses = {
            "monitor_control": client.post("/api/monitor/control", json={"enabled": False}),
            "monitor_refresh": client.post("/api/monitor/refresh"),
            "import_excel": client.post("/api/config/import/excel"),
            "funding_distribute": client.post(
                "/api/funding/groups/group_a/distribute",
                json={"asset": "USDT", "operation_id": "guest-test", "transfers": []},
            ),
            "funding_collect": client.post(
                "/api/funding/groups/group_a/collect",
                json={"asset": "USDT", "operation_id": "guest-test", "transfers": [], "account_ids": []},
            ),
            "telegram_test": client.post("/api/alerts/telegram/test", json={"message": "guest test"}),
            "unimmr_simulate": client.post(
                "/api/alerts/unimmr/simulate",
                json={"updates": [{"account_id": "group_a.sub1", "uni_mmr": "1.50"}]},
            ),
        }

    for name, response in responses.items():
        assert response.status_code == 403, name
        assert response.json()["error"]["code"] == "AUTH_ROLE_FORBIDDEN", name
        assert response.json()["error"]["message"] == "权限不足", name


def test_admin_login_requires_csrf_for_writes(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        client.app.state.monitor = FakeMonitor()
        client.app.state.access_control = build_auth_service(tmp_path, enabled=True)
        client.app.state.test_client_ip = "198.51.100.10"

        login = client.post("/api/auth/login", json={"password": "admin-pass"})
        assert login.status_code == 200
        csrf_token = login.json()["csrf_token"]

        blocked = client.post("/api/monitor/refresh")
        allowed = client.post("/api/monitor/refresh", headers={"X-CSRF-Token": csrf_token})

    assert blocked.status_code == 403
    assert blocked.json()["error"]["code"] == "AUTH_INVALID"
    assert allowed.status_code == 200


def test_admin_write_rejects_cross_origin_requests_even_with_valid_csrf(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        client.app.state.monitor = FakeMonitor()
        client.app.state.access_control = build_auth_service(tmp_path, enabled=True)
        client.app.state.test_client_ip = "198.51.100.10"

        login = client.post("/api/auth/login", json={"password": "admin-pass"})
        assert login.status_code == 200
        csrf_token = login.json()["csrf_token"]

        response = client.post(
            "/api/monitor/refresh",
            headers={
                "X-CSRF-Token": csrf_token,
                "Origin": "https://evil.example",
            },
        )

    assert response.status_code == 403
    assert response.json()["error"]["code"] == "AUTH_CROSS_ORIGIN_FORBIDDEN"


def test_whitelisted_ip_bypasses_login_but_receives_csrf_from_session_endpoint(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        client.app.state.monitor = FakeMonitor()
        client.app.state.access_control = build_auth_service(tmp_path, enabled=True, whitelist_ips=["198.51.100.10"])
        client.app.state.test_client_ip = "198.51.100.10"

        session_response = client.get("/api/auth/session")
        csrf_token = session_response.json()["csrf_token"]
        refresh_response = client.post("/api/monitor/refresh", headers={"X-CSRF-Token": csrf_token})

    assert session_response.status_code == 200
    assert session_response.json()["whitelisted"] is True
    assert session_response.json()["role"] == "admin"
    assert refresh_response.status_code == 200


def test_login_rate_limit_blocks_after_five_failures(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        client.app.state.monitor = FakeMonitor()
        client.app.state.access_control = build_auth_service(tmp_path, enabled=True)
        client.app.state.test_client_ip = "198.51.100.10"

        statuses = [client.post("/api/auth/login", json={"password": "wrong-pass"}).status_code for _ in range(5)]
        payload = client.post("/api/auth/login", json={"password": "admin-pass"}).json()

    assert statuses[:4] == [401, 401, 401, 401]
    assert statuses[4] == 429
    assert payload["error"]["code"] == "AUTH_RATE_LIMITED"


def test_login_rate_limit_persists_across_service_reload(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        client.app.state.monitor = FakeMonitor()
        client.app.state.access_control = build_auth_service(tmp_path, enabled=True)
        client.app.state.test_client_ip = "198.51.100.10"

        statuses = [client.post("/api/auth/login", json={"password": "wrong-pass"}).status_code for _ in range(5)]

    with TestClient(api_module.app) as client:
        client.app.state.monitor = FakeMonitor()
        client.app.state.access_control = build_auth_service(tmp_path, enabled=True)
        client.app.state.test_client_ip = "198.51.100.10"

        response = client.post("/api/auth/login", json={"password": "admin-pass"})

    assert statuses[:4] == [401, 401, 401, 401]
    assert statuses[4] == 429
    assert response.status_code == 429
    assert response.json()["error"]["code"] == "AUTH_RATE_LIMITED"


def test_missing_auth_config_enters_not_initialized_state(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        client.app.state.monitor = FakeMonitor()
        client.app.state.access_control = AccessControlService(
            settings,
            config_path=tmp_path / "missing_access_control.json",
            audit_db_path=tmp_path / "access_audit.db",
        )
        client.app.state.test_client_ip = "198.51.100.10"

        session_response = client.get("/api/auth/session")
        page_response = client.get("/login")

    assert session_response.status_code == 503
    assert session_response.json()["error"]["code"] == "AUTH_NOT_INITIALIZED"
    assert page_response.status_code == 200
    assert "\u8ba4\u8bc1\u672a\u521d\u59cb\u5316" in page_response.text
    assert "__LOGIN_TITLE__" not in page_response.text
    assert "__LOGIN_DESCRIPTION__" not in page_response.text
    assert "__LOGIN_PASSWORD_LABEL__" not in page_response.text
    assert "__LOGIN_PASSWORD_PLACEHOLDER__" not in page_response.text
    assert "__LOGIN_SUBMIT_BUTTON__" not in page_response.text
    assert "__LOGIN_I18N__" not in page_response.text


def test_break_glass_is_loopback_only_and_only_bypasses_auth_for_loopback(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        client.app.state.monitor = FakeMonitor()
        client.app.state.access_control = build_auth_service(tmp_path, enabled=True)
        client.app.state.test_client_ip = "198.51.100.10"

        remote_attempt = client.get("/api/auth/break-glass/challenge")
        remote_disable = client.post("/api/auth/break-glass/disable", json={"nonce": "bad"})
        client.app.state.test_client_ip = "127.0.0.1"
        challenge = client.get("/api/auth/break-glass/challenge")
        nonce = challenge.json()["nonce"]
        local_attempt = client.post("/api/auth/break-glass/disable", json={"nonce": nonce})
        session_response = client.get("/api/auth/session")
        loopback_after_disable = client.post(
            "/api/monitor/refresh",
            headers={"X-CSRF-Token": session_response.json()["csrf_token"]},
        )
        client.app.state.test_client_ip = "198.51.100.10"
        remote_after_disable = client.post("/api/monitor/refresh")

    assert remote_attempt.status_code == 403
    assert remote_attempt.json()["error"]["code"] == "AUTH_ROLE_FORBIDDEN"
    assert remote_disable.status_code == 403
    assert remote_disable.json()["error"]["code"] == "AUTH_ROLE_FORBIDDEN"
    assert challenge.status_code == 200
    assert local_attempt.status_code == 200
    assert loopback_after_disable.status_code == 200
    assert remote_after_disable.status_code == 401
    assert remote_after_disable.json()["error"]["code"] == "AUTH_REQUIRED"


def test_break_glass_disable_rejects_cross_origin_browser_post(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        client.app.state.monitor = FakeMonitor()
        client.app.state.access_control = build_auth_service(tmp_path, enabled=True)
        client.app.state.test_client_ip = "127.0.0.1"

        challenge = client.get("/api/auth/break-glass/challenge")
        nonce = challenge.json()["nonce"]
        response = client.post(
            "/api/auth/break-glass/disable",
            json={"nonce": nonce},
            headers={"Origin": "https://evil.example"},
        )

    assert response.status_code == 403
    assert response.json()["error"]["code"] == "AUTH_CROSS_ORIGIN_FORBIDDEN"


def test_break_glass_rejects_invalid_nonce(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        client.app.state.monitor = FakeMonitor()
        client.app.state.access_control = build_auth_service(tmp_path, enabled=True)
        client.app.state.test_client_ip = "127.0.0.1"

        response = client.post("/api/auth/break-glass/disable", json={"nonce": "bad"})

    assert response.status_code == 403
    assert response.json()["error"]["code"] == "AUTH_INVALID"


def test_cookie_secure_mode_auto_sets_secure_cookie_for_https(tmp_path: Path) -> None:
    config_path = tmp_path / "access_control.json"
    audit_db_path = tmp_path / "access_audit.db"
    config_path.write_text(
        json.dumps(
            {
                "enabled": True,
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
    with TestClient(api_module.app, base_url="https://example.test") as client:
        client.app.state.monitor = FakeMonitor()
        client.app.state.access_control = AccessControlService(settings, config_path=config_path, audit_db_path=audit_db_path)
        client.app.state.test_client_ip = "198.51.100.10"

        response = client.post("/api/auth/login", json={"password": "admin-pass"})

    cookie_header = response.headers.get("set-cookie", "")
    assert response.status_code == 200
    assert "Secure" in cookie_header


def test_session_idle_timeout_expires_stale_cookie(tmp_path: Path) -> None:
    service = build_auth_service(tmp_path, enabled=True)
    with TestClient(api_module.app) as client:
        client.app.state.monitor = FakeMonitor()
        client.app.state.access_control = service
        client.app.state.test_client_ip = "198.51.100.10"

        login = client.post("/api/auth/login", json={"password": "admin-pass"})
        assert login.status_code == 200
        cookie_value = client.cookies.get(AUTH_COOKIE_NAME)
        payload = service._decode_session(cookie_value)
        assert payload is not None
        stale_cookie = service._encode_session(
            payload.__class__(
                role=payload.role,
                issued_at=payload.issued_at,
                last_activity_at=payload.last_activity_at - (31 * 60 * 1000),
                session_revision=payload.session_revision,
                client_ip=payload.client_ip,
                user_agent_hash=payload.user_agent_hash,
                csrf_token=payload.csrf_token,
            )
        )
        client.cookies.set(AUTH_COOKIE_NAME, stale_cookie)

        response = client.get("/api/auth/session")

    assert response.status_code == 200
    assert response.json()["authenticated"] is False
    assert response.json()["error"]["code"] == "AUTH_SESSION_EXPIRED"
    assert "Max-Age=0" in response.headers.get("set-cookie", "")


def test_auth_audit_requires_admin_and_returns_recent_events(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        client.app.state.monitor = FakeMonitor()
        client.app.state.access_control = build_auth_service(tmp_path, enabled=True)
        client.app.state.test_client_ip = "198.51.100.10"

        guest_login = client.post("/api/auth/login", json={"password": "guest-pass"})
        assert guest_login.status_code == 200
        guest_response = client.get("/api/auth/audit")

        client.cookies.clear()
        admin_login = client.post("/api/auth/login", json={"password": "admin-pass"})
        assert admin_login.status_code == 200
        admin_response = client.get("/api/auth/audit")

    assert guest_response.status_code == 403
    assert guest_response.json()["error"]["code"] == "AUTH_ROLE_FORBIDDEN"
    assert admin_response.status_code == 200
    assert isinstance(admin_response.json()["items"], list)
    assert any(item["reason_code"] == "AUTH_LOGIN_SUCCESS" for item in admin_response.json()["items"])


def test_security_headers_are_applied_to_login_and_auth_responses(tmp_path: Path) -> None:
    with TestClient(api_module.app) as client:
        client.app.state.monitor = FakeMonitor()
        client.app.state.access_control = build_auth_service(tmp_path, enabled=True)
        client.app.state.test_client_ip = "198.51.100.10"

        login_page = client.get("/login")
        session_response = client.get("/api/auth/session")

    for response in (login_page, session_response):
        assert response.headers["X-Frame-Options"] == "DENY"
        assert response.headers["Referrer-Policy"] == "same-origin"
        assert response.headers["X-Content-Type-Options"] == "nosniff"
        assert "frame-ancestors 'none'" in response.headers["Content-Security-Policy"]


def test_plaintext_auth_config_is_rejected_in_refs_only_mode(tmp_path: Path) -> None:
    config_path = tmp_path / "access_control.json"
    config_path.write_text(
        json.dumps(
            {
                "enabled": True,
                "whitelist_ips": [],
                "guest_password": "guest-pass",
                "admin_password": "admin-pass",
                "session_secret": "dev-session-secret-1234567890",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="plaintext value is not allowed"):
        AccessControlService(settings, config_path=config_path, audit_db_path=tmp_path / "audit.db")


def test_invalid_whitelist_ip_is_rejected(tmp_path: Path) -> None:
    config_path = tmp_path / "access_control.json"
    config_path.write_text(
        json.dumps(
            {
                "enabled": True,
                "whitelist_ips": ["not-an-ip"],
                "guest_password_secret_ref": "",
                "admin_password_secret_ref": "",
                "session_secret_secret_ref": "",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Invalid whitelist IP"):
        AccessControlService(settings, config_path=config_path, audit_db_path=tmp_path / "audit.db")


def test_route_capability_snapshot_covers_all_application_routes() -> None:
    missing = route_capability_snapshot(api_module.app)
    assert missing == []
