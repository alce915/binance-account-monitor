from __future__ import annotations

import base64
import hashlib
import hmac
import ipaddress
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.routing import APIRoute

from monitor_app.access_control.config import (
    AccessControlConfig,
    ensure_access_control_template,
    load_access_control_config,
)
from monitor_app.access_control.store import AccessControlAuditStore
from monitor_app.config import Settings
from monitor_app.security import is_loopback_client

AUTH_REQUIRED = "AUTH_REQUIRED"
AUTH_INVALID = "AUTH_INVALID"
AUTH_RATE_LIMITED = "AUTH_RATE_LIMITED"
AUTH_ROLE_FORBIDDEN = "AUTH_ROLE_FORBIDDEN"
AUTH_SESSION_EXPIRED = "AUTH_SESSION_EXPIRED"
AUTH_SESSION_REVOKED = "AUTH_SESSION_REVOKED"
AUTH_NOT_INITIALIZED = "AUTH_NOT_INITIALIZED"
AUTH_POLICY_MISSING = "AUTH_POLICY_MISSING"
AUTH_CROSS_ORIGIN_FORBIDDEN = "AUTH_CROSS_ORIGIN_FORBIDDEN"

AUTH_COOKIE_NAME = "monitor_auth"
CSRF_HEADER_NAME = "X-CSRF-Token"
LOGIN_MAX_FAILURES = 5
LOGIN_LOCK_SECONDS = 600
WHITELIST_AUDIT_INTERVAL_SECONDS = 12 * 60 * 60
BREAK_GLASS_CHALLENGE_TTL_SECONDS = 60
LOGIN_FAILURE_MESSAGE = "\u8ba4\u8bc1\u5931\u8d25"
AUTH_NOT_INITIALIZED_MESSAGE = "\u8ba4\u8bc1\u672a\u521d\u59cb\u5316"
AUTH_ROLE_FORBIDDEN_MESSAGE = "\u6743\u9650\u4e0d\u8db3"
BREAK_GLASS_DISABLED_MESSAGE = "\u8ba4\u8bc1\u5df2\u4e34\u65f6\u7981\u7528\uff0c\u91cd\u542f\u540e\u6062\u590d"
AUTH_POLICY_MISSING_MESSAGE = "\u8bbf\u95ee\u63a7\u5236\u7b56\u7565\u7f3a\u5931"
CROSS_ORIGIN_FORBIDDEN_MESSAGE = "\u8de8\u7ad9\u8bf7\u6c42\u5df2\u88ab\u62d2\u7edd"
LOOPBACK_IPS = {"127.0.0.1", "::1"}


def _utc_now_ms() -> int:
    return int(time.time() * 1000)


def _iso_now_from_ms(value: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S+08:00", time.localtime(value / 1000))


def _base64url_encode(payload: bytes) -> str:
    return base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")


def _base64url_decode(payload: str) -> bytes:
    padding = "=" * ((4 - len(payload) % 4) % 4)
    return base64.urlsafe_b64decode(f"{payload}{padding}".encode("ascii"))


def _user_agent_hash(user_agent: str) -> str:
    return hashlib.sha256(user_agent.encode("utf-8")).hexdigest()


def _normalize_ip(value: str | None) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return ""
    try:
        return str(ipaddress.ip_address(normalized))
    except ValueError:
        return normalized


def _is_page_request(request: Request) -> bool:
    path = request.url.path
    if (
        path.startswith("/api/")
        or path.startswith("/stream/")
        or path.startswith("/static/")
        or path.startswith("/public/")
        or path == "/healthz"
    ):
        return False
    return request.method.upper() == "GET"


@dataclass(frozen=True, slots=True)
class SessionPayload:
    role: str
    issued_at: int
    last_activity_at: int
    session_revision: str
    client_ip: str
    user_agent_hash: str
    csrf_token: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "role": self.role,
            "issued_at": self.issued_at,
            "last_activity_at": self.last_activity_at,
            "session_revision": self.session_revision,
            "client_ip": self.client_ip,
            "user_agent_hash": self.user_agent_hash,
            "csrf_token": self.csrf_token,
        }


@dataclass(slots=True)
class AuthContext:
    enabled: bool
    initialized: bool
    authenticated: bool
    whitelisted: bool
    role: str | None
    auth_source: str
    client_ip: str
    csrf_token: str | None
    session_payload: SessionPayload | None
    session_cookie: str | None
    last_activity_at: str | None
    clear_session_cookie: bool = False
    reason_code: str | None = None


@dataclass(frozen=True, slots=True)
class RoutePolicy:
    route_paths: tuple[str, ...]
    methods: frozenset[str]
    capability: str | None
    runtime_pattern: re.Pattern[str]

    def matches_request(self, method: str, path: str) -> bool:
        return method.upper() in self.methods and self.runtime_pattern.fullmatch(path) is not None

    def matches_route(self, method: str, route_path: str) -> bool:
        return method.upper() in self.methods and route_path in self.route_paths


ROUTE_POLICIES: tuple[RoutePolicy, ...] = (
    RoutePolicy(("/healthz",), frozenset({"GET"}), None, re.compile(r"^/healthz$")),
    RoutePolicy(("/login",), frozenset({"GET"}), None, re.compile(r"^/login$")),
    RoutePolicy(("/api/auth/login",), frozenset({"POST"}), None, re.compile(r"^/api/auth/login$")),
    RoutePolicy(("/api/auth/logout",), frozenset({"POST"}), None, re.compile(r"^/api/auth/logout$")),
    RoutePolicy(("/api/auth/session",), frozenset({"GET"}), None, re.compile(r"^/api/auth/session$")),
    RoutePolicy(("/api/auth/audit",), frozenset({"GET"}), "auth.audit", re.compile(r"^/api/auth/audit$")),
    RoutePolicy(("/api/auth/break-glass/challenge",), frozenset({"GET"}), None, re.compile(r"^/api/auth/break-glass/challenge$")),
    RoutePolicy(("/api/auth/break-glass/disable",), frozenset({"POST"}), None, re.compile(r"^/api/auth/break-glass/disable$")),
    RoutePolicy(("/",), frozenset({"GET"}), "monitor.read", re.compile(r"^/$")),
    RoutePolicy(("/static/{path:path}",), frozenset({"GET"}), "monitor.read", re.compile(r"^/static(?:/.*)?$")),
    RoutePolicy(("/public/login/{path:path}",), frozenset({"GET"}), None, re.compile(r"^/public/login(?:/.*)?$")),
    RoutePolicy(("/stream/monitor",), frozenset({"GET"}), "monitor.read", re.compile(r"^/stream/monitor$")),
    RoutePolicy(("/api/monitor/summary", "/api/monitor/groups", "/api/monitor/accounts"), frozenset({"GET"}), "monitor.read", re.compile(r"^/api/monitor/(summary|groups|accounts)$")),
    RoutePolicy(("/api/monitor/control",), frozenset({"POST"}), "monitor.control", re.compile(r"^/api/monitor/control$")),
    RoutePolicy(("/api/monitor/refresh",), frozenset({"POST"}), "monitor.refresh", re.compile(r"^/api/monitor/refresh$")),
    RoutePolicy(("/api/config/import/excel", "/api/config/import/excel-template"), frozenset({"GET", "POST"}), "config.import", re.compile(r"^/api/config/import/(excel|excel-template)$")),
    RoutePolicy(("/api/funding/groups/{main_id}", "/api/funding/groups/{main_id}/audit", "/api/funding/groups/{main_id}/audit/{operation_id}"), frozenset({"GET"}), "funding.read", re.compile(r"^/api/funding/groups/[^/]+(?:/audit(?:/[^/]+)?)?$")),
    RoutePolicy(("/api/funding/groups/{main_id}/distribute", "/api/funding/groups/{main_id}/collect"), frozenset({"POST"}), "funding.write", re.compile(r"^/api/funding/groups/[^/]+/(distribute|collect)$")),
    RoutePolicy(("/api/alerts/telegram/test",), frozenset({"POST"}), "alerts.test", re.compile(r"^/api/alerts/telegram/test$")),
    RoutePolicy(("/api/alerts/unimmr/status",), frozenset({"GET"}), "monitor.read", re.compile(r"^/api/alerts/unimmr/status$")),
    RoutePolicy(("/api/alerts/unimmr/simulate",), frozenset({"POST"}), "alerts.test", re.compile(r"^/api/alerts/unimmr/simulate$")),
)

PROTECTED_ROUTE_PREFIXES: tuple[str, ...] = ("/api/", "/stream/", "/static/", "/public/")

ROLE_CAPABILITIES: dict[str, frozenset[str]] = {
    "guest": frozenset({"monitor.read", "funding.read"}),
    "admin": frozenset({"*"}),
}


def route_capability_snapshot(app: FastAPI) -> list[str]:
    missing: list[str] = []
    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue
        if route.path.startswith(("/docs", "/redoc", "/openapi.json")):
            continue
        methods = sorted(method for method in (route.methods or set()) if method not in {"HEAD", "OPTIONS"})
        for method in methods:
            if not any(policy.matches_route(method, route.path) for policy in ROUTE_POLICIES):
                missing.append(f"{method} {route.path}")
    return missing


class AccessControlService:
    def __init__(
        self,
        settings: Settings,
        *,
        config_path: Path | None = None,
        audit_db_path: Path | None = None,
    ) -> None:
        self._settings = settings
        self._config_path = Path(config_path or settings.access_control_config_file)
        self._audit_store = AccessControlAuditStore(audit_db_path or settings.monitor_history_db_path, max_rows=settings.auth_audit_max_rows)
        self._config: AccessControlConfig | None = None
        self._initialized = False
        self._break_glass_enabled = False
        self._break_glass_nonces: dict[str, int] = {}
        self._whitelist_audit_ms: dict[str, int] = {}
        self.reload()

    async def close(self) -> None:
        await self._audit_store.close()

    def reload(self) -> None:
        if not self._config_path.exists():
            ensure_access_control_template(self._config_path)
            self._config = None
            self._initialized = False
            self._break_glass_enabled = False
            self._break_glass_nonces.clear()
            return
        self._config = load_access_control_config(self._config_path, settings=self._settings)
        if self._config.enabled:
            self._initialized = bool(
                self._config.admin_password and self._config.guest_password and self._config.session_secret
            )
        else:
            self._initialized = True
        self._break_glass_enabled = False
        self._break_glass_nonces.clear()

    @property
    def config(self) -> AccessControlConfig | None:
        return self._config

    def is_enabled(self) -> bool:
        return bool(self._config and self._config.enabled)

    def is_initialized(self) -> bool:
        return self._initialized

    def resolve_client_ip(self, request: Request) -> str:
        override = getattr(request.app.state, "test_client_ip", None)
        if override:
            return _normalize_ip(override)
        return _normalize_ip(request.client.host if request.client else "")

    def _session_revision(self) -> str:
        if self._config is None:
            return ""
        payload = "|".join(
            [
                self._config.guest_password,
                self._config.admin_password,
                self._config.session_secret,
            ]
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _sign(self, payload: bytes) -> str:
        if self._config is None:
            return ""
        return _base64url_encode(hmac.new(self._config.session_secret.encode("utf-8"), payload, hashlib.sha256).digest())

    def _make_csrf_token(self, *, role: str, auth_source: str, client_ip: str, user_agent_hash: str) -> str:
        seed = "|".join([self._session_revision(), role, auth_source, client_ip, user_agent_hash])
        if self._config is None:
            return ""
        return hmac.new(self._config.session_secret.encode("utf-8"), seed.encode("utf-8"), hashlib.sha256).hexdigest()

    def _encode_session(self, payload: SessionPayload) -> str:
        body = _base64url_encode(json.dumps(payload.as_dict(), ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
        signature = self._sign(body.encode("utf-8"))
        return f"{body}.{signature}"

    def _decode_session(self, cookie_value: str) -> SessionPayload | None:
        if not cookie_value or "." not in cookie_value or self._config is None:
            return None
        body, signature = cookie_value.split(".", 1)
        expected = self._sign(body.encode("utf-8"))
        if not hmac.compare_digest(signature, expected):
            return None
        try:
            payload = json.loads(_base64url_decode(body).decode("utf-8"))
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        try:
            return SessionPayload(
                role=str(payload["role"]),
                issued_at=int(payload["issued_at"]),
                last_activity_at=int(payload["last_activity_at"]),
                session_revision=str(payload["session_revision"]),
                client_ip=str(payload["client_ip"]),
                user_agent_hash=str(payload["user_agent_hash"]),
                csrf_token=str(payload["csrf_token"]),
            )
        except Exception:
            return None

    def _session_idle_timeout_ms(self, role: str | None) -> int:
        if (role or "").strip().lower() == "admin":
            return max(int(self._settings.admin_idle_timeout_minutes), 1) * 60 * 1000
        return max(int(self._settings.guest_idle_timeout_minutes), 1) * 60 * 1000

    def _request_origin(self, request: Request) -> str:
        parts = urlsplit(str(request.base_url))
        return f"{parts.scheme}://{parts.netloc}".lower()

    def _header_origin(self, value: str) -> str:
        parts = urlsplit(str(value or "").strip())
        if not parts.scheme or not parts.netloc:
            return ""
        return f"{parts.scheme}://{parts.netloc}".lower()

    def is_same_origin_request(self, request: Request) -> bool:
        origin = str(request.headers.get("origin") or "").strip()
        if origin:
            return origin.lower() != "null" and self._header_origin(origin) == self._request_origin(request)
        referer = str(request.headers.get("referer") or "").strip()
        if referer:
            return self._header_origin(referer) == self._request_origin(request)
        return True

    def apply_security_headers(self, response: JSONResponse | RedirectResponse) -> None:
        response.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline'; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data:; "
            "font-src 'self' data:; "
            "connect-src 'self' http: https: ws: wss:; "
            "object-src 'none'; "
            "base-uri 'self'; "
            "frame-ancestors 'none'; "
            "form-action 'self'",
        )
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "same-origin")
        response.headers.setdefault("X-Content-Type-Options", "nosniff")

    async def build_context(self, request: Request) -> AuthContext:
        client_ip = self.resolve_client_ip(request)
        user_agent_hash = _user_agent_hash(request.headers.get("user-agent", ""))
        config = self._config
        cookie_value = request.cookies.get(AUTH_COOKIE_NAME)

        if config is None:
            return AuthContext(
                enabled=True,
                initialized=False,
                authenticated=False,
                whitelisted=False,
                role=None,
                auth_source="none",
                client_ip=client_ip,
                csrf_token=None,
                session_payload=None,
                session_cookie=None,
                last_activity_at=None,
                clear_session_cookie=bool(cookie_value),
                reason_code=AUTH_NOT_INITIALIZED,
            )

        if self._break_glass_enabled and is_loopback_client(client_ip):
            csrf_token = self._make_csrf_token(role="admin", auth_source="break_glass", client_ip=client_ip, user_agent_hash=user_agent_hash)
            return AuthContext(
                enabled=True,
                initialized=True,
                authenticated=True,
                whitelisted=is_loopback_client(client_ip),
                role="admin",
                auth_source="break_glass",
                client_ip=client_ip,
                csrf_token=csrf_token,
                session_payload=None,
                session_cookie=None,
                last_activity_at=_iso_now_from_ms(_utc_now_ms()),
            )

        if not config.enabled:
            return AuthContext(
                enabled=False,
                initialized=True,
                authenticated=True,
                whitelisted=is_loopback_client(client_ip),
                role="admin",
                auth_source="disabled",
                client_ip=client_ip,
                csrf_token=None,
                session_payload=None,
                session_cookie=None,
                last_activity_at=_iso_now_from_ms(_utc_now_ms()),
            )

        if not self._initialized:
            return AuthContext(
                enabled=True,
                initialized=False,
                authenticated=False,
                whitelisted=False,
                role=None,
                auth_source="none",
                client_ip=client_ip,
                csrf_token=None,
                session_payload=None,
                session_cookie=None,
                last_activity_at=None,
                clear_session_cookie=bool(cookie_value),
                reason_code=AUTH_NOT_INITIALIZED,
            )

        if client_ip in set(config.whitelist_ips) | LOOPBACK_IPS:
            csrf_token = self._make_csrf_token(role="admin", auth_source="whitelist", client_ip=client_ip, user_agent_hash=user_agent_hash)
            await self._audit_whitelist_access(client_ip=client_ip, method=request.method, path=request.url.path)
            return AuthContext(
                enabled=True,
                initialized=True,
                authenticated=True,
                whitelisted=True,
                role="admin",
                auth_source="whitelist",
                client_ip=client_ip,
                csrf_token=csrf_token,
                session_payload=None,
                session_cookie=None,
                last_activity_at=_iso_now_from_ms(_utc_now_ms()),
            )

        payload = self._decode_session(cookie_value or "")
        if payload is None:
            return AuthContext(
                enabled=True,
                initialized=True,
                authenticated=False,
                whitelisted=False,
                role=None,
                auth_source="none",
                client_ip=client_ip,
                csrf_token=None,
                session_payload=None,
                session_cookie=None,
                last_activity_at=None,
                clear_session_cookie=bool(cookie_value),
            )
        now_ms = _utc_now_ms()
        if payload.session_revision != self._session_revision():
            return AuthContext(
                enabled=True,
                initialized=True,
                authenticated=False,
                whitelisted=False,
                role=None,
                auth_source="none",
                client_ip=client_ip,
                csrf_token=None,
                session_payload=None,
                session_cookie=None,
                last_activity_at=None,
                clear_session_cookie=True,
                reason_code=AUTH_SESSION_REVOKED,
            )
        if payload.client_ip != client_ip or payload.user_agent_hash != user_agent_hash:
            return AuthContext(
                enabled=True,
                initialized=True,
                authenticated=False,
                whitelisted=False,
                role=None,
                auth_source="none",
                client_ip=client_ip,
                csrf_token=None,
                session_payload=None,
                session_cookie=None,
                last_activity_at=None,
                clear_session_cookie=True,
                reason_code=AUTH_SESSION_REVOKED,
            )
        if now_ms - payload.last_activity_at > self._session_idle_timeout_ms(payload.role):
            return AuthContext(
                enabled=True,
                initialized=True,
                authenticated=False,
                whitelisted=False,
                role=None,
                auth_source="none",
                client_ip=client_ip,
                csrf_token=None,
                session_payload=None,
                session_cookie=None,
                last_activity_at=None,
                clear_session_cookie=True,
                reason_code=AUTH_SESSION_EXPIRED,
            )

        updated_payload = SessionPayload(
            role=payload.role,
            issued_at=payload.issued_at,
            last_activity_at=now_ms,
            session_revision=payload.session_revision,
            client_ip=payload.client_ip,
            user_agent_hash=payload.user_agent_hash,
            csrf_token=payload.csrf_token,
        )
        return AuthContext(
            enabled=True,
            initialized=True,
            authenticated=True,
            whitelisted=False,
            role=updated_payload.role,
            auth_source="session",
            client_ip=client_ip,
            csrf_token=updated_payload.csrf_token,
            session_payload=updated_payload,
            session_cookie=self._encode_session(updated_payload),
            last_activity_at=_iso_now_from_ms(updated_payload.last_activity_at),
        )

    async def _audit_whitelist_access(self, *, client_ip: str, method: str, path: str) -> None:
        now_ms = _utc_now_ms()
        last_seen = self._whitelist_audit_ms.get(client_ip, 0)
        if path.startswith("/api/") and method.upper() != "GET":
            should_record = True
        else:
            should_record = now_ms - last_seen >= WHITELIST_AUDIT_INTERVAL_SECONDS * 1000
        if not should_record:
            return
        self._whitelist_audit_ms[client_ip] = now_ms
        await self._audit_store.record_event(
            created_at_ms=now_ms,
            client_ip=client_ip,
            method=method.upper(),
            path=path,
            role="admin",
            auth_source="whitelist",
            result="allowed",
            reason_code="AUTH_WHITELIST_BYPASS",
        )

    def session_payload_for_response(self, context: AuthContext) -> dict[str, Any]:
        payload = {
            "enabled": context.enabled,
            "initialized": context.initialized,
            "authenticated": context.authenticated,
            "whitelisted": context.whitelisted,
            "role": context.role,
            "auth_source": context.auth_source,
            "last_activity_at": context.last_activity_at,
            "csrf_token": context.csrf_token,
        }
        if context.reason_code:
            message = LOGIN_FAILURE_MESSAGE
            if context.reason_code == AUTH_NOT_INITIALIZED:
                message = AUTH_NOT_INITIALIZED_MESSAGE
            elif context.reason_code == AUTH_CROSS_ORIGIN_FORBIDDEN:
                message = CROSS_ORIGIN_FORBIDDEN_MESSAGE
            payload["error"] = {
                "code": context.reason_code,
                "message": message,
            }
        return payload

    def _resolve_policy(self, method: str, path: str) -> RoutePolicy | None:
        for policy in ROUTE_POLICIES:
            if policy.matches_request(method, path):
                return policy
        return None

    def has_capability(self, role: str | None, capability: str | None) -> bool:
        if capability is None:
            return True
        role_caps = ROLE_CAPABILITIES.get(role or "", frozenset())
        return "*" in role_caps or capability in role_caps

    async def authorize_request(self, request: Request, context: AuthContext) -> JSONResponse | RedirectResponse | None:
        policy = self._resolve_policy(request.method, request.url.path)
        if policy is None:
            if _is_page_request(request) and context.enabled and not context.authenticated:
                next_path = f"{request.url.path}{f'?{request.url.query}' if request.url.query else ''}"
                response = RedirectResponse(url=f"/login?next={quote(next_path, safe='/%?=&')}", status_code=307)
                response.headers["Cache-Control"] = "no-store"
                return response
            if request.url.path.startswith(PROTECTED_ROUTE_PREFIXES):
                return self.auth_error_response(500, AUTH_POLICY_MISSING, message=AUTH_POLICY_MISSING_MESSAGE)
            return None

        if policy.capability is None:
            return None

        if not context.initialized:
            if _is_page_request(request):
                return RedirectResponse(url="/login", status_code=307)
            return self.auth_error_response(503, AUTH_NOT_INITIALIZED, message=AUTH_NOT_INITIALIZED_MESSAGE)

        if not context.enabled:
            return None

        if not context.authenticated:
            if _is_page_request(request):
                next_path = f"{request.url.path}{f'?{request.url.query}' if request.url.query else ''}"
                response = RedirectResponse(url=f"/login?next={quote(next_path, safe='/%?=&')}", status_code=307)
                response.headers["Cache-Control"] = "no-store"
                return response
            return self.auth_error_response(401, context.reason_code or AUTH_REQUIRED)

        if not self.has_capability(context.role, policy.capability):
            await self._audit_store.record_event(
                created_at_ms=_utc_now_ms(),
                client_ip=context.client_ip,
                method=request.method.upper(),
                path=request.url.path,
                role=context.role or "",
                auth_source=context.auth_source,
                result="denied",
                reason_code=AUTH_ROLE_FORBIDDEN,
            )
            return self.auth_error_response(403, AUTH_ROLE_FORBIDDEN, message=AUTH_ROLE_FORBIDDEN_MESSAGE)

        if request.method.upper() not in {"GET", "HEAD", "OPTIONS"} and context.auth_source in {"session", "whitelist", "break_glass"}:
            if not self.is_same_origin_request(request):
                await self._audit_store.record_event(
                    created_at_ms=_utc_now_ms(),
                    client_ip=context.client_ip,
                    method=request.method.upper(),
                    path=request.url.path,
                    role=context.role or "",
                    auth_source=context.auth_source,
                    result="denied",
                    reason_code=AUTH_CROSS_ORIGIN_FORBIDDEN,
                )
                return self.auth_error_response(403, AUTH_CROSS_ORIGIN_FORBIDDEN, message=CROSS_ORIGIN_FORBIDDEN_MESSAGE)
            request_token = request.headers.get(CSRF_HEADER_NAME, "").strip()
            if not request_token or request_token != (context.csrf_token or ""):
                await self._audit_store.record_event(
                    created_at_ms=_utc_now_ms(),
                    client_ip=context.client_ip,
                    method=request.method.upper(),
                    path=request.url.path,
                    role=context.role or "",
                    auth_source=context.auth_source,
                    result="denied",
                    reason_code=AUTH_INVALID,
                )
                return self.auth_error_response(403, AUTH_INVALID)
        return None

    async def login(self, request: Request, password: str) -> tuple[dict[str, Any], str | None]:
        context = await self.build_context(request)
        if not context.initialized:
            return self.auth_error_payload(AUTH_NOT_INITIALIZED, AUTH_NOT_INITIALIZED_MESSAGE), None

        client_ip = context.client_ip
        failure = await self._audit_store.get_rate_limit_state(client_ip)
        now_ms = _utc_now_ms()
        locked_until_ms = int(failure.get("locked_until_ms") or 0)
        if locked_until_ms > now_ms:
            await self._audit_store.record_event(
                created_at_ms=now_ms,
                client_ip=client_ip,
                method="POST",
                path="/api/auth/login",
                role="",
                auth_source="none",
                result="denied",
                reason_code=AUTH_RATE_LIMITED,
            )
            return self.auth_error_payload(AUTH_RATE_LIMITED, LOGIN_FAILURE_MESSAGE), None

        role = None
        if self._config is not None and password == self._config.admin_password:
            role = "admin"
        elif self._config is not None and password == self._config.guest_password:
            role = "guest"

        if role is None:
            attempts = int(failure.get("fail_count") or 0) + 1
            next_state = {"fail_count": attempts, "updated_at_ms": now_ms}
            if attempts >= LOGIN_MAX_FAILURES:
                next_state["locked_until_ms"] = now_ms + LOGIN_LOCK_SECONDS * 1000
            await self._audit_store.set_rate_limit_state(
                client_ip=client_ip,
                fail_count=next_state["fail_count"],
                locked_until_ms=int(next_state.get("locked_until_ms") or 0),
                updated_at_ms=next_state["updated_at_ms"],
            )
            reason_code = AUTH_RATE_LIMITED if "locked_until_ms" in next_state else AUTH_INVALID
            await self._audit_store.record_event(
                created_at_ms=now_ms,
                client_ip=client_ip,
                method="POST",
                path="/api/auth/login",
                role="",
                auth_source="none",
                result="denied",
                reason_code=reason_code,
            )
            return self.auth_error_payload(reason_code, LOGIN_FAILURE_MESSAGE), None

        await self._audit_store.clear_rate_limit_state(client_ip)
        user_agent_hash = _user_agent_hash(request.headers.get("user-agent", ""))
        csrf_token = self._make_csrf_token(role=role, auth_source="session", client_ip=client_ip, user_agent_hash=user_agent_hash)
        now_ms = _utc_now_ms()
        payload = SessionPayload(
            role=role,
            issued_at=now_ms,
            last_activity_at=now_ms,
            session_revision=self._session_revision(),
            client_ip=client_ip,
            user_agent_hash=user_agent_hash,
            csrf_token=csrf_token,
        )
        cookie_value = self._encode_session(payload)
        await self._audit_store.record_event(
            created_at_ms=now_ms,
            client_ip=client_ip,
            method="POST",
            path="/api/auth/login",
            role=role,
            auth_source="session",
            result="allowed",
            reason_code="AUTH_LOGIN_SUCCESS",
        )
        return {
            "enabled": True,
            "initialized": True,
            "authenticated": True,
            "whitelisted": False,
            "role": role,
            "auth_source": "session",
            "last_activity_at": _iso_now_from_ms(now_ms),
            "csrf_token": csrf_token,
        }, cookie_value

    async def logout(self, request: Request) -> dict[str, Any]:
        context = await self.build_context(request)
        await self._audit_store.record_event(
            created_at_ms=_utc_now_ms(),
            client_ip=context.client_ip,
            method="POST",
            path="/api/auth/logout",
            role=context.role or "",
            auth_source=context.auth_source,
            result="allowed",
            reason_code="AUTH_LOGOUT",
        )
        return {
            "enabled": context.enabled,
            "initialized": context.initialized,
            "authenticated": False,
            "whitelisted": False,
            "role": None,
            "auth_source": "none",
            "last_activity_at": None,
            "csrf_token": None,
        }

    async def list_audit_events(
        self,
        *,
        limit: int = 50,
        result: str = "",
        reason_code: str = "",
    ) -> list[dict[str, Any]]:
        return await self._audit_store.list_events(limit=limit, result=result, reason_code=reason_code)

    async def issue_break_glass_challenge(self, request: Request) -> dict[str, Any] | None:
        client_ip = self.resolve_client_ip(request)
        if not is_loopback_client(client_ip):
            await self._audit_store.record_event(
                created_at_ms=_utc_now_ms(),
                client_ip=client_ip,
                method="GET",
                path="/api/auth/break-glass/challenge",
                role="",
                auth_source="none",
                result="denied",
                reason_code=AUTH_ROLE_FORBIDDEN,
            )
            return None
        nonce = _base64url_encode(hashlib.sha256(f"{client_ip}|{_utc_now_ms()}".encode("utf-8")).digest()[:18])
        expires_at_ms = _utc_now_ms() + BREAK_GLASS_CHALLENGE_TTL_SECONDS * 1000
        self._break_glass_nonces[nonce] = expires_at_ms
        await self._audit_store.record_event(
            created_at_ms=_utc_now_ms(),
            client_ip=client_ip,
            method="GET",
            path="/api/auth/break-glass/challenge",
            role="admin",
            auth_source="break_glass",
            result="allowed",
            reason_code="AUTH_BREAK_GLASS_CHALLENGE",
        )
        return {
            "nonce": nonce,
            "expires_in_seconds": BREAK_GLASS_CHALLENGE_TTL_SECONDS,
        }

    async def break_glass_disable(self, request: Request, *, nonce: str) -> dict[str, Any] | None:
        client_ip = self.resolve_client_ip(request)
        if not is_loopback_client(client_ip):
            await self._audit_store.record_event(
                created_at_ms=_utc_now_ms(),
                client_ip=client_ip,
                method="POST",
                path="/api/auth/break-glass/disable",
                role="",
                auth_source="none",
                result="denied",
                reason_code=AUTH_ROLE_FORBIDDEN,
            )
            return None
        normalized_nonce = str(nonce or "").strip()
        expires_at_ms = int(self._break_glass_nonces.pop(normalized_nonce, 0) or 0)
        now_ms = _utc_now_ms()
        if not normalized_nonce or expires_at_ms <= now_ms:
            await self._audit_store.record_event(
                created_at_ms=now_ms,
                client_ip=client_ip,
                method="POST",
                path="/api/auth/break-glass/disable",
                role="",
                auth_source="none",
                result="denied",
                reason_code=AUTH_INVALID,
            )
            return None
        self._break_glass_enabled = True
        await self._audit_store.record_event(
            created_at_ms=now_ms,
            client_ip=client_ip,
            method="POST",
            path="/api/auth/break-glass/disable",
            role="admin",
            auth_source="break_glass",
            result="allowed",
            reason_code="AUTH_BREAK_GLASS",
        )
        return {"enabled": True, "break_glass": True, "message": BREAK_GLASS_DISABLED_MESSAGE}

    def auth_error_payload(self, code: str, message: str = LOGIN_FAILURE_MESSAGE) -> dict[str, Any]:
        if message == LOGIN_FAILURE_MESSAGE:
            if code == AUTH_NOT_INITIALIZED:
                message = AUTH_NOT_INITIALIZED_MESSAGE
            elif code == AUTH_ROLE_FORBIDDEN:
                message = AUTH_ROLE_FORBIDDEN_MESSAGE
            elif code == AUTH_POLICY_MISSING:
                message = AUTH_POLICY_MISSING_MESSAGE
            elif code == AUTH_CROSS_ORIGIN_FORBIDDEN:
                message = CROSS_ORIGIN_FORBIDDEN_MESSAGE
        return {"detail": message, "error": {"code": code, "message": message}}

    def auth_error_response(self, status_code: int, code: str, *, message: str = LOGIN_FAILURE_MESSAGE) -> JSONResponse:
        return JSONResponse(
            status_code=status_code,
            content=self.auth_error_payload(code, message),
            headers={"Cache-Control": "no-store"},
        )

    def _cookie_secure(self, request: Request | None) -> bool:
        mode = self._config.cookie_secure_mode if self._config is not None else "auto"
        if mode == "always":
            return True
        if mode == "never":
            return False
        return bool(request is not None and request.url.scheme == "https")

    def apply_session_cookie(self, response: JSONResponse | RedirectResponse, cookie_value: str, *, request: Request | None = None) -> None:
        response.set_cookie(
            AUTH_COOKIE_NAME,
            cookie_value,
            httponly=True,
            samesite="lax",
            secure=self._cookie_secure(request),
            path="/",
        )

    def clear_session_cookie(self, response: JSONResponse, *, request: Request | None = None) -> None:
        response.delete_cookie(AUTH_COOKIE_NAME, path="/", secure=self._cookie_secure(request))
