from __future__ import annotations

from copy import deepcopy
from decimal import Decimal
import re
from typing import Any


REDACTED = "[redacted]"
REDACTED_EMAIL = "[redacted-email]"
MAX_ERROR_SUMMARY_LENGTH = 240
TRUSTED_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost", "[::1]", "testserver"})
LOOPBACK_CLIENT_HOSTS = frozenset({"127.0.0.1", "::1"})

_EMAIL_PATTERN = re.compile(r"(?i)\b[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}\b")
_AUTH_PATTERN = re.compile(r"(?i)(authorization)\s*[:=]\s*[^\s,;]+")
_SECRET_ASSIGNMENT_PATTERN = re.compile(
    r"(?i)\b(api[_ -]?key|api[_ -]?secret|transfer[_ -]?api[_ -]?secret|secret|token)\b\s*[:=]\s*[^\s,;]+"
)
_UID_PATTERN = re.compile(r"(?i)\b(uid)\b\s*[:= ]+\s*([a-z0-9._\-]{4,})")


def mask_uid(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if "*" in text:
        return text
    if len(text) <= 2:
        return "*" * len(text)
    if len(text) <= 6:
        return f"{text[:1]}{'*' * max(len(text) - 2, 1)}{text[-1:]}"
    return f"{text[:4]}{'*' * max(len(text) - 6, 2)}{text[-2:]}"


def mask_reference(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) <= 6:
        return text[:2] + "*" * max(len(text) - 2, 1)
    return f"{text[:4]}{'*' * max(len(text) - 8, 2)}{text[-4:]}"


def is_loopback_client(host: str | None) -> bool:
    normalized = str(host or "").strip().lower()
    return normalized in LOOPBACK_CLIENT_HOSTS


def is_trusted_loopback_host(host_header: str | None) -> bool:
    host = str(host_header or "").strip().lower()
    if not host:
        return False
    if host.startswith("["):
        closing_index = host.find("]")
        hostname = host if closing_index < 0 else host[: closing_index + 1]
    else:
        hostname = host.split(":", 1)[0]
    return hostname in TRUSTED_LOOPBACK_HOSTS


def sanitize_text(value: Any, *, default: str = "", max_length: int = MAX_ERROR_SUMMARY_LENGTH) -> str:
    text = str(value or "").strip()
    if not text:
        return default
    text = _EMAIL_PATTERN.sub(REDACTED_EMAIL, text)
    text = _AUTH_PATTERN.sub(r"\1: [redacted]", text)
    text = _SECRET_ASSIGNMENT_PATTERN.sub(lambda match: f"{match.group(1)}={REDACTED}", text)
    text = _UID_PATTERN.sub(lambda match: f"{match.group(1).upper()} {mask_uid(match.group(2))}", text)
    if len(text) > max_length:
        text = f"{text[: max_length - 3]}..."
    return text


def sanitize_error_summary(value: Any, *, fallback: str = "Operation failed") -> str:
    text = sanitize_text(value, default="")
    return text or fallback


def minimize_history_payload(
    *,
    source: str,
    event_time_ms: int,
    unique_key: str,
    asset: str,
    amount: Decimal | str,
    event_type: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    raw_payload = payload if isinstance(payload, dict) else {}
    amount_text = str(amount)
    minimized: dict[str, Any] = {
        "source": source,
        "event_time_ms": int(event_time_ms),
        "asset": str(asset or "").upper(),
        "amount": amount_text,
        "event_type": str(event_type or "").strip(),
        "event_id": mask_reference(unique_key),
    }
    symbol = str(raw_payload.get("symbol") or raw_payload.get("pair") or "").strip().upper()
    if symbol:
        minimized["symbol"] = symbol
    reference = (
        raw_payload.get("tranId")
        or raw_payload.get("id")
        or raw_payload.get("orderId")
        or raw_payload.get("subAccountTranId")
        or raw_payload.get("divTranId")
    )
    reference_text = mask_reference(reference)
    if reference_text:
        minimized["reference"] = reference_text
    info = sanitize_text(raw_payload.get("enInfo") or raw_payload.get("info") or raw_payload.get("incomeType"), default="")
    if info:
        minimized["detail"] = info
    return minimized


def sanitize_monitor_payload(payload: dict[str, Any]) -> dict[str, Any]:
    public_payload = deepcopy(payload)
    public_payload["message"] = sanitize_text(public_payload.get("message"))
    service = public_payload.get("service")
    if isinstance(service, dict):
        public_service = {"monitor_enabled": service.get("monitor_enabled", True)}
        for key in ("account_ids", "main_account_ids"):
            values = service.get(key)
            if isinstance(values, list):
                public_service[key] = [str(value) for value in values if value]
        public_payload["service"] = public_service
    public_payload.pop("refresh_meta", None)

    refresh_result = public_payload.get("refresh_result")
    if isinstance(refresh_result, dict):
        public_payload["refresh_result"] = _sanitize_refresh_result(refresh_result)

    accounts = public_payload.get("accounts")
    if isinstance(accounts, list):
        public_payload["accounts"] = [_sanitize_monitor_account(account) for account in accounts if isinstance(account, dict)]

    groups = public_payload.get("groups")
    if isinstance(groups, list):
        public_payload["groups"] = [_sanitize_monitor_group(group) for group in groups if isinstance(group, dict)]
    return public_payload


def sanitize_funding_payload(payload: dict[str, Any]) -> dict[str, Any]:
    public_payload = deepcopy(payload)
    for key in ("request_id", "timings", "error_context"):
        public_payload.pop(key, None)

    if "message" in public_payload:
        public_payload["message"] = sanitize_text(public_payload.get("message"))
    if "reason" in public_payload:
        public_payload["reason"] = sanitize_text(public_payload.get("reason"))

    main_account = public_payload.get("main_account")
    if isinstance(main_account, dict):
        public_payload["main_account"] = _sanitize_funding_account(main_account)

    children = public_payload.get("children")
    if isinstance(children, list):
        public_payload["children"] = [_sanitize_funding_child(child) for child in children if isinstance(child, dict)]

    results = public_payload.get("results")
    if isinstance(results, list):
        public_payload["results"] = [_sanitize_funding_result(result) for result in results if isinstance(result, dict)]

    precheck = public_payload.get("precheck")
    if isinstance(precheck, dict):
        public_payload["precheck"] = _sanitize_funding_precheck(precheck)

    reconciliation = public_payload.get("reconciliation")
    if isinstance(reconciliation, dict):
        public_payload["reconciliation"] = _sanitize_funding_reconciliation(reconciliation)

    overview = public_payload.get("overview")
    if isinstance(overview, dict):
        public_payload["overview"] = sanitize_funding_payload(overview)

    entries = public_payload.get("entries")
    if isinstance(entries, list):
        public_payload["entries"] = [sanitize_funding_payload(entry) for entry in entries if isinstance(entry, dict)]

    return public_payload


def _sanitize_monitor_group(group: dict[str, Any]) -> dict[str, Any]:
    public_group = deepcopy(group)
    accounts = public_group.get("accounts")
    if isinstance(accounts, list):
        public_group["accounts"] = [_sanitize_monitor_account(account) for account in accounts if isinstance(account, dict)]
    return public_group


def _sanitize_monitor_account(account: dict[str, Any]) -> dict[str, Any]:
    public_account = deepcopy(account)
    public_account.pop("diagnostics", None)
    public_account.pop("section_errors", None)
    if "message" in public_account:
        public_account["message"] = sanitize_text(public_account.get("message"))
    return public_account


def _sanitize_refresh_result(refresh_result: dict[str, Any]) -> dict[str, Any]:
    public_result = deepcopy(refresh_result)
    public_result["message"] = sanitize_text(public_result.get("message"))
    public_result.pop("refresh_id", None)
    public_result.pop("failed_accounts", None)
    public_result.pop("slow_accounts", None)

    fallback_sections = public_result.get("fallback_sections")
    if isinstance(fallback_sections, list):
        public_result["fallback_sections"] = [
            {"sections": [str(section) for section in (item.get("sections") or []) if section]}
            for item in fallback_sections
            if isinstance(item, dict)
        ]

    timings = public_result.get("timings")
    if isinstance(timings, dict) and "total_ms" in timings:
        public_result["timings"] = {"total_ms": timings.get("total_ms")}
    else:
        public_result.pop("timings", None)
    return public_result


def _sanitize_funding_account(account: dict[str, Any]) -> dict[str, Any]:
    public_account = deepcopy(account)
    public_account["uid"] = mask_uid(public_account.get("uid")) or "-"
    public_account["reason"] = sanitize_text(public_account.get("reason"))
    public_account.pop("error_context", None)
    return public_account


def _sanitize_funding_child(child: dict[str, Any]) -> dict[str, Any]:
    public_child = deepcopy(child)
    public_child["uid"] = mask_uid(public_child.get("uid")) or "-"
    for key in ("reason", "reason_distribute", "reason_collect"):
        if key in public_child:
            public_child[key] = sanitize_text(public_child.get(key))
    public_child.pop("error_context", None)
    return public_child


def _sanitize_funding_result(result: dict[str, Any]) -> dict[str, Any]:
    public_result = deepcopy(result)
    public_result["uid"] = mask_uid(public_result.get("uid")) or "-"
    public_result["message"] = sanitize_text(public_result.get("message"))
    return public_result


def _sanitize_funding_precheck(precheck: dict[str, Any]) -> dict[str, Any]:
    public_precheck = deepcopy(precheck)
    accounts = public_precheck.get("accounts")
    if isinstance(accounts, list):
        public_precheck["accounts"] = [
            {
                "account_id": str(account.get("account_id") or ""),
                "precheck_available_amount": account.get("precheck_available_amount", "-"),
            }
            for account in accounts
            if isinstance(account, dict)
        ]
    return public_precheck


def _sanitize_funding_reconciliation(reconciliation: dict[str, Any]) -> dict[str, Any]:
    public_reconciliation = deepcopy(reconciliation)
    results = public_reconciliation.get("results")
    if isinstance(results, list):
        public_reconciliation["results"] = [
            {
                "account_id": str(result.get("account_id") or ""),
                "before_available_amount": result.get("before_available_amount", "-"),
                "after_available_amount": result.get("after_available_amount", "-"),
                "expected_direction": str(result.get("expected_direction") or ""),
                "confirmed": bool(result.get("confirmed")),
                "message": sanitize_text(result.get("message")),
            }
            for result in results
            if isinstance(result, dict)
        ]
    return public_reconciliation
