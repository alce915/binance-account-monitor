from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from monitor_app.config import MainAccountConfig, MonitorAccountConfig, Settings
from monitor_app.funding_operation_store import FundingOperationStore
from monitor_app.funding_transfer import (
    BinanceCredentials,
    FundingTransferError,
    FundingTransferRequestError,
    FundingTransferRequestRejected,
    FundingTransferService,
)


def _build_child(account_id: str, *, uid: str = "", api_key: str = "child-k", api_secret: str = "child-s") -> MonitorAccountConfig:
    return MonitorAccountConfig(
        account_id=f"group_a.{account_id}",
        child_account_id=account_id,
        child_account_name=f"Child {account_id}",
        main_account_id="group_a",
        main_account_name="Group A",
        api_key=api_key,
        api_secret=api_secret,
        uid=uid,
    )


def _build_service(
    tmp_path: Path,
    *,
    children: tuple[MonitorAccountConfig, ...],
    with_main_transfer: bool = True,
    **setting_overrides,
) -> FundingTransferService:
    settings = Settings(_env_file=None, monitor_history_db_path=tmp_path / "history.db", **setting_overrides)
    main = MainAccountConfig(
        main_id="group_a",
        name="Group A",
        children=children,
        transfer_api_key="main-k" if with_main_transfer else "",
        transfer_api_secret="main-s" if with_main_transfer else "",
        transfer_uid="123456789" if with_main_transfer else "",
    )
    settings.monitor_main_accounts = {"group_a": main}
    settings.monitor_accounts = {child.account_id: child for child in children}
    return FundingTransferService(settings)


@pytest.mark.asyncio
async def test_get_group_overview_marks_missing_uid_child_as_mode_specific_ineligible(tmp_path: Path) -> None:
    eligible_child = _build_child("sub1", uid="223456789")
    missing_uid_child = _build_child("sub2", uid="")
    service = _build_service(tmp_path, children=(eligible_child, missing_uid_child))

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        assert main_account.main_id == "group_a"
        return {"223456789": "sub1@example.com"}

    async def fake_fetch_spot_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        if credentials.api_key == "main-k":
            return [{"asset": "USDT", "free": "100", "locked": "0", "total": "100"}]
        return [{"asset": "USDT", "free": "12.5", "locked": "0", "total": "12.5"}]

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_spot_assets = fake_fetch_spot_assets  # type: ignore[method-assign]
    try:
        overview = await service.get_group_overview("group_a")
    finally:
        await service.close()

    assert overview["available"] is True
    assert overview["assets"] == ["USDT"]
    assert overview["main_account"]["spot_available"] == {"USDT": "100"}

    eligible = next(item for item in overview["children"] if item["account_id"] == "group_a.sub1")
    ineligible = next(item for item in overview["children"] if item["account_id"] == "group_a.sub2")
    assert eligible["can_distribute"] is True
    assert eligible["can_collect"] is True
    assert eligible["spot_available"] == {"USDT": "12.5"}
    assert ineligible["can_distribute"] is False
    assert ineligible["can_collect"] is False
    assert "UID" in ineligible["reason_distribute"]
    assert "UID" in ineligible["reason_collect"]


@pytest.mark.asyncio
async def test_get_group_overview_uses_structured_request_error_context_when_main_query_fails(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        return {"223456789": "sub1@example.com"}

    async def fake_fetch_spot_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        if credentials.api_key == "main-k":
            raise FundingTransferRequestError(
                "spot account failed after 3 attempts: Binance network request failed: boom",
                label="spot account",
                attempts=3,
                duration_ms=245,
                timeout_s=1.2,
                source="network",
                error_type="ReadTimeout",
                status_code=None,
                retryable=True,
            )
        return [{"asset": "USDT", "free": "12.5", "locked": "0", "total": "12.5"}]

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_spot_assets = fake_fetch_spot_assets  # type: ignore[method-assign]
    try:
        overview = await service.get_group_overview("group_a")
    finally:
        await service.close()

    assert overview["available"] is False
    assert overview["reason"] == "Main transfer API is unavailable for this group (network issue)"
    assert overview["error_context"]["main_account_query"] == {
        "label": "spot account",
        "attempts": 3,
        "duration_ms": 245,
        "timeout_s": 1.2,
        "source": "network",
        "error_type": "ReadTimeout",
        "status_code": None,
        "retryable": True,
    }
    child_overview = overview["children"][0]
    assert child_overview["can_distribute"] is False
    assert child_overview["can_collect"] is False
    assert child_overview["reason_distribute"] == "Main transfer API is unavailable for this group"


@pytest.mark.asyncio
async def test_get_group_overview_keeps_main_spot_balances_when_email_map_query_fails(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        raise FundingTransferRequestError(
            "sub-account list page 1 failed after 3 attempts: Binance network request failed: boom",
            label="sub-account list page 1",
            attempts=3,
            duration_ms=210,
            timeout_s=1.2,
            source="network",
            error_type="ReadTimeout",
            status_code=None,
            retryable=True,
        )

    async def fake_fetch_spot_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        if credentials.api_key == "main-k":
            return [{"asset": "USDT", "free": "100", "locked": "0", "total": "100"}]
        return [{"asset": "USDT", "free": "12.5", "locked": "0", "total": "12.5"}]

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_spot_assets = fake_fetch_spot_assets  # type: ignore[method-assign]
    try:
        overview = await service.get_group_overview("group_a")
    finally:
        await service.close()

    assert overview["available"] is False
    assert overview["reason"] == "Main transfer API is unavailable for this group (network issue)"
    assert overview["main_account"]["spot_available"] == {"USDT": "100"}
    assert overview["error_context"]["email_map"] == {
        "label": "sub-account list page 1",
        "attempts": 3,
        "duration_ms": 210,
        "timeout_s": 1.2,
        "source": "network",
        "error_type": "ReadTimeout",
        "status_code": None,
        "retryable": True,
    }
    assert "main_account_query" not in overview["error_context"]
    child_overview = overview["children"][0]
    assert child_overview["can_distribute"] is False
    assert child_overview["can_collect"] is False
    assert child_overview["spot_available"] == {"USDT": "12.5"}


@pytest.mark.asyncio
async def test_signed_read_request_retries_before_succeeding(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))
    attempts = 0

    async def fake_signed_request_once(
        credentials: BinanceCredentials,
        method: str,
        path: str,
        params: dict[str, str] | None,
        *,
        timeout_s: float,
    ):
        nonlocal attempts
        attempts += 1
        assert path == "/api/v3/account"
        if attempts < 3:
            raise httpx.ReadTimeout("temporary", request=httpx.Request("GET", f"https://example.com{path}"))
        return {"balances": [{"asset": "USDT", "free": "5", "locked": "1"}]}

    service._signed_request_once = fake_signed_request_once  # type: ignore[method-assign]
    try:
        assets = await service._fetch_spot_assets(BinanceCredentials(api_key="main-k", api_secret="main-s"))
    finally:
        await service.close()

    assert attempts == 3
    assert assets == [{"asset": "USDT", "free": "5", "locked": "1", "total": "6"}]


@pytest.mark.asyncio
async def test_signed_read_request_returns_structured_error_after_retry_exhausted(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))
    attempts = 0

    async def fake_signed_request_once(
        credentials: BinanceCredentials,
        method: str,
        path: str,
        params: dict[str, str] | None,
        *,
        timeout_s: float,
    ):
        nonlocal attempts
        attempts += 1
        raise httpx.ReadTimeout("temporary", request=httpx.Request("GET", f"https://example.com{path}"))

    service._signed_request_once = fake_signed_request_once  # type: ignore[method-assign]
    try:
        with pytest.raises(FundingTransferRequestError) as exc_info:
            await service._fetch_spot_assets(BinanceCredentials(api_key="main-k", api_secret="main-s"))
    finally:
        await service.close()

    exc = exc_info.value
    assert attempts == service._settings.binance_secondary_retry_attempts
    assert exc.label == "spot account"
    assert exc.attempts == service._settings.binance_secondary_retry_attempts
    assert exc.source == "network"
    assert exc.error_type == "ReadTimeout"
    assert exc.retryable is True
    assert exc.duration_ms >= 0


@pytest.mark.asyncio
async def test_signed_read_request_logs_do_not_include_sensitive_exception_text(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))

    async def failing_operation():
        raise httpx.ReadTimeout(
            "token=abc email child@example.com uid 223456789",
            request=httpx.Request("GET", "https://example.com/api/v3/account"),
        )

    try:
        with caplog.at_level("WARNING", logger="uvicorn.error"):
            with pytest.raises(FundingTransferRequestError):
                await service._request_with_retry(
                    failing_operation,
                    label="spot account",
                    timeout_s=1.2,
                    max_attempts=1,
                )
    finally:
        await service.close()

    assert "token=abc" not in caplog.text
    assert "child@example.com" not in caplog.text
    assert "223456789" not in caplog.text
    assert "error_type=ReadTimeout" in caplog.text


@pytest.mark.asyncio
async def test_distribute_runs_single_spot_transfer_from_main_to_child(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))
    calls: list[tuple[str, str, dict[str, str]]] = []

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        return {"223456789": "sub1@example.com"}

    async def fake_fetch_spot_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        if credentials.api_key == "main-k":
            return [{"asset": "USDT", "free": "120.5", "locked": "0", "total": "120.5"}]
        return [{"asset": "USDT", "free": "6", "locked": "0", "total": "6"}]

    async def fake_signed_request(credentials: BinanceCredentials, method: str, path: str, params: dict[str, str] | None = None):
        calls.append((credentials.api_key, path, dict(params or {})))
        return {"success": True}

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_spot_assets = fake_fetch_spot_assets  # type: ignore[method-assign]
    service._signed_request = fake_signed_request  # type: ignore[method-assign]
    try:
        result = await service.distribute(
            "group_a",
            asset="usdt",
            operation_id="dist-basic",
            transfers=[{"account_id": "group_a.sub1", "amount": "12.5"}],
        )
    finally:
        await service.close()

    assert result["direction"] == "distribute"
    assert result["request_id"]
    assert result["operation_status"] == "operation_fully_succeeded"
    assert result["timings"]["transfer_ms"] >= 0
    assert result["results"][0]["success"] is True
    assert result["results"][0]["executed_amount"] == "12.5"
    assert result["overview_refresh"]["success"] is True
    assert result["reconciliation"]["status"] == "confirmed"
    assert calls == [
        (
            "main-k",
            "/sapi/v1/sub-account/universalTransfer",
            {"toEmail": "sub1@example.com", "fromAccountType": "SPOT", "toAccountType": "SPOT", "asset": "USDT", "amount": "12.5"},
        )
    ]


@pytest.mark.asyncio
async def test_distribute_does_not_retry_real_transfer_post(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))
    calls = 0

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        return {"223456789": "sub1@example.com"}

    async def fake_fetch_spot_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        if credentials.api_key == "main-k":
            return [{"asset": "USDT", "free": "120.5", "locked": "0", "total": "120.5"}]
        return [{"asset": "USDT", "free": "6", "locked": "0", "total": "6"}]

    async def fake_signed_request(credentials: BinanceCredentials, method: str, path: str, params: dict[str, str] | None = None):
        nonlocal calls
        calls += 1
        raise FundingTransferError("Binance returned an error: insufficient permission")

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_spot_assets = fake_fetch_spot_assets  # type: ignore[method-assign]
    service._signed_request = fake_signed_request  # type: ignore[method-assign]
    try:
        result = await service.distribute(
            "group_a",
            asset="USDT",
            operation_id="dist-no-retry",
            transfers=[{"account_id": "group_a.sub1", "amount": "12.5"}],
        )
    finally:
        await service.close()

    assert calls == 1
    assert result["results"][0]["success"] is False
    assert result["results"][0]["message"] == "Distribute failed"
    assert result["operation_status"] == "operation_failed"


@pytest.mark.asyncio
async def test_collect_uses_requested_spot_amount_from_selected_child(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))
    calls: list[tuple[str, str, dict[str, str]]] = []

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        return {"223456789": "sub1@example.com"}

    async def fake_fetch_spot_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        if credentials.api_key == "child-k":
            return [{"asset": "USDT", "free": "8.75", "locked": "1.25", "total": "10"}]
        return [{"asset": "USDT", "free": "100", "locked": "0", "total": "100"}]

    async def fake_signed_request(credentials: BinanceCredentials, method: str, path: str, params: dict[str, str] | None = None):
        calls.append((credentials.api_key, path, dict(params or {})))
        return {"success": True}

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_spot_assets = fake_fetch_spot_assets  # type: ignore[method-assign]
    service._signed_request = fake_signed_request  # type: ignore[method-assign]
    try:
        result = await service.collect(
            "group_a",
            asset="usdt",
            operation_id="collect-basic",
            transfers=[{"account_id": "group_a.sub1", "amount": "7.5"}],
        )
    finally:
        await service.close()

    assert result["direction"] == "collect"
    assert result["results"][0]["success"] is True
    assert result["results"][0]["amount"] == "7.5"
    assert result["operation_status"] == "operation_fully_succeeded"
    assert result["reconciliation"]["status"] == "confirmed"
    assert calls == [
        (
            "main-k",
            "/sapi/v1/sub-account/universalTransfer",
            {"fromEmail": "sub1@example.com", "fromAccountType": "SPOT", "toAccountType": "SPOT", "asset": "USDT", "amount": "7.5"},
        )
    ]


@pytest.mark.asyncio
async def test_collect_rejects_when_requested_amount_exceeds_available_spot_balance(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        return {"223456789": "sub1@example.com"}

    async def fake_fetch_spot_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        if credentials.api_key == "child-k":
            return [{"asset": "USDT", "free": "8.75", "locked": "1.25", "total": "10"}]
        return [{"asset": "USDT", "free": "100", "locked": "0", "total": "100"}]

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_spot_assets = fake_fetch_spot_assets  # type: ignore[method-assign]
    try:
        with pytest.raises(FundingTransferRequestRejected, match="can collect at most 8.75 USDT") as exc_info:
            await service.collect(
                "group_a",
                asset="USDT",
                operation_id="collect-too-much",
                transfers=[{"account_id": "group_a.sub1", "amount": "9"}],
            )
    finally:
        await service.close()

    assert exc_info.value.code == "INSUFFICIENT_BALANCE"


@pytest.mark.asyncio
async def test_collect_legacy_account_ids_still_collects_full_available_amount(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))
    calls: list[tuple[str, str, dict[str, str]]] = []

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        return {"223456789": "sub1@example.com"}

    async def fake_fetch_spot_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        if credentials.api_key == "child-k":
            return [{"asset": "USDT", "free": "8.75", "locked": "1.25", "total": "10"}]
        return [{"asset": "USDT", "free": "100", "locked": "0", "total": "100"}]

    async def fake_signed_request(credentials: BinanceCredentials, method: str, path: str, params: dict[str, str] | None = None):
        calls.append((credentials.api_key, path, dict(params or {})))
        return {"success": True}

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_spot_assets = fake_fetch_spot_assets  # type: ignore[method-assign]
    service._signed_request = fake_signed_request  # type: ignore[method-assign]
    try:
        result = await service.collect("group_a", asset="USDT", operation_id="collect-legacy", account_ids=["group_a.sub1"])
    finally:
        await service.close()

    assert result["results"][0]["amount"] == "8.75"
    assert result["operation_status"] == "operation_fully_succeeded"
    assert calls == [
        (
            "main-k",
            "/sapi/v1/sub-account/universalTransfer",
            {"fromEmail": "sub1@example.com", "fromAccountType": "SPOT", "toAccountType": "SPOT", "asset": "USDT", "amount": "8.75"},
        )
    ]


@pytest.mark.asyncio
async def test_distribute_rejects_when_group_has_no_transfer_api(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,), with_main_transfer=False)
    try:
        with pytest.raises(FundingTransferRequestRejected, match="Main transfer API is not configured for this group") as exc_info:
            await service.distribute("group_a", asset="USDT", operation_id="missing-transfer-api", transfers=[{"account_id": "group_a.sub1", "amount": "1"}])
    finally:
        await service.close()

    assert exc_info.value.code == "TRANSFER_API_NOT_CONFIGURED"


@pytest.mark.asyncio
async def test_distribute_rejects_unknown_sub_account_with_specific_code(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        return {"223456789": "sub1@example.com"}

    async def fake_fetch_spot_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        if credentials.api_key == "main-k":
            return [{"asset": "USDT", "free": "120.5", "locked": "0", "total": "120.5"}]
        return [{"asset": "USDT", "free": "6", "locked": "0", "total": "6"}]

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_spot_assets = fake_fetch_spot_assets  # type: ignore[method-assign]
    try:
        with pytest.raises(FundingTransferRequestRejected, match="Unknown sub-account: group_a.sub2") as exc_info:
            await service.distribute(
                "group_a",
                asset="USDT",
                operation_id="unknown-account",
                transfers=[{"account_id": "group_a.sub2", "amount": "1"}],
            )
    finally:
        await service.close()

    assert exc_info.value.code == "INVALID_ACCOUNT"


@pytest.mark.asyncio
async def test_distribute_rejects_empty_selection_with_specific_code(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        return {"223456789": "sub1@example.com"}

    async def fake_fetch_spot_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        if credentials.api_key == "main-k":
            return [{"asset": "USDT", "free": "120.5", "locked": "0", "total": "120.5"}]
        return [{"asset": "USDT", "free": "6", "locked": "0", "total": "6"}]

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_spot_assets = fake_fetch_spot_assets  # type: ignore[method-assign]
    try:
        with pytest.raises(FundingTransferRequestRejected, match="Select at least one sub-account") as exc_info:
            await service.distribute(
                "group_a",
                asset="USDT",
                operation_id="empty-selection",
                transfers=[{"account_id": "group_a.sub1", "amount": "0"}],
            )
    finally:
        await service.close()

    assert exc_info.value.code == "EMPTY_SELECTION"


@pytest.mark.asyncio
async def test_distribute_keeps_success_when_overview_refresh_fails(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))
    calls = 0

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        return {"223456789": "sub1@example.com"}

    async def fake_fetch_spot_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        if credentials.api_key == "main-k":
            return [{"asset": "USDT", "free": "120.5", "locked": "0", "total": "120.5"}]
        return [{"asset": "USDT", "free": "6", "locked": "0", "total": "6"}]

    async def fake_signed_request(credentials: BinanceCredentials, method: str, path: str, params: dict[str, str] | None = None):
        nonlocal calls
        calls += 1
        return {"success": True}

    async def failing_overview(main_id: str) -> dict[str, str]:
        raise FundingTransferError("overview refresh failed")

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_spot_assets = fake_fetch_spot_assets  # type: ignore[method-assign]
    service._signed_request = fake_signed_request  # type: ignore[method-assign]
    service.get_group_overview = failing_overview  # type: ignore[method-assign]
    try:
        result = await service.distribute(
            "group_a",
            asset="USDT",
            operation_id="op-overview-fail",
            transfers=[{"account_id": "group_a.sub1", "amount": "12.5"}],
        )
    finally:
        await service.close()

    assert calls == 1
    assert result["results"][0]["success"] is True
    assert result["operation_status"] == "operation_submitted"
    assert result["overview"] is None
    assert result["overview_refresh"]["success"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", ["NaN", "Infinity", "-Infinity", ""])
async def test_distribute_rejects_non_finite_or_blank_amounts(tmp_path: Path, amount: str) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        return {"223456789": "sub1@example.com"}

    async def fake_fetch_spot_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        if credentials.api_key == "main-k":
            return [{"asset": "USDT", "free": "120.5", "locked": "0", "total": "120.5"}]
        return [{"asset": "USDT", "free": "6", "locked": "0", "total": "6"}]

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_spot_assets = fake_fetch_spot_assets  # type: ignore[method-assign]
    try:
        with pytest.raises(FundingTransferError):
            await service.distribute(
                "group_a",
                asset="USDT",
                operation_id="invalid-amount",
                transfers=[{"account_id": "group_a.sub1", "amount": amount}],
            )
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_distribute_rejects_duplicate_account_ids_in_one_operation(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        return {"223456789": "sub1@example.com"}

    async def fake_fetch_spot_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        if credentials.api_key == "main-k":
            return [{"asset": "USDT", "free": "120.5", "locked": "0", "total": "120.5"}]
        return [{"asset": "USDT", "free": "6", "locked": "0", "total": "6"}]

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_spot_assets = fake_fetch_spot_assets  # type: ignore[method-assign]
    try:
        with pytest.raises(FundingTransferError, match="Duplicate sub-account"):
            await service.distribute(
                "group_a",
                asset="USDT",
                operation_id="dup-account",
                transfers=[
                    {"account_id": "group_a.sub1", "amount": "1"},
                    {"account_id": "group_a.sub1", "amount": "2"},
                ],
            )
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_distribute_reuses_idempotent_result_for_same_operation_id(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))
    calls = 0

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        return {"223456789": "sub1@example.com"}

    async def fake_fetch_spot_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        if credentials.api_key == "main-k":
            return [{"asset": "USDT", "free": "120.5", "locked": "0", "total": "120.5"}]
        return [{"asset": "USDT", "free": "6", "locked": "0", "total": "6"}]

    async def fake_signed_request(credentials: BinanceCredentials, method: str, path: str, params: dict[str, str] | None = None):
        nonlocal calls
        calls += 1
        return {"success": True}

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_spot_assets = fake_fetch_spot_assets  # type: ignore[method-assign]
    service._signed_request = fake_signed_request  # type: ignore[method-assign]
    try:
        first = await service.distribute(
            "group_a",
            asset="USDT",
            operation_id="same-op",
            transfers=[{"account_id": "group_a.sub1", "amount": "12.5"}],
        )
        second = await service.distribute(
            "group_a",
            asset="USDT",
            operation_id="same-op",
            transfers=[{"account_id": "group_a.sub1", "amount": "12.5"}],
        )
    finally:
        await service.close()

    assert calls == 1
    assert first["idempotent_hit"] is False
    assert second["idempotent_hit"] is True
    assert second["operation_id"] == "same-op"


@pytest.mark.asyncio
async def test_distribute_rejects_operation_id_reuse_with_different_payload(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        return {"223456789": "sub1@example.com"}

    async def fake_fetch_spot_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        if credentials.api_key == "main-k":
            return [{"asset": "USDT", "free": "120.5", "locked": "0", "total": "120.5"}]
        return [{"asset": "USDT", "free": "6", "locked": "0", "total": "6"}]

    async def fake_signed_request(credentials: BinanceCredentials, method: str, path: str, params: dict[str, str] | None = None):
        return {"success": True}

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_spot_assets = fake_fetch_spot_assets  # type: ignore[method-assign]
    service._signed_request = fake_signed_request  # type: ignore[method-assign]
    try:
        await service.distribute(
            "group_a",
            asset="USDT",
            operation_id="reuse-op",
            transfers=[{"account_id": "group_a.sub1", "amount": "12.5"}],
        )
        with pytest.raises(FundingTransferError, match="different request payload"):
            await service.distribute(
                "group_a",
                asset="USDT",
                operation_id="reuse-op",
                transfers=[{"account_id": "group_a.sub1", "amount": "13"}],
            )
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_collect_respects_write_protection_switch(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,), funding_transfer_write_enabled=False)
    try:
        with pytest.raises(FundingTransferError, match="disabled by configuration"):
            await service.collect(
                "group_a",
                asset="USDT",
                operation_id="write-disabled",
                transfers=[{"account_id": "group_a.sub1", "amount": "1"}],
            )
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_distribute_persists_audit_entries(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        return {"223456789": "sub1@example.com"}

    async def fake_fetch_spot_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        if credentials.api_key == "main-k":
            return [{"asset": "USDT", "free": "120.5", "locked": "0", "total": "120.5"}]
        return [{"asset": "USDT", "free": "6", "locked": "0", "total": "6"}]

    async def fake_signed_request(credentials: BinanceCredentials, method: str, path: str, params: dict[str, str] | None = None):
        return {"success": True}

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_spot_assets = fake_fetch_spot_assets  # type: ignore[method-assign]
    service._signed_request = fake_signed_request  # type: ignore[method-assign]
    try:
        await service.distribute(
            "group_a",
            asset="USDT",
            operation_id="audit-op",
            transfers=[{"account_id": "group_a.sub1", "amount": "12.5"}],
        )
        audit = await service.get_audit_entries("group_a")
        detail = await service.get_audit_entry_detail("group_a", "audit-op", direction="distribute")
    finally:
        await service.close()

    assert audit["main_account_id"] == "group_a"
    assert len(audit["entries"]) == 1
    assert audit["entries"][0]["operation_id"] == "audit-op"
    assert audit["entries"][0]["operation_status"] == "operation_fully_succeeded"
    assert "results" not in audit["entries"][0]
    assert detail["operation_id"] == "audit-op"
    assert detail["execution_stage"] == "completed"
    assert detail["results"][0]["account_id"] == "group_a.sub1"


@pytest.mark.asyncio
async def test_audit_detail_lookup_uses_direction_when_operation_id_is_reused(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        return {"223456789": "sub1@example.com"}

    async def fake_fetch_spot_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        if credentials.api_key == "main-k":
            return [{"asset": "USDT", "free": "120.5", "locked": "0", "total": "120.5"}]
        return [{"asset": "USDT", "free": "8.75", "locked": "0", "total": "8.75"}]

    async def fake_signed_request(credentials: BinanceCredentials, method: str, path: str, params: dict[str, str] | None = None):
        return {"success": True}

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_spot_assets = fake_fetch_spot_assets  # type: ignore[method-assign]
    service._signed_request = fake_signed_request  # type: ignore[method-assign]
    try:
        await service.distribute(
            "group_a",
            asset="USDT",
            operation_id="shared-op",
            transfers=[{"account_id": "group_a.sub1", "amount": "1"}],
        )
        await service.collect(
            "group_a",
            asset="USDT",
            operation_id="shared-op",
            transfers=[{"account_id": "group_a.sub1", "amount": "1"}],
        )
        distribute_detail = await service.get_audit_entry_detail("group_a", "shared-op", direction="distribute")
        collect_detail = await service.get_audit_entry_detail("group_a", "shared-op", direction="collect")
    finally:
        await service.close()

    assert distribute_detail["direction"] == "distribute"
    assert collect_detail["direction"] == "collect"
    assert distribute_detail["operation_summary"]["expected_main_direction"] == "decrease"
    assert collect_detail["operation_summary"]["expected_main_direction"] == "increase"


@pytest.mark.asyncio
async def test_distribute_requires_non_blank_operation_id(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))
    try:
        with pytest.raises(FundingTransferRequestRejected, match="operation_id is required") as exc_info:
            await service.distribute(
                "group_a",
                asset="USDT",
                operation_id="  ",
                transfers=[{"account_id": "group_a.sub1", "amount": "1"}],
            )
    finally:
        await service.close()

    assert exc_info.value.code == "OPERATION_ID_REQUIRED"


@pytest.mark.asyncio
async def test_collect_legacy_all_precheck_failures_return_no_eligible_transfer(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        return {"223456789": "sub1@example.com"}

    async def fake_fetch_spot_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        if credentials.api_key == "child-k":
            return [{"asset": "USDT", "free": "0", "locked": "0", "total": "0"}]
        return [{"asset": "USDT", "free": "100", "locked": "0", "total": "100"}]

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_spot_assets = fake_fetch_spot_assets  # type: ignore[method-assign]
    try:
        with pytest.raises(FundingTransferRequestRejected, match="No eligible sub-account") as exc_info:
            await service.collect(
                "group_a",
                asset="USDT",
                operation_id="legacy-empty",
                account_ids=["group_a.sub1"],
            )
    finally:
        await service.close()

    assert exc_info.value.code == "NO_ELIGIBLE_TRANSFER"


@pytest.mark.asyncio
async def test_collect_legacy_audit_summary_account_count_includes_precheck_failures(tmp_path: Path) -> None:
    child_ok = _build_child("sub1", uid="223456789")
    child_empty = _build_child("sub2", uid="323456789", api_key="child2-k", api_secret="child2-s")
    service = _build_service(tmp_path, children=(child_ok, child_empty))

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        return {
            "223456789": "sub1@example.com",
            "323456789": "sub2@example.com",
        }

    async def fake_fetch_spot_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        if credentials.api_key == "main-k":
            return [{"asset": "USDT", "free": "100", "locked": "0", "total": "100"}]
        if credentials.api_key == "child-k":
            return [{"asset": "USDT", "free": "8.75", "locked": "0", "total": "8.75"}]
        return [{"asset": "USDT", "free": "0", "locked": "0", "total": "0"}]

    async def fake_signed_request(credentials: BinanceCredentials, method: str, path: str, params: dict[str, str] | None = None):
        return {"success": True}

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_spot_assets = fake_fetch_spot_assets  # type: ignore[method-assign]
    service._signed_request = fake_signed_request  # type: ignore[method-assign]
    try:
        await service.collect(
            "group_a",
            asset="USDT",
            operation_id="legacy-mixed",
            account_ids=["group_a.sub1", "group_a.sub2"],
        )
        audit = await service.get_audit_entries("group_a")
    finally:
        await service.close()

    assert audit["entries"][0]["operation_id"] == "legacy-mixed"
    assert audit["entries"][0]["account_count"] == 2


@pytest.mark.asyncio
async def test_funding_operation_store_backfills_historical_account_count(tmp_path: Path) -> None:
    db_path = tmp_path / "history.db"
    store = FundingOperationStore(db_path, max_rows=2000, idempotency_ttl_seconds=600)
    try:
        await store.create_operation(
            main_id="group_a",
            direction="collect",
            asset="USDT",
            request_id="req-old",
            operation_id="old-summary",
            payload_hash="hash-old",
            execution_stage="completed",
            operation_status="operation_partially_succeeded",
            account_count=1,
            success_count=1,
            failure_count=1,
            confirmed_count=1,
            pending_confirmation_count=0,
            message="legacy mixed collect",
            response={
                "direction": "collect",
                "asset": "USDT",
                "operation_id": "old-summary",
                "operation_status": "operation_partially_succeeded",
                "execution_stage": "completed",
                "precheck": {
                    "selected_account_count": 2,
                    "validated_account_count": 1,
                },
                "results": [
                    {"account_id": "group_a.sub1", "success": True, "transfer_attempted": True},
                    {"account_id": "group_a.sub2", "success": False, "transfer_attempted": False},
                ],
                "reconciliation": {
                    "confirmed_count": 1,
                    "results": [{"account_id": "group_a.sub1", "confirmed": True}],
                },
                "message": "legacy mixed collect",
                "updated_at": "2026-04-18T12:00:00+08:00",
            },
        )
    finally:
        await store.close()

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            UPDATE funding_operations
            SET account_count = 1, message = '', updated_at_ms = 0
            WHERE main_id = ? AND direction = ? AND operation_id = ?
            """,
            ("group_a", "collect", "old-summary"),
        )
        conn.commit()
    finally:
        conn.close()

    reopened = FundingOperationStore(db_path, max_rows=2000, idempotency_ttl_seconds=600)
    try:
        entries = await reopened.list_operations("group_a")
    finally:
        await reopened.close()

    assert entries[0]["operation_id"] == "old-summary"
    assert entries[0]["account_count"] == 2


@pytest.mark.asyncio
async def test_funding_operation_store_preserves_newer_updated_at_when_backfilling_summary(tmp_path: Path) -> None:
    db_path = tmp_path / "history.db"
    store = FundingOperationStore(db_path, max_rows=2000, idempotency_ttl_seconds=600)
    try:
        await store.create_operation(
            main_id="group_a",
            direction="collect",
            asset="USDT",
            request_id="req-updated",
            operation_id="old-updated-at",
            payload_hash="hash-updated",
            execution_stage="completed",
            operation_status="operation_fully_succeeded",
            account_count=1,
            success_count=1,
            failure_count=0,
            confirmed_count=1,
            pending_confirmation_count=0,
            message="newer updated_at should survive",
            response={
                "direction": "collect",
                "asset": "USDT",
                "operation_id": "old-updated-at",
                "operation_status": "operation_fully_succeeded",
                "execution_stage": "completed",
                "results": [
                    {"account_id": "group_a.sub1", "success": True, "transfer_attempted": True},
                ],
                "reconciliation": {
                    "confirmed_count": 1,
                    "results": [{"account_id": "group_a.sub1", "confirmed": True}],
                },
                "message": "newer updated_at should survive",
            },
        )
    finally:
        await store.close()

    conn = sqlite3.connect(db_path)
    try:
        original_created_at_ms = int(
            conn.execute(
                """
                SELECT created_at_ms
                FROM funding_operations
                WHERE main_id = ? AND direction = ? AND operation_id = ?
                """,
                ("group_a", "collect", "old-updated-at"),
            ).fetchone()[0]
        )
        newer_updated_at_ms = original_created_at_ms + 60000
        conn.execute(
            """
            UPDATE funding_operations
            SET updated_at_ms = ?
            WHERE main_id = ? AND direction = ? AND operation_id = ?
            """,
            (newer_updated_at_ms, "group_a", "collect", "old-updated-at"),
        )
        conn.commit()
    finally:
        conn.close()

    reopened = FundingOperationStore(db_path, max_rows=2000, idempotency_ttl_seconds=600)
    try:
        entries = await reopened.list_operations("group_a")
    finally:
        await reopened.close()

    assert entries[0]["operation_id"] == "old-updated-at"
    assert entries[0]["updated_at"] == datetime.fromtimestamp(newer_updated_at_ms / 1000, UTC).isoformat()


@pytest.mark.asyncio
async def test_funding_operation_store_lists_most_recently_updated_entry_first(tmp_path: Path) -> None:
    db_path = tmp_path / "history.db"
    store = FundingOperationStore(db_path, max_rows=2000, idempotency_ttl_seconds=600)
    try:
        await store.create_operation(
            main_id="group_a",
            direction="distribute",
            asset="USDT",
            request_id="req-old",
            operation_id="older-created",
            payload_hash="hash-old",
            execution_stage="completed",
            operation_status="operation_fully_succeeded",
            account_count=1,
            success_count=1,
            failure_count=0,
            confirmed_count=1,
            pending_confirmation_count=0,
            message="older operation",
            response={
                "direction": "distribute",
                "asset": "USDT",
                "operation_id": "older-created",
                "operation_status": "operation_fully_succeeded",
                "execution_stage": "completed",
                "results": [{"account_id": "group_a.sub1", "success": True, "transfer_attempted": True}],
                "reconciliation": {"confirmed_count": 1, "results": [{"account_id": "group_a.sub1", "confirmed": True}]},
                "message": "older operation",
            },
        )
        await store.create_operation(
            main_id="group_a",
            direction="collect",
            asset="USDT",
            request_id="req-new",
            operation_id="newer-created",
            payload_hash="hash-new",
            execution_stage="completed",
            operation_status="operation_fully_succeeded",
            account_count=1,
            success_count=1,
            failure_count=0,
            confirmed_count=1,
            pending_confirmation_count=0,
            message="newer operation",
            response={
                "direction": "collect",
                "asset": "USDT",
                "operation_id": "newer-created",
                "operation_status": "operation_fully_succeeded",
                "execution_stage": "completed",
                "results": [{"account_id": "group_a.sub1", "success": True, "transfer_attempted": True}],
                "reconciliation": {"confirmed_count": 1, "results": [{"account_id": "group_a.sub1", "confirmed": True}]},
                "message": "newer operation",
            },
        )

        await store.update_operation(
            main_id="group_a",
            direction="distribute",
            operation_id="older-created",
            execution_stage="completed",
            operation_status="operation_fully_succeeded",
            account_count=1,
            success_count=1,
            failure_count=0,
            confirmed_count=1,
            pending_confirmation_count=0,
            message="older operation updated later",
            response={
                "direction": "distribute",
                "asset": "USDT",
                "operation_id": "older-created",
                "operation_status": "operation_fully_succeeded",
                "execution_stage": "completed",
                "results": [{"account_id": "group_a.sub1", "success": True, "transfer_attempted": True}],
                "reconciliation": {"confirmed_count": 1, "results": [{"account_id": "group_a.sub1", "confirmed": True}]},
                "message": "older operation updated later",
                "updated_at": datetime.now(UTC).isoformat(),
            },
        )

        entries = await store.list_operations("group_a")
    finally:
        await store.close()

    assert [entry["operation_id"] for entry in entries[:2]] == ["older-created", "newer-created"]


@pytest.mark.asyncio
async def test_collect_rejects_fractional_amount_when_available_balance_is_integer(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        return {"223456789": "sub1@example.com"}

    async def fake_fetch_spot_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        if credentials.api_key == "child-k":
            return [{"asset": "USDT", "free": "10", "locked": "0", "total": "10"}]
        return [{"asset": "USDT", "free": "100", "locked": "0", "total": "100"}]

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_spot_assets = fake_fetch_spot_assets  # type: ignore[method-assign]
    try:
        with pytest.raises(FundingTransferRequestRejected, match="at most 0 decimal places") as exc_info:
            await service.collect(
                "group_a",
                asset="USDT",
                operation_id="precision-op",
                transfers=[{"account_id": "group_a.sub1", "amount": "1.5"}],
            )
    finally:
        await service.close()

    assert exc_info.value.code == "PRECISION_EXCEEDED"


@pytest.mark.asyncio
async def test_collect_rejects_missing_sub_account_credentials_with_specific_code(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789", api_key="", api_secret="")
    service = _build_service(tmp_path, children=(child,))

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        return {"223456789": "sub1@example.com"}

    async def fake_fetch_spot_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        return [{"asset": "USDT", "free": "100", "locked": "0", "total": "100"}]

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_spot_assets = fake_fetch_spot_assets  # type: ignore[method-assign]
    try:
        with pytest.raises(FundingTransferRequestRejected, match="Sub-account API credentials are not configured") as exc_info:
            await service.collect(
                "group_a",
                asset="USDT",
                operation_id="collect-no-child-creds",
                transfers=[{"account_id": "group_a.sub1", "amount": "1"}],
            )
    finally:
        await service.close()

    assert exc_info.value.code == "SUB_ACCOUNT_API_NOT_CONFIGURED"


@pytest.mark.asyncio
async def test_distribute_rejects_main_email_map_query_failures_with_specific_code(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        raise FundingTransferError("mapping query failed")

    async def fake_fetch_spot_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        return [{"asset": "USDT", "free": "100", "locked": "0", "total": "100"}]

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_spot_assets = fake_fetch_spot_assets  # type: ignore[method-assign]
    try:
        with pytest.raises(FundingTransferRequestRejected, match="Failed to query sub-account mapping") as exc_info:
            await service.distribute(
                "group_a",
                asset="USDT",
                operation_id="dist-email-map-fail",
                transfers=[{"account_id": "group_a.sub1", "amount": "1"}],
            )
    finally:
        await service.close()

    assert exc_info.value.code == "SUB_ACCOUNT_MAPPING_QUERY_FAILED"
    assert exc_info.value.operation_id == "dist-email-map-fail"


@pytest.mark.asyncio
async def test_distribute_rejects_main_spot_query_failures_with_specific_code(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        return {"223456789": "sub1@example.com"}

    async def fake_fetch_spot_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        raise FundingTransferError("spot query failed")

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_spot_assets = fake_fetch_spot_assets  # type: ignore[method-assign]
    try:
        with pytest.raises(FundingTransferRequestRejected, match="Failed to query main account spot balances") as exc_info:
            await service.distribute(
                "group_a",
                asset="USDT",
                operation_id="dist-main-spot-fail",
                transfers=[{"account_id": "group_a.sub1", "amount": "1"}],
            )
    finally:
        await service.close()

    assert exc_info.value.code == "MAIN_BALANCE_QUERY_FAILED"
    assert exc_info.value.operation_id == "dist-main-spot-fail"


@pytest.mark.asyncio
async def test_distribute_persists_partial_results_when_execution_interrupts(tmp_path: Path) -> None:
    child_one = _build_child("sub1", uid="223456789")
    child_two = _build_child("sub2", uid="323456789", api_key="child2-k", api_secret="child2-s")
    service = _build_service(tmp_path, children=(child_one, child_two))

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        return {
            "223456789": "sub1@example.com",
            "323456789": "sub2@example.com",
        }

    async def fake_fetch_spot_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        if credentials.api_key == "main-k":
            return [{"asset": "USDT", "free": "100", "locked": "0", "total": "100"}]
        return [{"asset": "USDT", "free": "5", "locked": "0", "total": "5"}]

    call_count = 0

    async def fake_distribute_to_child(**kwargs):
        nonlocal call_count
        call_count += 1
        child = kwargs["child"]
        amount = kwargs["amount"]
        if call_count == 1:
            return {
                "account_id": child.account_id,
                "name": child.child_account_name,
                "uid": child.uid,
                "requested_amount": str(amount),
                "normalized_amount": str(amount),
                "precheck_available_amount": None,
                "executed_amount": str(amount),
                "transfer_attempted": True,
                "success": True,
                "message": "Distribute succeeded",
            }
        raise RuntimeError("simulated crash")

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_spot_assets = fake_fetch_spot_assets  # type: ignore[method-assign]
    service._distribute_to_child = fake_distribute_to_child  # type: ignore[method-assign]
    try:
        with pytest.raises(RuntimeError, match="simulated crash"):
            await service.distribute(
                "group_a",
                asset="USDT",
                operation_id="dist-interrupted",
                transfers=[
                    {"account_id": "group_a.sub1", "amount": "1"},
                    {"account_id": "group_a.sub2", "amount": "1"},
                ],
            )
        record = await service._operation_store.get_operation("group_a", "distribute", "dist-interrupted")
    finally:
        await service.close()

    assert record is not None
    assert record.execution_stage == "executing"
    assert len(record.response.get("results") or []) == 1
    assert record.response["results"][0]["account_id"] == "group_a.sub1"


@pytest.mark.asyncio
async def test_distribute_reconciliation_retries_once_before_marking_unconfirmed(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))
    transfer_calls = 0
    main_fetch_calls = 0
    child_fetch_calls = 0

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        return {"223456789": "sub1@example.com"}

    async def fake_fetch_spot_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        nonlocal main_fetch_calls, child_fetch_calls
        if credentials.api_key == "main-k":
            main_fetch_calls += 1
            if main_fetch_calls == 1:
                return [{"asset": "USDT", "free": "100", "locked": "0", "total": "100"}]
            return [{"asset": "USDT", "free": "99", "locked": "0", "total": "99"}]
        child_fetch_calls += 1
        if child_fetch_calls == 1:
            return [{"asset": "USDT", "free": "6", "locked": "0", "total": "6"}]
        return [{"asset": "USDT", "free": "7", "locked": "0", "total": "7"}]

    async def fake_signed_request(credentials: BinanceCredentials, method: str, path: str, params: dict[str, str] | None = None):
        nonlocal transfer_calls
        transfer_calls += 1
        return {"success": True}

    original_sleep = asyncio.sleep

    async def fast_sleep(delay: float) -> None:
        assert delay == 1.5
        await original_sleep(0)

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_spot_assets = fake_fetch_spot_assets  # type: ignore[method-assign]
    service._signed_request = fake_signed_request  # type: ignore[method-assign]
    try:
        asyncio_sleep = asyncio.sleep
        asyncio.sleep = fast_sleep  # type: ignore[assignment]
        result = await service.distribute(
            "group_a",
            asset="USDT",
            operation_id="retry-reconcile",
            transfers=[{"account_id": "group_a.sub1", "amount": "1"}],
        )
    finally:
        asyncio.sleep = asyncio_sleep  # type: ignore[assignment]
        await service.close()

    assert transfer_calls == 1
    assert main_fetch_calls >= 3
    assert child_fetch_calls >= 3
    assert result["reconciliation"]["status"] == "confirmed"
    assert result["operation_status"] == "operation_fully_succeeded"
