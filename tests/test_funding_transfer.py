from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from monitor_app.config import MainAccountConfig, MonitorAccountConfig, Settings
from monitor_app.funding_transfer import (
    BinanceCredentials,
    FundingTransferError,
    FundingTransferRequestError,
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


def _build_service(tmp_path: Path, *, children: tuple[MonitorAccountConfig, ...], with_main_transfer: bool = True) -> FundingTransferService:
    settings = Settings(_env_file=None, monitor_history_db_path=tmp_path / "history.db")
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
            return [{"asset": "USDT", "free": "120", "locked": "0", "total": "120"}]
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
            transfers=[{"account_id": "group_a.sub1", "amount": "12.5"}],
        )
    finally:
        await service.close()

    assert result["direction"] == "distribute"
    assert result["request_id"]
    assert result["timings"]["transfer_ms"] >= 0
    assert result["results"][0]["success"] is True
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
            return [{"asset": "USDT", "free": "120", "locked": "0", "total": "120"}]
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
            transfers=[{"account_id": "group_a.sub1", "amount": "12.5"}],
        )
    finally:
        await service.close()

    assert calls == 1
    assert result["results"][0]["success"] is False
    assert result["results"][0]["message"] == "Distribute failed"


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
            transfers=[{"account_id": "group_a.sub1", "amount": "7.5"}],
        )
    finally:
        await service.close()

    assert result["direction"] == "collect"
    assert result["results"][0]["success"] is True
    assert result["results"][0]["amount"] == "7.5"
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
        with pytest.raises(FundingTransferError, match="can collect at most 8.75 USDT"):
            await service.collect(
                "group_a",
                asset="USDT",
                transfers=[{"account_id": "group_a.sub1", "amount": "9"}],
            )
    finally:
        await service.close()


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
        result = await service.collect("group_a", asset="USDT", account_ids=["group_a.sub1"])
    finally:
        await service.close()

    assert result["results"][0]["amount"] == "8.75"
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
        with pytest.raises(FundingTransferError, match="Main transfer API is not configured for this group"):
            await service.distribute("group_a", asset="USDT", transfers=[{"account_id": "group_a.sub1", "amount": "1"}])
    finally:
        await service.close()
