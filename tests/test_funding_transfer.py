from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from monitor_app.config import MainAccountConfig, MonitorAccountConfig, Settings
from monitor_app.funding_transfer import BinanceCredentials, FundingTransferError, FundingTransferService


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
async def test_get_group_overview_marks_missing_uid_child_as_ineligible(tmp_path: Path) -> None:
    eligible_child = _build_child("sub1", uid="223456789")
    missing_uid_child = _build_child("sub2", uid="")
    service = _build_service(tmp_path, children=(eligible_child, missing_uid_child))

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        assert main_account.main_id == "group_a"
        return {"223456789": "sub1@example.com"}

    async def fake_fetch_funding_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        if credentials.api_key == "main-k":
            return [{"asset": "USDT", "free": "100", "locked": "0", "freeze": "0", "withdrawing": "0", "total": "100"}]
        return [{"asset": "USDT", "free": "12.5", "locked": "0", "freeze": "0", "withdrawing": "0", "total": "12.5"}]

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_funding_assets = fake_fetch_funding_assets  # type: ignore[method-assign]
    try:
        overview = await service.get_group_overview("group_a")
    finally:
        await service.close()

    assert overview["available"] is True
    assert overview["assets"] == ["USDT"]
    eligible = next(item for item in overview["children"] if item["account_id"] == "group_a.sub1")
    ineligible = next(item for item in overview["children"] if item["account_id"] == "group_a.sub2")
    assert eligible["eligible"] is True
    assert ineligible["eligible"] is False
    assert "UID" in ineligible["reason"]


@pytest.mark.asyncio
async def test_get_group_overview_preserves_missing_uid_reason_when_balance_lookup_fails(tmp_path: Path) -> None:
    missing_uid_child = _build_child("sub1", uid="")
    service = _build_service(tmp_path, children=(missing_uid_child,))

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        return {}

    async def fake_fetch_funding_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        raise FundingTransferError("boom")

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_funding_assets = fake_fetch_funding_assets  # type: ignore[method-assign]
    try:
        overview = await service.get_group_overview("group_a")
    finally:
        await service.close()

    child = overview["children"][0]
    assert child["eligible"] is False
    assert child["reason"] == "未配置子账号 UID"


@pytest.mark.asyncio
async def test_distribute_runs_main_and_child_transfer_steps_in_order(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))
    calls: list[tuple[str, str, dict[str, str]]] = []

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        return {"223456789": "sub1@example.com"}

    async def fake_fetch_funding_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        if credentials.api_key == "main-k":
            return [{"asset": "USDT", "free": "120", "locked": "0", "freeze": "0", "withdrawing": "0", "total": "120"}]
        return [{"asset": "USDT", "free": "6", "locked": "0", "freeze": "0", "withdrawing": "0", "total": "6"}]

    async def fake_signed_request(credentials: BinanceCredentials, method: str, path: str, params: dict[str, str] | None = None):
        calls.append((credentials.api_key, path, dict(params or {})))
        return {"success": True}

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_funding_assets = fake_fetch_funding_assets  # type: ignore[method-assign]
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
    assert result["results"][0]["success"] is True
    assert calls[:3] == [
        ("main-k", "/sapi/v1/asset/transfer", {"type": "FUNDING_MAIN", "asset": "USDT", "amount": "12.5"}),
        (
            "main-k",
            "/sapi/v1/sub-account/universalTransfer",
            {"toEmail": "sub1@example.com", "fromAccountType": "SPOT", "toAccountType": "SPOT", "asset": "USDT", "amount": "12.5"},
        ),
        ("child-k", "/sapi/v1/asset/transfer", {"type": "MAIN_FUNDING", "asset": "USDT", "amount": "12.5"}),
    ]


@pytest.mark.asyncio
async def test_collect_uses_available_funding_balance_from_selected_child(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,))
    calls: list[tuple[str, str, dict[str, str]]] = []

    async def fake_get_sub_account_email_map(main_account: MainAccountConfig) -> dict[str, str]:
        return {"223456789": "sub1@example.com"}

    async def fake_fetch_funding_assets(credentials: BinanceCredentials) -> list[dict[str, str]]:
        if credentials.api_key == "child-k":
            return [{"asset": "USDT", "free": "8.75", "locked": "0", "freeze": "0", "withdrawing": "0", "total": "8.75"}]
        return [{"asset": "USDT", "free": "100", "locked": "0", "freeze": "0", "withdrawing": "0", "total": "100"}]

    async def fake_signed_request(credentials: BinanceCredentials, method: str, path: str, params: dict[str, str] | None = None):
        calls.append((credentials.api_key, path, dict(params or {})))
        return {"success": True}

    service._get_sub_account_email_map = fake_get_sub_account_email_map  # type: ignore[method-assign]
    service._fetch_funding_assets = fake_fetch_funding_assets  # type: ignore[method-assign]
    service._signed_request = fake_signed_request  # type: ignore[method-assign]
    try:
        result = await service.collect("group_a", asset="usdt", account_ids=["group_a.sub1"])
    finally:
        await service.close()

    assert result["direction"] == "collect"
    assert result["results"][0]["success"] is True
    assert result["results"][0]["amount"] == "8.75"
    assert calls[:3] == [
        ("child-k", "/sapi/v1/asset/transfer", {"type": "FUNDING_MAIN", "asset": "USDT", "amount": "8.75"}),
        (
            "main-k",
            "/sapi/v1/sub-account/universalTransfer",
            {"fromEmail": "sub1@example.com", "fromAccountType": "SPOT", "toAccountType": "SPOT", "asset": "USDT", "amount": "8.75"},
        ),
        ("main-k", "/sapi/v1/asset/transfer", {"type": "MAIN_FUNDING", "asset": "USDT", "amount": "8.75"}),
    ]


@pytest.mark.asyncio
async def test_distribute_rejects_when_group_has_no_transfer_api(tmp_path: Path) -> None:
    child = _build_child("sub1", uid="223456789")
    service = _build_service(tmp_path, children=(child,), with_main_transfer=False)
    try:
        with pytest.raises(FundingTransferError, match="主账号归集 API"):
            await service.distribute("group_a", asset="USDT", transfers=[{"account_id": "group_a.sub1", "amount": "1"}])
    finally:
        await service.close()
