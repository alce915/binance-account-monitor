from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from time import perf_counter

import httpx
import pytest

from monitor_app.binance import BinanceMonitorGateway
from monitor_app.config import MonitorAccountConfig, Settings
from monitor_app.history_store import HistoryEvent


@pytest.mark.asyncio
async def test_get_unified_account_snapshot_aggregates_distribution_and_assets(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, monitor_history_db_path=tmp_path / "history.db")
    gateway = BinanceMonitorGateway(
        settings,
        MonitorAccountConfig(
            account_id="group_a.sub1",
            child_account_id="sub1",
            child_account_name="Sub One",
            main_account_id="group_a",
            main_account_name="Group A",
            api_key="k",
            api_secret="s",
        ),
    )

    async def fake_signed_request(path: str, params: dict | None = None, *, timeout_s: float | None = None):
        if path == "/papi/v1/account":
            return {
                "accountStatus": "NORMAL",
                "accountEquity": "1500.5",
                "accountInitialMargin": "210.2",
                "totalAvailableBalance": "1001.1",
            }
        if path == "/papi/v1/um/account":
            return {
                "assets": [
                    {
                        "asset": "USDT",
                        "crossWalletBalance": "1200",
                        "crossUnPnl": "12.5",
                        "availableBalance": "1001.1",
                        "initialMargin": "210.2",
                        "maintMargin": "20",
                        "marginBalance": "1212.5",
                        "maxWithdrawAmount": "1000",
                    }
                ],
                "positions": [
                    {
                        "symbol": "BTCUSDT",
                        "positionSide": "LONG",
                        "positionAmt": "0.02",
                        "entryPrice": "80000",
                        "markPrice": "0",
                        "unrealizedProfit": "10.0",
                        "notional": "1610",
                        "leverage": "10",
                        "liquidationPrice": "70000",
                    },
                    {
                        "symbol": "ETHUSDT",
                        "positionSide": "SHORT",
                        "positionAmt": "-0.5",
                        "entryPrice": "2000",
                        "markPrice": "0",
                        "unrealizedProfit": "2.5",
                        "notional": "997.5",
                        "leverage": "5",
                        "liquidationPrice": "2500",
                    },
                ],
            }
        if path == "/papi/v1/um/income":
            return [
                {
                    "incomeType": "COMMISSION",
                    "income": "-1.25",
                    "asset": "USDT",
                    "time": 1774404965000,
                    "symbol": "BTCUSDT",
                },
                {
                    "incomeType": "FUNDING_FEE",
                    "income": "0.3",
                    "asset": "USDT",
                    "time": 1774404966000,
                    "symbol": "BTCUSDT",
                },
            ]
        raise AssertionError(path)

    async def fake_signed_request_sapi(path: str, params: dict | None = None, *, timeout_s: float | None = None):
        if path == "/sapi/v1/asset/assetDividend":
            return {
                "total": 1,
                "rows": [
                    {
                        "asset": "RWUSD",
                        "amount": "3.5",
                        "divTime": 1774404965000,
                        "enInfo": "RWUSD rewards distribution",
                    }
                ],
            }
        if path == "/api/v3/account":
            return {
                "balances": [
                    {"asset": "RWUSD", "free": "0.64438356", "locked": "0"},
                    {"asset": "BNB", "free": "1.25", "locked": "0"},
                ]
            }
        raise AssertionError(path)

    async def fake_public_request_market(path: str, params: dict | None = None, *, timeout_s: float | None = None):
        assert path == "/fapi/v1/premiumIndex"
        assert not params
        return [
            {"symbol": "BTCUSDT", "markPrice": "80500"},
            {"symbol": "ETHUSDT", "markPrice": "1995"},
        ]

    gateway._signed_request = fake_signed_request  # type: ignore[method-assign]
    gateway._signed_request_sapi = fake_signed_request_sapi  # type: ignore[method-assign]
    gateway._public_request_market = fake_public_request_market  # type: ignore[method-assign]
    try:
        snapshot = await gateway.get_unified_account_snapshot(history_window_days=5)
    finally:
        await gateway.close()

    assert snapshot["status"] == "ok"
    assert snapshot["account_id"] == "group_a.sub1"
    assert snapshot["totals"]["equity"] == Decimal("1500.5")
    assert snapshot["totals"]["unrealized_pnl"] == Decimal("12.5")
    assert snapshot["totals"]["total_income"] == Decimal("-0.95")
    assert snapshot["totals"]["total_commission"] == Decimal("-1.25")
    assert snapshot["totals"]["total_distribution"] == Decimal("3.5")
    assert snapshot["totals"]["distribution_apy_7d"].quantize(Decimal("0.00000001")) == Decimal("0.12162612")
    assert snapshot["income_summary"]["records"] == 2
    assert snapshot["income_summary"]["total_commission"] == Decimal("-1.25")
    assert snapshot["income_summary"]["by_type"]["COMMISSION"] == Decimal("-1.25")
    assert snapshot["distribution_summary"]["records"] == 1
    assert snapshot["distribution_summary"]["window_days"] == 7
    assert snapshot["distribution_summary"]["by_type"]["RWUSD rewards distribution"] == Decimal("3.5")
    assert snapshot["distribution_profit_summary"]["today"]["label"] == "今日收益丨收益率"
    assert "all" in snapshot["distribution_profit_summary"]
    assert snapshot["totals"]["total_interest"] == Decimal("0")
    assert snapshot["interest_summary"]["records"] == 0
    assert len(snapshot["positions"]) == 2
    assert snapshot["positions"][0]["mark_price"] == Decimal("80500")
    assert snapshot["positions"][1]["mark_price"] == Decimal("1995")
    rwusd_asset = next(item for item in snapshot["assets"] if item["asset"] == "RWUSD")
    assert rwusd_asset["wallet_balance"] == Decimal("0.64438356")
    bnb_asset = next(item for item in snapshot["assets"] if item["asset"] == "BNB")
    assert bnb_asset["wallet_balance"] == Decimal("1.25")


@pytest.mark.asyncio
async def test_get_unified_account_snapshot_uses_core_and_secondary_retry_budgets(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        monitor_history_db_path=tmp_path / "history.db",
        binance_core_retry_attempts=5,
        binance_secondary_retry_attempts=3,
        binance_core_timeout_ms=1,
        binance_secondary_timeout_ms=1,
    )
    gateway = BinanceMonitorGateway(
        settings,
        MonitorAccountConfig(
            account_id="group_a.sub1",
            child_account_id="sub1",
            child_account_name="Sub One",
            main_account_id="group_a",
            main_account_name="Group A",
            api_key="k",
            api_secret="s",
        ),
    )

    core_attempts = {"/papi/v1/account": 0, "/papi/v1/um/account": 0}
    distribution_attempts = 0
    spot_attempts = 0
    premium_index_attempts = 0

    async def fake_signed_request(path: str, params: dict | None = None, *, timeout_s: float | None = None):
        if path in core_attempts:
            core_attempts[path] += 1
            if core_attempts[path] < 3:
                raise httpx.ReadTimeout("temporary", request=httpx.Request("GET", f"https://example.com{path}"))
        if path == "/papi/v1/account":
            return {
                "accountStatus": "NORMAL",
                "accountEquity": "1000",
                "accountInitialMargin": "100",
                "totalAvailableBalance": "800",
            }
        if path == "/papi/v1/um/account":
            return {
                "assets": [],
                "positions": [
                    {
                        "symbol": "BTCUSDT",
                        "positionSide": "LONG",
                        "positionAmt": "0.01",
                        "entryPrice": "80000",
                        "markPrice": "0",
                        "unrealizedProfit": "5",
                        "notional": "805",
                        "leverage": "10",
                        "liquidationPrice": "70000",
                    }
                ],
            }
        raise AssertionError(path)

    async def fake_signed_request_sapi(path: str, params: dict | None = None, *, timeout_s: float | None = None):
        nonlocal distribution_attempts, spot_attempts
        if path == "/sapi/v1/asset/assetDividend":
            distribution_attempts += 1
            if distribution_attempts < 3:
                raise httpx.ReadTimeout("temporary", request=httpx.Request("GET", f"https://example.com{path}"))
            return {"rows": []}
        if path == "/api/v3/account":
            spot_attempts += 1
            if spot_attempts < 3:
                raise httpx.ReadTimeout("temporary", request=httpx.Request("GET", f"https://example.com{path}"))
            return {"balances": []}
        raise AssertionError(path)

    async def fake_public_request_market(path: str, params: dict | None = None, *, timeout_s: float | None = None):
        nonlocal premium_index_attempts
        premium_index_attempts += 1
        assert path == "/fapi/v1/premiumIndex"
        assert not params
        if premium_index_attempts < 3:
            raise httpx.ReadTimeout("temporary", request=httpx.Request("GET", "https://example.com/fapi/v1/premiumIndex"))
        return [{"symbol": "BTCUSDT", "markPrice": "80500"}]

    gateway._signed_request = fake_signed_request  # type: ignore[method-assign]
    gateway._signed_request_sapi = fake_signed_request_sapi  # type: ignore[method-assign]
    gateway._public_request_market = fake_public_request_market  # type: ignore[method-assign]
    try:
        snapshot = await gateway.get_unified_account_snapshot(history_window_days=5)
    finally:
        await gateway.close()

    assert core_attempts["/papi/v1/account"] == 3
    assert core_attempts["/papi/v1/um/account"] == 3
    assert distribution_attempts >= 3
    assert spot_attempts == 3
    assert premium_index_attempts == 3
    assert snapshot["totals"]["total_distribution"] == Decimal("0")
    assert snapshot["totals"]["total_income"] == Decimal("0")
    assert snapshot["totals"]["total_interest"] == Decimal("0")
    assert snapshot["distribution_profit_summary"]["all"]["complete"] is False
    assert snapshot["positions"][0]["mark_price"] == Decimal("80500")


@pytest.mark.asyncio
async def test_get_unified_account_snapshot_uses_cached_commission_when_income_refresh_is_slow(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, monitor_history_db_path=tmp_path / "history.db")
    gateway = BinanceMonitorGateway(
        settings,
        MonitorAccountConfig(
            account_id="group_a.sub1",
            child_account_id="sub1",
            child_account_name="Sub One",
            main_account_id="group_a",
            main_account_name="Group A",
            api_key="k",
            api_secret="s",
        ),
    )

    await gateway._history_store.record_history_batch(
        "group_a.sub1",
        "income",
        [
            HistoryEvent(
                source="income",
                event_time_ms=1774404965000,
                unique_key="cached-commission",
                asset="USDT",
                amount=Decimal("-1.25"),
                event_type="COMMISSION",
                payload={},
            )
        ],
        last_successful_end_time=1774404965000,
        retain_after_ms=0,
    )

    async def fake_signed_request(path: str, params: dict | None = None, *, timeout_s: float | None = None):
        if path == "/papi/v1/account":
            return {
                "accountStatus": "NORMAL",
                "accountEquity": "1500.5",
                "accountInitialMargin": "210.2",
                "totalAvailableBalance": "1001.1",
            }
        if path == "/papi/v1/um/account":
            return {"assets": [], "positions": []}
        if path == "/papi/v1/um/income":
            await asyncio.sleep(0.3)
            return [
                {
                    "incomeType": "COMMISSION",
                    "income": "-3.00",
                    "asset": "USDT",
                    "time": 1774404966000,
                    "symbol": "BTCUSDT",
                }
            ]
        raise AssertionError(path)

    async def fake_signed_request_sapi(path: str, params: dict | None = None, *, timeout_s: float | None = None):
        if path == "/sapi/v1/asset/assetDividend":
            return {"rows": []}
        if path == "/api/v3/account":
            return {"balances": []}
        raise AssertionError(path)

    async def fake_public_request_market(path: str, params: dict | None = None, *, timeout_s: float | None = None):
        assert path == "/fapi/v1/premiumIndex"
        return []

    gateway._signed_request = fake_signed_request  # type: ignore[method-assign]
    gateway._signed_request_sapi = fake_signed_request_sapi  # type: ignore[method-assign]
    gateway._public_request_market = fake_public_request_market  # type: ignore[method-assign]
    try:
        started_at = perf_counter()
        snapshot = await gateway.get_unified_account_snapshot(history_window_days=7)
        duration_s = perf_counter() - started_at
    finally:
        await gateway.close()

    assert duration_s < 0.25
    assert snapshot["totals"]["total_commission"] == Decimal("-1.25")
    assert snapshot["income_summary"]["total_commission"] == Decimal("-1.25")


@pytest.mark.asyncio
async def test_distribution_backfill_marks_complete_when_no_older_records_exist(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, monitor_history_db_path=tmp_path / "history.db")
    gateway = BinanceMonitorGateway(
        settings,
        MonitorAccountConfig(
            account_id="group_a.sub1",
            child_account_id="sub1",
            child_account_name="Sub One",
            main_account_id="group_a",
            main_account_name="Group A",
            api_key="k",
            api_secret="s",
        ),
    )
    earliest_ms = 1774404965000

    await gateway._history_store.record_history_batch(
        "group_a.sub1",
        "distribution",
        [
            HistoryEvent(
                source="distribution",
                event_time_ms=earliest_ms,
                unique_key="distribution-existing",
                asset="RWUSD",
                amount=Decimal("0.5"),
                event_type="RWUSD rewards distribution",
                payload={},
            )
        ],
        last_successful_end_time=earliest_ms,
        retain_after_ms=None,
        update_fetch_state=False,
    )

    async def fake_signed_request_sapi(path: str, params: dict | None = None, *, timeout_s: float | None = None):
        assert path == "/sapi/v1/asset/assetDividend"
        return {
            "rows": [
                {
                    "asset": "RWUSD",
                    "amount": "0.5",
                    "divTime": earliest_ms,
                    "enInfo": "RWUSD rewards distribution",
                }
            ]
        }

    gateway._signed_request_sapi = fake_signed_request_sapi  # type: ignore[method-assign]
    try:
        await gateway._run_distribution_backfill(backfill_limit=100)
        completed = await gateway._history_store.is_distribution_backfill_complete("group_a.sub1")
    finally:
        await gateway.close()

    assert completed is True
