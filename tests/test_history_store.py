from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from monitor_app.history_store import HistoryEvent, MonitorHistoryStore


@pytest.mark.asyncio
async def test_history_store_deduplicates_and_summarizes_recent_window(tmp_path: Path) -> None:
    store = MonitorHistoryStore(tmp_path / "history.db")
    now_ms = int(datetime.now(UTC).timestamp() * 1000)
    old_ms = int((datetime.now(UTC) - timedelta(days=10)).timestamp() * 1000)

    try:
        await store.record_history_batch(
            "group_a.sub1",
            "income",
            [
                HistoryEvent(
                    source="income",
                    event_time_ms=now_ms,
                    unique_key="income-1",
                    asset="USDT",
                    amount=Decimal("5.2"),
                    event_type="REALIZED_PNL",
                    payload={"income": "5.2"},
                ),
                HistoryEvent(
                    source="income",
                    event_time_ms=now_ms,
                    unique_key="income-1",
                    asset="USDT",
                    amount=Decimal("5.2"),
                    event_type="REALIZED_PNL",
                    payload={"income": "5.2"},
                ),
                HistoryEvent(
                    source="income",
                    event_time_ms=old_ms,
                    unique_key="income-old",
                    asset="USDT",
                    amount=Decimal("9.9"),
                    event_type="FUNDING_FEE",
                    payload={"income": "9.9"},
                ),
            ],
            last_successful_end_time=now_ms,
            retain_after_ms=int((datetime.now(UTC) - timedelta(days=7)).timestamp() * 1000),
        )

        summary = await store.summarize_income("group_a.sub1", 7)
        last_successful_end_time = await store.get_last_successful_end_time("group_a.sub1", "income")
    finally:
        await store.close()

    assert last_successful_end_time == now_ms
    assert summary["records"] == 1
    assert summary["total_income"] == Decimal("5.2")
    assert summary["by_type"]["REALIZED_PNL"] == Decimal("5.2")


@pytest.mark.asyncio
async def test_history_store_persists_mark_prices(tmp_path: Path) -> None:
    store = MonitorHistoryStore(tmp_path / "history.db")
    try:
        await store.save_mark_prices({"BTCUSDT": Decimal("80000.12")}, updated_at_ms=1700000000000)
        prices = await store.get_mark_prices(["BTCUSDT", "ETHUSDT"])
    finally:
        await store.close()

    assert prices == {"BTCUSDT": Decimal("80000.12")}


@pytest.mark.asyncio
async def test_history_store_summarizes_distribution_periods_and_backfill_state(tmp_path: Path) -> None:
    store = MonitorHistoryStore(tmp_path / "history.db")
    now = datetime.now(UTC)
    today_ms = int(now.timestamp() * 1000)
    week_ms = int((now - timedelta(days=3)).timestamp() * 1000)
    month_ms = int((now - timedelta(days=20)).timestamp() * 1000)
    year_ms = int((now - timedelta(days=90)).timestamp() * 1000)

    try:
        await store.record_history_batch(
            "group_a.sub1",
            "distribution",
            [
                HistoryEvent(
                    source="distribution",
                    event_time_ms=today_ms,
                    unique_key="distribution-today",
                    asset="RWUSD",
                    amount=Decimal("0.5"),
                    event_type="RWUSD rewards distribution",
                    payload={"amount": "0.5"},
                ),
                HistoryEvent(
                    source="distribution",
                    event_time_ms=week_ms,
                    unique_key="distribution-week",
                    asset="RWUSD",
                    amount=Decimal("0.7"),
                    event_type="RWUSD rewards distribution",
                    payload={"amount": "0.7"},
                ),
                HistoryEvent(
                    source="distribution",
                    event_time_ms=month_ms,
                    unique_key="distribution-month",
                    asset="RWUSD",
                    amount=Decimal("1.1"),
                    event_type="RWUSD rewards distribution",
                    payload={"amount": "1.1"},
                ),
                HistoryEvent(
                    source="distribution",
                    event_time_ms=year_ms,
                    unique_key="distribution-year",
                    asset="RWUSD",
                    amount=Decimal("2.4"),
                    event_type="RWUSD rewards distribution",
                    payload={"amount": "2.4"},
                ),
            ],
            last_successful_end_time=today_ms,
            retain_after_ms=None,
        )
        periods = await store.summarize_distribution_periods(
            "group_a.sub1",
            {
                "today": today_ms - 1,
                "week": week_ms - 1,
                "month": month_ms - 1,
                "year": year_ms - 1,
                "all": None,
            },
        )
        await store.set_distribution_backfill_complete(
            "group_a.sub1",
            completed=True,
            updated_at_ms=today_ms,
        )
        backfill_complete = await store.is_distribution_backfill_complete("group_a.sub1")
        earliest_event_time_ms = await store.get_earliest_event_time_ms("group_a.sub1", "distribution")
    finally:
        await store.close()

    assert periods["amounts"]["today"] == Decimal("0.5")
    assert periods["amounts"]["week"] == Decimal("1.2")
    assert periods["amounts"]["month"] == Decimal("2.3")
    assert periods["amounts"]["year"] == Decimal("4.7")
    assert periods["amounts"]["all"] == Decimal("4.7")
    assert periods["earliest_event_time_ms"] == year_ms
    assert backfill_complete is True
    assert earliest_event_time_ms == year_ms
