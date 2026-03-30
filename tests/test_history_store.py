from __future__ import annotations

import json
import sqlite3
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


@pytest.mark.asyncio
async def test_history_store_invalidates_cached_distribution_summaries_on_new_batch(tmp_path: Path) -> None:
    store = MonitorHistoryStore(tmp_path / "history.db")
    now = datetime.now(UTC)
    first_ms = int((now - timedelta(days=1)).timestamp() * 1000)
    second_ms = int(now.timestamp() * 1000)

    try:
        await store.record_history_batch(
            "group_a.sub1",
            "distribution",
            [
                HistoryEvent(
                    source="distribution",
                    event_time_ms=first_ms,
                    unique_key="distribution-first",
                    asset="RWUSD",
                    amount=Decimal("1.2"),
                    event_type="RWUSD rewards distribution",
                    payload={"amount": "1.2"},
                ),
            ],
            last_successful_end_time=first_ms,
            retain_after_ms=None,
        )

        first_summary = await store.summarize_distribution("group_a.sub1", 7)
        first_periods = await store.summarize_distribution_periods(
            "group_a.sub1",
            {"today": second_ms - 1, "all": None},
        )

        await store.record_history_batch(
            "group_a.sub1",
            "distribution",
            [
                HistoryEvent(
                    source="distribution",
                    event_time_ms=second_ms,
                    unique_key="distribution-second",
                    asset="RWUSD",
                    amount=Decimal("0.8"),
                    event_type="RWUSD rewards distribution",
                    payload={"amount": "0.8"},
                ),
            ],
            last_successful_end_time=second_ms,
            retain_after_ms=None,
        )

        second_summary = await store.summarize_distribution("group_a.sub1", 7)
        second_periods = await store.summarize_distribution_periods(
            "group_a.sub1",
            {"today": second_ms - 1, "all": None},
        )
    finally:
        await store.close()

    assert first_summary["total_distribution"] == Decimal("1.2")
    assert first_periods["amounts"]["today"] == Decimal("0")
    assert first_periods["amounts"]["all"] == Decimal("1.2")
    assert second_summary["total_distribution"] == Decimal("2.0")
    assert second_periods["amounts"]["today"] == Decimal("0.8")
    assert second_periods["amounts"]["all"] == Decimal("2.0")


@pytest.mark.asyncio
async def test_history_store_keeps_cache_version_when_only_fetch_state_changes(tmp_path: Path) -> None:
    store = MonitorHistoryStore(tmp_path / "history.db")
    now_ms = int(datetime.now(UTC).timestamp() * 1000)

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
                    amount=Decimal("1.5"),
                    event_type="COMMISSION",
                    payload={"income": "1.5"},
                ),
            ],
            last_successful_end_time=now_ms,
            retain_after_ms=None,
        )

        _ = await store.summarize_income("group_a.sub1", 7)
        initial_version = store._source_versions.get(("group_a.sub1", "income"), 0)

        await store.record_history_batch(
            "group_a.sub1",
            "income",
            [],
            last_successful_end_time=now_ms + 1000,
            retain_after_ms=None,
        )
        later_version = store._source_versions.get(("group_a.sub1", "income"), 0)
    finally:
        await store.close()

    assert initial_version == 1
    assert later_version == initial_version


@pytest.mark.asyncio
async def test_history_store_tracks_source_success_and_failure_state(tmp_path: Path) -> None:
    store = MonitorHistoryStore(tmp_path / "history.db")
    try:
        await store.record_source_failure(
            "group_a.sub1",
            "distribution",
            error_summary="temporary network error email foo@example.com token=abc uid 223456789",
            failed_at_ms=1700000001000,
        )
        failed = await store.get_source_status("group_a.sub1", "distribution")
        await store.record_source_success(
            "group_a.sub1",
            "distribution",
            last_successful_end_time=1700000002000,
            success_at_ms=1700000003000,
        )
        succeeded = await store.get_source_status("group_a.sub1", "distribution")
    finally:
        await store.close()

    assert failed["consecutive_failures"] == 1
    assert failed["last_failed_at_ms"] == 1700000001000
    assert failed["last_error_summary"] == "temporary network error email [redacted-email] token=[redacted] UID 2234***89"
    assert succeeded["consecutive_failures"] == 0
    assert succeeded["last_failed_at_ms"] is None
    assert succeeded["last_successful_end_time"] == 1700000002000


@pytest.mark.asyncio
async def test_history_store_persists_minimized_payload_json(tmp_path: Path) -> None:
    db_path = tmp_path / "history.db"
    store = MonitorHistoryStore(db_path)
    now_ms = int(datetime.now(UTC).timestamp() * 1000)

    try:
        await store.record_history_batch(
            "group_a.sub1",
            "income",
            [
                HistoryEvent(
                    source="income",
                    event_time_ms=now_ms,
                    unique_key="income-1234567890",
                    asset="USDT",
                    amount=Decimal("5.2"),
                    event_type="COMMISSION",
                    payload={
                        "income": "5.2",
                        "symbol": "BTCUSDT",
                        "email": "foo@example.com",
                        "token": "abc",
                        "orderId": "1234567890123",
                    },
                ),
            ],
            last_successful_end_time=now_ms,
            retain_after_ms=None,
        )
    finally:
        await store.close()

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute("SELECT payload_json FROM history_events").fetchone()
    finally:
        conn.close()

    payload = json.loads(row[0])
    assert payload["source"] == "income"
    assert payload["asset"] == "USDT"
    assert payload["amount"] == "5.2"
    assert payload["event_type"] == "COMMISSION"
    assert payload["symbol"] == "BTCUSDT"
    assert "email" not in payload
    assert "token" not in payload
    assert payload["reference"].startswith("1234")
    assert "*" in payload["reference"]


@pytest.mark.asyncio
async def test_history_store_defers_security_migration_until_async_usage(tmp_path: Path) -> None:
    db_path = tmp_path / "history.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE history_events (
                account_id TEXT NOT NULL,
                source TEXT NOT NULL,
                event_time_ms INTEGER NOT NULL,
                unique_key TEXT NOT NULL,
                asset TEXT NOT NULL,
                amount TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (account_id, source, unique_key)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE fetch_state (
                account_id TEXT NOT NULL,
                source TEXT NOT NULL,
                last_successful_end_time INTEGER NOT NULL,
                PRIMARY KEY (account_id, source)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE mark_prices (
                symbol TEXT PRIMARY KEY,
                mark_price TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE distribution_backfill_state (
                account_id TEXT PRIMARY KEY,
                completed INTEGER NOT NULL DEFAULT 0,
                updated_at_ms INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE history_source_status (
                account_id TEXT NOT NULL,
                source TEXT NOT NULL,
                last_success_at_ms INTEGER,
                last_successful_end_time INTEGER,
                last_failed_at_ms INTEGER,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                last_error_summary TEXT,
                PRIMARY KEY (account_id, source)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO history_events (account_id, source, event_time_ms, unique_key, asset, amount, event_type, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "group_a.sub1",
                "income",
                1700000000000,
                "income-1234567890",
                "USDT",
                "7.8",
                "COMMISSION",
                json.dumps(
                    {
                        "symbol": "ETHUSDT",
                        "email": "foo@example.com",
                        "token": "abc",
                    }
                ),
            ),
        )
        conn.execute("PRAGMA user_version=0")
        conn.commit()
    finally:
        conn.close()

    store = MonitorHistoryStore(db_path)
    try:
        conn = sqlite3.connect(db_path)
        try:
            payload_row = conn.execute("SELECT payload_json FROM history_events").fetchone()
            version = conn.execute("PRAGMA user_version").fetchone()[0]
        finally:
            conn.close()
    finally:
        await store.close()

    payload = json.loads(payload_row[0])
    assert version == 0
    assert payload["email"] == "foo@example.com"
    assert payload["token"] == "abc"


@pytest.mark.asyncio
async def test_history_store_migrates_existing_payloads_and_error_summaries(tmp_path: Path) -> None:
    db_path = tmp_path / "history.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE history_events (
                account_id TEXT NOT NULL,
                source TEXT NOT NULL,
                event_time_ms INTEGER NOT NULL,
                unique_key TEXT NOT NULL,
                asset TEXT NOT NULL,
                amount TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (account_id, source, unique_key)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE fetch_state (
                account_id TEXT NOT NULL,
                source TEXT NOT NULL,
                last_successful_end_time INTEGER NOT NULL,
                PRIMARY KEY (account_id, source)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE mark_prices (
                symbol TEXT PRIMARY KEY,
                mark_price TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE distribution_backfill_state (
                account_id TEXT PRIMARY KEY,
                completed INTEGER NOT NULL DEFAULT 0,
                updated_at_ms INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE history_source_status (
                account_id TEXT NOT NULL,
                source TEXT NOT NULL,
                last_success_at_ms INTEGER,
                last_successful_end_time INTEGER,
                last_failed_at_ms INTEGER,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                last_error_summary TEXT,
                PRIMARY KEY (account_id, source)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO history_events (account_id, source, event_time_ms, unique_key, asset, amount, event_type, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "group_a.sub1",
                "income",
                1700000000000,
                "income-1234567890",
                "USDT",
                "7.8",
                "COMMISSION",
                json.dumps(
                    {
                        "symbol": "ETHUSDT",
                        "email": "foo@example.com",
                        "token": "abc",
                        "tranId": "1234567890123",
                    }
                ),
            ),
        )
        conn.execute(
            """
            INSERT INTO history_source_status (account_id, source, last_failed_at_ms, consecutive_failures, last_error_summary)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                "group_a.sub1",
                "income",
                1700000001000,
                1,
                "token=abc email foo@example.com uid 223456789",
            ),
        )
        conn.execute("PRAGMA user_version=0")
        conn.commit()
    finally:
        conn.close()

    store = MonitorHistoryStore(db_path)
    try:
        status = await store.get_source_status("group_a.sub1", "income")
    finally:
        await store.close()

    conn = sqlite3.connect(db_path)
    try:
        payload_row = conn.execute("SELECT payload_json FROM history_events").fetchone()
        version = conn.execute("PRAGMA user_version").fetchone()[0]
    finally:
        conn.close()

    payload = json.loads(payload_row[0])
    assert version == 2
    assert payload["symbol"] == "ETHUSDT"
    assert "email" not in payload
    assert "token" not in payload
    assert payload["reference"].startswith("1234")
    assert "*" in payload["reference"]
    assert status["last_error_summary"] == "token=[redacted] email [redacted-email] UID 2234***89"
