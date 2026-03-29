from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from monitor_app.security import minimize_history_payload, sanitize_error_summary

logger = logging.getLogger("uvicorn.error")
HISTORY_STORE_SCHEMA_VERSION = 2


@dataclass(frozen=True, slots=True)
class HistoryEvent:
    source: str
    event_time_ms: int
    unique_key: str
    asset: str
    amount: Decimal
    event_type: str
    payload: dict[str, Any]


class MonitorHistoryStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._source_versions: dict[tuple[str, str], int] = {}
        self._income_summary_cache: dict[tuple[str, int], tuple[int, dict[str, Any]]] = {}
        self._distribution_summary_cache: dict[tuple[str, int], tuple[int, dict[str, Any]]] = {}
        self._distribution_periods_cache: dict[tuple[str, tuple[tuple[str, int | None], ...]], tuple[int, dict[str, Any]]] = {}
        self._initialize()

    def _initialize(self) -> None:
        with self._conn:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS history_events (
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
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS fetch_state (
                    account_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    last_successful_end_time INTEGER NOT NULL,
                    PRIMARY KEY (account_id, source)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS mark_prices (
                    symbol TEXT PRIMARY KEY,
                    mark_price TEXT NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS distribution_backfill_state (
                    account_id TEXT PRIMARY KEY,
                    completed INTEGER NOT NULL DEFAULT 0,
                    updated_at_ms INTEGER NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS history_source_status (
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
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_history_events_account_source_time
                ON history_events (account_id, source, event_time_ms)
                """
            )
            current_version = int(self._conn.execute("PRAGMA user_version").fetchone()[0])
            if current_version < HISTORY_STORE_SCHEMA_VERSION:
                self._migrate_security_payloads(current_version)
                self._conn.execute(f"PRAGMA user_version={HISTORY_STORE_SCHEMA_VERSION}")

    def _migrate_security_payloads(self, from_version: int) -> None:
        updated_history_rows = 0
        updated_status_rows = 0
        history_rows = self._conn.execute(
            """
            SELECT rowid, source, event_time_ms, unique_key, asset, amount, event_type, payload_json
            FROM history_events
            """
        ).fetchall()
        for row in history_rows:
            payload = self._load_payload_json(row["payload_json"])
            minimized_payload = minimize_history_payload(
                source=row["source"],
                event_time_ms=int(row["event_time_ms"]),
                unique_key=str(row["unique_key"]),
                asset=str(row["asset"]),
                amount=str(row["amount"]),
                event_type=str(row["event_type"]),
                payload=payload,
            )
            minimized_payload_json = json.dumps(minimized_payload, ensure_ascii=False, sort_keys=True)
            if minimized_payload_json != row["payload_json"]:
                self._conn.execute(
                    "UPDATE history_events SET payload_json = ? WHERE rowid = ?",
                    (minimized_payload_json, row["rowid"]),
                )
                updated_history_rows += 1

        status_rows = self._conn.execute(
            "SELECT rowid, last_error_summary FROM history_source_status WHERE last_error_summary IS NOT NULL"
        ).fetchall()
        for row in status_rows:
            sanitized_summary = sanitize_error_summary(row["last_error_summary"], fallback="History source failed")
            if sanitized_summary != row["last_error_summary"]:
                self._conn.execute(
                    "UPDATE history_source_status SET last_error_summary = ? WHERE rowid = ?",
                    (sanitized_summary, row["rowid"]),
                )
                updated_status_rows += 1

        logger.info(
            "History store security migration applied from_version=%s updated_history_rows=%s updated_status_rows=%s",
            from_version,
            updated_history_rows,
            updated_status_rows,
        )

    def _load_payload_json(self, payload_json: str) -> dict[str, Any]:
        try:
            payload = json.loads(payload_json)
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    async def close(self) -> None:
        async with self._lock:
            self._conn.close()

    async def get_last_successful_end_time(self, account_id: str, source: str) -> int | None:
        async with self._lock:
            row = self._conn.execute(
                """
                SELECT last_successful_end_time
                FROM fetch_state
                WHERE account_id = ? AND source = ?
                """,
                (account_id, source),
            ).fetchone()
        if row is None:
            return None
        return int(row["last_successful_end_time"])

    async def record_history_batch(
        self,
        account_id: str,
        source: str,
        events: list[HistoryEvent],
        *,
        last_successful_end_time: int | None,
        retain_after_ms: int | None,
        update_fetch_state: bool = True,
    ) -> dict[str, int | bool]:
        history_rows_changed = False
        inserted_count = 0
        trimmed_count = 0
        async with self._lock:
            with self._conn:
                if events:
                    before_changes = self._conn.total_changes
                    self._conn.executemany(
                        """
                        INSERT OR IGNORE INTO history_events (
                            account_id,
                            source,
                            event_time_ms,
                            unique_key,
                            asset,
                            amount,
                            event_type,
                            payload_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            (
                                account_id,
                                source,
                                event.event_time_ms,
                                event.unique_key,
                                event.asset,
                                str(event.amount),
                                event.event_type,
                                json.dumps(
                                    minimize_history_payload(
                                        source=source,
                                        event_time_ms=event.event_time_ms,
                                        unique_key=event.unique_key,
                                        asset=event.asset,
                                        amount=event.amount,
                                        event_type=event.event_type,
                                        payload=event.payload,
                                    ),
                                    ensure_ascii=False,
                                    sort_keys=True,
                                ),
                            )
                            for event in events
                        ],
                    )
                    inserted_count = self._conn.total_changes - before_changes
                    history_rows_changed = history_rows_changed or inserted_count > 0
                if update_fetch_state and last_successful_end_time is not None:
                    self._conn.execute(
                        """
                        INSERT INTO fetch_state (account_id, source, last_successful_end_time)
                        VALUES (?, ?, ?)
                        ON CONFLICT(account_id, source)
                        DO UPDATE SET last_successful_end_time = excluded.last_successful_end_time
                        """,
                        (account_id, source, last_successful_end_time),
                    )
                if retain_after_ms is not None:
                    before_changes = self._conn.total_changes
                    self._conn.execute(
                        """
                        DELETE FROM history_events
                        WHERE account_id = ? AND source = ? AND event_time_ms < ?
                        """,
                        (account_id, source, retain_after_ms),
                    )
                    trimmed_count = self._conn.total_changes - before_changes
                    history_rows_changed = history_rows_changed or trimmed_count > 0
            if history_rows_changed:
                self._bump_source_version(account_id, source)
        return {
            "inserted_count": max(inserted_count, 0),
            "trimmed_count": max(trimmed_count, 0),
            "history_changed": history_rows_changed,
        }

    async def record_source_success(
        self,
        account_id: str,
        source: str,
        *,
        last_successful_end_time: int | None,
        success_at_ms: int | None = None,
    ) -> None:
        success_time = int(success_at_ms or datetime.now(UTC).timestamp() * 1000)
        async with self._lock:
            with self._conn:
                self._conn.execute(
                    """
                    INSERT INTO history_source_status (
                        account_id,
                        source,
                        last_success_at_ms,
                        last_successful_end_time,
                        last_failed_at_ms,
                        consecutive_failures,
                        last_error_summary
                    ) VALUES (?, ?, ?, ?, NULL, 0, NULL)
                    ON CONFLICT(account_id, source)
                    DO UPDATE SET
                        last_success_at_ms = excluded.last_success_at_ms,
                        last_successful_end_time = COALESCE(excluded.last_successful_end_time, history_source_status.last_successful_end_time),
                        last_failed_at_ms = NULL,
                        consecutive_failures = 0,
                        last_error_summary = NULL
                    """,
                    (account_id, source, success_time, last_successful_end_time),
                )

    async def record_source_failure(
        self,
        account_id: str,
        source: str,
        *,
        error_summary: str,
        failed_at_ms: int | None = None,
    ) -> None:
        failure_time = int(failed_at_ms or datetime.now(UTC).timestamp() * 1000)
        safe_error_summary = sanitize_error_summary(error_summary, fallback="History source failed")
        async with self._lock:
            with self._conn:
                self._conn.execute(
                    """
                    INSERT INTO history_source_status (
                        account_id,
                        source,
                        last_success_at_ms,
                        last_successful_end_time,
                        last_failed_at_ms,
                        consecutive_failures,
                        last_error_summary
                    ) VALUES (?, ?, NULL, NULL, ?, 1, ?)
                    ON CONFLICT(account_id, source)
                    DO UPDATE SET
                        last_failed_at_ms = excluded.last_failed_at_ms,
                        consecutive_failures = history_source_status.consecutive_failures + 1,
                        last_error_summary = excluded.last_error_summary
                    """,
                    (account_id, source, failure_time, safe_error_summary),
                )

    async def get_source_status(self, account_id: str, source: str) -> dict[str, Any]:
        async with self._lock:
            row = self._conn.execute(
                """
                SELECT
                    last_success_at_ms,
                    last_successful_end_time,
                    last_failed_at_ms,
                    consecutive_failures,
                    last_error_summary
                FROM history_source_status
                WHERE account_id = ? AND source = ?
                """,
                (account_id, source),
            ).fetchone()
        if row is None:
            return {
                "last_success_at_ms": None,
                "last_successful_end_time": None,
                "last_failed_at_ms": None,
                "consecutive_failures": 0,
                "last_error_summary": None,
            }
        return {
            "last_success_at_ms": int(row["last_success_at_ms"]) if row["last_success_at_ms"] is not None else None,
            "last_successful_end_time": int(row["last_successful_end_time"]) if row["last_successful_end_time"] is not None else None,
            "last_failed_at_ms": int(row["last_failed_at_ms"]) if row["last_failed_at_ms"] is not None else None,
            "consecutive_failures": int(row["consecutive_failures"] or 0),
            "last_error_summary": row["last_error_summary"],
        }

    async def summarize_income(self, account_id: str, history_window_days: int) -> dict[str, Any]:
        version = self._source_version(account_id, "income")
        cache_key = (account_id, history_window_days)
        cached = self._income_summary_cache.get(cache_key)
        if cached and cached[0] == version:
            return self._clone_summary(cached[1])

        rows = await self._read_history_rows(account_id, "income", history_window_days)
        by_type: dict[str, Decimal] = {}
        by_asset: dict[str, Decimal] = {}
        total_income = Decimal("0")

        for row in rows:
            amount = Decimal(str(row["amount"]))
            event_type = str(row["event_type"])
            asset = str(row["asset"])
            total_income += amount
            by_type[event_type] = by_type.get(event_type, Decimal("0")) + amount
            by_asset[asset] = by_asset.get(asset, Decimal("0")) + amount

        summary = {
            "window_days": history_window_days,
            "records": len(rows),
            "total_income": total_income,
            "by_type": dict(sorted(by_type.items())),
            "by_asset": dict(sorted(by_asset.items())),
        }
        self._income_summary_cache[cache_key] = (version, self._clone_summary(summary))
        return summary

    async def summarize_distribution(self, account_id: str, history_window_days: int) -> dict[str, Any]:
        version = self._source_version(account_id, "distribution")
        cache_key = (account_id, history_window_days)
        cached = self._distribution_summary_cache.get(cache_key)
        if cached and cached[0] == version:
            return self._clone_summary(cached[1])

        rows = await self._read_history_rows(account_id, "distribution", history_window_days)
        by_type: dict[str, Decimal] = {}
        by_asset: dict[str, Decimal] = {}
        total_distribution = Decimal("0")

        for row in rows:
            amount = Decimal(str(row["amount"]))
            event_type = str(row["event_type"])
            asset = str(row["asset"])
            total_distribution += amount
            by_type[event_type] = by_type.get(event_type, Decimal("0")) + amount
            by_asset[asset] = by_asset.get(asset, Decimal("0")) + amount

        summary = {
            "window_days": history_window_days,
            "records": len(rows),
            "total_distribution": total_distribution,
            "by_type": dict(sorted(by_type.items())),
            "by_asset": dict(sorted(by_asset.items())),
        }
        self._distribution_summary_cache[cache_key] = (version, self._clone_summary(summary))
        return summary

    async def summarize_distribution_periods(
        self,
        account_id: str,
        period_starts_ms: dict[str, int | None],
    ) -> dict[str, Any]:
        version = self._source_version(account_id, "distribution")
        cache_key = (
            account_id,
            tuple(sorted(period_starts_ms.items(), key=lambda item: item[0])),
        )
        cached = self._distribution_periods_cache.get(cache_key)
        if cached and cached[0] == version:
            return self._clone_summary(cached[1])

        rows = await self._read_all_history_rows(account_id, "distribution")
        earliest_event_time_ms = int(rows[0]["event_time_ms"]) if rows else None
        amounts: dict[str, Decimal] = {key: Decimal("0") for key in period_starts_ms}

        for row in rows:
            amount = Decimal(str(row["amount"]))
            event_time_ms = int(row["event_time_ms"])
            for key, start_ms in period_starts_ms.items():
                if start_ms is None or event_time_ms >= start_ms:
                    amounts[key] += amount

        summary = {
            "amounts": amounts,
            "earliest_event_time_ms": earliest_event_time_ms,
            "records": len(rows),
        }
        self._distribution_periods_cache[cache_key] = (version, self._clone_summary(summary))
        return summary

    async def is_distribution_backfill_complete(self, account_id: str) -> bool:
        async with self._lock:
            row = self._conn.execute(
                """
                SELECT completed
                FROM distribution_backfill_state
                WHERE account_id = ?
                """,
                (account_id,),
            ).fetchone()
        if row is None:
            return False
        return bool(row["completed"])

    async def set_distribution_backfill_complete(
        self,
        account_id: str,
        *,
        completed: bool,
        updated_at_ms: int,
    ) -> None:
        async with self._lock:
            with self._conn:
                self._conn.execute(
                    """
                    INSERT INTO distribution_backfill_state (account_id, completed, updated_at_ms)
                    VALUES (?, ?, ?)
                    ON CONFLICT(account_id)
                    DO UPDATE SET
                        completed = excluded.completed,
                        updated_at_ms = excluded.updated_at_ms
                    """,
                    (account_id, 1 if completed else 0, updated_at_ms),
                )

    async def get_earliest_event_time_ms(self, account_id: str, source: str) -> int | None:
        async with self._lock:
            row = self._conn.execute(
                """
                SELECT MIN(event_time_ms) AS earliest_event_time_ms
                FROM history_events
                WHERE account_id = ? AND source = ?
                """,
                (account_id, source),
            ).fetchone()
        if row is None or row["earliest_event_time_ms"] is None:
            return None
        return int(row["earliest_event_time_ms"])

    async def summarize_interest(self, account_id: str, history_window_days: int) -> dict[str, Any]:
        margin_rows = await self._read_history_rows(account_id, "margin_interest", history_window_days)
        negative_rows = await self._read_history_rows(account_id, "negative_interest", history_window_days)

        margin_interest_total = sum((Decimal(str(row["amount"])) for row in margin_rows), Decimal("0"))
        negative_balance_interest_total = sum((Decimal(str(row["amount"])) for row in negative_rows), Decimal("0"))

        return {
            "window_days": history_window_days,
            "records": len(margin_rows) + len(negative_rows),
            "margin_interest_total": margin_interest_total,
            "negative_balance_interest_total": negative_balance_interest_total,
            "total_interest": margin_interest_total + negative_balance_interest_total,
        }

    async def get_mark_prices(self, symbols: list[str]) -> dict[str, Decimal]:
        normalized_symbols = sorted({symbol for symbol in symbols if symbol})
        if not normalized_symbols:
            return {}
        placeholders = ", ".join("?" for _ in normalized_symbols)
        async with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT symbol, mark_price
                FROM mark_prices
                WHERE symbol IN ({placeholders})
                """,
                normalized_symbols,
            ).fetchall()
        return {
            str(row["symbol"]): Decimal(str(row["mark_price"]))
            for row in rows
        }

    async def save_mark_prices(self, prices: dict[str, Decimal], *, updated_at_ms: int) -> None:
        if not prices:
            return
        async with self._lock:
            with self._conn:
                self._conn.executemany(
                    """
                    INSERT INTO mark_prices (symbol, mark_price, updated_at_ms)
                    VALUES (?, ?, ?)
                    ON CONFLICT(symbol)
                    DO UPDATE SET
                        mark_price = excluded.mark_price,
                        updated_at_ms = excluded.updated_at_ms
                    """,
                    [(symbol, str(price), updated_at_ms) for symbol, price in prices.items()],
                )

    async def _read_history_rows(
        self,
        account_id: str,
        source: str,
        history_window_days: int,
    ) -> list[sqlite3.Row]:
        window_start = self._window_start_ms(history_window_days)
        async with self._lock:
            rows = self._conn.execute(
                """
                SELECT asset, amount, event_type, event_time_ms
                FROM history_events
                WHERE account_id = ? AND source = ? AND event_time_ms >= ?
                ORDER BY event_time_ms ASC, unique_key ASC
                """,
                (account_id, source, window_start),
            ).fetchall()
        return rows

    async def _read_all_history_rows(
        self,
        account_id: str,
        source: str,
    ) -> list[sqlite3.Row]:
        async with self._lock:
            rows = self._conn.execute(
                """
                SELECT asset, amount, event_type, event_time_ms
                FROM history_events
                WHERE account_id = ? AND source = ?
                ORDER BY event_time_ms ASC, unique_key ASC
                """,
                (account_id, source),
            ).fetchall()
        return rows

    def _window_start_ms(self, history_window_days: int) -> int:
        start = datetime.now(UTC) - timedelta(days=max(history_window_days, 1))
        return int(start.timestamp() * 1000)

    def _source_version(self, account_id: str, source: str) -> int:
        return self._source_versions.get((account_id, source), 0)

    def _bump_source_version(self, account_id: str, source: str) -> None:
        key = (account_id, source)
        self._source_versions[key] = self._source_versions.get(key, 0) + 1

    def _clone_summary(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {key: self._clone_summary(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._clone_summary(item) for item in value]
        return value
