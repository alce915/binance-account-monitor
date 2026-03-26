from __future__ import annotations

import asyncio
import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any


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
    ) -> None:
        async with self._lock:
            with self._conn:
                if events:
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
                                json.dumps(event.payload, ensure_ascii=False, sort_keys=True),
                            )
                            for event in events
                        ],
                    )
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
                    self._conn.execute(
                        """
                        DELETE FROM history_events
                        WHERE account_id = ? AND source = ? AND event_time_ms < ?
                        """,
                        (account_id, source, retain_after_ms),
                    )

    async def summarize_income(self, account_id: str, history_window_days: int) -> dict[str, Any]:
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

        return {
            "window_days": history_window_days,
            "records": len(rows),
            "total_income": total_income,
            "by_type": dict(sorted(by_type.items())),
            "by_asset": dict(sorted(by_asset.items())),
        }

    async def summarize_distribution(self, account_id: str, history_window_days: int) -> dict[str, Any]:
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

        return {
            "window_days": history_window_days,
            "records": len(rows),
            "total_distribution": total_distribution,
            "by_type": dict(sorted(by_type.items())),
            "by_asset": dict(sorted(by_asset.items())),
        }

    async def summarize_distribution_periods(
        self,
        account_id: str,
        period_starts_ms: dict[str, int | None],
    ) -> dict[str, Any]:
        rows = await self._read_all_history_rows(account_id, "distribution")
        earliest_event_time_ms = int(rows[0]["event_time_ms"]) if rows else None
        amounts: dict[str, Decimal] = {key: Decimal("0") for key in period_starts_ms}

        for row in rows:
            amount = Decimal(str(row["amount"]))
            event_time_ms = int(row["event_time_ms"])
            for key, start_ms in period_starts_ms.items():
                if start_ms is None or event_time_ms >= start_ms:
                    amounts[key] += amount

        return {
            "amounts": amounts,
            "earliest_event_time_ms": earliest_event_time_ms,
            "records": len(rows),
        }

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
