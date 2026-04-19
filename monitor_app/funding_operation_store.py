from __future__ import annotations

import asyncio
import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class FundingOperationRecord:
    created_at_ms: int
    updated_at_ms: int
    expires_at_ms: int
    main_id: str
    direction: str
    asset: str
    request_id: str
    operation_id: str
    payload_hash: str
    execution_stage: str
    operation_status: str
    account_count: int
    success_count: int
    failure_count: int
    confirmed_count: int
    pending_confirmation_count: int
    message: str
    response: dict[str, Any]


class FundingOperationStore:
    def __init__(self, db_path: Path, *, max_rows: int, idempotency_ttl_seconds: int) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._max_rows = max(1, int(max_rows))
        self._idempotency_ttl_seconds = max(1, int(idempotency_ttl_seconds))
        self._lock = asyncio.Lock()
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._initialize()

    def _initialize(self) -> None:
        with self._conn:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS funding_operations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    expires_at_ms INTEGER NOT NULL,
                    main_id TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    asset TEXT NOT NULL,
                    request_id TEXT NOT NULL,
                    operation_id TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    execution_stage TEXT NOT NULL DEFAULT 'completed',
                    operation_status TEXT NOT NULL,
                    account_count INTEGER NOT NULL,
                    success_count INTEGER NOT NULL DEFAULT 0,
                    failure_count INTEGER NOT NULL DEFAULT 0,
                    confirmed_count INTEGER NOT NULL DEFAULT 0,
                    pending_confirmation_count INTEGER NOT NULL DEFAULT 0,
                    message TEXT NOT NULL DEFAULT '',
                    response_json TEXT NOT NULL
                )
                """
            )
            self._ensure_column("updated_at_ms", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("execution_stage", "TEXT NOT NULL DEFAULT 'completed'")
            self._ensure_column("success_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("failure_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("confirmed_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("pending_confirmation_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("message", "TEXT NOT NULL DEFAULT ''")
            self._conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_funding_operations_main_direction_operation
                ON funding_operations (main_id, direction, operation_id)
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_funding_operations_main_created
                ON funding_operations (main_id, created_at_ms DESC, id DESC)
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_funding_operations_main_updated
                ON funding_operations (main_id, updated_at_ms DESC, created_at_ms DESC, id DESC)
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_funding_operations_main_operation
                ON funding_operations (main_id, operation_id, created_at_ms DESC, id DESC)
                """
            )
            self._backfill_summary_columns_locked()

    def _ensure_column(self, column_name: str, column_type: str) -> None:
        existing_columns = {
            str(row["name"])
            for row in self._conn.execute("PRAGMA table_info(funding_operations)").fetchall()
        }
        if column_name in existing_columns:
            return
        self._conn.execute(f"ALTER TABLE funding_operations ADD COLUMN {column_name} {column_type}")

    async def close(self) -> None:
        async with self._lock:
            self._conn.close()

    async def get_operation(self, main_id: str, direction: str, operation_id: str) -> FundingOperationRecord | None:
        async with self._lock:
            row = self._conn.execute(
                """
                SELECT
                    created_at_ms,
                    updated_at_ms,
                    expires_at_ms,
                    main_id,
                    direction,
                    asset,
                    request_id,
                    operation_id,
                    payload_hash,
                    execution_stage,
                    operation_status,
                    account_count,
                    success_count,
                    failure_count,
                    confirmed_count,
                    pending_confirmation_count,
                    message,
                    response_json
                FROM funding_operations
                WHERE main_id = ? AND direction = ? AND operation_id = ?
                """,
                (main_id, direction, operation_id),
            ).fetchone()
        return self._row_to_record(row)

    async def get_operation_detail(
        self,
        main_id: str,
        operation_id: str,
        *,
        direction: str,
    ) -> FundingOperationRecord | None:
        async with self._lock:
            row = self._conn.execute(
                """
                SELECT
                    created_at_ms,
                    updated_at_ms,
                    expires_at_ms,
                    main_id,
                    direction,
                    asset,
                    request_id,
                    operation_id,
                    payload_hash,
                    execution_stage,
                    operation_status,
                    account_count,
                    success_count,
                    failure_count,
                    confirmed_count,
                    pending_confirmation_count,
                    message,
                    response_json
                FROM funding_operations
                WHERE main_id = ? AND direction = ? AND operation_id = ?
                ORDER BY created_at_ms DESC, id DESC
                LIMIT 1
                """,
                (main_id, direction, operation_id),
            ).fetchone()
        return self._row_to_record(row)

    async def create_operation(
        self,
        *,
        main_id: str,
        direction: str,
        asset: str,
        request_id: str,
        operation_id: str,
        payload_hash: str,
        execution_stage: str,
        operation_status: str,
        account_count: int,
        success_count: int,
        failure_count: int,
        confirmed_count: int,
        pending_confirmation_count: int,
        message: str,
        response: dict[str, Any],
    ) -> FundingOperationRecord:
        created_at_ms = int(datetime.now(UTC).timestamp() * 1000)
        expires_at_ms = created_at_ms + self._idempotency_ttl_seconds * 1000
        response_json = json.dumps(response, ensure_ascii=False, sort_keys=True)
        async with self._lock:
            with self._conn:
                self._conn.execute(
                    """
                    INSERT INTO funding_operations (
                        created_at_ms,
                        updated_at_ms,
                        expires_at_ms,
                        main_id,
                        direction,
                        asset,
                        request_id,
                        operation_id,
                        payload_hash,
                        execution_stage,
                        operation_status,
                        account_count,
                        success_count,
                        failure_count,
                        confirmed_count,
                        pending_confirmation_count,
                        message,
                        response_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        created_at_ms,
                        created_at_ms,
                        expires_at_ms,
                        main_id,
                        direction,
                        asset,
                        request_id,
                        operation_id,
                        payload_hash,
                        execution_stage,
                        operation_status,
                        account_count,
                        success_count,
                        failure_count,
                        confirmed_count,
                        pending_confirmation_count,
                        message,
                        response_json,
                    ),
                )
                self._trim_operations_locked()
                row = self._conn.execute(
                    """
                    SELECT
                        created_at_ms,
                        updated_at_ms,
                        expires_at_ms,
                        main_id,
                        direction,
                        asset,
                        request_id,
                        operation_id,
                        payload_hash,
                        execution_stage,
                        operation_status,
                        account_count,
                        success_count,
                        failure_count,
                        confirmed_count,
                        pending_confirmation_count,
                        message,
                        response_json
                    FROM funding_operations
                    WHERE main_id = ? AND direction = ? AND operation_id = ?
                    """,
                    (main_id, direction, operation_id),
                ).fetchone()
        record = self._row_to_record(row)
        assert record is not None
        return record

    async def update_operation(
        self,
        *,
        main_id: str,
        direction: str,
        operation_id: str,
        execution_stage: str,
        operation_status: str,
        account_count: int,
        success_count: int,
        failure_count: int,
        confirmed_count: int,
        pending_confirmation_count: int,
        message: str,
        response: dict[str, Any],
    ) -> FundingOperationRecord:
        updated_at_ms = int(datetime.now(UTC).timestamp() * 1000)
        response_json = json.dumps(response, ensure_ascii=False, sort_keys=True)
        async with self._lock:
            with self._conn:
                self._conn.execute(
                    """
                    UPDATE funding_operations
                    SET
                        updated_at_ms = ?,
                        execution_stage = ?,
                        operation_status = ?,
                        account_count = ?,
                        success_count = ?,
                        failure_count = ?,
                        confirmed_count = ?,
                        pending_confirmation_count = ?,
                        message = ?,
                        response_json = ?
                    WHERE main_id = ? AND direction = ? AND operation_id = ?
                    """,
                    (
                        updated_at_ms,
                        execution_stage,
                        operation_status,
                        account_count,
                        success_count,
                        failure_count,
                        confirmed_count,
                        pending_confirmation_count,
                        message,
                        response_json,
                        main_id,
                        direction,
                        operation_id,
                    ),
                )
                row = self._conn.execute(
                    """
                    SELECT
                        created_at_ms,
                        updated_at_ms,
                        expires_at_ms,
                        main_id,
                        direction,
                        asset,
                        request_id,
                        operation_id,
                        payload_hash,
                        execution_stage,
                        operation_status,
                        account_count,
                        success_count,
                        failure_count,
                        confirmed_count,
                        pending_confirmation_count,
                        message,
                        response_json
                    FROM funding_operations
                    WHERE main_id = ? AND direction = ? AND operation_id = ?
                    """,
                    (main_id, direction, operation_id),
                ).fetchone()
        record = self._row_to_record(row)
        assert record is not None
        return record

    async def list_operations(self, main_id: str, *, limit: int = 50) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit), self._max_rows))
        async with self._lock:
            rows = self._conn.execute(
                """
                SELECT
                    created_at_ms,
                    updated_at_ms,
                    operation_id,
                    direction,
                    asset,
                    execution_stage,
                    operation_status,
                    message,
                    account_count,
                    success_count,
                    failure_count,
                    confirmed_count,
                    pending_confirmation_count
                FROM funding_operations
                WHERE main_id = ?
                ORDER BY updated_at_ms DESC, created_at_ms DESC, id DESC
                LIMIT ?
                """,
                (main_id, safe_limit),
            ).fetchall()
        return [
            {
                "created_at": self._iso_from_ms(int(row["created_at_ms"])),
                "updated_at": self._iso_from_ms(int(row["updated_at_ms"] or row["created_at_ms"])),
                "operation_id": str(row["operation_id"]),
                "direction": str(row["direction"]),
                "asset": str(row["asset"]),
                "execution_stage": str(row["execution_stage"]),
                "operation_status": str(row["operation_status"]),
                "message": str(row["message"] or ""),
                "account_count": int(row["account_count"] or 0),
                "success_count": int(row["success_count"] or 0),
                "failure_count": int(row["failure_count"] or 0),
                "confirmed_count": int(row["confirmed_count"] or 0),
                "pending_confirmation_count": int(row["pending_confirmation_count"] or 0),
            }
            for row in rows
        ]

    def _trim_operations_locked(self) -> None:
        self._conn.execute(
            """
            DELETE FROM funding_operations
            WHERE id IN (
                SELECT id
                FROM funding_operations
                ORDER BY updated_at_ms DESC, created_at_ms DESC, id DESC
                LIMIT -1 OFFSET ?
            )
            """,
            (self._max_rows,),
        )

    def _backfill_summary_columns_locked(self) -> None:
        rows = self._conn.execute(
            """
            SELECT id, created_at_ms, updated_at_ms, response_json
            FROM funding_operations
            """
        ).fetchall()
        for row in rows:
            payload = self._decode_payload(row["response_json"])
            summary = self._summary_from_payload(payload)
            existing_updated_at_ms = int(row["updated_at_ms"] or 0)
            payload_updated_at_ms = self._extract_updated_at_ms(payload) or 0
            created_at_ms = int(row["created_at_ms"])
            updated_at_ms = max(existing_updated_at_ms, payload_updated_at_ms, created_at_ms)
            self._conn.execute(
                """
                UPDATE funding_operations
                SET
                    updated_at_ms = ?,
                    execution_stage = COALESCE(NULLIF(execution_stage, ''), 'completed'),
                    account_count = ?,
                    success_count = ?,
                    failure_count = ?,
                    confirmed_count = ?,
                    pending_confirmation_count = ?,
                    message = ?
                WHERE id = ?
                """,
                (
                    updated_at_ms,
                    summary["account_count"],
                    summary["success_count"],
                    summary["failure_count"],
                    summary["confirmed_count"],
                    summary["pending_confirmation_count"],
                    summary["message"],
                    int(row["id"]),
                ),
            )

    def _extract_updated_at_ms(self, payload: dict[str, Any]) -> int | None:
        updated_at = payload.get("updated_at")
        if not updated_at:
            return None
        try:
            return int(datetime.fromisoformat(str(updated_at)).timestamp() * 1000)
        except ValueError:
            return None

    def _summary_from_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        results = payload.get("results")
        success_count = 0
        failure_count = 0
        account_count = 0
        if isinstance(results, list):
            account_count = len(results)
            success_count = sum(1 for item in results if isinstance(item, dict) and item.get("success"))
            failure_count = max(0, len(results) - success_count)
        precheck = payload.get("precheck")
        if account_count <= 0 and isinstance(precheck, dict):
            try:
                account_count = int(precheck.get("selected_account_count") or 0)
            except (TypeError, ValueError):
                account_count = 0
        if account_count <= 0 and isinstance(precheck, dict):
            try:
                account_count = int(precheck.get("validated_account_count") or 0)
            except (TypeError, ValueError):
                account_count = 0
        reconciliation = payload.get("reconciliation")
        confirmed_count = 0
        if isinstance(reconciliation, dict):
            try:
                confirmed_count = int(reconciliation.get("confirmed_count") or 0)
            except (TypeError, ValueError):
                confirmed_count = 0
        pending_confirmation_count = max(success_count - confirmed_count, 0)
        return {
            "account_count": account_count,
            "success_count": success_count,
            "failure_count": failure_count,
            "confirmed_count": confirmed_count,
            "pending_confirmation_count": pending_confirmation_count,
            "message": str(payload.get("message") or ""),
        }

    def _row_to_record(self, row: sqlite3.Row | None) -> FundingOperationRecord | None:
        if row is None:
            return None
        payload = self._decode_payload(row["response_json"])
        return FundingOperationRecord(
            created_at_ms=int(row["created_at_ms"]),
            updated_at_ms=int(row["updated_at_ms"] or row["created_at_ms"]),
            expires_at_ms=int(row["expires_at_ms"]),
            main_id=str(row["main_id"]),
            direction=str(row["direction"]),
            asset=str(row["asset"]),
            request_id=str(row["request_id"]),
            operation_id=str(row["operation_id"]),
            payload_hash=str(row["payload_hash"]),
            execution_stage=str(row["execution_stage"] or "completed"),
            operation_status=str(row["operation_status"]),
            account_count=int(row["account_count"]),
            success_count=int(row["success_count"] or 0),
            failure_count=int(row["failure_count"] or 0),
            confirmed_count=int(row["confirmed_count"] or 0),
            pending_confirmation_count=int(row["pending_confirmation_count"] or 0),
            message=str(row["message"] or ""),
            response=payload,
        )

    def _decode_payload(self, payload_text: str) -> dict[str, Any]:
        try:
            payload = json.loads(payload_text)
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        return payload

    def _iso_from_ms(self, value: int) -> str:
        return datetime.fromtimestamp(value / 1000, UTC).isoformat()
