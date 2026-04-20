from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Any

RATE_LIMIT_STATE_RETENTION_MS = 7 * 24 * 60 * 60 * 1000


class AccessControlAuditStore:
    def __init__(self, db_path: Path, *, max_rows: int = 2_000) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._max_rows = max(int(max_rows), 100)
        self._initialize()

    def _initialize(self) -> None:
        with self._conn:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS access_control_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at_ms INTEGER NOT NULL,
                    client_ip TEXT NOT NULL,
                    method TEXT NOT NULL,
                    path TEXT NOT NULL,
                    role TEXT,
                    auth_source TEXT,
                    result TEXT NOT NULL,
                    reason_code TEXT NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_access_control_audit_created_at
                ON access_control_audit (created_at_ms DESC, id DESC)
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS access_control_rate_limit_state (
                    client_ip TEXT PRIMARY KEY,
                    fail_count INTEGER NOT NULL,
                    locked_until_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_access_control_rate_limit_updated_at
                ON access_control_rate_limit_state (updated_at_ms DESC)
                """
            )

    async def record_event(
        self,
        *,
        created_at_ms: int,
        client_ip: str,
        method: str,
        path: str,
        role: str,
        auth_source: str,
        result: str,
        reason_code: str,
    ) -> None:
        async with self._lock:
            with self._conn:
                self._conn.execute(
                    """
                    INSERT INTO access_control_audit (
                        created_at_ms,
                        client_ip,
                        method,
                        path,
                        role,
                        auth_source,
                        result,
                        reason_code
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (created_at_ms, client_ip, method, path, role, auth_source, result, reason_code),
                )
                self._trim_locked()

    async def list_events(
        self,
        *,
        limit: int = 50,
        result: str = "",
        reason_code: str = "",
    ) -> list[dict[str, Any]]:
        normalized_limit = max(1, min(int(limit), 200))
        clauses: list[str] = []
        params: list[Any] = []
        if str(result or "").strip():
            clauses.append("result = ?")
            params.append(str(result).strip())
        if str(reason_code or "").strip():
            clauses.append("reason_code = ?")
            params.append(str(reason_code).strip())
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        query = (
            "SELECT id, created_at_ms, client_ip, method, path, role, auth_source, result, reason_code "
            "FROM access_control_audit "
            f"{where_clause} "
            "ORDER BY created_at_ms DESC, id DESC "
            "LIMIT ?"
        )
        params.append(normalized_limit)
        async with self._lock:
            rows = self._conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]

    async def get_rate_limit_state(self, client_ip: str) -> dict[str, int]:
        normalized_client_ip = str(client_ip or "").strip()
        if not normalized_client_ip:
            return {}
        async with self._lock:
            row = self._conn.execute(
                """
                SELECT fail_count, locked_until_ms, updated_at_ms
                FROM access_control_rate_limit_state
                WHERE client_ip = ?
                """,
                (normalized_client_ip,),
            ).fetchone()
        if row is None:
            return {}
        return {
            "fail_count": int(row["fail_count"] or 0),
            "locked_until_ms": int(row["locked_until_ms"] or 0),
            "updated_at_ms": int(row["updated_at_ms"] or 0),
        }

    async def set_rate_limit_state(
        self,
        *,
        client_ip: str,
        fail_count: int,
        locked_until_ms: int,
        updated_at_ms: int,
    ) -> None:
        normalized_client_ip = str(client_ip or "").strip()
        if not normalized_client_ip:
            return
        async with self._lock:
            with self._conn:
                self._conn.execute(
                    """
                    INSERT INTO access_control_rate_limit_state (
                        client_ip,
                        fail_count,
                        locked_until_ms,
                        updated_at_ms
                    ) VALUES (?, ?, ?, ?)
                    ON CONFLICT(client_ip) DO UPDATE SET
                        fail_count = excluded.fail_count,
                        locked_until_ms = excluded.locked_until_ms,
                        updated_at_ms = excluded.updated_at_ms
                    """,
                    (
                        normalized_client_ip,
                        max(int(fail_count), 0),
                        max(int(locked_until_ms), 0),
                        max(int(updated_at_ms), 0),
                    ),
                )
                self._trim_rate_limit_state_locked(updated_at_ms)

    async def clear_rate_limit_state(self, client_ip: str) -> None:
        normalized_client_ip = str(client_ip or "").strip()
        if not normalized_client_ip:
            return
        async with self._lock:
            with self._conn:
                self._conn.execute(
                    "DELETE FROM access_control_rate_limit_state WHERE client_ip = ?",
                    (normalized_client_ip,),
                )

    def _trim_locked(self) -> None:
        row = self._conn.execute("SELECT COUNT(*) AS row_count FROM access_control_audit").fetchone()
        row_count = int(row["row_count"]) if row is not None else 0
        overflow = row_count - self._max_rows
        if overflow <= 0:
            return
        self._conn.execute(
            """
            DELETE FROM access_control_audit
            WHERE id IN (
                SELECT id
                FROM access_control_audit
                ORDER BY created_at_ms DESC, id DESC
                LIMIT -1 OFFSET ?
            )
            """,
            (self._max_rows,),
        )

    def _trim_rate_limit_state_locked(self, now_ms: int) -> None:
        cutoff_ms = max(int(now_ms), 0) - RATE_LIMIT_STATE_RETENTION_MS
        self._conn.execute(
            """
            DELETE FROM access_control_rate_limit_state
            WHERE locked_until_ms < ? AND updated_at_ms < ?
            """,
            (max(int(now_ms), 0), cutoff_ms),
        )

    async def close(self) -> None:
        async with self._lock:
            self._conn.close()
