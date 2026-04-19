from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, ROUND_FLOOR
from pathlib import Path
from typing import Any, Callable

from monitor_app.config import Settings
from monitor_app.i18n import (
    default_unimmr_test_message,
    format_unimmr_reason_text,
    format_unimmr_telegram_message,
)
from monitor_app.telegram_notifications import TelegramNotificationService

logger = logging.getLogger("uvicorn.error")

SAFE_THRESHOLD = Decimal("1.5")
WARNING_THRESHOLD = Decimal("1.2")
WARNING_STEP = Decimal("0.1")
DANGER_STEP = Decimal("0.05")
WARNING_REPEAT_MS = 12 * 60 * 60 * 1000
DANGER_REPEAT_MS = 5 * 60 * 1000
WARNING_REENTRY_COOLDOWN_MS = 2 * 60 * 60 * 1000
RECOVERY_DEDUPE_MS = 10 * 60 * 1000
WARNING_DEDUPE_MS = 10 * 60 * 1000
DANGER_DEDUPE_MS = 2 * 60 * 1000
DEDUPE_RETENTION_MS = max(RECOVERY_DEDUPE_MS, WARNING_DEDUPE_MS, DANGER_DEDUPE_MS)

SEVERITY_RANK = {"safe": 0, "warning": 1, "danger": 2}


def _utc_iso_from_ms(value: int | None) -> str | None:
    if not value:
        return None
    return datetime.fromtimestamp(value / 1000, tz=UTC).isoformat()


@dataclass(slots=True)
class UniMmrAccountState:
    account_id: str
    main_account_id: str
    main_account_name: str
    child_account_id: str
    child_account_name: str
    current_band: str
    last_value: str
    warning_notified_step: int | None
    warning_last_notified_at_ms: int
    danger_notified_step: int | None
    danger_last_notified_at_ms: int
    warning_reentry_cooldown_until_ms: int
    warning_entry_pending: int
    danger_entry_pending: int
    pending_recovery_from_band: str | None
    recovery_candidate_band: str | None
    recovery_candidate_count: int
    last_reason: str
    last_notification_at_ms: int
    updated_at_ms: int


@dataclass(slots=True)
class UniMmrTrigger:
    account_id: str
    main_account_id: str
    main_account_name: str
    child_account_id: str
    child_account_name: str
    band: str
    severity: str
    value: Decimal
    reason_code: str
    reason_text: str
    warning_step: int | None
    danger_step: int | None
    updated_state: UniMmrAccountState


class UniMmrAlertStore:
    def __init__(self, db_path: Path, *, event_max_rows: int) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._event_max_rows = max(1, int(event_max_rows))
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = asyncio.Lock()
        self._initialize()

    def _initialize(self) -> None:
        with self._conn:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS unimmr_alert_states (
                    account_id TEXT PRIMARY KEY,
                    main_account_id TEXT NOT NULL,
                    main_account_name TEXT NOT NULL,
                    child_account_id TEXT NOT NULL,
                    child_account_name TEXT NOT NULL,
                    current_band TEXT NOT NULL,
                    last_value TEXT NOT NULL,
                    warning_notified_step INTEGER,
                    warning_last_notified_at_ms INTEGER NOT NULL DEFAULT 0,
                    danger_notified_step INTEGER,
                    danger_last_notified_at_ms INTEGER NOT NULL DEFAULT 0,
                    warning_reentry_cooldown_until_ms INTEGER NOT NULL DEFAULT 0,
                    warning_entry_pending INTEGER NOT NULL DEFAULT 0,
                    danger_entry_pending INTEGER NOT NULL DEFAULT 0,
                    pending_recovery_from_band TEXT,
                    recovery_candidate_band TEXT,
                    recovery_candidate_count INTEGER NOT NULL DEFAULT 0,
                    last_reason TEXT NOT NULL DEFAULT '',
                    last_notification_at_ms INTEGER NOT NULL DEFAULT 0,
                    updated_at_ms INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS unimmr_alert_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at_ms INTEGER NOT NULL,
                    account_id TEXT NOT NULL,
                    main_account_id TEXT NOT NULL,
                    band TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    reason_code TEXT NOT NULL,
                    reason_text TEXT NOT NULL,
                    uni_mmr TEXT NOT NULL,
                    sent INTEGER NOT NULL DEFAULT 0,
                    detail TEXT NOT NULL DEFAULT ''
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS telegram_dedupe_state (
                    dedupe_key TEXT PRIMARY KEY,
                    last_sent_at_ms INTEGER NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_unimmr_alert_events_created
                ON unimmr_alert_events (created_at_ms DESC, id DESC)
                """
            )
            self._ensure_column("unimmr_alert_states", "last_notification_at_ms", "INTEGER NOT NULL DEFAULT 0")

    def _ensure_column(self, table: str, column: str, spec: str) -> None:
        columns = {
            str(row["name"])
            for row in self._conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if column not in columns:
            self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {spec}")

    async def close(self) -> None:
        async with self._lock:
            self._conn.close()

    async def get_state(self, account_id: str) -> UniMmrAccountState | None:
        async with self._lock:
            row = self._conn.execute(
                """
                SELECT *
                FROM unimmr_alert_states
                WHERE account_id = ?
                """,
                (account_id,),
            ).fetchone()
        return self._row_to_state(row)

    async def save_state(self, state: UniMmrAccountState) -> None:
        async with self._lock:
            with self._conn:
                self._save_state_locked(state)

    def _save_state_locked(self, state: UniMmrAccountState) -> None:
        self._conn.execute(
            """
            INSERT INTO unimmr_alert_states (
                account_id,
                main_account_id,
                main_account_name,
                child_account_id,
                child_account_name,
                current_band,
                last_value,
                warning_notified_step,
                warning_last_notified_at_ms,
                danger_notified_step,
                danger_last_notified_at_ms,
                warning_reentry_cooldown_until_ms,
                warning_entry_pending,
                danger_entry_pending,
                pending_recovery_from_band,
                recovery_candidate_band,
                recovery_candidate_count,
                last_reason,
                last_notification_at_ms,
                updated_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id) DO UPDATE SET
                main_account_id=excluded.main_account_id,
                main_account_name=excluded.main_account_name,
                child_account_id=excluded.child_account_id,
                child_account_name=excluded.child_account_name,
                current_band=excluded.current_band,
                last_value=excluded.last_value,
                warning_notified_step=excluded.warning_notified_step,
                warning_last_notified_at_ms=excluded.warning_last_notified_at_ms,
                danger_notified_step=excluded.danger_notified_step,
                danger_last_notified_at_ms=excluded.danger_last_notified_at_ms,
                warning_reentry_cooldown_until_ms=excluded.warning_reentry_cooldown_until_ms,
                warning_entry_pending=excluded.warning_entry_pending,
                danger_entry_pending=excluded.danger_entry_pending,
                pending_recovery_from_band=excluded.pending_recovery_from_band,
                recovery_candidate_band=excluded.recovery_candidate_band,
                recovery_candidate_count=excluded.recovery_candidate_count,
                last_reason=excluded.last_reason,
                last_notification_at_ms=excluded.last_notification_at_ms,
                updated_at_ms=excluded.updated_at_ms
            """,
            (
                state.account_id,
                state.main_account_id,
                state.main_account_name,
                state.child_account_id,
                state.child_account_name,
                state.current_band,
                state.last_value,
                state.warning_notified_step,
                state.warning_last_notified_at_ms,
                state.danger_notified_step,
                state.danger_last_notified_at_ms,
                state.warning_reentry_cooldown_until_ms,
                state.warning_entry_pending,
                state.danger_entry_pending,
                state.pending_recovery_from_band,
                state.recovery_candidate_band,
                state.recovery_candidate_count,
                state.last_reason,
                state.last_notification_at_ms,
                state.updated_at_ms,
            ),
        )

    async def record_event(
        self,
        *,
        created_at_ms: int,
        account_id: str,
        main_account_id: str,
        band: str,
        severity: str,
        reason_code: str,
        reason_text: str,
        uni_mmr: Decimal,
        sent: bool,
        detail: str = "",
    ) -> None:
        async with self._lock:
            with self._conn:
                self._conn.execute(
                    """
                    INSERT INTO unimmr_alert_events (
                        created_at_ms,
                        account_id,
                        main_account_id,
                        band,
                        severity,
                        reason_code,
                        reason_text,
                        uni_mmr,
                        sent,
                        detail
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        created_at_ms,
                        account_id,
                        main_account_id,
                        band,
                        severity,
                        reason_code,
                        reason_text,
                        str(uni_mmr),
                        1 if sent else 0,
                        detail,
                    ),
                )
                self._trim_alert_events_locked()

    async def is_dedupe_active(self, dedupe_key: str, *, window_ms: int, now_ms: int) -> bool:
        async with self._lock:
            row = self._conn.execute(
                """
                SELECT last_sent_at_ms
                FROM telegram_dedupe_state
                WHERE dedupe_key = ?
                """,
                (dedupe_key,),
            ).fetchone()
        if row is None:
            return False
        return now_ms - int(row["last_sent_at_ms"]) < window_ms

    async def mark_dedupe_sent(self, dedupe_key: str, *, now_ms: int) -> None:
        async with self._lock:
            with self._conn:
                self._conn.execute(
                    """
                    INSERT INTO telegram_dedupe_state (dedupe_key, last_sent_at_ms)
                    VALUES (?, ?)
                    ON CONFLICT(dedupe_key) DO UPDATE SET last_sent_at_ms = excluded.last_sent_at_ms
                    """,
                    (dedupe_key, now_ms),
                )
                self._conn.execute(
                    """
                    DELETE FROM telegram_dedupe_state
                    WHERE last_sent_at_ms < ?
                    """,
                    (max(now_ms - DEDUPE_RETENTION_MS, 0),),
                )

    async def list_states(self) -> list[dict[str, Any]]:
        async with self._lock:
            rows = self._conn.execute(
                """
                SELECT *
                FROM unimmr_alert_states
                ORDER BY updated_at_ms DESC, account_id ASC
                """
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            current_band = str(row["current_band"])
            next_allowed = 0
            if current_band == "warning":
                next_allowed = int(row["warning_reentry_cooldown_until_ms"] or 0)
                if int(row["warning_last_notified_at_ms"] or 0):
                    next_allowed = max(next_allowed, int(row["warning_last_notified_at_ms"]) + WARNING_REPEAT_MS)
            elif current_band == "danger" and int(row["danger_last_notified_at_ms"] or 0):
                next_allowed = int(row["danger_last_notified_at_ms"]) + DANGER_REPEAT_MS

            result.append(
                {
                    "account_id": str(row["account_id"]),
                    "main_account_id": str(row["main_account_id"]),
                    "main_account_name": str(row["main_account_name"]),
                    "child_account_id": str(row["child_account_id"]),
                    "child_account_name": str(row["child_account_name"]),
                    "current_band": current_band,
                    "last_value": str(row["last_value"]),
                    "last_sent_at": _utc_iso_from_ms(int(row["last_notification_at_ms"] or 0)),
                    "next_allowed_at": _utc_iso_from_ms(next_allowed),
                    "warning_step": row["warning_notified_step"],
                    "danger_step": row["danger_notified_step"],
                    "last_reason": str(row["last_reason"] or ""),
                    "updated_at": _utc_iso_from_ms(int(row["updated_at_ms"] or 0)),
                }
            )
        return result

    def _trim_alert_events_locked(self) -> None:
        self._conn.execute(
            """
            DELETE FROM unimmr_alert_events
            WHERE id IN (
                SELECT id
                FROM unimmr_alert_events
                ORDER BY created_at_ms DESC, id DESC
                LIMIT -1 OFFSET ?
            )
            """,
            (self._event_max_rows,),
        )

    def _row_to_state(self, row: sqlite3.Row | None) -> UniMmrAccountState | None:
        if row is None:
            return None
        return UniMmrAccountState(
            account_id=str(row["account_id"]),
            main_account_id=str(row["main_account_id"]),
            main_account_name=str(row["main_account_name"]),
            child_account_id=str(row["child_account_id"]),
            child_account_name=str(row["child_account_name"]),
            current_band=str(row["current_band"]),
            last_value=str(row["last_value"]),
            warning_notified_step=row["warning_notified_step"],
            warning_last_notified_at_ms=int(row["warning_last_notified_at_ms"] or 0),
            danger_notified_step=row["danger_notified_step"],
            danger_last_notified_at_ms=int(row["danger_last_notified_at_ms"] or 0),
            warning_reentry_cooldown_until_ms=int(row["warning_reentry_cooldown_until_ms"] or 0),
            warning_entry_pending=int(row["warning_entry_pending"] or 0),
            danger_entry_pending=int(row["danger_entry_pending"] or 0),
            pending_recovery_from_band=row["pending_recovery_from_band"],
            recovery_candidate_band=row["recovery_candidate_band"],
            recovery_candidate_count=int(row["recovery_candidate_count"] or 0),
            last_reason=str(row["last_reason"] or ""),
            last_notification_at_ms=int(row["last_notification_at_ms"] or 0),
            updated_at_ms=int(row["updated_at_ms"] or 0),
        )


class UniMmrAlertService:
    def __init__(
        self,
        settings: Settings,
        *,
        notifier: TelegramNotificationService | Any,
        now_ms: Callable[[], int] | None = None,
    ) -> None:
        self._settings = settings
        self._notifier = notifier
        self._now_ms = now_ms or (lambda: int(datetime.now(UTC).timestamp() * 1000))
        self._store = UniMmrAlertStore(
            settings.monitor_history_db_path,
            event_max_rows=settings.unimmr_alert_event_max_rows,
        )
        self._has_danger_accounts = False

    async def close(self) -> None:
        await self._store.close()

    def has_danger_accounts(self) -> bool:
        return self._has_danger_accounts

    async def status_summary(self, *, monitor_enabled: bool | None = None) -> dict[str, Any]:
        effective_enabled = bool(self._settings.unimmr_alerts_enabled and (True if monitor_enabled is None else monitor_enabled))
        return {
            "enabled": effective_enabled,
            "configured": bool(self._settings.unimmr_alerts_enabled),
            "monitor_enabled": True if monitor_enabled is None else bool(monitor_enabled),
            "telegram": self._notifier.stats() if hasattr(self._notifier, "stats") else {},
            "accounts": await self._store.list_states(),
        }

    async def send_test_notification(self, message: str | None = None) -> dict[str, Any]:
        text = (message or default_unimmr_test_message()).strip()
        return await self._notifier.send_telegram_notification(
            text,
            severity="info",
            category="telegram_test",
        )

    async def evaluate_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._evaluate_payload(payload, persist_state=True, send_notifications=True, use_dedupe=True)

    async def simulate_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._evaluate_payload(
            payload,
            persist_state=False,
            send_notifications=True,
            use_dedupe=False,
            simulated_state={},
        )

    async def _evaluate_payload(
        self,
        payload: dict[str, Any],
        *,
        persist_state: bool,
        send_notifications: bool,
        use_dedupe: bool,
        simulated_state: dict[str, UniMmrAccountState] | None = None,
    ) -> dict[str, Any]:
        accounts = payload.get("accounts", [])
        if not self._settings.unimmr_alerts_enabled:
            self._has_danger_accounts = any(
                isinstance(account, dict)
                and account.get("status") == "ok"
                and (value := self._account_value(account)) is not None
                and self._band_for_value(value) == "danger"
                for account in accounts
            )
            return {"triggered": 0, "has_danger": self._has_danger_accounts}

        now_ms = self._now_ms()
        triggers: list[UniMmrTrigger] = []
        has_danger = False
        for account in accounts:
            if not isinstance(account, dict) or account.get("status") != "ok":
                continue
            value = self._account_value(account)
            if value is None:
                continue
            if self._band_for_value(value) == "danger":
                has_danger = True
            trigger = await self._evaluate_account(
                account,
                value,
                now_ms=now_ms,
                persist_state=persist_state,
                simulated_state=simulated_state,
            )
            if trigger is not None:
                triggers.append(trigger)

        if persist_state:
            self._has_danger_accounts = has_danger
        if not triggers:
            return {"triggered": 0, "has_danger": has_danger}

        triggers.sort(key=lambda item: (0 if item.band == "danger" else 1, item.main_account_id, item.account_id))
        severity = "critical" if any(trigger.band == "danger" for trigger in triggers) else "warn"
        dedupe_window_ms = self._dedupe_window_ms(triggers)
        dedupe_key = self._dedupe_key(triggers)
        if use_dedupe and await self._store.is_dedupe_active(dedupe_key, window_ms=dedupe_window_ms, now_ms=now_ms):
            await self._commit_triggers(triggers, now_ms=now_ms, detail="deduped")
            return {"triggered": len(triggers), "has_danger": has_danger, "deduped": True}

        if not send_notifications:
            return {"triggered": len(triggers), "has_danger": has_danger, "simulated": True}

        async def on_sent() -> None:
            if persist_state:
                await self._commit_triggers(triggers, now_ms=now_ms, detail="sent")
            if use_dedupe:
                await self._store.mark_dedupe_sent(dedupe_key, now_ms=now_ms)

        async def on_failed(error_message: str) -> None:
            if not persist_state:
                return
            for trigger in triggers:
                await self._store.record_event(
                    created_at_ms=now_ms,
                    account_id=trigger.account_id,
                    main_account_id=trigger.main_account_id,
                    band=trigger.band,
                    severity=trigger.severity,
                    reason_code=trigger.reason_code,
                    reason_text=trigger.reason_text,
                    uni_mmr=trigger.value,
                    sent=False,
                    detail=error_message,
                )

        result = await self._notifier.send_telegram_notification(
            self._compose_message(triggers),
            severity=severity,
            category="unimmr_simulation" if not persist_state else "unimmr",
            dedupe_key=dedupe_key,
            dedupe_window_ms=dedupe_window_ms,
            on_sent=on_sent,
            on_failed=on_failed,
        )
        if result.get("status") == "disabled":
            await on_failed("telegram disabled")
        elif result.get("status") == "dropped":
            await on_failed(str(result.get("reason") or "telegram dropped"))
        return {
            "triggered": len(triggers),
            "has_danger": has_danger,
            "queued": result.get("status") == "queued",
            "simulated": not persist_state,
        }

    async def _commit_triggers(self, triggers: list[UniMmrTrigger], *, now_ms: int, detail: str) -> None:
        for trigger in triggers:
            await self._store.save_state(trigger.updated_state)
            await self._store.record_event(
                created_at_ms=now_ms,
                account_id=trigger.account_id,
                main_account_id=trigger.main_account_id,
                band=trigger.band,
                severity=trigger.severity,
                reason_code=trigger.reason_code,
                reason_text=trigger.reason_text,
                uni_mmr=trigger.value,
                sent=True,
                detail=detail,
            )

    async def _evaluate_account(
        self,
        account: dict[str, Any],
        value: Decimal,
        *,
        now_ms: int,
        persist_state: bool,
        simulated_state: dict[str, UniMmrAccountState] | None = None,
    ) -> UniMmrTrigger | None:
        account_id = str(account.get("account_id") or "")
        if simulated_state is not None:
            previous = simulated_state.get(account_id) or self._default_state(account, value, now_ms=now_ms)
        else:
            previous = await self._store.get_state(account_id) or self._default_state(account, value, now_ms=now_ms)
        current_band = self._band_for_value(value)
        state = self._update_observed_state(previous, account, value, current_band, now_ms=now_ms)

        trigger_payload: dict[str, Any] | None = None
        if state.pending_recovery_from_band and state.recovery_candidate_count >= 2:
            trigger_payload = self._evaluate_recovery_trigger(state, value=value, now_ms=now_ms)
        elif state.pending_recovery_from_band:
            trigger_payload = None
        elif current_band == "warning":
            trigger_payload = self._evaluate_warning_trigger(state, value, now_ms=now_ms)
        elif current_band == "danger":
            trigger_payload = self._evaluate_danger_trigger(state, value, now_ms=now_ms)
        else:
            trigger_payload = self._evaluate_recovery_trigger(state, value=value, now_ms=now_ms)

        if persist_state:
            await self._store.save_state(state)
        elif simulated_state is not None:
            simulated_state[account_id] = state
        if trigger_payload is None:
            return None
        return self._build_trigger(trigger_payload["updated_state"], trigger_payload, value)

    def _default_state(self, account: dict[str, Any], value: Decimal, *, now_ms: int) -> UniMmrAccountState:
        return UniMmrAccountState(
            account_id=str(account.get("account_id") or ""),
            main_account_id=str(account.get("main_account_id") or ""),
            main_account_name=str(account.get("main_account_name") or ""),
            child_account_id=str(account.get("child_account_id") or ""),
            child_account_name=str(account.get("child_account_name") or ""),
            current_band="safe",
            last_value=str(value),
            warning_notified_step=None,
            warning_last_notified_at_ms=0,
            danger_notified_step=None,
            danger_last_notified_at_ms=0,
            warning_reentry_cooldown_until_ms=0,
            warning_entry_pending=0,
            danger_entry_pending=0,
            pending_recovery_from_band=None,
            recovery_candidate_band=None,
            recovery_candidate_count=0,
            last_reason="",
            last_notification_at_ms=0,
            updated_at_ms=now_ms,
        )

    def _update_observed_state(
        self,
        previous: UniMmrAccountState,
        account: dict[str, Any],
        value: Decimal,
        current_band: str,
        *,
        now_ms: int,
    ) -> UniMmrAccountState:
        old_band = previous.current_band
        warning_entry_pending = previous.warning_entry_pending
        danger_entry_pending = previous.danger_entry_pending
        warning_reentry_cooldown_until_ms = previous.warning_reentry_cooldown_until_ms
        pending_recovery_from_band = previous.pending_recovery_from_band
        recovery_candidate_band = previous.recovery_candidate_band
        recovery_candidate_count = previous.recovery_candidate_count

        if old_band != current_band:
            if old_band == "warning" and current_band != "warning":
                warning_reentry_cooldown_until_ms = now_ms + WARNING_REENTRY_COOLDOWN_MS

            warning_entry_pending = 0
            danger_entry_pending = 0
            if current_band == "warning" and old_band != "danger":
                if previous.warning_notified_step is None or now_ms >= previous.warning_reentry_cooldown_until_ms:
                    warning_entry_pending = 1
            if current_band == "danger":
                danger_entry_pending = 1

            if SEVERITY_RANK[current_band] < SEVERITY_RANK[old_band]:
                pending_recovery_from_band = old_band
                recovery_candidate_band = current_band
                recovery_candidate_count = 1
            else:
                pending_recovery_from_band = None
                recovery_candidate_band = None
                recovery_candidate_count = 0
        elif pending_recovery_from_band:
            target_rank = SEVERITY_RANK.get(recovery_candidate_band or current_band, 0)
            if SEVERITY_RANK[current_band] <= target_rank:
                recovery_candidate_band = current_band
                recovery_candidate_count += 1
            else:
                pending_recovery_from_band = None
                recovery_candidate_band = None
                recovery_candidate_count = 0

        return UniMmrAccountState(
            account_id=str(account.get("account_id") or previous.account_id),
            main_account_id=str(account.get("main_account_id") or previous.main_account_id),
            main_account_name=str(account.get("main_account_name") or previous.main_account_name),
            child_account_id=str(account.get("child_account_id") or previous.child_account_id),
            child_account_name=str(account.get("child_account_name") or previous.child_account_name),
            current_band=current_band,
            last_value=str(value),
            warning_notified_step=previous.warning_notified_step,
            warning_last_notified_at_ms=previous.warning_last_notified_at_ms,
            danger_notified_step=previous.danger_notified_step,
            danger_last_notified_at_ms=previous.danger_last_notified_at_ms,
            warning_reentry_cooldown_until_ms=warning_reentry_cooldown_until_ms,
            warning_entry_pending=warning_entry_pending,
            danger_entry_pending=danger_entry_pending,
            pending_recovery_from_band=pending_recovery_from_band,
            recovery_candidate_band=recovery_candidate_band,
            recovery_candidate_count=recovery_candidate_count,
            last_reason=previous.last_reason,
            last_notification_at_ms=previous.last_notification_at_ms,
            updated_at_ms=now_ms,
        )

    def _evaluate_warning_trigger(self, state: UniMmrAccountState, value: Decimal, *, now_ms: int) -> dict[str, Any] | None:
        current_step = self._warning_step(value)
        if state.warning_entry_pending:
            updated = self._replace_state(
                state,
                warning_entry_pending=0,
                warning_notified_step=current_step,
                warning_last_notified_at_ms=now_ms,
                last_notification_at_ms=now_ms,
                last_reason="warning_entry",
            )
            return {
                "reason_code": "warning_entry",
                "reason_text": format_unimmr_reason_text("warning_entry"),
                "updated_state": updated,
            }

        last_step = state.warning_notified_step if state.warning_notified_step is not None else -1
        if current_step > last_step:
            updated = self._replace_state(
                state,
                warning_notified_step=current_step,
                warning_last_notified_at_ms=now_ms,
                last_notification_at_ms=now_ms,
                last_reason="warning_step_drop",
            )
            return {
                "reason_code": "warning_step_drop",
                "reason_text": format_unimmr_reason_text("warning_step_drop"),
                "updated_state": updated,
            }

        if state.warning_last_notified_at_ms and now_ms - state.warning_last_notified_at_ms >= WARNING_REPEAT_MS:
            updated = self._replace_state(
                state,
                warning_last_notified_at_ms=now_ms,
                last_notification_at_ms=now_ms,
                last_reason="warning_repeat",
            )
            return {
                "reason_code": "warning_repeat",
                "reason_text": format_unimmr_reason_text("warning_repeat"),
                "updated_state": updated,
            }
        return None

    def _evaluate_danger_trigger(self, state: UniMmrAccountState, value: Decimal, *, now_ms: int) -> dict[str, Any] | None:
        current_step = self._danger_step(value)
        if state.danger_entry_pending:
            updated = self._replace_state(
                state,
                danger_entry_pending=0,
                danger_notified_step=current_step,
                danger_last_notified_at_ms=now_ms,
                last_notification_at_ms=now_ms,
                last_reason="danger_entry",
                pending_recovery_from_band=None,
                recovery_candidate_band=None,
                recovery_candidate_count=0,
            )
            return {
                "reason_code": "danger_entry",
                "reason_text": format_unimmr_reason_text("danger_entry"),
                "updated_state": updated,
            }

        last_step = state.danger_notified_step if state.danger_notified_step is not None else -1
        if current_step > last_step:
            updated = self._replace_state(
                state,
                danger_notified_step=current_step,
                danger_last_notified_at_ms=now_ms,
                last_notification_at_ms=now_ms,
                last_reason="danger_step_drop",
            )
            return {
                "reason_code": "danger_step_drop",
                "reason_text": format_unimmr_reason_text("danger_step_drop"),
                "updated_state": updated,
            }

        if state.danger_last_notified_at_ms and now_ms - state.danger_last_notified_at_ms >= DANGER_REPEAT_MS:
            updated = self._replace_state(
                state,
                danger_last_notified_at_ms=now_ms,
                last_notification_at_ms=now_ms,
                last_reason="danger_repeat",
            )
            return {
                "reason_code": "danger_repeat",
                "reason_text": format_unimmr_reason_text("danger_repeat"),
                "updated_state": updated,
            }
        return None

    def _evaluate_recovery_trigger(self, state: UniMmrAccountState, *, value: Decimal, now_ms: int) -> dict[str, Any] | None:
        if not state.pending_recovery_from_band or state.recovery_candidate_count < 2:
            return None
        warning_step = state.warning_notified_step
        warning_last_notified_at_ms = state.warning_last_notified_at_ms
        danger_step = state.danger_notified_step
        danger_last_notified_at_ms = state.danger_last_notified_at_ms
        if state.current_band == "warning":
            warning_step = max(state.warning_notified_step or -1, self._warning_step(value))
            warning_last_notified_at_ms = now_ms
        if state.current_band == "danger":
            danger_step = max(state.danger_notified_step or -1, self._danger_step(value))
            danger_last_notified_at_ms = now_ms
        updated = self._replace_state(
            state,
            pending_recovery_from_band=None,
            recovery_candidate_band=None,
            recovery_candidate_count=0,
            warning_notified_step=warning_step,
            warning_last_notified_at_ms=warning_last_notified_at_ms,
            danger_notified_step=danger_step,
            danger_last_notified_at_ms=danger_last_notified_at_ms,
            last_notification_at_ms=now_ms,
            last_reason="recovery",
        )
        return {
            "reason_code": "recovery",
            "reason_text": format_unimmr_reason_text(
                "recovery",
                from_band=state.pending_recovery_from_band,
                to_band=state.current_band,
            ),
            "updated_state": updated,
        }

    def _build_trigger(self, state: UniMmrAccountState, trigger: dict[str, Any], value: Decimal) -> UniMmrTrigger:
        return UniMmrTrigger(
            account_id=state.account_id,
            main_account_id=state.main_account_id,
            main_account_name=state.main_account_name,
            child_account_id=state.child_account_id,
            child_account_name=state.child_account_name,
            band=state.current_band,
            severity="critical" if state.current_band == "danger" else ("info" if state.current_band == "safe" else "warn"),
            value=value,
            reason_code=str(trigger["reason_code"]),
            reason_text=str(trigger["reason_text"]),
            warning_step=state.warning_notified_step,
            danger_step=state.danger_notified_step,
            updated_state=state,
        )

    def _compose_message(self, triggers: list[UniMmrTrigger]) -> str:
        return format_unimmr_telegram_message(triggers)

    def _dedupe_window_ms(self, triggers: list[UniMmrTrigger]) -> int:
        if any(trigger.band == "danger" for trigger in triggers):
            return DANGER_DEDUPE_MS
        if any(trigger.reason_code == "recovery" for trigger in triggers):
            return RECOVERY_DEDUPE_MS
        return WARNING_DEDUPE_MS

    def _dedupe_key(self, triggers: list[UniMmrTrigger]) -> str:
        parts = [
            f"{trigger.account_id}:{trigger.reason_code}:{trigger.warning_step}:{trigger.danger_step}:{trigger.value}"
            for trigger in triggers
        ]
        return json.dumps(parts, ensure_ascii=False)

    def _account_value(self, account: dict[str, Any]) -> Decimal | None:
        value = account.get("uni_mmr")
        if value in (None, ""):
            return None
        try:
            return Decimal(str(value))
        except Exception:
            return None

    def _band_for_value(self, value: Decimal) -> str:
        if value <= WARNING_THRESHOLD:
            return "danger"
        if value <= SAFE_THRESHOLD:
            return "warning"
        return "safe"

    def _warning_step(self, value: Decimal) -> int:
        if value > SAFE_THRESHOLD:
            return -1
        delta = (SAFE_THRESHOLD - value) / WARNING_STEP
        return int(delta.to_integral_value(rounding=ROUND_FLOOR))

    def _danger_step(self, value: Decimal) -> int:
        if value > WARNING_THRESHOLD:
            return -1
        delta = (WARNING_THRESHOLD - value) / DANGER_STEP
        return int(delta.to_integral_value(rounding=ROUND_FLOOR))

    def _replace_state(self, state: UniMmrAccountState, **changes: Any) -> UniMmrAccountState:
        payload = {
            "account_id": state.account_id,
            "main_account_id": state.main_account_id,
            "main_account_name": state.main_account_name,
            "child_account_id": state.child_account_id,
            "child_account_name": state.child_account_name,
            "current_band": state.current_band,
            "last_value": state.last_value,
            "warning_notified_step": state.warning_notified_step,
            "warning_last_notified_at_ms": state.warning_last_notified_at_ms,
            "danger_notified_step": state.danger_notified_step,
            "danger_last_notified_at_ms": state.danger_last_notified_at_ms,
            "warning_reentry_cooldown_until_ms": state.warning_reentry_cooldown_until_ms,
            "warning_entry_pending": state.warning_entry_pending,
            "danger_entry_pending": state.danger_entry_pending,
            "pending_recovery_from_band": state.pending_recovery_from_band,
            "recovery_candidate_band": state.recovery_candidate_band,
            "recovery_candidate_count": state.recovery_candidate_count,
            "last_reason": state.last_reason,
            "last_notification_at_ms": state.last_notification_at_ms,
            "updated_at_ms": state.updated_at_ms,
        }
        payload.update(changes)
        return UniMmrAccountState(**payload)
