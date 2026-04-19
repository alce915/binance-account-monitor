from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from decimal import Decimal
from time import perf_counter
from typing import Any, Callable, Protocol
from uuid import uuid4

from monitor_app.binance import BinanceMonitorGateway, RefreshMarkPriceProvider
from monitor_app.config import MonitorAccountConfig, Settings
from monitor_app.history_store import MonitorHistoryStore
from monitor_app.i18n import (
    account_snapshot_updated_message,
    all_accounts_failed_message,
    all_accounts_healthy_message,
    auto_refresh_failed_message,
    auto_refresh_timeout_message,
    monitor_accounts_reloaded_message,
    monitoring_disabled_message,
    no_accounts_available_message,
    refresh_completed_message,
    refresh_failed_message,
    refresh_timeout_message,
    some_accounts_failed_message,
    waiting_for_monitor_connection_message,
)
from monitor_app.security import sanitize_error_summary
from monitor_app.telegram_notifications import TelegramNotificationService
from monitor_app.unimmr_alerts import UniMmrAlertService

logger = logging.getLogger("uvicorn.error")


class UnifiedAccountGateway(Protocol):
    async def get_unified_account_snapshot(
        self,
        *,
        history_window_days: int = 7,
        income_limit: int = 100,
        interest_limit: int = 100,
        previous_snapshot: dict[str, Any] | None = None,
        mark_price_provider: RefreshMarkPriceProvider | None = None,
        refresh_id: str | None = None,
    ) -> dict[str, Any]: ...

    async def close(self) -> None: ...


class RefreshTimeoutError(RuntimeError):
    def __init__(self, duration_ms: int, *, refresh_id: str) -> None:
        super().__init__("Refresh timed out, previous data preserved")
        self.duration_ms = duration_ms
        self.refresh_id = refresh_id


class AccountMonitorController:
    def __init__(
        self,
        settings: Settings,
        gateway_factory: Callable[[MonitorAccountConfig], UnifiedAccountGateway] | None = None,
    ) -> None:
        self._settings = settings
        self._history_store = MonitorHistoryStore(
            settings.monitor_history_db_path,
            max_rows_per_source=settings.monitor_history_max_rows,
        )
        self._gateway_factory = gateway_factory or (
            lambda account: BinanceMonitorGateway(settings, account, history_store=self._history_store)
        )
        self._telegram_notifications = TelegramNotificationService(settings)
        self._unimmr_alerts = UniMmrAlertService(settings, notifier=self._telegram_notifications)
        self._gateways: dict[str, UnifiedAccountGateway] = {}
        self._subscriptions: dict[asyncio.Queue[dict[str, Any]], set[str] | None] = {}
        self._lock = asyncio.Lock()
        self._refresh_lock = asyncio.Lock()
        self._refresh_task: asyncio.Task[None] | None = None
        self._monitor_enabled = True
        self._last_payload = self._build_idle_payload("idle", waiting_for_monitor_connection_message())

    async def start(self) -> None:
        await self._telegram_notifications.start()
        if self._monitor_enabled and self._settings.unimmr_alerts_enabled and self._refresh_task is None:
            self._refresh_task = asyncio.create_task(self._run_loop())

    def _utc_now(self) -> str:
        return datetime.now(UTC).isoformat()

    def _normalize(self, payload: Any) -> Any:
        if isinstance(payload, Decimal):
            return str(payload)
        if isinstance(payload, datetime):
            return payload.isoformat()
        if isinstance(payload, dict):
            return {key: self._normalize(value) for key, value in payload.items()}
        if isinstance(payload, list):
            return [self._normalize(value) for value in payload]
        return payload

    def current_snapshot(self, account_ids: list[str] | None = None) -> dict[str, Any]:
        normalized_ids = self._normalize_account_ids(account_ids)
        payload = self._filter_payload(self._last_payload, normalized_ids)
        return self._normalize(self._decorate_payload(payload))

    def current_summary(self, account_ids: list[str] | None = None) -> dict[str, Any]:
        payload = self.current_snapshot(account_ids)
        return {
            "status": payload["status"],
            "updated_at": payload["updated_at"],
            "message": payload["message"],
            "service": payload["service"],
            "summary": payload["summary"],
            "profit_summary": payload.get("profit_summary", {}),
        }

    def current_groups(self, account_ids: list[str] | None = None) -> dict[str, Any]:
        payload = self.current_snapshot(account_ids)
        return {
            "status": payload["status"],
            "updated_at": payload["updated_at"],
            "message": payload["message"],
            "service": payload["service"],
            "summary": payload["summary"],
            "profit_summary": payload.get("profit_summary", {}),
            "groups": payload["groups"],
        }

    def current_accounts(self, account_ids: list[str] | None = None) -> dict[str, Any]:
        payload = self.current_snapshot(account_ids)
        return {
            "status": payload["status"],
            "updated_at": payload["updated_at"],
            "message": payload["message"],
            "service": payload["service"],
            "summary": payload["summary"],
            "profit_summary": payload.get("profit_summary", {}),
            "accounts": payload["accounts"],
        }

    async def subscribe(self, account_ids: list[str] | None = None) -> asyncio.Queue[dict[str, Any]]:
        normalized_ids = self._normalize_account_ids(account_ids)
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=20)
        async with self._lock:
            self._subscriptions[queue] = normalized_ids
            if self._monitor_enabled and self._refresh_task is None:
                self._refresh_task = asyncio.create_task(self._run_loop())
        await queue.put(
            {
                "event": "monitor_snapshot",
                "data": self._normalize(self._decorate_payload(self._filter_payload(self._last_payload, normalized_ids))),
            }
        )
        return queue

    @property
    def monitor_enabled(self) -> bool:
        return self._monitor_enabled

    async def set_monitor_enabled(self, enabled: bool) -> dict[str, Any]:
        if self._monitor_enabled == enabled:
            return self.current_summary()
        self._monitor_enabled = enabled
        if not enabled:
            if self._refresh_task is not None:
                self._refresh_task.cancel()
                try:
                    await self._refresh_task
                except asyncio.CancelledError:
                    pass
                self._refresh_task = None
        elif (self._subscriptions or self._settings.unimmr_alerts_enabled) and self._refresh_task is None:
            self._refresh_task = asyncio.create_task(self._run_loop())
        await self._broadcast(self._last_payload)
        return self.current_summary()

    async def refresh_now(self) -> dict[str, Any]:
        return await self._refresh_now_i18n()

    async def _refresh_now_i18n(self) -> dict[str, Any]:
        started_at = perf_counter()
        refresh_id = self._new_refresh_id()
        try:
            candidate_payload, committed = await self._refresh_once(refresh_id=refresh_id, reason="manual")
            duration_ms = int((perf_counter() - started_at) * 1000)
        except RefreshTimeoutError as exc:
            payload = self.current_groups()
            payload["refresh_result"] = {
                "success": False,
                "timeout": True,
                "message": refresh_timeout_message(),
                "updated_at": self._utc_now(),
                "duration_ms": exc.duration_ms,
                "refresh_id": exc.refresh_id,
                "failed_accounts": [],
                "fallback_sections": [],
                "fallback_section_count": 0,
                "slow_accounts": [],
                "timings": {"total_ms": exc.duration_ms},
            }
            return payload
        except Exception as exc:
            duration_ms = int((perf_counter() - started_at) * 1000)
            payload = self.current_groups()
            payload["refresh_result"] = {
                "success": False,
                "timeout": False,
                "message": refresh_failed_message(sanitize_error_summary(exc, fallback="Refresh failed")),
                "updated_at": self._utc_now(),
                "duration_ms": duration_ms,
                "refresh_id": refresh_id,
                "failed_accounts": [],
                "fallback_sections": [],
                "fallback_section_count": 0,
                "slow_accounts": [],
                "timings": {"total_ms": duration_ms},
            }
            return payload

        payload = self.current_groups()
        refresh_meta = candidate_payload.get("refresh_meta") or {}
        refresh_message = refresh_completed_message()
        if not committed:
            refresh_detail = candidate_payload.get("message") or payload.get("message") or refresh_completed_message()
            refresh_message = refresh_failed_message(refresh_detail)
        payload["refresh_result"] = {
            "success": committed,
            "timeout": False,
            "message": refresh_message,
            "updated_at": candidate_payload.get("updated_at", self._utc_now()),
            "duration_ms": duration_ms,
            "refresh_id": refresh_meta.get("refresh_id", refresh_id),
            "failed_accounts": refresh_meta.get("failed_accounts", []),
            "fallback_sections": refresh_meta.get("fallback_sections", []),
            "fallback_section_count": int(refresh_meta.get("fallback_section_count") or 0),
            "slow_accounts": refresh_meta.get("slow_accounts", []),
            "timings": refresh_meta.get("timings", {"total_ms": duration_ms}),
        }
        return payload

    async def reload_accounts(self) -> dict[str, Any]:
        async with self._refresh_lock:
            previous_accounts = dict(self._settings.monitor_accounts)
            self._settings.load_monitor_accounts()

            for account_id, gateway in list(self._gateways.items()):
                next_account = self._settings.monitor_accounts.get(account_id)
                previous_account = previous_accounts.get(account_id)
                if next_account is None or next_account != previous_account:
                    await gateway.close()
                    self._gateways.pop(account_id, None)

            self._last_payload = self._build_idle_payload("idle", monitor_accounts_reloaded_message())
            broadcast_payload = self._last_payload
            reloaded_payload = self.current_groups()

        await self._broadcast(broadcast_payload)
        return reloaded_payload

    def unsubscribe(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        self._subscriptions.pop(queue, None)
        if not self._subscriptions and self._refresh_task is not None and not self._settings.unimmr_alerts_enabled:
            self._refresh_task.cancel()
            self._refresh_task = None

    async def close(self) -> None:
        if self._refresh_task is not None:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
            self._refresh_task = None
        for gateway in self._gateways.values():
            await gateway.close()
        self._gateways.clear()
        await self._unimmr_alerts.close()
        await self._telegram_notifications.close()
        await self._history_store.close()

    async def send_test_telegram_notification(self, message: str | None = None) -> dict[str, Any]:
        return await self._unimmr_alerts.send_test_notification(message)

    async def unimmr_alert_status(self) -> dict[str, Any]:
        return await self._unimmr_alerts.status_summary(monitor_enabled=self._monitor_enabled)

    async def simulate_unimmr_alerts(self, updates: list[dict[str, Any]]) -> dict[str, Any]:
        if not updates:
            raise ValueError("UniMMR simulation updates are required")

        normalized_updates: dict[str, Decimal] = {}
        for item in updates:
            account_id = str(item.get("account_id") or "").strip().lower()
            if not account_id:
                raise ValueError("UniMMR simulation account_id is required")
            try:
                normalized_updates[account_id] = Decimal(str(item.get("uni_mmr")))
            except Exception as exc:
                raise ValueError(f"Invalid UniMMR simulation value for {account_id}") from exc

        async with self._refresh_lock:
            accounts = [
                dict(account)
                for account in self._last_payload.get("accounts", [])
                if isinstance(account, dict)
            ]

        if not accounts:
            raise ValueError("No current accounts available for UniMMR simulation")

        account_map = {
            str(account.get("account_id") or "").strip().lower(): account
            for account in accounts
        }
        missing = [account_id for account_id in normalized_updates if account_id not in account_map]
        if missing:
            raise ValueError(f"Unknown UniMMR simulation account_id: {missing[0]}")

        simulation_accounts: list[dict[str, Any]] = []
        applied_updates: list[dict[str, Any]] = []
        for account_id, value in normalized_updates.items():
            account = dict(account_map[account_id])
            account["status"] = "ok"
            account["uni_mmr"] = value
            simulation_accounts.append(account)
            applied_updates.append({"account_id": account_id, "uni_mmr": str(value)})

        result = await self._unimmr_alerts.simulate_payload({"accounts": simulation_accounts})
        result["updates"] = applied_updates
        return result

    async def _run_loop(self) -> None:
        await self._run_loop_i18n()

    async def _run_loop_i18n(self) -> None:
        while True:
            refresh_id = self._new_refresh_id()
            try:
                await self._refresh_once(refresh_id=refresh_id, reason="auto")
            except asyncio.CancelledError:
                raise
            except RefreshTimeoutError as exc:
                logger.warning(
                    "Auto refresh timed out refresh_id=%s duration_ms=%s",
                    exc.refresh_id,
                    exc.duration_ms,
                )
                await self._publish_refresh_warning(auto_refresh_timeout_message())
            except Exception as exc:
                safe_message = sanitize_error_summary(exc, fallback="Auto refresh failed")
                logger.exception(
                    "Auto refresh failed refresh_id=%s message=%s",
                    refresh_id,
                    safe_message,
                )
                await self._publish_refresh_warning(auto_refresh_failed_message(safe_message))
            await asyncio.sleep(self._next_refresh_interval_seconds())

    def _next_refresh_interval_seconds(self) -> float:
        base_interval_seconds = max(self._settings.monitor_refresh_interval_ms / 1000, 1.0)
        if self._unimmr_alerts.has_danger_accounts():
            return min(base_interval_seconds, 300.0)
        return base_interval_seconds

    async def _publish_refresh_warning(self, message: str) -> None:
        async with self._refresh_lock:
            self._last_payload = {
                **self._last_payload,
                "status": "error",
                "message": message,
            }
            warning_payload = self._last_payload
        await self._broadcast(warning_payload)

    async def _refresh_once(self, *, refresh_id: str, reason: str) -> tuple[dict[str, Any], bool]:
        async with self._refresh_lock:
            timeout_seconds = max(self._settings.monitor_refresh_timeout_ms, 1) / 1000
            started_at = perf_counter()
            logger.info(
                "Refresh started refresh_id=%s reason=%s timeout_ms=%s",
                refresh_id,
                reason,
                self._settings.monitor_refresh_timeout_ms,
            )
            try:
                payload = await asyncio.wait_for(self._collect_payload(refresh_id=refresh_id), timeout=timeout_seconds)
            except asyncio.TimeoutError as exc:
                duration_ms = int((perf_counter() - started_at) * 1000)
                logger.warning(
                    "Refresh timed out refresh_id=%s reason=%s duration_ms=%s",
                    refresh_id,
                    reason,
                    duration_ms,
                )
                raise RefreshTimeoutError(duration_ms, refresh_id=refresh_id) from exc
            collect_payload_ms = int((perf_counter() - started_at) * 1000)
            payload.setdefault("refresh_meta", {})
            payload["refresh_meta"].setdefault("timings", {})
            payload["refresh_meta"]["timings"]["collect_payload_ms"] = collect_payload_ms
            payload["refresh_meta"]["reason"] = reason
            if self._should_commit_payload(payload):
                await self._unimmr_alerts.evaluate_payload(payload)
                broadcast_started_at = perf_counter()
                self._last_payload = payload
                await self._broadcast(payload)
                broadcast_ms = int((perf_counter() - broadcast_started_at) * 1000)
                duration_ms = int((perf_counter() - started_at) * 1000)
                payload["refresh_meta"]["timings"]["broadcast_ms"] = broadcast_ms
                payload["refresh_meta"]["timings"]["total_ms"] = duration_ms
                logger.info(
                    "Refresh committed refresh_id=%s reason=%s duration_ms=%s collect_payload_ms=%s broadcast_ms=%s success_count=%s error_count=%s fallback_count=%s slowest_account_ms=%s",
                    refresh_id,
                    reason,
                    duration_ms,
                    collect_payload_ms,
                    broadcast_ms,
                    payload.get("summary", {}).get("success_count", 0),
                    payload.get("summary", {}).get("error_count", 0),
                    len(payload.get("refresh_meta", {}).get("fallback_sections", [])),
                    payload.get("refresh_meta", {}).get("timings", {}).get("slowest_account_ms", 0),
                )
                return payload, True
            if self._should_publish_failure_payload(payload):
                broadcast_started_at = perf_counter()
                self._last_payload = payload
                await self._broadcast(payload)
                broadcast_ms = int((perf_counter() - broadcast_started_at) * 1000)
                duration_ms = int((perf_counter() - started_at) * 1000)
                payload["refresh_meta"]["timings"]["broadcast_ms"] = broadcast_ms
                payload["refresh_meta"]["timings"]["total_ms"] = duration_ms
                logger.warning(
                    "Refresh published failure payload refresh_id=%s reason=%s duration_ms=%s failed_accounts=%s",
                    refresh_id,
                    reason,
                    duration_ms,
                    payload.get("refresh_meta", {}).get("failed_accounts", []),
                )
                return payload, False
            duration_ms = int((perf_counter() - started_at) * 1000)
            payload["refresh_meta"]["timings"]["broadcast_ms"] = 0
            payload["refresh_meta"]["timings"]["total_ms"] = duration_ms
            logger.warning(
                "Refresh preserved previous payload refresh_id=%s reason=%s duration_ms=%s collect_payload_ms=%s failed_accounts=%s",
                refresh_id,
                reason,
                duration_ms,
                collect_payload_ms,
                payload.get("refresh_meta", {}).get("failed_accounts", []),
            )
            return payload, False

    async def _collect_payload(self, *, refresh_id: str) -> dict[str, Any]:
        accounts = list(self._settings.monitor_accounts.values())
        if not accounts:
            return self._build_idle_payload("error", no_accounts_available_message())

        previous_snapshots = {
            str(account.get("account_id") or ""): account
            for account in self._last_payload.get("accounts", [])
            if isinstance(account, dict)
        }
        mark_price_provider = RefreshMarkPriceProvider()
        semaphore = asyncio.Semaphore(max(1, self._settings.monitor_account_concurrency))
        snapshots = await asyncio.gather(
            *(
                self._fetch_account_snapshot(
                    account,
                    previous_snapshot=previous_snapshots.get(account.account_id),
                    mark_price_provider=mark_price_provider,
                    semaphore=semaphore,
                    refresh_id=refresh_id,
                )
                for account in accounts
            )
        )
        refresh_meta = self._build_refresh_meta(refresh_id=refresh_id, accounts=snapshots)
        return self._build_payload(snapshots, refresh_meta=refresh_meta)

    async def _fetch_account_snapshot(
        self,
        account: MonitorAccountConfig,
        *,
        previous_snapshot: dict[str, Any] | None,
        mark_price_provider: RefreshMarkPriceProvider,
        semaphore: asyncio.Semaphore,
        refresh_id: str,
    ) -> dict[str, Any]:
        gateway = self._gateways.get(account.account_id)
        if gateway is None:
            gateway = self._gateway_factory(account)
            self._gateways[account.account_id] = gateway
        started_at = perf_counter()
        try:
            async with semaphore:
                snapshot = await gateway.get_unified_account_snapshot(
                    history_window_days=self._settings.monitor_history_window_days,
                    previous_snapshot=previous_snapshot,
                    mark_price_provider=mark_price_provider,
                    refresh_id=refresh_id,
                )
            snapshot.setdefault("account_id", account.account_id)
            snapshot.setdefault("account_name", account.display_name)
            snapshot.setdefault("main_account_id", account.main_account_id)
            snapshot.setdefault("main_account_name", account.main_account_name)
            snapshot.setdefault("child_account_id", account.child_account_id)
            snapshot.setdefault("child_account_name", account.child_account_name)
            snapshot.setdefault("message", account_snapshot_updated_message())
            snapshot.setdefault("diagnostics", {})
            snapshot["diagnostics"].setdefault("refresh_id", refresh_id)
            snapshot["diagnostics"].setdefault("timings", {})
            snapshot["diagnostics"]["timings"]["controller_total_ms"] = int((perf_counter() - started_at) * 1000)
            return snapshot
        except Exception as exc:
            return {
                "status": "error",
                "source": "papi",
                "account_id": account.account_id,
                "account_name": account.display_name,
                "main_account_id": account.main_account_id,
                "main_account_name": account.main_account_name,
                "child_account_id": account.child_account_id,
                "child_account_name": account.child_account_name,
                "updated_at": datetime.now(UTC),
                "message": sanitize_error_summary(exc, fallback="Account snapshot failed"),
                "totals": self._empty_totals(),
                "positions": [],
                "assets": [],
                "spot_assets": [],
                "income_summary": {
                    "window_days": self._settings.monitor_history_window_days,
                    "records": 0,
                    "total_income": Decimal("0"),
                    "total_commission": Decimal("0"),
                    "by_type": {},
                    "by_asset": {},
                },
                "distribution_summary": {
                    "window_days": self._settings.monitor_history_window_days,
                    "records": 0,
                    "total_distribution": Decimal("0"),
                    "by_type": {},
                    "by_asset": {},
                },
                "interest_summary": {
                    "window_days": self._settings.monitor_history_window_days,
                    "records": 0,
                    "margin_interest_total": Decimal("0"),
                    "negative_balance_interest_total": Decimal("0"),
                    "total_interest": Decimal("0"),
                },
                "distribution_profit_summary": self._empty_distribution_profit_summary(),
                "section_errors": {},
                "diagnostics": {
                    "refresh_id": refresh_id,
                    "timings": {
                        "controller_total_ms": int((perf_counter() - started_at) * 1000),
                    },
                    "fallback_sections": [],
                },
            }

    async def _broadcast(self, payload: dict[str, Any]) -> None:
        stale: list[asyncio.Queue[dict[str, Any]]] = []
        for queue, account_ids in list(self._subscriptions.items()):
            try:
                queue.put_nowait(
                    {
                        "event": "monitor_snapshot",
                        "data": self._normalize(self._decorate_payload(self._filter_payload(payload, account_ids))),
                    }
                )
            except asyncio.QueueFull:
                stale.append(queue)
        for queue in stale:
            self._subscriptions.pop(queue, None)
        if not self._subscriptions and self._refresh_task is not None and not self._settings.unimmr_alerts_enabled:
            self._refresh_task.cancel()
            self._refresh_task = None

    def _filter_payload(self, payload: dict[str, Any], account_ids: set[str] | None) -> dict[str, Any]:
        if not account_ids:
            return payload
        filtered_accounts = [
            account
            for account in payload.get("accounts", [])
            if str(account.get("account_id", "")).lower() in account_ids
        ]
        return self._compose_payload(filtered_accounts, refresh_meta=payload.get("refresh_meta"))

    def _build_payload(self, accounts: list[dict[str, Any]], *, refresh_meta: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._compose_payload(accounts, refresh_meta=refresh_meta)

    def _new_refresh_id(self) -> str:
        return uuid4().hex[:12]

    def _build_refresh_meta(self, *, refresh_id: str, accounts: list[dict[str, Any]]) -> dict[str, Any]:
        failed_accounts: list[dict[str, Any]] = []
        fallback_sections: list[dict[str, Any]] = []
        timings: dict[str, Any] = {"accounts": {}}
        slow_accounts: list[dict[str, Any]] = []
        for account in accounts:
            account_id = str(account.get("account_id") or "")
            diagnostics = account.get("diagnostics") or {}
            account_timings = diagnostics.get("timings") or {}
            if account_id:
                timings["accounts"][account_id] = account_timings
            account_duration_ms = self._account_duration_ms(account_timings)
            if account_id and account_duration_ms > 0:
                slow_accounts.append(
                    {
                        "account_id": account_id,
                        "duration_ms": account_duration_ms,
                    }
                )
            if account.get("status") != "ok":
                failed_accounts.append(
                    {
                        "account_id": account_id,
                        "message": account.get("message", ""),
                    }
                )
            sections = [
                section_name
                for section_name, details in (account.get("section_errors") or {}).items()
                if isinstance(details, dict) and details.get("used_fallback")
            ]
            if sections:
                fallback_sections.append(
                    {
                        "account_id": account_id,
                        "sections": sections,
                    }
                )
        slow_accounts.sort(key=lambda item: int(item.get("duration_ms") or 0), reverse=True)
        timings["slowest_account_ms"] = int(slow_accounts[0]["duration_ms"]) if slow_accounts else 0
        return {
            "refresh_id": refresh_id,
            "failed_accounts": failed_accounts,
            "fallback_sections": fallback_sections,
            "fallback_section_count": len(fallback_sections),
            "slow_accounts": slow_accounts[:3],
            "timings": timings,
        }

    def _account_duration_ms(self, account_timings: dict[str, Any]) -> int:
        numeric_values: list[int] = []
        for value in account_timings.values():
            if isinstance(value, (int, float)):
                numeric_values.append(int(value))
        return max(numeric_values, default=0)

    def _should_commit_payload(self, payload: dict[str, Any]) -> bool:
        summary = payload.get("summary") or {}
        account_count = int(summary.get("account_count") or 0)
        success_count = int(summary.get("success_count") or 0)
        if account_count == 0:
            return True
        return success_count > 0

    def _should_publish_failure_payload(self, payload: dict[str, Any]) -> bool:
        current_summary = self._last_payload.get("summary") or {}
        if int(current_summary.get("account_count") or 0) > 0:
            return False
        next_summary = payload.get("summary") or {}
        return int(next_summary.get("account_count") or 0) > 0

    def _decorate_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        service = dict(payload.get("service") or {})
        service["monitor_enabled"] = self._monitor_enabled
        if self._monitor_enabled:
            return {
                **payload,
                "service": service,
            }
        return {
            **payload,
            "status": "disabled",
            "message": monitoring_disabled_message(),
            "service": service,
        }

    def _compose_payload(self, accounts: list[dict[str, Any]], *, refresh_meta: dict[str, Any] | None = None) -> dict[str, Any]:
        groups = self._build_groups(accounts)
        summary = self._summarize_accounts(accounts)
        profit_summary = self._aggregate_profit_summary(accounts, summary)
        status, message = self._status_and_message(summary)
        return {
            "status": status,
            "updated_at": self._utc_now(),
            "message": message,
            "service": {
                "refresh_interval_ms": self._settings.monitor_refresh_interval_ms,
                "refresh_timeout_ms": self._settings.monitor_refresh_timeout_ms,
                "history_window_days": self._settings.monitor_history_window_days,
                "account_ids": sorted(self._settings.monitor_accounts.keys()),
                "main_account_ids": sorted(self._settings.monitor_main_accounts.keys()),
                "monitor_enabled": self._monitor_enabled,
            },
            "summary": summary,
            "profit_summary": profit_summary,
            "groups": groups,
            "accounts": accounts,
            "refresh_meta": refresh_meta or {},
        }

    def _build_idle_payload(self, status: str, message: str) -> dict[str, Any]:
        return {
            "status": status,
            "updated_at": self._utc_now(),
            "message": message,
            "service": {
                "refresh_interval_ms": self._settings.monitor_refresh_interval_ms,
                "refresh_timeout_ms": self._settings.monitor_refresh_timeout_ms,
                "history_window_days": self._settings.monitor_history_window_days,
                "account_ids": sorted(self._settings.monitor_accounts.keys()),
                "main_account_ids": sorted(self._settings.monitor_main_accounts.keys()),
                "monitor_enabled": self._monitor_enabled,
            },
            "summary": self._summarize_accounts([]),
            "profit_summary": self._empty_distribution_profit_summary(),
            "groups": [],
            "accounts": [],
            "refresh_meta": {},
        }

    def _build_groups(self, accounts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for account in accounts:
            main_account_id = str(account.get("main_account_id") or "unknown")
            grouped.setdefault(main_account_id, []).append(account)
        groups: list[dict[str, Any]] = []
        for main_account_id in sorted(grouped):
            group_accounts = sorted(grouped[main_account_id], key=lambda item: str(item.get("account_id", "")))
            group_summary = self._summarize_accounts(group_accounts)
            groups.append(
                {
                    "main_account_id": main_account_id,
                    "main_account_name": group_accounts[0].get("main_account_name", main_account_id),
                    "summary": group_summary,
                    "profit_summary": self._aggregate_profit_summary(group_accounts, group_summary),
                    "accounts": group_accounts,
                }
            )
        return groups

    def _status_and_message(self, summary: dict[str, Any]) -> tuple[str, str]:
        if summary["account_count"] == 0:
            return "idle", no_accounts_available_message()
        if summary["error_count"] == 0:
            return "ok", all_accounts_healthy_message()
        if summary["success_count"] == 0:
            return "error", all_accounts_failed_message()
        return "partial", some_accounts_failed_message()

    def _summarize_accounts(self, accounts: list[dict[str, Any]]) -> dict[str, Any]:
        totals = self._empty_totals()
        success_count = 0
        error_count = 0
        for account in accounts:
            if account.get("status") == "ok":
                success_count += 1
                account_totals = account.get("totals") or {}
                for key in totals:
                    if key == "distribution_apy_7d":
                        continue
                    totals[key] += Decimal(str(account_totals.get(key) or "0"))
            else:
                error_count += 1
        totals["distribution_apy_7d"] = self._calculate_distribution_apy(
            totals["total_distribution"],
            totals["equity"],
        )
        return {
            "account_count": len(accounts),
            "success_count": success_count,
            "error_count": error_count,
            **totals,
        }

    def _aggregate_profit_summary(
        self,
        accounts: list[dict[str, Any]],
        summary: dict[str, Any],
    ) -> dict[str, Any]:
        aggregated = self._empty_distribution_profit_summary()
        for key in ("today", "week", "month", "year", "all"):
            aggregated[key]["complete"] = True
        equity = Decimal(str(summary.get("equity") or "0"))
        has_successful_accounts = False
        for account in accounts:
            if account.get("status") != "ok":
                continue
            has_successful_accounts = True
            account_profit_summary = account.get("distribution_profit_summary") or {}
            for key, label in (
                ("today", "今日收益丨收益率"),
                ("week", "本周收益丨收益率"),
                ("month", "本月收益丨收益率"),
                ("year", "年度收益丨收益率"),
                ("all", "全部收益丨收益率"),
            ):
                period = account_profit_summary.get(key) or {}
                amount = Decimal(str(period.get("amount") or "0"))
                aggregated[key]["label"] = label
                aggregated[key]["amount"] += amount
                aggregated[key]["complete"] = aggregated[key]["complete"] and bool(period.get("complete"))
                period_start_at = period.get("start_at")
                if aggregated[key]["start_at"] is None:
                    aggregated[key]["start_at"] = period_start_at
                elif key == "all" and period_start_at is not None:
                    aggregated[key]["start_at"] = min(str(aggregated[key]["start_at"]), str(period_start_at))

        if not has_successful_accounts:
            return self._empty_distribution_profit_summary()
        for key in ("today", "week", "month", "year", "all"):
            aggregated[key]["rate"] = self._calculate_ratio(aggregated[key]["amount"], equity)
        aggregated["backfill_complete"] = all(bool(aggregated[key]["complete"]) for key in ("today", "week", "month", "year", "all"))
        return aggregated

    def _empty_totals(self) -> dict[str, Decimal]:
        return {
            "equity": Decimal("0"),
            "margin": Decimal("0"),
            "available_balance": Decimal("0"),
            "unrealized_pnl": Decimal("0"),
            "total_income": Decimal("0"),
            "total_commission": Decimal("0"),
            "total_distribution": Decimal("0"),
            "distribution_apy_7d": Decimal("0"),
            "total_interest": Decimal("0"),
        }

    def _empty_distribution_profit_summary(self) -> dict[str, Any]:
        def _period(label: str) -> dict[str, Any]:
            return {
                "label": label,
                "amount": Decimal("0"),
                "rate": Decimal("0"),
                "start_at": None,
                "complete": False,
            }

        return {
            "today": _period("今日收益丨收益率"),
            "week": _period("本周收益丨收益率"),
            "month": _period("本月收益丨收益率"),
            "year": _period("年度收益丨收益率"),
            "all": _period("全部收益丨收益率"),
            "backfill_complete": False,
        }

    def _calculate_distribution_apy(
        self,
        total_distribution: Decimal,
        equity: Decimal,
    ) -> Decimal:
        if equity <= Decimal("0"):
            return Decimal("0")
        return (total_distribution / equity) * (Decimal("365") / Decimal("7"))

    def _calculate_ratio(self, amount: Decimal, equity: Decimal) -> Decimal:
        if equity <= Decimal("0"):
            return Decimal("0")
        return amount / equity

    def _normalize_account_ids(self, account_ids: list[str] | None) -> set[str] | None:
        if not account_ids:
            return None
        normalized = {
            account_id.strip().lower()
            for account_id in account_ids
            if account_id and account_id.strip()
        }
        return normalized or None
