from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Awaitable, Callable
from urllib.parse import urlencode

import httpx

from monitor_app.config import MonitorAccountConfig, Settings
from monitor_app.history_store import HistoryEvent, MonitorHistoryStore

logger = logging.getLogger("uvicorn.error")


class MonitorGatewayError(RuntimeError):
    pass


class RetriedRequestError(RuntimeError):
    def __init__(self, message: str, *, attempts: int, cause: Exception) -> None:
        super().__init__(message)
        self.attempts = attempts
        self.cause = cause


class RefreshMarkPriceProvider:
    def __init__(self) -> None:
        self._task: asyncio.Task[dict[str, Decimal]] | None = None
        self._lock = asyncio.Lock()

    async def get_mark_prices(
        self,
        symbols: list[str],
        fetcher: Callable[[], Awaitable[dict[str, Decimal]]],
    ) -> dict[str, Decimal]:
        normalized_symbols = sorted({symbol for symbol in symbols if symbol})
        if not normalized_symbols:
            return {}

        async with self._lock:
            if self._task is None:
                self._task = asyncio.create_task(fetcher())
            task = self._task

        try:
            prices = await task
        except Exception:
            return {}
        return {symbol: price for symbol, price in prices.items() if symbol in normalized_symbols}


EXCLUDED_INCOME_TYPE_KEYWORDS = ("TRANSFER",)
ANNUALIZATION_DAYS = Decimal("365")
DISTRIBUTION_WINDOW_DAYS = 7
BACKFILL_QUERY_WINDOW_DAYS = 90
HISTORY_SOURCE_INCOME = "income"
HISTORY_SOURCE_DISTRIBUTION = "distribution"
HISTORY_SOURCE_MARGIN_INTEREST = "margin_interest"
HISTORY_SOURCE_NEGATIVE_INTEREST = "negative_interest"
CORE_RETRY_DELAYS_SECONDS = (0.2, 0.4, 0.8, 1.6)
SECONDARY_RETRY_DELAYS_SECONDS = (0.2, 0.5)
BEIJING_TZ = timezone(timedelta(hours=8))


class BinanceMonitorGateway:
    def __init__(
        self,
        settings: Settings,
        account: MonitorAccountConfig,
        *,
        history_store: MonitorHistoryStore | None = None,
    ) -> None:
        self._settings = settings
        self._account = account
        self._owns_history_store = history_store is None
        self._history_store = history_store or MonitorHistoryStore(settings.monitor_history_db_path)
        self._distribution_backfill_task: asyncio.Task[None] | None = None
        self._income_refresh_task: asyncio.Task[dict[str, Any] | None] | None = None
        headers = {"X-MBX-APIKEY": self._account.api_key}
        self._papi_client = httpx.AsyncClient(
            base_url="https://papi.binance.com",
            headers=headers,
            timeout=None,
        )
        self._sapi_client = httpx.AsyncClient(
            base_url="https://api.binance.com",
            headers=headers,
            timeout=None,
        )
        self._market_client = httpx.AsyncClient(
            base_url=self._account.effective_rest_base_url,
            timeout=None,
        )

    async def close(self) -> None:
        if self._distribution_backfill_task is not None:
            self._distribution_backfill_task.cancel()
            try:
                await self._distribution_backfill_task
            except asyncio.CancelledError:
                pass
            self._distribution_backfill_task = None
        if self._income_refresh_task is not None:
            self._income_refresh_task.cancel()
            try:
                await self._income_refresh_task
            except asyncio.CancelledError:
                pass
            self._income_refresh_task = None
        await self._papi_client.aclose()
        await self._sapi_client.aclose()
        await self._market_client.aclose()
        if self._owns_history_store:
            await self._history_store.close()

    async def _signed_request(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        timeout_s: float | None = None,
    ) -> Any:
        return await self._signed_request_with_client(self._papi_client, path, params, timeout_s=timeout_s)

    async def _signed_request_sapi(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        timeout_s: float | None = None,
    ) -> Any:
        return await self._signed_request_with_client(self._sapi_client, path, params, timeout_s=timeout_s)

    async def _public_request_market(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        timeout_s: float | None = None,
    ) -> Any:
        response = await self._market_client.get(path, params=params, timeout=timeout_s)
        response.raise_for_status()
        return response.json()

    async def _signed_request_with_client(
        self,
        client: httpx.AsyncClient,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        timeout_s: float | None = None,
    ) -> Any:
        params = dict(params or {})
        params["timestamp"] = int(datetime.now(UTC).timestamp() * 1000)
        params["recvWindow"] = self._settings.binance_recv_window_ms
        query = urlencode(params, doseq=True)
        signature = hmac.new(
            self._account.api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        response = await client.get(f"{path}?{query}&signature={signature}", timeout=timeout_s)
        response.raise_for_status()
        return response.json()

    async def get_unified_account_snapshot(
        self,
        *,
        history_window_days: int = 7,
        income_limit: int = 100,
        interest_limit: int = 100,
        previous_snapshot: dict[str, Any] | None = None,
        mark_price_provider: RefreshMarkPriceProvider | None = None,
    ) -> dict[str, Any]:
        if not self._account.api_key or not self._account.api_secret:
            raise MonitorGatewayError("Binance API credentials are not configured")

        bounded_window_days = max(1, min(history_window_days, 30))
        now = datetime.now(UTC)
        end_time = int(now.timestamp() * 1000)

        try:
            account_payload, um_account_payload = await asyncio.gather(
                self._signed_request_with_retry("/papi/v1/account", is_core=True),
                self._signed_request_with_retry("/papi/v1/um/account", is_core=True),
            )
        except RetriedRequestError as exc:
            raise MonitorGatewayError(f"Failed to fetch unified account snapshot: {exc}") from exc

        positions = self._parse_positions(um_account_payload.get("positions", []))
        await self._enrich_positions_with_mark_prices(
            positions,
            end_time_ms=end_time,
            previous_snapshot=previous_snapshot,
            mark_price_provider=mark_price_provider,
        )

        cached_income_summary = await self._history_store.summarize_income(
            self._account.account_id,
            bounded_window_days,
        )
        cached_income_summary["total_commission"] = self._extract_commission_total(cached_income_summary)
        income_task = self._ensure_income_refresh_task(
            history_window_days=bounded_window_days,
            income_limit=income_limit,
            end_time=end_time,
        )
        distribution_task = asyncio.create_task(
            self._refresh_distribution_summary(
                income_limit=income_limit,
                end_time=end_time,
            )
        )
        spot_task = asyncio.create_task(
            self._optional_request_sapi_with_retry(
                "/api/v3/account",
                {},
                label="spot account",
            )
        )
        distribution_summary, distribution_error = await distribution_task
        spot_account_payload, spot_error = await spot_task
        income_summary, income_error = await self._resolve_income_summary(
            cached_income_summary=cached_income_summary,
            income_task=income_task,
            history_window_days=bounded_window_days,
        )

        await self._ensure_distribution_backfill(backfill_limit=income_limit)
        interest_summary = self._compat_interest_summary(previous_snapshot, bounded_window_days)
        distribution_profit_summary = await self._build_distribution_profit_summary(
            equity=Decimal(account_payload.get("accountEquity") or "0"),
            now=now,
        )
        previous_spot_balances = self._extract_previous_spot_balances(previous_snapshot)
        if spot_account_payload is not None:
            spot_balances = self._parse_spot_balances(spot_account_payload)
        else:
            spot_balances = previous_spot_balances
        assets = self._parse_assets(um_account_payload.get("assets", []), spot_balances)

        unrealized_pnl = sum((entry["unrealized_pnl"] for entry in positions), Decimal("0"))
        if unrealized_pnl == Decimal("0") and assets:
            unrealized_pnl = sum((entry["cross_unrealized_pnl"] for entry in assets), Decimal("0"))

        section_errors: dict[str, Any] = {}
        if income_error is not None:
            section_errors["income_history"] = self._build_section_error(
                income_error,
                used_fallback=bool(income_summary["records"]),
                stale=bool(income_summary["records"]),
            )
        if distribution_error is not None:
            section_errors["distribution_history"] = self._build_section_error(
                distribution_error,
                used_fallback=bool(distribution_summary["records"]),
                stale=bool(distribution_summary["records"]),
            )
        if spot_error is not None:
            section_errors["spot_account"] = self._build_section_error(
                spot_error,
                used_fallback=bool(previous_spot_balances),
                stale=bool(previous_spot_balances),
            )

        equity = Decimal(account_payload.get("accountEquity") or "0")
        return {
            "status": "ok",
            "source": "papi",
            "account_id": self._account.account_id,
            "main_account_id": self._account.main_account_id,
            "main_account_name": self._account.main_account_name,
            "child_account_id": self._account.child_account_id,
            "child_account_name": self._account.child_account_name,
            "account_name": self._account.display_name,
            "account_status": account_payload.get("accountStatus", ""),
            "updated_at": now,
            "totals": {
                "equity": equity,
                "margin": Decimal(account_payload.get("accountInitialMargin") or "0"),
                "available_balance": Decimal(
                    account_payload.get("totalAvailableBalance")
                    or account_payload.get("virtualMaxWithdrawAmount")
                    or "0"
                ),
                "unrealized_pnl": unrealized_pnl,
                "total_income": income_summary["total_income"],
                "total_commission": self._extract_commission_total(income_summary),
                "total_distribution": distribution_summary["total_distribution"],
                "distribution_apy_7d": self._calculate_distribution_apy(
                    distribution_summary["total_distribution"],
                    equity,
                    DISTRIBUTION_WINDOW_DAYS,
                ),
                "total_interest": interest_summary["total_interest"],
            },
            "positions": positions,
            "assets": assets,
            "income_summary": income_summary,
            "distribution_summary": distribution_summary,
            "distribution_profit_summary": distribution_profit_summary,
            "interest_summary": interest_summary,
            "section_errors": section_errors,
        }

    async def _refresh_distribution_summary(
        self,
        *,
        income_limit: int,
        end_time: int,
    ) -> tuple[dict[str, Any], dict[str, Any] | None]:
        distribution_error = await self._refresh_distribution_history(income_limit, end_time)
        distribution_summary = await self._history_store.summarize_distribution(
            self._account.account_id,
            DISTRIBUTION_WINDOW_DAYS,
        )
        return distribution_summary, distribution_error

    async def _refresh_income_summary(
        self,
        *,
        history_window_days: int,
        income_limit: int,
        end_time: int,
    ) -> tuple[dict[str, Any], dict[str, Any] | None]:
        income_error = await self._refresh_income_history(history_window_days, income_limit, end_time)
        income_summary = await self._history_store.summarize_income(self._account.account_id, history_window_days)
        income_summary["total_commission"] = self._extract_commission_total(income_summary)
        return income_summary, income_error

    def _ensure_income_refresh_task(
        self,
        *,
        history_window_days: int,
        income_limit: int,
        end_time: int,
    ) -> asyncio.Task[dict[str, Any] | None]:
        if self._income_refresh_task is None or self._income_refresh_task.done():
            self._income_refresh_task = asyncio.create_task(
                self._refresh_income_history(history_window_days, income_limit, end_time)
            )
            self._income_refresh_task.add_done_callback(self._clear_income_refresh_task)
        return self._income_refresh_task

    def _clear_income_refresh_task(self, task: asyncio.Task[dict[str, Any] | None]) -> None:
        if self._income_refresh_task is task:
            self._income_refresh_task = None

    async def _resolve_income_summary(
        self,
        *,
        cached_income_summary: dict[str, Any],
        income_task: asyncio.Task[dict[str, Any] | None],
        history_window_days: int,
    ) -> tuple[dict[str, Any], dict[str, Any] | None]:
        if income_task.done() or not int(cached_income_summary.get("records") or 0):
            income_error = await income_task
            income_summary = await self._history_store.summarize_income(self._account.account_id, history_window_days)
            income_summary["total_commission"] = self._extract_commission_total(income_summary)
            return income_summary, income_error
        return cached_income_summary, None

    async def _build_distribution_profit_summary(
        self,
        *,
        equity: Decimal,
        now: datetime,
    ) -> dict[str, Any]:
        period_starts = self._distribution_period_starts(now)
        distribution_periods = await self._history_store.summarize_distribution_periods(
            self._account.account_id,
            period_starts,
        )
        earliest_event_time_ms = distribution_periods.get("earliest_event_time_ms")
        amounts = distribution_periods.get("amounts") or {}
        backfill_complete = await self._history_store.is_distribution_backfill_complete(self._account.account_id)

        summary: dict[str, Any] = {}
        for key, label in (
            ("today", "今日收益丨收益率"),
            ("week", "本周收益丨收益率"),
            ("month", "本月收益丨收益率"),
            ("year", "年度收益丨收益率"),
            ("all", "全部收益丨收益率"),
        ):
            amount = Decimal(str(amounts.get(key) or "0"))
            start_at = self._distribution_period_start_at(key, period_starts, earliest_event_time_ms)
            summary[key] = {
                "label": label,
                "amount": amount,
                "rate": self._calculate_ratio(amount, equity),
                "start_at": start_at,
                "complete": self._distribution_period_complete(
                    key,
                    period_starts.get(key),
                    earliest_event_time_ms,
                    backfill_complete,
                ),
            }
        summary["backfill_complete"] = backfill_complete
        return summary

    async def _ensure_distribution_backfill(self, *, backfill_limit: int) -> None:
        if self._distribution_backfill_task is not None and not self._distribution_backfill_task.done():
            return
        if await self._history_store.is_distribution_backfill_complete(self._account.account_id):
            return
        self._distribution_backfill_task = asyncio.create_task(
            self._run_distribution_backfill(backfill_limit=backfill_limit)
        )

    async def _run_distribution_backfill(self, *, backfill_limit: int) -> None:
        try:
            backfill_window_ms = int(timedelta(days=BACKFILL_QUERY_WINDOW_DAYS).total_seconds() * 1000)
            pending_end_time_ms: int | None = None
            while True:
                if pending_end_time_ms is None:
                    earliest_event_time_ms = await self._history_store.get_earliest_event_time_ms(
                        self._account.account_id,
                        HISTORY_SOURCE_DISTRIBUTION,
                    )
                    query_end_time = (
                        int(datetime.now(UTC).timestamp() * 1000)
                        if earliest_event_time_ms is None
                        else max(earliest_event_time_ms - 1, 0)
                    )
                else:
                    earliest_event_time_ms = await self._history_store.get_earliest_event_time_ms(
                        self._account.account_id,
                        HISTORY_SOURCE_DISTRIBUTION,
                    )
                    query_end_time = pending_end_time_ms
                    pending_end_time_ms = None
                query_start_time = max(query_end_time - backfill_window_ms + 1, 0)
                payload, error = await self._optional_request_sapi_with_retry(
                    "/sapi/v1/asset/assetDividend",
                    {
                        "startTime": query_start_time,
                        "endTime": query_end_time,
                        "limit": backfill_limit,
                    },
                    label="distribution backfill",
                )
                if payload is None:
                    logger.warning(
                        "Distribution backfill failed for %s in window %s-%s: %s",
                        self._account.account_id,
                        query_start_time,
                        query_end_time,
                        error,
                    )
                    return
                events = self._build_distribution_events(payload, default_event_time_ms=query_end_time)
                if not events:
                    if query_start_time == 0:
                        logger.info(
                            "Distribution backfill completed for %s with empty oldest window ending at %s",
                            self._account.account_id,
                            query_end_time,
                        )
                        await self._history_store.set_distribution_backfill_complete(
                            self._account.account_id,
                            completed=True,
                            updated_at_ms=query_end_time,
                        )
                        return
                    logger.info(
                        "Distribution backfill found no rows for %s in window %s-%s, stepping earlier",
                        self._account.account_id,
                        query_start_time,
                        query_end_time,
                    )
                    pending_end_time_ms = query_start_time - 1
                    continue
                oldest_event_time_ms = min(event.event_time_ms for event in events)
                await self._history_store.record_history_batch(
                    self._account.account_id,
                    HISTORY_SOURCE_DISTRIBUTION,
                    events,
                    last_successful_end_time=None,
                    retain_after_ms=None,
                    update_fetch_state=False,
                )
                if oldest_event_time_ms >= earliest_event_time_ms:
                    logger.info(
                        "Distribution backfill completed for %s after duplicate boundary page at %s",
                        self._account.account_id,
                        oldest_event_time_ms,
                    )
                    await self._history_store.set_distribution_backfill_complete(
                        self._account.account_id,
                        completed=True,
                        updated_at_ms=query_end_time,
                    )
                    return
                logger.info(
                    "Distribution backfill stored %s older rows for %s, next earliest %s",
                    len(events),
                    self._account.account_id,
                    oldest_event_time_ms,
                )
                if oldest_event_time_ms <= 0:
                    await self._history_store.set_distribution_backfill_complete(
                        self._account.account_id,
                        completed=True,
                        updated_at_ms=query_end_time,
                    )
                    return
                pending_end_time_ms = oldest_event_time_ms - 1
        finally:
            self._distribution_backfill_task = None

    def _extract_commission_total(self, income_summary: dict[str, Any] | None) -> Decimal:
        if not income_summary:
            return Decimal("0")
        by_type = income_summary.get("by_type") or {}
        return Decimal(str(by_type.get("COMMISSION") or "0"))

    def _compat_income_summary(
        self,
        previous_snapshot: dict[str, Any] | None,
        history_window_days: int,
    ) -> dict[str, Any]:
        summary = self._previous_section_summary(previous_snapshot, "income_summary")
        if summary is not None:
            return summary
        return {
            "window_days": history_window_days,
            "records": 0,
            "total_income": Decimal("0"),
            "total_commission": Decimal("0"),
            "by_type": {},
            "by_asset": {},
        }

    def _compat_interest_summary(
        self,
        previous_snapshot: dict[str, Any] | None,
        history_window_days: int,
    ) -> dict[str, Any]:
        summary = self._previous_section_summary(previous_snapshot, "interest_summary")
        if summary is not None:
            return summary
        return {
            "window_days": history_window_days,
            "records": 0,
            "margin_interest_total": Decimal("0"),
            "negative_balance_interest_total": Decimal("0"),
            "total_interest": Decimal("0"),
        }

    async def _refresh_income_history(
        self,
        history_window_days: int,
        income_limit: int,
        end_time: int,
    ) -> dict[str, Any] | None:
        start_time = await self._history_start_time(
            HISTORY_SOURCE_INCOME,
            history_window_days=history_window_days,
            end_time=end_time,
        )
        payload, error = await self._optional_request_with_retry(
            "/papi/v1/um/income",
            {"startTime": start_time, "endTime": end_time, "limit": income_limit},
            label="income history",
        )
        if payload is None:
            return error

        events = self._build_income_events(payload, default_event_time_ms=end_time)
        await self._history_store.record_history_batch(
            self._account.account_id,
            HISTORY_SOURCE_INCOME,
            events,
            last_successful_end_time=end_time,
            retain_after_ms=self._retention_start_ms(history_window_days),
        )
        return None

    async def _refresh_distribution_history(
        self,
        income_limit: int,
        end_time: int,
    ) -> dict[str, Any] | None:
        start_time = await self._history_start_time(
            HISTORY_SOURCE_DISTRIBUTION,
            history_window_days=DISTRIBUTION_WINDOW_DAYS,
            end_time=end_time,
        )
        payload, error = await self._optional_request_sapi_with_retry(
            "/sapi/v1/asset/assetDividend",
            {"startTime": start_time, "endTime": end_time, "limit": income_limit},
            label="distribution history",
        )
        if payload is None:
            return error

        events = self._build_distribution_events(payload, default_event_time_ms=end_time)
        await self._history_store.record_history_batch(
            self._account.account_id,
            HISTORY_SOURCE_DISTRIBUTION,
            events,
            last_successful_end_time=end_time,
            retain_after_ms=None,
        )
        return None

    async def _refresh_interest_source(
        self,
        *,
        source: str,
        path: str,
        size: int,
        history_window_days: int,
        end_time: int,
    ) -> dict[str, Any] | None:
        start_time = await self._history_start_time(
            source,
            history_window_days=history_window_days,
            end_time=end_time,
        )
        payload, error = await self._optional_request_with_retry(
            path,
            {"startTime": start_time, "endTime": end_time, "size": size},
            label=source.replace("_", " "),
        )
        if payload is None:
            return error

        events = self._build_interest_events(payload, default_event_time_ms=end_time, source=source)
        await self._history_store.record_history_batch(
            self._account.account_id,
            source,
            events,
            last_successful_end_time=end_time,
            retain_after_ms=self._retention_start_ms(history_window_days),
        )
        return None

    async def _history_start_time(self, source: str, *, history_window_days: int, end_time: int) -> int:
        last_successful_end_time = await self._history_store.get_last_successful_end_time(
            self._account.account_id,
            source,
        )
        if last_successful_end_time is not None:
            return last_successful_end_time + 1
        return max(end_time - int(timedelta(days=history_window_days).total_seconds() * 1000), 0)

    def _retention_start_ms(self, history_window_days: int) -> int:
        now = datetime.now(UTC) - timedelta(days=max(history_window_days, 1))
        return int(now.timestamp() * 1000)

    async def _signed_request_with_retry(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        is_core: bool,
    ) -> Any:
        timeout_s, max_attempts, retry_delays = self._retry_budget(is_core=is_core)
        return await self._request_with_retry(
            label=path,
            operation=lambda: self._signed_request(path, params, timeout_s=timeout_s),
            max_attempts=max_attempts,
            retry_delays=retry_delays,
        )

    async def _signed_request_sapi_with_retry(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        is_core: bool = False,
    ) -> Any:
        timeout_s, max_attempts, retry_delays = self._retry_budget(is_core=is_core)
        return await self._request_with_retry(
            label=path,
            operation=lambda: self._signed_request_sapi(path, params, timeout_s=timeout_s),
            max_attempts=max_attempts,
            retry_delays=retry_delays,
        )

    async def _public_request_market_with_retry(self, path: str, params: dict[str, Any] | None = None) -> Any:
        timeout_s, max_attempts, retry_delays = self._retry_budget(is_core=False)
        return await self._request_with_retry(
            label=path,
            operation=lambda: self._public_request_market(path, params, timeout_s=timeout_s),
            max_attempts=max_attempts,
            retry_delays=retry_delays,
        )

    async def _optional_request_with_retry(
        self,
        path: str,
        params: dict[str, Any],
        *,
        label: str,
    ) -> tuple[Any, dict[str, Any] | None]:
        try:
            payload = await self._signed_request_with_retry(path, params, is_core=False)
            return payload, None
        except RetriedRequestError as exc:
            return None, {"message": str(exc), "attempts": exc.attempts}

    async def _optional_request_sapi_with_retry(
        self,
        path: str,
        params: dict[str, Any],
        *,
        label: str,
    ) -> tuple[Any, dict[str, Any] | None]:
        try:
            payload = await self._signed_request_sapi_with_retry(path, params, is_core=False)
            return payload, None
        except RetriedRequestError as exc:
            return None, {"message": str(exc), "attempts": exc.attempts}

    async def _request_with_retry(
        self,
        *,
        label: str,
        operation: Callable[[], Awaitable[Any]],
        max_attempts: int,
        retry_delays: tuple[float, ...],
    ) -> Any:
        last_error: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                return await operation()
            except Exception as exc:
                last_error = exc
                should_retry = attempt < max_attempts and self._is_retryable_error(exc)
                if not should_retry:
                    raise RetriedRequestError(
                        f"{label} failed after {attempt} attempts: {exc}",
                        attempts=attempt,
                        cause=exc,
                    ) from exc
                delay = retry_delays[min(attempt - 1, len(retry_delays) - 1)] if retry_delays else 0
                if delay > 0:
                    await asyncio.sleep(delay)

        assert last_error is not None
        raise RetriedRequestError(
            f"{label} failed after {max_attempts} attempts: {last_error}",
            attempts=max_attempts,
            cause=last_error,
        ) from last_error

    def _is_retryable_error(self, exc: Exception) -> bool:
        if isinstance(exc, (httpx.TimeoutException, httpx.NetworkError)):
            return True
        if isinstance(exc, httpx.HTTPStatusError):
            return exc.response.status_code in {408, 409, 429, 500, 502, 503, 504}
        return False

    def _retry_budget(self, *, is_core: bool) -> tuple[float, int, tuple[float, ...]]:
        if is_core:
            return (
                max(self._settings.binance_core_timeout_ms, 1) / 1000,
                max(1, self._settings.binance_core_retry_attempts),
                CORE_RETRY_DELAYS_SECONDS,
            )
        return (
            max(self._settings.binance_secondary_timeout_ms, 1) / 1000,
            max(1, self._settings.binance_secondary_retry_attempts),
            SECONDARY_RETRY_DELAYS_SECONDS,
        )

    def _parse_positions(self, payload: list[dict[str, Any]]) -> list[dict[str, Any]]:
        positions: list[dict[str, Any]] = []
        for item in payload:
            position_amt = Decimal(item.get("positionAmt") or "0")
            pnl = Decimal(item.get("unrealizedProfit") or item.get("unRealizedProfit") or "0")
            if position_amt == Decimal("0") and pnl == Decimal("0"):
                continue
            position_side = item.get("positionSide") or ("LONG" if position_amt > 0 else "SHORT")
            positions.append(
                {
                    "symbol": item.get("symbol", ""),
                    "position_side": position_side,
                    "qty": abs(position_amt),
                    "entry_price": Decimal(item.get("entryPrice") or "0"),
                    "mark_price": Decimal(item.get("markPrice") or "0"),
                    "unrealized_pnl": pnl,
                    "notional": abs(Decimal(item.get("notional") or "0")),
                    "leverage": int(item.get("leverage") or 0),
                    "liquidation_price": Decimal(item.get("liquidationPrice") or "0"),
                }
            )
        positions.sort(key=lambda entry: (entry["symbol"], entry["position_side"]))
        return positions

    async def _enrich_positions_with_mark_prices(
        self,
        positions: list[dict[str, Any]],
        *,
        end_time_ms: int,
        previous_snapshot: dict[str, Any] | None,
        mark_price_provider: RefreshMarkPriceProvider | None,
    ) -> None:
        symbols = sorted({str(position.get("symbol") or "") for position in positions if position.get("symbol")})
        if not symbols:
            return

        provider = mark_price_provider or RefreshMarkPriceProvider()
        current_prices = await provider.get_mark_prices(symbols, self._fetch_all_mark_prices)
        if current_prices:
            await self._history_store.save_mark_prices(current_prices, updated_at_ms=end_time_ms)
        stored_prices = await self._history_store.get_mark_prices(symbols)
        previous_prices = self._extract_previous_mark_prices(previous_snapshot)

        for position in positions:
            symbol = str(position.get("symbol") or "")
            mark_price = current_prices.get(symbol) or stored_prices.get(symbol) or previous_prices.get(symbol)
            if mark_price is not None and mark_price > Decimal("0"):
                position["mark_price"] = mark_price

    async def _fetch_all_mark_prices(self) -> dict[str, Decimal]:
        payload = await self._public_request_market_with_retry("/fapi/v1/premiumIndex")
        rows = payload if isinstance(payload, list) else [payload] if isinstance(payload, dict) else []
        prices: dict[str, Decimal] = {}
        for item in rows:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol") or "")
            if not symbol:
                continue
            mark_price = Decimal(str(item.get("markPrice") or "0"))
            if mark_price > Decimal("0"):
                prices[symbol] = mark_price
        if not prices:
            raise MonitorGatewayError("No valid mark prices returned")
        return prices

    def _extract_previous_mark_prices(self, previous_snapshot: dict[str, Any] | None) -> dict[str, Decimal]:
        if not isinstance(previous_snapshot, dict):
            return {}
        prices: dict[str, Decimal] = {}
        for item in previous_snapshot.get("positions", []) or []:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol") or "")
            if not symbol:
                continue
            price = Decimal(str(item.get("mark_price") or "0"))
            if price > Decimal("0"):
                prices[symbol] = price
        return prices

    def _parse_assets(
        self,
        payload: list[dict[str, Any]],
        spot_balances: dict[str, Decimal] | None = None,
    ) -> list[dict[str, Any]]:
        assets: list[dict[str, Any]] = []
        spot_balances = dict(spot_balances or {})
        for item in payload:
            asset = str(item.get("asset") or "")
            spot_balance = spot_balances.pop(asset, Decimal("0"))
            wallet_balance = Decimal(item.get("crossWalletBalance") or "0") + spot_balance
            cross_unrealized_pnl = Decimal(item.get("crossUnPnl") or "0")
            if wallet_balance == Decimal("0") and cross_unrealized_pnl == Decimal("0"):
                continue
            assets.append(
                {
                    "asset": asset,
                    "wallet_balance": wallet_balance,
                    "cross_wallet_balance": Decimal(item.get("crossWalletBalance") or "0"),
                    "cross_unrealized_pnl": cross_unrealized_pnl,
                    "available_balance": Decimal(item.get("availableBalance") or "0"),
                    "initial_margin": Decimal(item.get("initialMargin") or "0"),
                    "maintenance_margin": Decimal(item.get("maintMargin") or "0"),
                    "margin_balance": Decimal(item.get("marginBalance") or "0"),
                    "max_withdraw_amount": Decimal(item.get("maxWithdrawAmount") or "0"),
                }
            )
        for asset, spot_balance in spot_balances.items():
            if spot_balance == Decimal("0"):
                continue
            assets.append(
                {
                    "asset": asset,
                    "wallet_balance": spot_balance,
                    "cross_wallet_balance": Decimal("0"),
                    "cross_unrealized_pnl": Decimal("0"),
                    "available_balance": spot_balance,
                    "initial_margin": Decimal("0"),
                    "maintenance_margin": Decimal("0"),
                    "margin_balance": Decimal("0"),
                    "max_withdraw_amount": spot_balance,
                }
            )
        assets.sort(key=lambda entry: entry["asset"])
        return assets

    def _extract_previous_spot_balances(self, previous_snapshot: dict[str, Any] | None) -> dict[str, Decimal]:
        if not isinstance(previous_snapshot, dict):
            return {}
        balances: dict[str, Decimal] = {}
        for item in previous_snapshot.get("assets", []) or []:
            if not isinstance(item, dict):
                continue
            asset = str(item.get("asset") or "")
            if not asset:
                continue
            wallet_balance = Decimal(str(item.get("wallet_balance") or "0"))
            cross_wallet_balance = Decimal(str(item.get("cross_wallet_balance") or "0"))
            spot_balance = wallet_balance - cross_wallet_balance
            if spot_balance != Decimal("0"):
                balances[asset] = spot_balance
        return balances

    def _previous_section_summary(
        self,
        previous_snapshot: dict[str, Any] | None,
        key: str,
    ) -> dict[str, Any] | None:
        if not isinstance(previous_snapshot, dict):
            return None
        summary = previous_snapshot.get(key)
        if not isinstance(summary, dict):
            return None
        return self._normalize_summary_values(summary)

    def _normalize_summary_values(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {key: self._normalize_summary_values(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._normalize_summary_values(item) for item in value]
        if isinstance(value, Decimal):
            return value
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return Decimal(str(value))
        if isinstance(value, str):
            try:
                return Decimal(value)
            except Exception:
                return value
        return value

    def _parse_spot_balances(self, payload: Any) -> dict[str, Decimal]:
        if not isinstance(payload, dict):
            return {}
        balances = payload.get("balances")
        if not isinstance(balances, list):
            return {}
        spot_balances: dict[str, Decimal] = {}
        for item in balances:
            if not isinstance(item, dict):
                continue
            asset = str(item.get("asset") or "")
            if not asset:
                continue
            balance = Decimal(item.get("free") or "0") + Decimal(item.get("locked") or "0")
            if balance != Decimal("0"):
                spot_balances[asset] = balance
        return spot_balances

    def _build_income_events(self, payload: Any, *, default_event_time_ms: int) -> list[HistoryEvent]:
        rows = payload if isinstance(payload, list) else []
        events: list[HistoryEvent] = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            income_type = str(item.get("incomeType") or "UNKNOWN")
            if self._is_excluded_income_type(income_type):
                continue
            amount = Decimal(item.get("income") or "0")
            asset = str(item.get("asset") or "UNKNOWN")
            event_time_ms = self._coerce_event_time(item, default_event_time_ms)
            events.append(
                HistoryEvent(
                    source=HISTORY_SOURCE_INCOME,
                    event_time_ms=event_time_ms,
                    unique_key=self._history_unique_key(HISTORY_SOURCE_INCOME, item),
                    asset=asset,
                    amount=amount,
                    event_type=income_type,
                    payload=item,
                )
            )
        return events

    def _build_distribution_events(self, payload: Any, *, default_event_time_ms: int) -> list[HistoryEvent]:
        rows = self._extract_rows(payload)
        events: list[HistoryEvent] = []
        for item in rows:
            amount = Decimal(item.get("amount") or "0")
            asset = str(item.get("asset") or "UNKNOWN")
            info = str(item.get("enInfo") or item.get("info") or "DISTRIBUTION")
            event_time_ms = self._coerce_event_time(item, default_event_time_ms)
            events.append(
                HistoryEvent(
                    source=HISTORY_SOURCE_DISTRIBUTION,
                    event_time_ms=event_time_ms,
                    unique_key=self._history_unique_key(HISTORY_SOURCE_DISTRIBUTION, item),
                    asset=asset,
                    amount=amount,
                    event_type=info,
                    payload=item,
                )
            )
        return events

    def _build_interest_events(
        self,
        payload: Any,
        *,
        default_event_time_ms: int,
        source: str,
    ) -> list[HistoryEvent]:
        rows = self._extract_rows(payload)
        events: list[HistoryEvent] = []
        event_type = "margin_interest" if source == HISTORY_SOURCE_MARGIN_INTEREST else "negative_balance_interest"
        for item in rows:
            if not isinstance(item, dict):
                continue
            amount = Decimal(item.get("interest") or item.get("amount") or "0")
            asset = str(item.get("asset") or item.get("currency") or "UNKNOWN")
            event_time_ms = self._coerce_event_time(item, default_event_time_ms)
            events.append(
                HistoryEvent(
                    source=source,
                    event_time_ms=event_time_ms,
                    unique_key=self._history_unique_key(source, item),
                    asset=asset,
                    amount=amount,
                    event_type=event_type,
                    payload=item,
                )
            )
        return events

    def _history_unique_key(self, source: str, payload: dict[str, Any]) -> str:
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
        digest = hashlib.sha256(f"{source}:{encoded}".encode("utf-8")).hexdigest()
        return digest

    def _coerce_event_time(self, payload: dict[str, Any], default_event_time_ms: int) -> int:
        for key in ("time", "divTime", "tranIdTime", "interestAccuredTime", "timestamp", "updateTime"):
            value = payload.get(key)
            if value is None:
                continue
            try:
                numeric = int(str(value))
            except (TypeError, ValueError):
                continue
            if numeric > 0:
                return numeric
        return default_event_time_ms

    def _is_excluded_income_type(self, income_type: str) -> bool:
        normalized = income_type.upper()
        return any(keyword in normalized for keyword in EXCLUDED_INCOME_TYPE_KEYWORDS)

    def _distribution_period_starts(self, now: datetime) -> dict[str, int | None]:
        local_now = now.astimezone(BEIJING_TZ)
        today_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        week_start = today_start - timedelta(days=today_start.weekday())
        month_start = today_start.replace(day=1)
        year_start = today_start.replace(month=1, day=1)
        return {
            "today": int(today_start.astimezone(UTC).timestamp() * 1000),
            "week": int(week_start.astimezone(UTC).timestamp() * 1000),
            "month": int(month_start.astimezone(UTC).timestamp() * 1000),
            "year": int(year_start.astimezone(UTC).timestamp() * 1000),
            "all": None,
        }

    def _distribution_period_start_at(
        self,
        key: str,
        period_starts: dict[str, int | None],
        earliest_event_time_ms: int | None,
    ) -> str | None:
        if key == "all":
            if earliest_event_time_ms is None:
                return None
            return datetime.fromtimestamp(earliest_event_time_ms / 1000, UTC).isoformat()
        start_ms = period_starts.get(key)
        if start_ms is None:
            return None
        return datetime.fromtimestamp(start_ms / 1000, UTC).isoformat()

    def _distribution_period_complete(
        self,
        key: str,
        start_ms: int | None,
        earliest_event_time_ms: int | None,
        backfill_complete: bool,
    ) -> bool:
        if backfill_complete:
            return True
        if earliest_event_time_ms is None:
            return False
        if key == "all":
            return False
        if start_ms is None:
            return False
        return earliest_event_time_ms <= start_ms

    def _calculate_ratio(self, amount: Decimal, equity: Decimal) -> Decimal:
        if equity <= Decimal("0"):
            return Decimal("0")
        return amount / equity

    def _calculate_distribution_apy(
        self,
        total_distribution: Decimal,
        equity: Decimal,
        history_window_days: int,
    ) -> Decimal:
        window_days = Decimal(str(max(history_window_days, 1)))
        if equity <= Decimal("0"):
            return Decimal("0")
        return (total_distribution / equity) * (ANNUALIZATION_DAYS / window_days)

    def _extract_rows(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            rows = payload.get("rows")
            if isinstance(rows, list):
                return [item for item in rows if isinstance(item, dict)]
            data = payload.get("data")
            if isinstance(data, list):
                return [item for item in data if isinstance(item, dict)]
        return []

    def _build_section_error(
        self,
        error: dict[str, Any],
        *,
        used_fallback: bool,
        stale: bool,
    ) -> dict[str, Any]:
        return {
            "message": error["message"],
            "attempts": int(error.get("attempts") or 1),
            "used_fallback": used_fallback,
            "stale": stale,
        }
