from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from time import perf_counter
from typing import Any, Awaitable, Callable
from urllib.parse import urlencode
from uuid import uuid4

import httpx

from monitor_app.config import MainAccountConfig, MonitorAccountConfig, Settings


logger = logging.getLogger("uvicorn.error")


class FundingTransferError(RuntimeError):
    pass


class FundingTransferRequestError(FundingTransferError):
    def __init__(
        self,
        message: str,
        *,
        label: str,
        attempts: int,
        duration_ms: int,
        timeout_s: float,
        source: str,
        error_type: str,
        status_code: int | None = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.label = label
        self.attempts = attempts
        self.duration_ms = duration_ms
        self.timeout_s = timeout_s
        self.source = source
        self.error_type = error_type
        self.status_code = status_code
        self.retryable = retryable


@dataclass(frozen=True, slots=True)
class BinanceCredentials:
    api_key: str
    api_secret: str


class FundingTransferService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._clients: dict[str, httpx.AsyncClient] = {}
        self._lock = asyncio.Lock()

    async def close(self) -> None:
        async with self._lock:
            clients = list(self._clients.values())
            self._clients.clear()
        for client in clients:
            await client.aclose()

    async def get_group_overview(self, main_id: str) -> dict[str, Any]:
        request_id = self._new_request_id()
        started_at = perf_counter()
        main_account = self._get_main_account(main_id)
        timings: dict[str, Any] = {}
        error_context: dict[str, Any] = {}

        main_reason = ""
        email_by_uid: dict[str, str] = {}
        main_spot_assets: list[dict[str, str]] = []

        if main_account.has_transfer_credentials:
            try:
                email_started_at = perf_counter()
                email_by_uid = await self._get_sub_account_email_map(main_account)
                timings["email_map_ms"] = int((perf_counter() - email_started_at) * 1000)

                main_spot_started_at = perf_counter()
                main_spot_assets = await self._fetch_spot_assets(self._main_credentials(main_account))
                timings["main_spot_query_ms"] = int((perf_counter() - main_spot_started_at) * 1000)
            except Exception as exc:
                main_reason = f"Main transfer API unavailable: {exc}"
                error_context["main_account_query"] = self._error_context_payload(exc)
                logger.warning(
                    "Funding overview main account query failed request_id=%s main_id=%s error=%s",
                    request_id,
                    main_account.main_id,
                    exc,
                )
        else:
            main_reason = "Main transfer API is not configured for this group (Excel requires an account_id=main row)."

        children_started_at = perf_counter()
        children = await asyncio.gather(
            *(self._build_child_overview(child, email_by_uid, main_ready=main_reason == "") for child in main_account.children)
        )
        timings["children_query_ms"] = int((perf_counter() - children_started_at) * 1000)

        main_spot_available = self._spot_available_map(main_spot_assets)
        assets = sorted(
            {
                *main_spot_available.keys(),
                *(asset for child in children for asset in child["spot_available"].keys()),
            }
        )
        any_executable = any(child["can_distribute"] or child["can_collect"] for child in children)
        available = main_reason == "" and any_executable
        reason = main_reason or (
            "" if any_executable else "No eligible sub-account is available for this group. Check UID, sub-account API, or spot balances."
        )

        timings["total_ms"] = int((perf_counter() - started_at) * 1000)
        logger.info(
            "Funding overview resolved request_id=%s main_id=%s available=%s children=%s total_ms=%s",
            request_id,
            main_account.main_id,
            available,
            len(children),
            timings["total_ms"],
        )

        return {
            "main_account_id": main_account.main_id,
            "main_account_name": main_account.name,
            "available": available,
            "reason": reason,
            "assets": assets,
            "request_id": request_id,
            "timings": timings,
            "error_context": error_context,
            "main_account": {
                "uid": main_account.transfer_uid,
                "transfer_ready": main_reason == "",
                "reason": main_reason,
                "spot_assets": main_spot_assets,
                "spot_available": main_spot_available,
                "funding_assets": main_spot_assets,
                "funding_available": main_spot_available,
            },
            "children": children,
            "updated_at": datetime.now(UTC).isoformat(),
        }

    async def distribute(self, main_id: str, *, asset: str, transfers: list[dict[str, Any]]) -> dict[str, Any]:
        request_id = self._new_request_id()
        started_at = perf_counter()
        timings: dict[str, Any] = {}
        error_context: dict[str, Any] = {}

        normalized_asset = self._normalize_asset(asset)
        main_account = self._get_main_account(main_id)
        if not main_account.has_transfer_credentials:
            raise FundingTransferError("Main transfer API is not configured for this group")

        try:
            email_started_at = perf_counter()
            email_by_uid = await self._get_sub_account_email_map(main_account)
            timings["email_map_ms"] = int((perf_counter() - email_started_at) * 1000)
        except Exception as exc:
            error_context["email_map"] = self._error_context_payload(exc)
            raise FundingTransferError(f"Failed to query sub-account email mapping: {exc}") from exc

        child_by_id = {child.account_id: child for child in main_account.children}
        main_credentials = self._main_credentials(main_account)
        try:
            main_spot_started_at = perf_counter()
            main_spot_assets = await self._fetch_spot_assets(main_credentials)
            timings["main_spot_query_ms"] = int((perf_counter() - main_spot_started_at) * 1000)
        except Exception as exc:
            error_context["main_spot_query"] = self._error_context_payload(exc)
            raise FundingTransferError(f"Failed to query main account spot balances: {exc}") from exc

        main_available = Decimal(self._spot_available_map(main_spot_assets).get(normalized_asset, "0"))
        executable: list[tuple[MonitorAccountConfig, str, Decimal]] = []
        total_amount = Decimal("0")
        for item in transfers:
            account_id = str(item.get("account_id") or "").strip().lower()
            amount = self._parse_positive_amount(item.get("amount"), field_name=f"amount for {account_id}")
            if amount <= Decimal("0"):
                continue
            child = child_by_id.get(account_id)
            if child is None:
                raise FundingTransferError(f"Unknown sub-account: {account_id}")
            child_email = self._resolve_child_email(child, email_by_uid)
            executable.append((child, child_email, amount))
            total_amount += amount

        if not executable:
            raise FundingTransferError("Select at least one sub-account and enter a distribute amount greater than 0")
        if total_amount > main_available:
            raise FundingTransferError(
                f"Main account spot balance is insufficient: {normalized_asset} only has {self._format_decimal(main_available)} available"
            )

        transfer_started_at = perf_counter()
        results = [
            await self._distribute_to_child(
                main_credentials=main_credentials,
                child=child,
                child_email=child_email,
                asset=normalized_asset,
                amount=amount,
            )
            for child, child_email, amount in executable
        ]
        timings["transfer_ms"] = int((perf_counter() - transfer_started_at) * 1000)

        overview_started_at = perf_counter()
        overview = await self.get_group_overview(main_id)
        timings["overview_refresh_ms"] = int((perf_counter() - overview_started_at) * 1000)
        timings["total_ms"] = int((perf_counter() - started_at) * 1000)

        success_count, failure_count = self._operation_counts(results)
        logger.info(
            "Funding transfer completed request_id=%s direction=distribute main_id=%s asset=%s child_count=%s success_count=%s failure_count=%s total_ms=%s",
            request_id,
            main_id,
            normalized_asset,
            len(executable),
            success_count,
            failure_count,
            timings["total_ms"],
        )

        return {
            "direction": "distribute",
            "asset": normalized_asset,
            "results": results,
            "overview": overview,
            "request_id": request_id,
            "timings": timings,
            "error_context": error_context,
            "message": self._summarize_operation("Distribute", results),
        }

    async def collect(
        self,
        main_id: str,
        *,
        asset: str,
        transfers: list[dict[str, Any]] | None = None,
        account_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        request_id = self._new_request_id()
        started_at = perf_counter()
        timings: dict[str, Any] = {}
        error_context: dict[str, Any] = {}

        normalized_asset = self._normalize_asset(asset)
        main_account = self._get_main_account(main_id)
        if not main_account.has_transfer_credentials:
            raise FundingTransferError("Main transfer API is not configured for this group")

        try:
            email_started_at = perf_counter()
            email_by_uid = await self._get_sub_account_email_map(main_account)
            timings["email_map_ms"] = int((perf_counter() - email_started_at) * 1000)
        except Exception as exc:
            error_context["email_map"] = self._error_context_payload(exc)
            raise FundingTransferError(f"Failed to query sub-account email mapping: {exc}") from exc

        child_by_id = {child.account_id: child for child in main_account.children}
        main_credentials = self._main_credentials(main_account)
        requested_transfers = list(transfers or [])
        legacy_account_ids = [
            str(account_id or "").strip().lower()
            for account_id in (account_ids or [])
            if str(account_id or "").strip()
        ]

        results: list[dict[str, Any]] = []
        if requested_transfers:
            executable: list[tuple[MonitorAccountConfig, str, Decimal]] = []
            collectable_started_at = perf_counter()
            for item in requested_transfers:
                account_id = str(item.get("account_id") or "").strip().lower()
                amount = self._parse_positive_amount(item.get("amount"), field_name=f"amount for {account_id}")
                if amount <= Decimal("0"):
                    continue
                child = child_by_id.get(account_id)
                if child is None:
                    raise FundingTransferError(f"Unknown sub-account: {account_id}")
                child_email = self._resolve_child_email(child, email_by_uid)
                available_amount = await self._collectable_amount(child, normalized_asset)
                if amount > available_amount:
                    raise FundingTransferError(
                        f"Sub-account {child.child_account_name or child.account_id} can collect at most {self._format_decimal(available_amount)} {normalized_asset}"
                    )
                executable.append((child, child_email, amount))
            timings["collectable_query_ms"] = int((perf_counter() - collectable_started_at) * 1000)

            if not executable:
                raise FundingTransferError("Select at least one sub-account and enter a collect amount greater than 0")

            transfer_started_at = perf_counter()
            results = [
                await self._collect_from_child(
                    main_credentials=main_credentials,
                    child=child,
                    child_email=child_email,
                    asset=normalized_asset,
                    amount=amount,
                )
                for child, child_email, amount in executable
            ]
            timings["transfer_ms"] = int((perf_counter() - transfer_started_at) * 1000)
        else:
            if not legacy_account_ids:
                raise FundingTransferError("Select at least one sub-account")

            collectable_started_at = perf_counter()
            transfer_started_at = perf_counter()
            for account_id in legacy_account_ids:
                child = child_by_id.get(account_id)
                if child is None:
                    raise FundingTransferError(f"Unknown sub-account: {account_id}")
                child_email = self._resolve_child_email(child, email_by_uid)
                try:
                    available_amount = await self._collectable_amount(child, normalized_asset)
                except FundingTransferError as exc:
                    result = self._base_result(child, "0")
                    result["message"] = str(exc)
                    results.append(result)
                    continue
                if available_amount <= Decimal("0"):
                    result = self._base_result(child, "0")
                    result["message"] = "No collectable spot balance is available for this asset"
                    results.append(result)
                    continue
                results.append(
                    await self._collect_from_child(
                        main_credentials=main_credentials,
                        child=child,
                        child_email=child_email,
                        asset=normalized_asset,
                        amount=available_amount,
                    )
                )
            timings["collectable_query_ms"] = int((perf_counter() - collectable_started_at) * 1000)
            timings["transfer_ms"] = int((perf_counter() - transfer_started_at) * 1000)

        overview_started_at = perf_counter()
        overview = await self.get_group_overview(main_id)
        timings["overview_refresh_ms"] = int((perf_counter() - overview_started_at) * 1000)
        timings["total_ms"] = int((perf_counter() - started_at) * 1000)

        success_count, failure_count = self._operation_counts(results)
        logger.info(
            "Funding transfer completed request_id=%s direction=collect main_id=%s asset=%s child_count=%s success_count=%s failure_count=%s total_ms=%s",
            request_id,
            main_id,
            normalized_asset,
            len(results),
            success_count,
            failure_count,
            timings["total_ms"],
        )

        return {
            "direction": "collect",
            "asset": normalized_asset,
            "results": results,
            "overview": overview,
            "request_id": request_id,
            "timings": timings,
            "error_context": error_context,
            "message": self._summarize_operation("Collect", results),
        }

    async def _build_child_overview(
        self,
        child: MonitorAccountConfig,
        email_by_uid: dict[str, str],
        *,
        main_ready: bool,
    ) -> dict[str, Any]:
        spot_assets: list[dict[str, str]] = []
        spot_reason = ""
        error_context: dict[str, Any] = {}

        if child.api_key and child.api_secret:
            try:
                spot_assets = await self._fetch_spot_assets(self._child_credentials(child))
            except Exception as exc:
                spot_reason = f"Spot balance query failed: {exc}"
                error_context["spot_query"] = self._error_context_payload(exc)
        else:
            spot_reason = "Sub-account API is not configured"

        transfer_reason = ""
        if not main_ready:
            transfer_reason = "Main transfer API is unavailable for this group"
        elif not child.uid:
            transfer_reason = "Sub-account UID is not configured"
        elif child.uid not in email_by_uid:
            transfer_reason = "Main transfer API could not resolve an email for this UID"

        can_distribute = transfer_reason == ""
        can_collect = transfer_reason == "" and spot_reason == ""
        reason_distribute = "" if can_distribute else transfer_reason
        reason_collect = "" if can_collect else (transfer_reason or spot_reason)
        spot_available = self._spot_available_map(spot_assets)

        return {
            "account_id": child.account_id,
            "child_account_id": child.child_account_id,
            "name": child.child_account_name,
            "uid": child.uid,
            "eligible": can_distribute or can_collect,
            "reason": reason_collect or reason_distribute,
            "can_distribute": can_distribute,
            "can_collect": can_collect,
            "reason_distribute": reason_distribute,
            "reason_collect": reason_collect,
            "spot_assets": spot_assets,
            "spot_available": spot_available,
            "funding_assets": spot_assets,
            "funding_available": spot_available,
            "error_context": error_context,
        }

    async def _distribute_to_child(
        self,
        *,
        main_credentials: BinanceCredentials,
        child: MonitorAccountConfig,
        child_email: str,
        asset: str,
        amount: Decimal,
    ) -> dict[str, Any]:
        amount_text = self._format_decimal(amount)
        result = self._base_result(child, amount_text)

        try:
            await self._signed_request(
                main_credentials,
                "POST",
                "/sapi/v1/sub-account/universalTransfer",
                {
                    "toEmail": child_email,
                    "fromAccountType": "SPOT",
                    "toAccountType": "SPOT",
                    "asset": asset,
                    "amount": amount_text,
                },
            )
            result["success"] = True
            result["message"] = "Distribute succeeded"
        except Exception as exc:
            result["message"] = f"Distribute failed: {exc}"
        return result

    async def _collect_from_child(
        self,
        *,
        main_credentials: BinanceCredentials,
        child: MonitorAccountConfig,
        child_email: str,
        asset: str,
        amount: Decimal,
    ) -> dict[str, Any]:
        amount_text = self._format_decimal(amount)
        result = self._base_result(child, amount_text)
        if amount <= Decimal("0"):
            result["message"] = "No collectable spot balance is available for this asset"
            return result

        try:
            await self._signed_request(
                main_credentials,
                "POST",
                "/sapi/v1/sub-account/universalTransfer",
                {
                    "fromEmail": child_email,
                    "fromAccountType": "SPOT",
                    "toAccountType": "SPOT",
                    "asset": asset,
                    "amount": amount_text,
                },
            )
            result["success"] = True
            result["message"] = "Collect succeeded"
        except Exception as exc:
            result["message"] = f"Collect failed: {exc}"
        return result

    async def _collectable_amount(self, child: MonitorAccountConfig, asset: str) -> Decimal:
        if not child.api_key or not child.api_secret:
            raise FundingTransferError(f"Sub-account {child.child_account_name or child.account_id} does not have API credentials configured")
        try:
            child_spot_assets = await self._fetch_spot_assets(self._child_credentials(child))
        except Exception as exc:
            raise FundingTransferError(
                f"Failed to query spot balance for sub-account {child.child_account_name or child.account_id}: {exc}"
            ) from exc
        return Decimal(self._spot_available_map(child_spot_assets).get(asset, "0"))

    async def _get_sub_account_email_map(self, main_account: MainAccountConfig) -> dict[str, str]:
        credentials = self._main_credentials(main_account)
        page = 1
        result: dict[str, str] = {}
        while True:
            payload = await self._signed_read_request_with_retry(
                credentials,
                "GET",
                "/sapi/v1/sub-account/list",
                {"page": page, "limit": 200},
                label=f"sub-account list page {page}",
            )
            rows = payload.get("subAccounts") if isinstance(payload, dict) else []
            if not isinstance(rows, list):
                break
            for item in rows:
                if not isinstance(item, dict):
                    continue
                email = str(item.get("email") or "").strip()
                if not email:
                    continue
                for raw_uid in (item.get("subUserId"), item.get("uid"), item.get("subAccountId")):
                    uid = str(raw_uid or "").strip()
                    if uid:
                        result[uid] = email
            if len(rows) < 200:
                break
            page += 1
        return result

    async def _fetch_spot_assets(self, credentials: BinanceCredentials) -> list[dict[str, str]]:
        payload = await self._signed_read_request_with_retry(
            credentials,
            "GET",
            "/api/v3/account",
            None,
            label="spot account",
        )
        rows = payload.get("balances") if isinstance(payload, dict) else []
        assets: list[dict[str, str]] = []
        for item in rows if isinstance(rows, list) else []:
            if not isinstance(item, dict):
                continue
            asset = str(item.get("asset") or "").strip().upper()
            if not asset:
                continue
            free = Decimal(str(item.get("free") or "0"))
            locked = Decimal(str(item.get("locked") or "0"))
            total = free + locked
            if total <= Decimal("0"):
                continue
            assets.append(
                {
                    "asset": asset,
                    "free": self._format_decimal(free),
                    "locked": self._format_decimal(locked),
                    "total": self._format_decimal(total),
                }
            )
        assets.sort(key=lambda entry: entry["asset"])
        return assets

    async def _signed_read_request_with_retry(
        self,
        credentials: BinanceCredentials,
        method: str,
        path: str,
        params: dict[str, Any] | None,
        *,
        label: str,
    ) -> Any:
        timeout_s = self._request_timeout_s()
        return await self._request_with_retry(
            lambda: self._signed_request_once(credentials, method, path, params, timeout_s=timeout_s),
            label=label,
            timeout_s=timeout_s,
            max_attempts=self._read_retry_attempts(),
        )

    async def _request_with_retry(
        self,
        operation: Callable[[], Awaitable[Any]],
        *,
        label: str,
        timeout_s: float,
        max_attempts: int,
    ) -> Any:
        started_at = perf_counter()
        last_error: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                return await operation()
            except Exception as exc:
                last_error = exc
                source, error_type, status_code = self._classify_error(exc)
                retryable = self._is_retryable_error(exc)
                should_retry = retryable and attempt < max_attempts
                logger.warning(
                    "Funding read request failure label=%s attempt=%s/%s timeout_s=%s source=%s error_type=%s status_code=%s retry=%s error=%s",
                    label,
                    attempt,
                    max_attempts,
                    timeout_s,
                    source,
                    error_type,
                    status_code,
                    should_retry,
                    exc,
                )
                if not should_retry:
                    raise FundingTransferRequestError(
                        f"{label} failed after {attempt} attempts: {self._format_request_exception(exc)}",
                        label=label,
                        attempts=attempt,
                        duration_ms=int((perf_counter() - started_at) * 1000),
                        timeout_s=timeout_s,
                        source=source,
                        error_type=error_type,
                        status_code=status_code,
                        retryable=retryable,
                    ) from exc
                await asyncio.sleep(0.2 * attempt)

        assert last_error is not None
        source, error_type, status_code = self._classify_error(last_error)
        raise FundingTransferRequestError(
            f"{label} failed after {max_attempts} attempts: {self._format_request_exception(last_error)}",
            label=label,
            attempts=max_attempts,
            duration_ms=int((perf_counter() - started_at) * 1000),
            timeout_s=timeout_s,
            source=source,
            error_type=error_type,
            status_code=status_code,
            retryable=self._is_retryable_error(last_error),
        ) from last_error

    async def _signed_request(
        self,
        credentials: BinanceCredentials,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        timeout_s: float | None = None,
    ) -> Any:
        try:
            return await self._signed_request_once(
                credentials,
                method,
                path,
                params,
                timeout_s=timeout_s or self._request_timeout_s(),
            )
        except Exception as exc:
            raise FundingTransferError(self._format_request_exception(exc)) from exc

    async def _signed_request_once(
        self,
        credentials: BinanceCredentials,
        method: str,
        path: str,
        params: dict[str, Any] | None,
        *,
        timeout_s: float,
    ) -> Any:
        client = await self._get_client(credentials.api_key)
        query_params = dict(params or {})
        query_params["timestamp"] = int(datetime.now(UTC).timestamp() * 1000)
        query_params["recvWindow"] = self._settings.binance_recv_window_ms
        query = urlencode(query_params, doseq=True)
        signature = hmac.new(
            credentials.api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        response = await client.request(method, f"{path}?{query}&signature={signature}", timeout=timeout_s)
        response.raise_for_status()
        return response.json()

    async def _get_client(self, api_key: str) -> httpx.AsyncClient:
        async with self._lock:
            client = self._clients.get(api_key)
            if client is None:
                client = httpx.AsyncClient(
                    base_url="https://api.binance.com",
                    headers={"X-MBX-APIKEY": api_key},
                    timeout=None,
                )
                self._clients[api_key] = client
            return client

    def _new_request_id(self) -> str:
        return uuid4().hex[:12]

    def _request_timeout_s(self) -> float:
        return max(self._settings.binance_secondary_timeout_ms, 1) / 1000

    def _read_retry_attempts(self) -> int:
        return max(1, self._settings.binance_secondary_retry_attempts)

    def _classify_error(self, exc: Exception) -> tuple[str, str, int | None]:
        if isinstance(exc, httpx.TimeoutException):
            return "network", exc.__class__.__name__, None
        if isinstance(exc, httpx.NetworkError):
            return "network", exc.__class__.__name__, None
        if isinstance(exc, httpx.HTTPStatusError):
            status_code = exc.response.status_code
            source = "network" if status_code in {408, 409, 429, 500, 502, 503, 504} else "binance"
            return source, exc.__class__.__name__, status_code
        if isinstance(exc, FundingTransferRequestError):
            return exc.source, exc.error_type, exc.status_code
        return "binance", exc.__class__.__name__, None

    def _is_retryable_error(self, exc: Exception) -> bool:
        if isinstance(exc, FundingTransferRequestError):
            return exc.retryable
        if isinstance(exc, (httpx.TimeoutException, httpx.NetworkError)):
            return True
        if isinstance(exc, httpx.HTTPStatusError):
            return exc.response.status_code in {408, 409, 429, 500, 502, 503, 504}
        return False

    def _request_error_payload(self, exc: FundingTransferRequestError) -> dict[str, Any]:
        return {
            "label": exc.label,
            "attempts": exc.attempts,
            "duration_ms": exc.duration_ms,
            "timeout_s": exc.timeout_s,
            "source": exc.source,
            "error_type": exc.error_type,
            "status_code": exc.status_code,
            "retryable": exc.retryable,
        }

    def _error_context_payload(self, exc: Exception) -> dict[str, Any]:
        if isinstance(exc, FundingTransferRequestError):
            return self._request_error_payload(exc)
        return {
            "message": str(exc),
            "error_type": exc.__class__.__name__,
        }

    def _get_main_account(self, main_id: str) -> MainAccountConfig:
        normalized = str(main_id or "").strip().lower()
        main_account = self._settings.monitor_main_accounts.get(normalized)
        if main_account is None:
            raise FundingTransferError(f"Unknown group: {normalized}")
        return main_account

    def _main_credentials(self, main_account: MainAccountConfig) -> BinanceCredentials:
        return BinanceCredentials(
            api_key=main_account.transfer_api_key,
            api_secret=main_account.transfer_api_secret,
        )

    def _child_credentials(self, child: MonitorAccountConfig) -> BinanceCredentials:
        return BinanceCredentials(api_key=child.api_key, api_secret=child.api_secret)

    def _resolve_child_email(self, child: MonitorAccountConfig, email_by_uid: dict[str, str]) -> str:
        if not child.uid:
            raise FundingTransferError(f"Sub-account {child.account_id} does not have a configured UID")
        child_email = email_by_uid.get(child.uid)
        if not child_email:
            raise FundingTransferError(f"Main transfer API could not resolve an email for UID {child.uid}")
        return child_email

    def _base_result(self, child: MonitorAccountConfig, amount_text: str) -> dict[str, Any]:
        return {
            "account_id": child.account_id,
            "name": child.child_account_name,
            "uid": child.uid,
            "amount": amount_text,
            "success": False,
            "message": "",
        }

    def _spot_available_map(self, assets: list[dict[str, str]]) -> dict[str, str]:
        return {asset["asset"]: asset["free"] for asset in assets}

    def _normalize_asset(self, asset: Any) -> str:
        normalized = str(asset or "").strip().upper()
        if not normalized:
            raise FundingTransferError("asset is required")
        return normalized

    def _parse_positive_amount(self, value: Any, *, field_name: str) -> Decimal:
        try:
            amount = Decimal(str(value or "0").strip())
        except (InvalidOperation, ValueError) as exc:
            raise FundingTransferError(f"{field_name} must be a valid number") from exc
        if amount < Decimal("0"):
            raise FundingTransferError(f"{field_name} must be greater than or equal to 0")
        return amount

    def _format_decimal(self, value: Decimal) -> str:
        normalized = format(value.normalize(), "f")
        if "." in normalized:
            normalized = normalized.rstrip("0").rstrip(".")
        return normalized or "0"

    def _extract_error_message(self, response: httpx.Response) -> str:
        try:
            payload = response.json()
        except Exception:
            payload = None
        if isinstance(payload, dict):
            message = str(payload.get("msg") or payload.get("message") or "").strip()
            if message:
                return f"Binance returned an error: {message}"
        return f"Binance returned an HTTP {response.status_code} error"

    def _format_request_exception(self, exc: Exception) -> str:
        if isinstance(exc, FundingTransferRequestError):
            return str(exc)
        if isinstance(exc, (httpx.TimeoutException, httpx.NetworkError)):
            return f"Binance network request failed: {exc}"
        if isinstance(exc, httpx.HTTPStatusError):
            return self._extract_error_message(exc.response)
        return str(exc)

    def _operation_counts(self, results: list[dict[str, Any]]) -> tuple[int, int]:
        success_count = sum(1 for result in results if result.get("success"))
        failure_count = max(0, len(results) - success_count)
        return success_count, failure_count

    def _summarize_operation(self, action_label: str, results: list[dict[str, Any]]) -> str:
        success_count = sum(1 for result in results if result["success"])
        total_count = len(results)
        if success_count == total_count:
            return f"{action_label} succeeded for {total_count} sub-accounts"
        if success_count == 0:
            return f"{action_label} failed for all selected sub-accounts"
        return f"{action_label} partially succeeded: {success_count} succeeded, {total_count - success_count} failed"
