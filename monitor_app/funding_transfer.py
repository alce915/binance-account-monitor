from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from time import perf_counter
from typing import Any, Awaitable, Callable
from urllib.parse import urlencode
from uuid import uuid4

import httpx

from monitor_app.config import MainAccountConfig, MonitorAccountConfig, Settings
from monitor_app.funding_operation_store import FundingOperationStore
from monitor_app.security import mask_uid, sanitize_error_summary, sanitize_text


logger = logging.getLogger("uvicorn.error")


class FundingTransferError(RuntimeError):
    pass


class FundingTransferRequestRejected(FundingTransferError):
    def __init__(self, message: str, *, code: str, operation_id: str = "") -> None:
        super().__init__(message)
        self.code = code
        self.operation_id = str(operation_id or "").strip()


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


@dataclass(slots=True)
class FundingTransferCandidate:
    child: MonitorAccountConfig
    child_email: str
    requested_amount: Decimal
    normalized_amount: str
    precheck_available_amount: Decimal | None
    precheck_available_text: str | None
    child_before_available_amount: Decimal | None
    child_before_available_text: str | None


class FundingTransferService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._clients: dict[str, httpx.AsyncClient] = {}
        self._lock = asyncio.Lock()
        self._operation_store = FundingOperationStore(
            settings.monitor_history_db_path,
            max_rows=settings.funding_audit_max_rows,
            idempotency_ttl_seconds=settings.funding_idempotency_ttl_seconds,
        )

    async def close(self) -> None:
        async with self._lock:
            clients = list(self._clients.values())
            self._clients.clear()
        for client in clients:
            await client.aclose()
        await self._operation_store.close()

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
            except Exception as exc:
                main_reason = self._safe_failure_message(exc, "Main transfer API is unavailable for this group")
                logger.warning(
                    "Funding overview email map query failed request_id=%s main_id=%s error_type=%s",
                    request_id,
                    main_account.main_id,
                    exc.__class__.__name__,
                )
                error_context["email_map"] = self._error_context_payload(exc)
            try:
                main_spot_started_at = perf_counter()
                main_spot_assets = await self._fetch_spot_assets(self._main_credentials(main_account))
                timings["main_spot_query_ms"] = int((perf_counter() - main_spot_started_at) * 1000)
            except Exception as exc:
                if not main_reason:
                    main_reason = self._safe_failure_message(exc, "Main transfer API is unavailable for this group")
                error_context["main_account_query"] = self._error_context_payload(exc)
                logger.warning(
                    "Funding overview main spot query failed request_id=%s main_id=%s error_type=%s",
                    request_id,
                    main_account.main_id,
                    exc.__class__.__name__,
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
        write_enabled = bool(self._settings.funding_transfer_write_enabled)
        write_disabled_reason = "" if write_enabled else "Live funding transfers are disabled by configuration"

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
            "write_enabled": write_enabled,
            "write_disabled_reason": write_disabled_reason,
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

    async def get_audit_entries(self, main_id: str, *, limit: int = 50) -> dict[str, Any]:
        main_account = self._get_main_account(main_id)
        entries = await self._operation_store.list_operations(main_account.main_id, limit=limit)
        return {
            "main_account_id": main_account.main_id,
            "entries": entries,
            "updated_at": datetime.now(UTC).isoformat(),
        }

    async def get_audit_entry_detail(
        self,
        main_id: str,
        operation_id: str,
        *,
        direction: str,
    ) -> dict[str, Any]:
        main_account = self._get_main_account(main_id)
        normalized_operation_id = self._normalize_operation_id(operation_id)
        normalized_direction = str(direction or "").strip().lower()
        if not normalized_direction:
            raise self._reject(
                "direction is required for audit detail lookup",
                code="DIRECTION_REQUIRED",
                operation_id=normalized_operation_id,
            )
        record = await self._operation_store.get_operation_detail(
            main_account.main_id,
            normalized_operation_id,
            direction=normalized_direction,
        )
        if record is None:
            raise FundingTransferRequestRejected(
                "No audit detail was found for this operation_id",
                code="INVALID_OPERATION_ID",
                operation_id=normalized_operation_id,
            )
        payload = dict(record.response)
        payload["execution_stage"] = record.execution_stage
        payload["operation_id"] = record.operation_id
        return payload

    async def distribute(
        self,
        main_id: str,
        *,
        asset: str,
        transfers: list[dict[str, Any]],
        operation_id: str = "",
    ) -> dict[str, Any]:
        request_id = self._new_request_id()
        started_at = perf_counter()
        timings: dict[str, Any] = {}
        error_context: dict[str, Any] = {}

        normalized_asset = self._normalize_asset(asset)
        normalized_operation_id = self._normalize_operation_id(operation_id)
        main_account = self._get_main_account(main_id)
        self._ensure_write_enabled()
        if not main_account.has_transfer_credentials:
            raise self._reject(
                "Main transfer API is not configured for this group",
                code="TRANSFER_API_NOT_CONFIGURED",
                operation_id=normalized_operation_id,
            )

        child_by_id = {child.account_id: child for child in main_account.children}
        main_credentials = self._main_credentials(main_account)
        email_by_uid = await self._load_email_map(
            main_account,
            timings=timings,
            error_context=error_context,
            operation_id=normalized_operation_id,
        )
        main_spot_assets = await self._load_main_spot_assets(
            main_credentials,
            timings=timings,
            error_context=error_context,
            context_message="Failed to query main account spot balances",
            operation_id=normalized_operation_id,
        )
        main_available_map = self._spot_available_map(main_spot_assets)
        main_available_text = main_available_map.get(normalized_asset, "0")
        main_available_amount = Decimal(main_available_text)

        candidates: list[FundingTransferCandidate] = []
        total_amount = Decimal("0")
        seen_account_ids: set[str] = set()
        child_snapshot_started_at = perf_counter()
        for item in transfers:
            account_id = self._normalize_account_id(item.get("account_id"))
            self._ensure_unique_account_id(account_id, seen_account_ids)
            amount = self._parse_positive_amount(item.get("amount"), field_name=f"amount for {account_id}")
            if amount <= Decimal("0"):
                continue
            child = child_by_id.get(account_id)
            if child is None:
                raise self._reject(
                    f"Unknown sub-account: {account_id}",
                    code="INVALID_ACCOUNT",
                    operation_id=normalized_operation_id,
                )
            self._ensure_amount_precision(
                amount,
                available_text=main_available_text,
                field_name=f"amount for {account_id}",
                asset=normalized_asset,
            )
            child_email = self._resolve_child_email(child, email_by_uid)
            child_before_available_amount, child_before_available_text = await self._load_child_before_snapshot(
                child,
                normalized_asset,
            )
            normalized_amount = self._format_decimal(amount)
            candidates.append(
                FundingTransferCandidate(
                    child=child,
                    child_email=child_email,
                    requested_amount=amount,
                    normalized_amount=normalized_amount,
                    precheck_available_amount=None,
                    precheck_available_text=None,
                    child_before_available_amount=child_before_available_amount,
                    child_before_available_text=child_before_available_text,
                )
            )
            total_amount += amount
        timings["child_snapshot_ms"] = int((perf_counter() - child_snapshot_started_at) * 1000)

        if not candidates:
            raise self._reject(
                "Select at least one sub-account and enter a distribute amount greater than 0",
                code="EMPTY_SELECTION",
                operation_id=normalized_operation_id,
            )

        self._enforce_operation_limits(account_count=len(candidates), total_amount=total_amount, asset=normalized_asset)

        if total_amount > main_available_amount:
            raise self._reject(
                f"Main account spot balance is insufficient: {normalized_asset} only has {self._format_decimal(main_available_amount)} available",
                code="INSUFFICIENT_BALANCE",
                operation_id=normalized_operation_id,
            )

        precheck = self._build_precheck_payload(
            asset=normalized_asset,
            requested_total_amount=total_amount,
            selected_account_count=len(candidates),
            validated_account_count=len(candidates),
            main_available_amount=main_available_text,
            candidates=candidates,
        )
        payload_hash = self._payload_hash(
            {
                "direction": "distribute",
                "main_id": main_account.main_id,
                "asset": normalized_asset,
                "transfers": [
                    {"account_id": candidate.child.account_id, "amount": candidate.normalized_amount}
                    for candidate in sorted(candidates, key=lambda item: item.child.account_id)
                ],
            }
        )
        cached_response = await self._resolve_idempotent_response(
            main_id=main_account.main_id,
            direction="distribute",
            operation_id=normalized_operation_id,
            payload_hash=payload_hash,
        )
        if cached_response is not None:
            cached_response["idempotent_hit"] = True
            return cached_response

        pending_response = self._build_pending_operation_response(
            direction="distribute",
            asset=normalized_asset,
            request_id=request_id,
            operation_id=normalized_operation_id,
            precheck=precheck,
            main_before_available_amount=main_available_text,
            expected_main_direction="decrease",
            execution_stage="accepted",
        )
        accepted_response = await self._create_pending_operation(
            main_id=main_account.main_id,
            direction="distribute",
            asset=normalized_asset,
            request_id=request_id,
            operation_id=normalized_operation_id,
            payload_hash=payload_hash,
            account_count=len(candidates),
            response=pending_response,
        )
        if accepted_response is not None:
            accepted_response["idempotent_hit"] = True
            return accepted_response
        pending_response["execution_stage"] = "executing"
        pending_response["message"] = "Distribute accepted and is now executing"
        await self._update_operation_record(
            main_id=main_account.main_id,
            direction="distribute",
            operation_id=normalized_operation_id,
            response=pending_response,
        )

        transfer_started_at = perf_counter()
        results: list[dict[str, Any]] = []
        for candidate in candidates:
            result = await self._distribute_to_child(
                main_credentials=main_credentials,
                child=candidate.child,
                child_email=candidate.child_email,
                asset=normalized_asset,
                amount=candidate.requested_amount,
                precheck_available_text=candidate.precheck_available_text,
            )
            results.append(result)
            timings["transfer_ms"] = int((perf_counter() - transfer_started_at) * 1000)
            await self._update_operation_record(
                main_id=main_account.main_id,
                direction="distribute",
                operation_id=normalized_operation_id,
                response=self._build_in_progress_operation_response(
                    direction="distribute",
                    asset=normalized_asset,
                    request_id=request_id,
                    operation_id=normalized_operation_id,
                    precheck=precheck,
                    main_before_available_amount=main_available_text,
                    expected_main_direction="decrease",
                    results=results,
                    timings=timings,
                    error_context=error_context,
                ),
            )
        timings["transfer_ms"] = int((perf_counter() - transfer_started_at) * 1000)

        overview_refresh, overview = await self._refresh_overview_result(
            main_account.main_id,
            timings=timings,
            error_context=error_context,
        )
        reconciliation = await self._reconcile_operation(
            direction="distribute",
            asset=normalized_asset,
            main_credentials=main_credentials,
            main_before_available_amount=main_available_amount,
            main_before_available_text=main_available_text,
            candidates=candidates,
            results=results,
            timings=timings,
            error_context=error_context,
        )
        operation_status = self._determine_operation_status(results, overview_refresh, reconciliation)
        operation_summary = self._build_operation_summary(
            asset=normalized_asset,
            requested_total_amount=total_amount,
            main_before_available_amount=main_available_text,
            main_after_available_amount=str(reconciliation.get("main_after_available_amount") or main_available_text),
            expected_main_direction="decrease",
            results=results,
            reconciliation=reconciliation,
        )
        timings["total_ms"] = int((perf_counter() - started_at) * 1000)
        response = self._build_operation_response(
            direction="distribute",
            asset=normalized_asset,
            request_id=request_id,
            operation_id=normalized_operation_id,
            results=results,
            precheck=precheck,
            overview_refresh=overview_refresh,
            overview=overview,
            reconciliation=reconciliation,
            operation_summary=operation_summary,
            execution_stage="completed",
            timings=timings,
            error_context=error_context,
            operation_status=operation_status,
        )
        await self._update_operation_record(
            main_id=main_account.main_id,
            direction="distribute",
            operation_id=normalized_operation_id,
            response=response,
        )
        self._log_operation_completion(
            request_id=request_id,
            direction="distribute",
            main_id=main_account.main_id,
            asset=normalized_asset,
            results=results,
            total_ms=timings["total_ms"],
            operation_status=operation_status,
        )
        return response

    async def collect(
        self,
        main_id: str,
        *,
        asset: str,
        transfers: list[dict[str, Any]] | None = None,
        account_ids: list[str] | None = None,
        operation_id: str = "",
    ) -> dict[str, Any]:
        request_id = self._new_request_id()
        started_at = perf_counter()
        timings: dict[str, Any] = {}
        error_context: dict[str, Any] = {}

        normalized_asset = self._normalize_asset(asset)
        normalized_operation_id = self._normalize_operation_id(operation_id)
        main_account = self._get_main_account(main_id)
        self._ensure_write_enabled()
        if not main_account.has_transfer_credentials:
            raise self._reject(
                "Main transfer API is not configured for this group",
                code="TRANSFER_API_NOT_CONFIGURED",
                operation_id=normalized_operation_id,
            )

        child_by_id = {child.account_id: child for child in main_account.children}
        main_credentials = self._main_credentials(main_account)
        email_by_uid = await self._load_email_map(
            main_account,
            timings=timings,
            error_context=error_context,
            operation_id=normalized_operation_id,
        )
        main_spot_assets = await self._load_main_spot_assets(
            main_credentials,
            timings=timings,
            error_context=error_context,
            context_message="Failed to query main account spot balances",
            operation_id=normalized_operation_id,
        )
        main_available_map = self._spot_available_map(main_spot_assets)
        main_available_text = main_available_map.get(normalized_asset, "0")
        main_available_amount = Decimal(main_available_text)

        requested_transfers = list(transfers or [])
        legacy_account_ids = [
            self._normalize_account_id(account_id)
            for account_id in (account_ids or [])
            if str(account_id or "").strip()
        ]

        results: list[dict[str, Any]] = []
        candidates: list[FundingTransferCandidate] = []
        total_amount = Decimal("0")
        seen_account_ids: set[str] = set()
        collectable_started_at = perf_counter()
        if requested_transfers:
            for item in requested_transfers:
                account_id = self._normalize_account_id(item.get("account_id"))
                self._ensure_unique_account_id(account_id, seen_account_ids)
                amount = self._parse_positive_amount(item.get("amount"), field_name=f"amount for {account_id}")
                if amount <= Decimal("0"):
                    continue
                child = child_by_id.get(account_id)
                if child is None:
                    raise self._reject(
                        f"Unknown sub-account: {account_id}",
                        code="INVALID_ACCOUNT",
                        operation_id=normalized_operation_id,
                    )
                child_email = self._resolve_child_email(child, email_by_uid)
                available_amount, available_text = await self._collectable_amount_snapshot(
                    child,
                    normalized_asset,
                    operation_id=normalized_operation_id,
                )
                self._ensure_amount_precision(
                    amount,
                    available_text=available_text,
                    field_name=f"amount for {account_id}",
                    asset=normalized_asset,
                )
                if amount > available_amount:
                    raise self._reject(
                        f"Sub-account {child.child_account_name or child.account_id} can collect at most {self._format_decimal(available_amount)} {normalized_asset}",
                        code="INSUFFICIENT_BALANCE",
                        operation_id=normalized_operation_id,
                    )
                normalized_amount = self._format_decimal(amount)
                candidates.append(
                    FundingTransferCandidate(
                        child=child,
                        child_email=child_email,
                        requested_amount=amount,
                        normalized_amount=normalized_amount,
                        precheck_available_amount=available_amount,
                        precheck_available_text=available_text,
                        child_before_available_amount=available_amount,
                        child_before_available_text=available_text,
                    )
                )
                total_amount += amount
            if not candidates:
                raise self._reject(
                    "Select at least one sub-account and enter a collect amount greater than 0",
                    code="EMPTY_SELECTION",
                    operation_id=normalized_operation_id,
                )
        else:
            if not legacy_account_ids:
                raise self._reject(
                    "Select at least one sub-account",
                    code="EMPTY_SELECTION",
                    operation_id=normalized_operation_id,
                )
            self._ensure_max_account_count(len(legacy_account_ids))
            for account_id in legacy_account_ids:
                self._ensure_unique_account_id(account_id, seen_account_ids)
                child = child_by_id.get(account_id)
                if child is None:
                    raise self._reject(
                        f"Unknown sub-account: {account_id}",
                        code="INVALID_ACCOUNT",
                        operation_id=normalized_operation_id,
                    )
                child_email = self._resolve_child_email(child, email_by_uid)
                try:
                    available_amount, available_text = await self._collectable_amount_snapshot(
                        child,
                        normalized_asset,
                        operation_id=normalized_operation_id,
                    )
                except FundingTransferError as exc:
                    results.append(
                        self._failed_precheck_result(
                            child=child,
                            amount_text="0",
                            precheck_available_text=None,
                            message=sanitize_error_summary(exc, fallback="Failed to query sub-account spot balances"),
                        )
                    )
                    continue
                if available_amount <= Decimal("0"):
                    results.append(
                        self._failed_precheck_result(
                            child=child,
                            amount_text="0",
                            precheck_available_text=available_text,
                            message="No collectable spot balance is available for this asset",
                        )
                    )
                    continue
                normalized_amount = self._format_decimal(available_amount)
                candidates.append(
                    FundingTransferCandidate(
                        child=child,
                        child_email=child_email,
                        requested_amount=available_amount,
                        normalized_amount=normalized_amount,
                        precheck_available_amount=available_amount,
                        precheck_available_text=available_text,
                        child_before_available_amount=available_amount,
                        child_before_available_text=available_text,
                    )
                )
                total_amount += available_amount
        timings["collectable_query_ms"] = int((perf_counter() - collectable_started_at) * 1000)

        if not candidates:
            raise self._reject(
                "No eligible sub-account can collect the requested asset",
                code="NO_ELIGIBLE_TRANSFER",
                operation_id=normalized_operation_id,
            )

        self._enforce_operation_limits(account_count=len(candidates) or len(legacy_account_ids), total_amount=total_amount, asset=normalized_asset)

        precheck = self._build_precheck_payload(
            asset=normalized_asset,
            requested_total_amount=total_amount,
            selected_account_count=len(requested_transfers) if requested_transfers else len(legacy_account_ids),
            validated_account_count=len(candidates),
            main_available_amount=main_available_text,
            candidates=candidates,
        )
        payload_hash = self._payload_hash(
            {
                "direction": "collect",
                "main_id": main_account.main_id,
                "asset": normalized_asset,
                "transfers": [
                    {"account_id": candidate.child.account_id, "amount": candidate.normalized_amount}
                    for candidate in sorted(candidates, key=lambda item: item.child.account_id)
                ],
                "account_ids": sorted(legacy_account_ids),
            }
        )
        cached_response = await self._resolve_idempotent_response(
            main_id=main_account.main_id,
            direction="collect",
            operation_id=normalized_operation_id,
            payload_hash=payload_hash,
        )
        if cached_response is not None:
            cached_response["idempotent_hit"] = True
            return cached_response

        pending_response = self._build_pending_operation_response(
            direction="collect",
            asset=normalized_asset,
            request_id=request_id,
            operation_id=normalized_operation_id,
            precheck=precheck,
            main_before_available_amount=main_available_text,
            expected_main_direction="increase",
            execution_stage="accepted",
        )
        accepted_response = await self._create_pending_operation(
            main_id=main_account.main_id,
            direction="collect",
            asset=normalized_asset,
            request_id=request_id,
            operation_id=normalized_operation_id,
            payload_hash=payload_hash,
            account_count=max(len(candidates), len(legacy_account_ids)),
            response=pending_response,
        )
        if accepted_response is not None:
            accepted_response["idempotent_hit"] = True
            return accepted_response
        pending_response["execution_stage"] = "executing"
        pending_response["message"] = "Collect accepted and is now executing"
        await self._update_operation_record(
            main_id=main_account.main_id,
            direction="collect",
            operation_id=normalized_operation_id,
            response=pending_response,
        )

        transfer_started_at = perf_counter()
        transfer_results: list[dict[str, Any]] = []
        for candidate in candidates:
            result = await self._collect_from_child(
                main_credentials=main_credentials,
                child=candidate.child,
                child_email=candidate.child_email,
                asset=normalized_asset,
                amount=candidate.requested_amount,
                precheck_available_text=candidate.precheck_available_text,
            )
            transfer_results.append(result)
            results.append(result)
            timings["transfer_ms"] = int((perf_counter() - transfer_started_at) * 1000)
            await self._update_operation_record(
                main_id=main_account.main_id,
                direction="collect",
                operation_id=normalized_operation_id,
                response=self._build_in_progress_operation_response(
                    direction="collect",
                    asset=normalized_asset,
                    request_id=request_id,
                    operation_id=normalized_operation_id,
                    precheck=precheck,
                    main_before_available_amount=main_available_text,
                    expected_main_direction="increase",
                    results=results,
                    timings=timings,
                    error_context=error_context,
                ),
            )
        timings["transfer_ms"] = int((perf_counter() - transfer_started_at) * 1000)

        overview_refresh, overview = await self._refresh_overview_result(
            main_account.main_id,
            timings=timings,
            error_context=error_context,
        )
        reconciliation = await self._reconcile_operation(
            direction="collect",
            asset=normalized_asset,
            main_credentials=main_credentials,
            main_before_available_amount=main_available_amount,
            main_before_available_text=main_available_text,
            candidates=candidates,
            results=transfer_results,
            timings=timings,
            error_context=error_context,
        )
        operation_status = self._determine_operation_status(results, overview_refresh, reconciliation)
        operation_summary = self._build_operation_summary(
            asset=normalized_asset,
            requested_total_amount=total_amount,
            main_before_available_amount=main_available_text,
            main_after_available_amount=str(reconciliation.get("main_after_available_amount") or main_available_text),
            expected_main_direction="increase",
            results=results,
            reconciliation=reconciliation,
        )
        timings["total_ms"] = int((perf_counter() - started_at) * 1000)
        response = self._build_operation_response(
            direction="collect",
            asset=normalized_asset,
            request_id=request_id,
            operation_id=normalized_operation_id,
            results=results,
            precheck=precheck,
            overview_refresh=overview_refresh,
            overview=overview,
            reconciliation=reconciliation,
            operation_summary=operation_summary,
            execution_stage="completed",
            timings=timings,
            error_context=error_context,
            operation_status=operation_status,
        )
        await self._update_operation_record(
            main_id=main_account.main_id,
            direction="collect",
            operation_id=normalized_operation_id,
            response=response,
        )
        self._log_operation_completion(
            request_id=request_id,
            direction="collect",
            main_id=main_account.main_id,
            asset=normalized_asset,
            results=results,
            total_ms=timings["total_ms"],
            operation_status=operation_status,
        )
        return response

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
                spot_reason = self._safe_failure_message(exc, "Spot balance query failed")
                error_context["spot_query"] = self._error_context_payload(exc)
        else:
            spot_reason = "Sub-account API is not configured"

        transfer_reason = ""
        if not main_ready:
            transfer_reason = "Main transfer API is unavailable for this group"
        elif not child.uid:
            transfer_reason = "Sub-account UID is not configured"
        elif child.uid not in email_by_uid:
            transfer_reason = "Main transfer API could not resolve a sub-account mapping for this UID"

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
        precheck_available_text: str | None,
    ) -> dict[str, Any]:
        amount_text = self._format_decimal(amount)
        result = self._base_result(
            child,
            amount_text=amount_text,
            requested_amount_text=amount_text,
            normalized_amount_text=amount_text,
            precheck_available_text=precheck_available_text,
        )

        try:
            result["transfer_attempted"] = True
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
            result["executed_amount"] = amount_text
            result["message"] = "Distribute succeeded"
        except Exception as exc:
            result["message"] = self._safe_failure_message(exc, "Distribute failed")
        return result

    async def _collect_from_child(
        self,
        *,
        main_credentials: BinanceCredentials,
        child: MonitorAccountConfig,
        child_email: str,
        asset: str,
        amount: Decimal,
        precheck_available_text: str | None,
    ) -> dict[str, Any]:
        amount_text = self._format_decimal(amount)
        result = self._base_result(
            child,
            amount_text=amount_text,
            requested_amount_text=amount_text,
            normalized_amount_text=amount_text,
            precheck_available_text=precheck_available_text,
        )
        if amount <= Decimal("0"):
            result["message"] = "No collectable spot balance is available for this asset"
            return result

        try:
            result["transfer_attempted"] = True
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
            result["executed_amount"] = amount_text
            result["message"] = "Collect succeeded"
        except Exception as exc:
            result["message"] = self._safe_failure_message(exc, "Collect failed")
        return result

    async def _collectable_amount_snapshot(
        self,
        child: MonitorAccountConfig,
        asset: str,
        *,
        operation_id: str = "",
    ) -> tuple[Decimal, str]:
        if not child.api_key or not child.api_secret:
            raise self._reject(
                "Sub-account API credentials are not configured",
                code="SUB_ACCOUNT_API_NOT_CONFIGURED",
                operation_id=operation_id,
            )
        try:
            child_spot_assets = await self._fetch_spot_assets(self._child_credentials(child))
        except Exception as exc:
            raise self._reject(
                self._safe_failure_message(exc, "Failed to query sub-account spot balances"),
                code="BALANCE_QUERY_FAILED",
                operation_id=operation_id,
            ) from exc
        available_text = self._spot_available_map(child_spot_assets).get(asset, "0")
        return Decimal(available_text), available_text

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

    async def _load_email_map(
        self,
        main_account: MainAccountConfig,
        *,
        timings: dict[str, Any],
        error_context: dict[str, Any],
        operation_id: str = "",
    ) -> dict[str, str]:
        try:
            email_started_at = perf_counter()
            email_by_uid = await self._get_sub_account_email_map(main_account)
            timings["email_map_ms"] = int((perf_counter() - email_started_at) * 1000)
            return email_by_uid
        except Exception as exc:
            error_context["email_map"] = self._error_context_payload(exc)
            raise self._reject(
                self._safe_failure_message(exc, "Failed to query sub-account mapping"),
                code="SUB_ACCOUNT_MAPPING_QUERY_FAILED",
                operation_id=operation_id,
            ) from exc

    async def _load_main_spot_assets(
        self,
        credentials: BinanceCredentials,
        *,
        timings: dict[str, Any],
        error_context: dict[str, Any],
        context_message: str,
        operation_id: str = "",
    ) -> list[dict[str, str]]:
        try:
            started_at = perf_counter()
            assets = await self._fetch_spot_assets(credentials)
            timings["main_spot_query_ms"] = int((perf_counter() - started_at) * 1000)
            return assets
        except Exception as exc:
            error_context["main_spot_query"] = self._error_context_payload(exc)
            raise self._reject(
                self._safe_failure_message(exc, context_message),
                code="MAIN_BALANCE_QUERY_FAILED",
                operation_id=operation_id,
            ) from exc

    async def _load_child_before_snapshot(self, child: MonitorAccountConfig, asset: str) -> tuple[Decimal | None, str | None]:
        try:
            return await self._collectable_amount_snapshot(child, asset)
        except FundingTransferError:
            return None, None

    async def _refresh_overview_result(
        self,
        main_id: str,
        *,
        timings: dict[str, Any],
        error_context: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any] | None]:
        overview_started_at = perf_counter()
        try:
            overview = await self.get_group_overview(main_id)
            timings["overview_refresh_ms"] = int((perf_counter() - overview_started_at) * 1000)
            return (
                {
                    "success": True,
                    "message": "Overview refreshed",
                    "updated_at": datetime.now(UTC).isoformat(),
                },
                overview,
            )
        except Exception as exc:
            timings["overview_refresh_ms"] = int((perf_counter() - overview_started_at) * 1000)
            error_context["overview_refresh"] = self._error_context_payload(exc)
            return (
                {
                    "success": False,
                    "message": self._safe_failure_message(exc, "Post-transfer overview refresh failed"),
                    "updated_at": datetime.now(UTC).isoformat(),
                },
                None,
            )

    async def _reconcile_operation(
        self,
        *,
        direction: str,
        asset: str,
        main_credentials: BinanceCredentials,
        main_before_available_amount: Decimal,
        main_before_available_text: str,
        candidates: list[FundingTransferCandidate],
        results: list[dict[str, Any]],
        timings: dict[str, Any],
        error_context: dict[str, Any],
    ) -> dict[str, Any]:
        successful_account_ids = {
            str(result.get("account_id") or "")
            for result in results
            if result.get("success") and result.get("transfer_attempted")
        }
        if not successful_account_ids:
            return {
                "status": "unconfirmed",
                "confirmed_count": 0,
                "failed_count": 0,
                "results": [],
            }

        candidate_by_account_id = {candidate.child.account_id: candidate for candidate in candidates}
        started_at = perf_counter()
        expected_direction = "increase" if direction == "distribute" else "decrease"

        async def fetch_main_snapshot() -> tuple[Decimal | None, str | None]:
            try:
                main_spot_assets = await self._fetch_spot_assets(main_credentials)
                snapshot_text = self._spot_available_map(main_spot_assets).get(asset, "0")
                return Decimal(snapshot_text), snapshot_text
            except Exception as exc:  # noqa: PERF203
                error_context["reconciliation_main_query"] = self._error_context_payload(exc)
                return None, None

        async def confirm_accounts(account_ids: list[str]) -> tuple[list[dict[str, Any]], Decimal | None, str | None]:
            main_after_amount, main_after_text = await fetch_main_snapshot()
            rows: list[dict[str, Any]] = []
            for account_id in account_ids:
                candidate = candidate_by_account_id.get(account_id)
                if candidate is None:
                    continue
                before_amount = candidate.child_before_available_amount
                before_text = candidate.child_before_available_text
                try:
                    after_amount, after_text = await self._collectable_amount_snapshot(candidate.child, asset)
                except FundingTransferError as exc:
                    rows.append(
                        {
                            "account_id": candidate.child.account_id,
                            "before_available_amount": before_text or "-",
                            "after_available_amount": "-",
                            "expected_direction": expected_direction,
                            "confirmed": False,
                            "message": sanitize_error_summary(exc, fallback="Post-transfer balance confirmation failed"),
                        }
                    )
                    continue

                child_direction_confirmed = False
                if before_amount is not None:
                    if direction == "distribute":
                        child_direction_confirmed = after_amount >= before_amount
                    else:
                        child_direction_confirmed = after_amount <= before_amount

                main_direction_confirmed = False
                if main_after_amount is not None:
                    if direction == "distribute":
                        main_direction_confirmed = main_after_amount <= main_before_available_amount
                    else:
                        main_direction_confirmed = main_after_amount >= main_before_available_amount

                confirmed = child_direction_confirmed and main_direction_confirmed
                if confirmed:
                    message = "Balance reconciliation confirmed"
                elif not child_direction_confirmed and before_amount is not None:
                    message = "Sub-account balance change did not match the expected direction"
                elif main_after_amount is None:
                    message = "Main account balance confirmation is unavailable"
                else:
                    message = "Main account balance change did not match the expected direction"
                rows.append(
                    {
                        "account_id": candidate.child.account_id,
                        "before_available_amount": before_text or "-",
                        "after_available_amount": after_text,
                        "expected_direction": expected_direction,
                        "confirmed": confirmed,
                        "message": message,
                    }
                )
            return rows, main_after_amount, main_after_text

        reconciliation_results, main_after_available_amount, main_after_available_text = await confirm_accounts(sorted(successful_account_ids))
        unconfirmed_account_ids = [
            str(item.get("account_id") or "")
            for item in reconciliation_results
            if isinstance(item, dict) and not item.get("confirmed")
        ]
        if unconfirmed_account_ids:
            await asyncio.sleep(1.5)
            retried_results, retried_main_after_amount, retried_main_after_text = await confirm_accounts(unconfirmed_account_ids)
            retried_by_account_id = {
                str(item.get("account_id") or ""): item
                for item in retried_results
                if isinstance(item, dict)
            }
            reconciliation_results = [
                retried_by_account_id.get(str(item.get("account_id") or ""), item)
                for item in reconciliation_results
            ]
            if retried_main_after_text is not None:
                main_after_available_text = retried_main_after_text
                main_after_available_amount = retried_main_after_amount

        timings["reconciliation_ms"] = int((perf_counter() - started_at) * 1000)
        confirmed_count = sum(1 for item in reconciliation_results if item.get("confirmed"))
        failed_count = max(0, len(reconciliation_results) - confirmed_count)
        if reconciliation_results and confirmed_count == len(reconciliation_results):
            status = "confirmed"
        elif confirmed_count > 0:
            status = "partially_confirmed"
        else:
            status = "unconfirmed"
        return {
            "status": status,
            "confirmed_count": confirmed_count,
            "failed_count": failed_count,
            "results": reconciliation_results,
            "main_before_available_amount": main_before_available_text,
            "main_after_available_amount": main_after_available_text or "-",
        }

    def _build_precheck_payload(
        self,
        *,
        asset: str,
        requested_total_amount: Decimal,
        selected_account_count: int,
        validated_account_count: int,
        main_available_amount: str | None,
        candidates: list[FundingTransferCandidate],
    ) -> dict[str, Any]:
        return {
            "asset": asset,
            "requested_total_amount": self._format_decimal(requested_total_amount),
            "selected_account_count": selected_account_count,
            "validated_account_count": validated_account_count,
            "main_available_amount": main_available_amount or "0",
            "accounts": [
                {
                    "account_id": candidate.child.account_id,
                    "precheck_available_amount": candidate.precheck_available_text or "-",
                }
                for candidate in candidates
            ],
        }

    def _build_operation_summary(
        self,
        *,
        asset: str,
        requested_total_amount: Decimal,
        main_before_available_amount: str,
        main_after_available_amount: str,
        expected_main_direction: str,
        results: list[dict[str, Any]],
        reconciliation: dict[str, Any],
    ) -> dict[str, Any]:
        attempted_count = sum(1 for result in results if result.get("transfer_attempted"))
        success_count = sum(1 for result in results if result.get("success"))
        failure_count = max(0, len(results) - success_count)
        confirmed_count = int(reconciliation.get("confirmed_count") or 0) if isinstance(reconciliation, dict) else 0
        unconfirmed_account_ids = [
            str(item.get("account_id") or "")
            for item in (reconciliation.get("results") if isinstance(reconciliation, dict) else [])
            if isinstance(item, dict) and not item.get("confirmed")
        ]
        return {
            "asset": asset,
            "requested_total_amount": self._format_decimal(requested_total_amount),
            "attempted_count": attempted_count,
            "success_count": success_count,
            "failure_count": failure_count,
            "confirmed_count": confirmed_count,
            "pending_confirmation_count": max(success_count - confirmed_count, 0),
            "main_before_available_amount": main_before_available_amount,
            "main_after_available_amount": main_after_available_amount,
            "expected_main_direction": expected_main_direction,
            "unconfirmed_account_ids": unconfirmed_account_ids,
        }

    def _build_pending_operation_response(
        self,
        *,
        direction: str,
        asset: str,
        request_id: str,
        operation_id: str,
        precheck: dict[str, Any],
        main_before_available_amount: str,
        expected_main_direction: str,
        execution_stage: str,
    ) -> dict[str, Any]:
        operation_summary = {
            "asset": asset,
            "requested_total_amount": str(precheck.get("requested_total_amount") or "0"),
            "attempted_count": 0,
            "success_count": 0,
            "failure_count": 0,
            "confirmed_count": 0,
            "pending_confirmation_count": 0,
            "main_before_available_amount": main_before_available_amount,
            "main_after_available_amount": main_before_available_amount,
            "expected_main_direction": expected_main_direction,
            "unconfirmed_account_ids": [],
        }
        return {
            "direction": direction,
            "asset": asset,
            "operation_id": operation_id,
            "operation_status": "operation_submitted",
            "execution_stage": execution_stage,
            "idempotent_hit": False,
            "precheck": precheck,
            "results": [],
            "overview_refresh": {"success": False, "message": "Operation is still processing"},
            "overview": None,
            "reconciliation": {"status": "unconfirmed", "confirmed_count": 0, "failed_count": 0, "results": []},
            "operation_summary": operation_summary,
            "request_id": request_id,
            "timings": {},
            "error_context": {},
            "message": f"{'Distribute' if direction == 'distribute' else 'Collect'} accepted and waiting for execution",
            "updated_at": datetime.now(UTC).isoformat(),
        }

    def _build_in_progress_operation_response(
        self,
        *,
        direction: str,
        asset: str,
        request_id: str,
        operation_id: str,
        precheck: dict[str, Any],
        main_before_available_amount: str,
        expected_main_direction: str,
        results: list[dict[str, Any]],
        timings: dict[str, Any],
        error_context: dict[str, Any],
    ) -> dict[str, Any]:
        reconciliation = {"status": "unconfirmed", "confirmed_count": 0, "failed_count": 0, "results": []}
        operation_summary = self._build_operation_summary(
            asset=asset,
            requested_total_amount=Decimal(str(precheck.get("requested_total_amount") or "0")),
            main_before_available_amount=main_before_available_amount,
            main_after_available_amount=main_before_available_amount,
            expected_main_direction=expected_main_direction,
            results=results,
            reconciliation=reconciliation,
        )
        processed_count = len(results)
        selected_count = int(precheck.get("selected_account_count") or processed_count)
        action = "Distribute" if direction == "distribute" else "Collect"
        return {
            "direction": direction,
            "asset": asset,
            "operation_id": operation_id,
            "operation_status": "operation_submitted",
            "execution_stage": "executing",
            "idempotent_hit": False,
            "precheck": precheck,
            "results": results,
            "overview_refresh": {"success": False, "message": "Operation is still processing"},
            "overview": None,
            "reconciliation": reconciliation,
            "operation_summary": operation_summary,
            "request_id": request_id,
            "timings": dict(timings),
            "error_context": dict(error_context),
            "message": f"{action} is executing ({processed_count}/{selected_count} processed)",
            "updated_at": datetime.now(UTC).isoformat(),
        }

    def _build_operation_response(
        self,
        *,
        direction: str,
        asset: str,
        request_id: str,
        operation_id: str,
        results: list[dict[str, Any]],
        precheck: dict[str, Any],
        overview_refresh: dict[str, Any],
        overview: dict[str, Any] | None,
        reconciliation: dict[str, Any],
        operation_summary: dict[str, Any],
        execution_stage: str,
        timings: dict[str, Any],
        error_context: dict[str, Any],
        operation_status: str,
    ) -> dict[str, Any]:
        return {
            "direction": direction,
            "asset": asset,
            "operation_id": operation_id,
            "operation_status": operation_status,
            "execution_stage": execution_stage,
            "idempotent_hit": False,
            "precheck": precheck,
            "results": results,
            "overview_refresh": overview_refresh,
            "overview": overview,
            "reconciliation": reconciliation,
            "operation_summary": operation_summary,
            "request_id": request_id,
            "timings": timings,
            "error_context": error_context,
            "message": self._summarize_operation_status(direction, operation_status, results),
            "updated_at": datetime.now(UTC).isoformat(),
        }

    async def _create_pending_operation(
        self,
        *,
        main_id: str,
        direction: str,
        asset: str,
        request_id: str,
        operation_id: str,
        payload_hash: str,
        account_count: int,
        response: dict[str, Any],
    ) -> dict[str, Any] | None:
        try:
            await self._operation_store.create_operation(
                main_id=main_id,
                direction=direction,
                asset=asset,
                request_id=request_id,
                operation_id=operation_id,
                payload_hash=payload_hash,
                execution_stage="accepted",
                operation_status="operation_submitted",
                account_count=account_count,
                success_count=0,
                failure_count=0,
                confirmed_count=0,
                pending_confirmation_count=0,
                message=str(response.get("message") or ""),
                response=response,
            )
            return None
        except sqlite3.IntegrityError:
            return await self._resolve_idempotent_response(
                main_id=main_id,
                direction=direction,
                operation_id=operation_id,
                payload_hash=payload_hash,
            )

    async def _update_operation_record(
        self,
        *,
        main_id: str,
        direction: str,
        operation_id: str,
        response: dict[str, Any],
    ) -> None:
        operation_summary = response.get("operation_summary")
        if not isinstance(operation_summary, dict):
            operation_summary = {}
        reconciliation = response.get("reconciliation")
        if not isinstance(reconciliation, dict):
            reconciliation = {}
        precheck = response.get("precheck")
        if not isinstance(precheck, dict):
            precheck = {}
        results = response.get("results")
        account_count = len(results) if isinstance(results, list) else 0
        if account_count <= 0:
            account_count = int(precheck.get("selected_account_count") or 0)
        if account_count <= 0:
            account_count = int(precheck.get("validated_account_count") or 0)
        await self._operation_store.update_operation(
            main_id=main_id,
            direction=direction,
            operation_id=operation_id,
            execution_stage=str(response.get("execution_stage") or "completed"),
            operation_status=str(response.get("operation_status") or "operation_submitted"),
            account_count=account_count,
            success_count=int(operation_summary.get("success_count") or 0),
            failure_count=int(operation_summary.get("failure_count") or 0),
            confirmed_count=int(reconciliation.get("confirmed_count") or 0),
            pending_confirmation_count=int(operation_summary.get("pending_confirmation_count") or 0),
            message=str(response.get("message") or ""),
            response=response,
        )

    async def _resolve_idempotent_response(
        self,
        *,
        main_id: str,
        direction: str,
        operation_id: str,
        payload_hash: str,
    ) -> dict[str, Any] | None:
        existing = await self._operation_store.get_operation(main_id, direction, operation_id)
        if existing is None:
            return None
        if existing.payload_hash != payload_hash:
            raise self._reject(
                "operation_id has already been used with a different request payload",
                code="OPERATION_ID_MISMATCH",
                operation_id=operation_id,
            )
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        if now_ms > existing.expires_at_ms:
            raise self._reject(
                "operation_id has expired; submit the operation again to generate a new id",
                code="OPERATION_ID_EXPIRED",
                operation_id=operation_id,
            )
        response = dict(existing.response)
        response["operation_id"] = existing.operation_id
        response["execution_stage"] = existing.execution_stage
        if existing.execution_stage in {"accepted", "executing"}:
            response["operation_status"] = "operation_submitted"
            response["message"] = f"{response.get('message', 'Operation is still processing')} (idempotent replay)"
        else:
            response["message"] = f"{response.get('message', 'Operation completed')} (idempotent replay)"
        return response

    def _determine_operation_status(
        self,
        results: list[dict[str, Any]],
        overview_refresh: dict[str, Any],
        reconciliation: dict[str, Any],
    ) -> str:
        success_count = sum(1 for result in results if result.get("success"))
        if success_count == 0:
            return "operation_failed"
        if success_count < len(results):
            return "operation_partially_succeeded"
        if not overview_refresh.get("success") or reconciliation.get("status") != "confirmed":
            return "operation_submitted"
        return "operation_fully_succeeded"

    def _summarize_operation_status(self, direction: str, operation_status: str, results: list[dict[str, Any]]) -> str:
        action = "Distribute" if direction == "distribute" else "Collect"
        success_count, failure_count = self._operation_counts(results)
        if operation_status == "operation_fully_succeeded":
            return f"{action} fully succeeded for {success_count} sub-accounts"
        if operation_status == "operation_partially_succeeded":
            return f"{action} partially succeeded: {success_count} succeeded, {failure_count} failed"
        if operation_status == "operation_submitted":
            return f"{action} submitted for {success_count} sub-accounts; confirmation is still pending"
        return f"{action} failed for all selected sub-accounts"

    def _failed_precheck_result(
        self,
        *,
        child: MonitorAccountConfig,
        amount_text: str,
        precheck_available_text: str | None,
        message: str,
    ) -> dict[str, Any]:
        result = self._base_result(
            child,
            amount_text=amount_text,
            requested_amount_text=amount_text,
            normalized_amount_text=amount_text,
            precheck_available_text=precheck_available_text,
        )
        result["message"] = message
        return result

    def _log_operation_completion(
        self,
        *,
        request_id: str,
        direction: str,
        main_id: str,
        asset: str,
        results: list[dict[str, Any]],
        total_ms: int,
        operation_status: str,
    ) -> None:
        success_count, failure_count = self._operation_counts(results)
        logger.info(
            "Funding transfer completed request_id=%s direction=%s main_id=%s asset=%s child_count=%s success_count=%s failure_count=%s operation_status=%s total_ms=%s",
            request_id,
            direction,
            main_id,
            asset,
            len(results),
            success_count,
            failure_count,
            operation_status,
            total_ms,
        )

    def _ensure_write_enabled(self) -> None:
        if self._settings.funding_transfer_write_enabled:
            return
        raise FundingTransferRequestRejected(
            "Live funding transfers are disabled by configuration",
            code="WRITE_DISABLED",
        )

    def _reject(self, message: str, *, code: str, operation_id: str = "") -> FundingTransferRequestRejected:
        return FundingTransferRequestRejected(message, code=code, operation_id=operation_id)

    def _normalize_operation_id(self, operation_id: Any, *, code: str = "OPERATION_ID_REQUIRED") -> str:
        normalized = str(operation_id or "").strip()
        if not normalized:
            raise self._reject("operation_id is required", code=code)
        return normalized

    def _normalize_account_id(self, account_id: Any) -> str:
        normalized = str(account_id or "").strip().lower()
        if not normalized:
            raise self._reject("account_id is required", code="INVALID_ACCOUNT")
        return normalized

    def _ensure_unique_account_id(self, account_id: str, seen_account_ids: set[str]) -> None:
        if account_id in seen_account_ids:
            raise self._reject(
                f"Duplicate sub-account is not allowed in one operation: {account_id}",
                code="DUPLICATE_ACCOUNT",
            )
        seen_account_ids.add(account_id)

    def _ensure_max_account_count(self, account_count: int) -> None:
        if account_count > self._settings.funding_max_accounts_per_operation:
            raise self._reject(
                f"One operation can include at most {self._settings.funding_max_accounts_per_operation} sub-accounts",
                code="MAX_ACCOUNTS_EXCEEDED",
            )

    def _enforce_operation_limits(self, *, account_count: int, total_amount: Decimal, asset: str) -> None:
        self._ensure_max_account_count(account_count)
        if total_amount > self._settings.funding_max_total_amount_per_operation:
            raise self._reject(
                f"One {asset} operation can request at most {self._format_decimal(self._settings.funding_max_total_amount_per_operation)}",
                code="MAX_TOTAL_AMOUNT_EXCEEDED",
            )

    def _ensure_amount_precision(self, amount: Decimal, *, available_text: str, field_name: str, asset: str) -> None:
        allowed_scale = self._decimal_scale(available_text)
        amount_scale = self._decimal_scale(self._format_decimal(amount))
        if amount_scale > allowed_scale:
            raise self._reject(
                f"{field_name} exceeds the allowed precision for {asset}; at most {allowed_scale} decimal places are allowed",
                code="PRECISION_EXCEEDED",
            )

    def _decimal_scale(self, value: str) -> int:
        text = str(value or "").strip()
        if not text or "." not in text:
            return 0
        fraction = text.split(".", 1)[1].rstrip("0")
        return len(fraction)

    def _payload_hash(self, payload: dict[str, Any]) -> str:
        serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

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
                    "Funding read request failure label=%s attempt=%s/%s timeout_s=%s source=%s error_type=%s status_code=%s retry=%s",
                    label,
                    attempt,
                    max_attempts,
                    timeout_s,
                    source,
                    error_type,
                    status_code,
                    should_retry,
                )
                if not should_retry:
                    raise FundingTransferRequestError(
                        self._safe_request_error_message(label, attempt, source=source, status_code=status_code),
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
            self._safe_request_error_message(label, max_attempts, source=source, status_code=status_code),
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
            "message": sanitize_error_summary(exc, fallback="Funding request failed"),
            "error_type": exc.__class__.__name__,
        }

    def _get_main_account(self, main_id: str) -> MainAccountConfig:
        normalized = str(main_id or "").strip().lower()
        main_account = self._settings.monitor_main_accounts.get(normalized)
        if main_account is None:
            raise self._reject(f"Unknown group: {normalized}", code="INVALID_GROUP")
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
            raise self._reject(
                f"Sub-account {child.account_id} does not have a configured UID",
                code="PRECHECK_UNAVAILABLE",
            )
        child_email = email_by_uid.get(child.uid)
        if not child_email:
            raise self._reject(
                f"Main transfer API could not resolve a sub-account mapping for UID {mask_uid(child.uid)}",
                code="PRECHECK_UNAVAILABLE",
            )
        return child_email

    def _base_result(
        self,
        child: MonitorAccountConfig,
        *,
        amount_text: str,
        requested_amount_text: str,
        normalized_amount_text: str,
        precheck_available_text: str | None,
    ) -> dict[str, Any]:
        return {
            "account_id": child.account_id,
            "name": child.child_account_name,
            "uid": child.uid,
            "amount": amount_text,
            "requested_amount": requested_amount_text,
            "normalized_amount": normalized_amount_text,
            "precheck_available_amount": precheck_available_text or "-",
            "executed_amount": "0",
            "transfer_attempted": False,
            "success": False,
            "message": "",
        }

    def _spot_available_map(self, assets: list[dict[str, str]]) -> dict[str, str]:
        return {asset["asset"]: asset["free"] for asset in assets}

    def _normalize_asset(self, asset: Any) -> str:
        normalized = str(asset or "").strip().upper()
        if not normalized:
            raise self._reject("asset is required", code="INVALID_ASSET")
        return normalized

    def _parse_positive_amount(self, value: Any, *, field_name: str) -> Decimal:
        try:
            amount = Decimal(str(value or "0").strip())
        except (InvalidOperation, ValueError) as exc:
            raise self._reject(f"{field_name} must be a valid number", code="INVALID_AMOUNT") from exc
        if not amount.is_finite():
            raise self._reject(f"{field_name} must be a finite number", code="INVALID_AMOUNT")
        if amount < Decimal("0"):
            raise self._reject(f"{field_name} must be greater than or equal to 0", code="INVALID_AMOUNT")
        return amount

    def _format_decimal(self, value: Decimal) -> str:
        normalized = format(value.normalize(), "f")
        if "." in normalized:
            normalized = normalized.rstrip("0").rstrip(".")
        return normalized or "0"

    def _extract_error_message(self, response: httpx.Response) -> str:
        return f"Binance returned an HTTP {response.status_code} error"

    def _format_request_exception(self, exc: Exception) -> str:
        if isinstance(exc, FundingTransferRequestError):
            return str(exc)
        if isinstance(exc, (httpx.TimeoutException, httpx.NetworkError)):
            return "Binance network request failed"
        if isinstance(exc, httpx.HTTPStatusError):
            return self._extract_error_message(exc.response)
        return sanitize_error_summary(exc, fallback="Funding request failed")

    def _safe_failure_message(self, exc: Exception, default_message: str) -> str:
        if isinstance(exc, FundingTransferRequestError):
            if exc.source == "network":
                return f"{default_message} (network issue)"
            if exc.status_code is not None:
                return f"{default_message} (HTTP {exc.status_code})"
        if isinstance(exc, httpx.TimeoutException):
            return f"{default_message} (network timeout)"
        if isinstance(exc, httpx.NetworkError):
            return f"{default_message} (network issue)"
        if isinstance(exc, httpx.HTTPStatusError):
            return f"{default_message} (HTTP {exc.response.status_code})"
        return sanitize_error_summary(default_message, fallback="Funding request failed")

    def _safe_request_error_message(self, label: str, attempts: int, *, source: str, status_code: int | None) -> str:
        if source == "network":
            suffix = "network issue"
        elif status_code is not None:
            suffix = f"HTTP {status_code}"
        else:
            suffix = "request failure"
        return f"{label} failed after {attempts} attempts ({suffix})"

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
