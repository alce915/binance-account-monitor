from __future__ import annotations

import asyncio
import hashlib
import hmac
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlencode

import httpx

from monitor_app.config import MainAccountConfig, MonitorAccountConfig, Settings


class FundingTransferError(RuntimeError):
    pass


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
        main_account = self._get_main_account(main_id)
        email_by_uid: dict[str, str] = {}
        main_reason = ""
        main_funding_assets: list[dict[str, str]] = []

        if main_account.has_transfer_credentials:
            try:
                email_by_uid = await self._get_sub_account_email_map(main_account)
                main_funding_assets = await self._fetch_funding_assets(
                    self._main_credentials(main_account),
                )
            except Exception as exc:
                main_reason = f"主账号归集 API 不可用：{exc}"
        else:
            main_reason = "当前分组未配置主账号归集 API（Excel 需提供 account_id=main 行）"

        children = await asyncio.gather(
            *(self._build_child_overview(main_account, child, email_by_uid, main_reason == "") for child in main_account.children)
        )
        selectable_children = [child for child in children if child["eligible"]]
        assets = sorted(
            {
                *(row["asset"] for row in main_funding_assets),
                *(
                    asset
                    for child in children
                    for asset in child["funding_available"].keys()
                ),
            }
        )
        available = main_reason == "" and bool(selectable_children)
        reason = main_reason or (
            "当前分组暂无可操作子账号，请检查子账号 UID 是否填写且主账号 API 能识别该 UID"
            if not selectable_children
            else ""
        )

        return {
            "main_account_id": main_account.main_id,
            "main_account_name": main_account.name,
            "available": available,
            "reason": reason,
            "assets": assets,
            "main_account": {
                "uid": main_account.transfer_uid,
                "transfer_ready": main_reason == "",
                "reason": main_reason,
                "funding_assets": main_funding_assets,
                "funding_available": self._funding_available_map(main_funding_assets),
            },
            "children": children,
            "updated_at": datetime.now(UTC).isoformat(),
        }

    async def distribute(self, main_id: str, *, asset: str, transfers: list[dict[str, Any]]) -> dict[str, Any]:
        normalized_asset = self._normalize_asset(asset)
        main_account = self._get_main_account(main_id)
        if not main_account.has_transfer_credentials:
            raise FundingTransferError("当前分组未配置主账号归集 API")

        email_by_uid = await self._get_sub_account_email_map(main_account)
        child_by_id = {child.account_id: child for child in main_account.children}
        main_credentials = self._main_credentials(main_account)
        main_funding_assets = await self._fetch_funding_assets(main_credentials)
        main_available = Decimal(self._funding_available_map(main_funding_assets).get(normalized_asset, "0"))

        executable: list[tuple[MonitorAccountConfig, str, Decimal]] = []
        total_amount = Decimal("0")
        for item in transfers:
            account_id = str(item.get("account_id") or "").strip().lower()
            amount = self._parse_positive_amount(item.get("amount"), field_name=f"amount for {account_id}")
            if amount <= Decimal("0"):
                continue
            child = child_by_id.get(account_id)
            if child is None:
                raise FundingTransferError(f"未知子账号：{account_id}")
            child_email = self._resolve_child_email(child, email_by_uid)
            executable.append((child, child_email, amount))
            total_amount += amount

        if not executable:
            raise FundingTransferError("请至少勾选一个子账号并填写大于 0 的金额")
        if total_amount > main_available:
            raise FundingTransferError(
                f"主账号 Funding 可用余额不足：{normalized_asset} 仅有 {self._format_decimal(main_available)}"
            )

        results: list[dict[str, Any]] = []
        for child, child_email, amount in executable:
            results.append(
                await self._distribute_to_child(
                    main_account=main_account,
                    main_credentials=main_credentials,
                    child=child,
                    child_email=child_email,
                    asset=normalized_asset,
                    amount=amount,
                )
            )

        overview = await self.get_group_overview(main_id)
        return {
            "direction": "distribute",
            "asset": normalized_asset,
            "results": results,
            "overview": overview,
            "message": self._summarize_operation("分发", results),
        }

    async def collect(self, main_id: str, *, asset: str, account_ids: list[str]) -> dict[str, Any]:
        normalized_asset = self._normalize_asset(asset)
        main_account = self._get_main_account(main_id)
        if not main_account.has_transfer_credentials:
            raise FundingTransferError("当前分组未配置主账号归集 API")

        email_by_uid = await self._get_sub_account_email_map(main_account)
        child_by_id = {child.account_id: child for child in main_account.children}
        main_credentials = self._main_credentials(main_account)
        selected_ids = [str(account_id or "").strip().lower() for account_id in account_ids if str(account_id or "").strip()]
        if not selected_ids:
            raise FundingTransferError("请至少勾选一个子账号")

        results: list[dict[str, Any]] = []
        for account_id in selected_ids:
            child = child_by_id.get(account_id)
            if child is None:
                raise FundingTransferError(f"未知子账号：{account_id}")
            child_email = self._resolve_child_email(child, email_by_uid)
            results.append(
                await self._collect_from_child(
                    main_credentials=main_credentials,
                    child=child,
                    child_email=child_email,
                    asset=normalized_asset,
                )
            )

        overview = await self.get_group_overview(main_id)
        return {
            "direction": "collect",
            "asset": normalized_asset,
            "results": results,
            "overview": overview,
            "message": self._summarize_operation("归集", results),
        }

    async def _build_child_overview(
        self,
        main_account: MainAccountConfig,
        child: MonitorAccountConfig,
        email_by_uid: dict[str, str],
        main_ready: bool,
    ) -> dict[str, Any]:
        child_reason = ""
        eligible = True
        if not child.uid:
            eligible = False
            child_reason = "未配置子账号 UID"
        elif main_ready and child.uid not in email_by_uid:
            eligible = False
            child_reason = "主账号 API 未识别该 UID 对应的子账号"

        funding_assets: list[dict[str, str]] = []
        if child.api_key and child.api_secret:
            try:
                funding_assets = await self._fetch_funding_assets(self._child_credentials(child))
            except Exception as exc:
                eligible = False
                if not child_reason:
                    child_reason = f"Funding 余额查询失败：{exc}"
        else:
            eligible = False
            if not child_reason:
                child_reason = "未配置子账号 API"

        return {
            "account_id": child.account_id,
            "child_account_id": child.child_account_id,
            "name": child.child_account_name,
            "uid": child.uid,
            "eligible": main_ready and eligible,
            "reason": "" if main_ready and eligible else (child_reason or "当前分组主账号归集 API 不可用"),
            "funding_assets": funding_assets,
            "funding_available": self._funding_available_map(funding_assets),
        }

    async def _distribute_to_child(
        self,
        *,
        main_account: MainAccountConfig,
        main_credentials: BinanceCredentials,
        child: MonitorAccountConfig,
        child_email: str,
        asset: str,
        amount: Decimal,
    ) -> dict[str, Any]:
        child_credentials = self._child_credentials(child)
        amount_text = self._format_decimal(amount)
        result = self._base_result(child, amount_text)
        moved_main_to_spot = False
        moved_main_to_child = False

        try:
            await self._signed_request(
                main_credentials,
                "POST",
                "/sapi/v1/asset/transfer",
                {"type": "FUNDING_MAIN", "asset": asset, "amount": amount_text},
            )
            moved_main_to_spot = True
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
            moved_main_to_child = True
            await self._signed_request(
                child_credentials,
                "POST",
                "/sapi/v1/asset/transfer",
                {"type": "MAIN_FUNDING", "asset": asset, "amount": amount_text},
            )
            result["success"] = True
            result["message"] = "分发成功"
            return result
        except Exception as exc:
            rollback_message = await self._rollback_distribution(
                main_credentials=main_credentials,
                child_email=child_email,
                asset=asset,
                amount_text=amount_text,
                moved_main_to_spot=moved_main_to_spot,
                moved_main_to_child=moved_main_to_child,
            )
            result["success"] = False
            result["message"] = f"分发失败：{exc}{rollback_message}"
            return result

    async def _collect_from_child(
        self,
        *,
        main_credentials: BinanceCredentials,
        child: MonitorAccountConfig,
        child_email: str,
        asset: str,
    ) -> dict[str, Any]:
        child_credentials = self._child_credentials(child)
        child_funding_assets = await self._fetch_funding_assets(child_credentials)
        available_amount = Decimal(self._funding_available_map(child_funding_assets).get(asset, "0"))
        amount_text = self._format_decimal(available_amount)
        result = self._base_result(child, amount_text)
        moved_child_to_spot = False
        moved_child_to_main = False

        if available_amount <= Decimal("0"):
            result["success"] = False
            result["message"] = "当前代币在子账号 Funding 中无可归集余额"
            return result

        try:
            await self._signed_request(
                child_credentials,
                "POST",
                "/sapi/v1/asset/transfer",
                {"type": "FUNDING_MAIN", "asset": asset, "amount": amount_text},
            )
            moved_child_to_spot = True
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
            moved_child_to_main = True
            await self._signed_request(
                main_credentials,
                "POST",
                "/sapi/v1/asset/transfer",
                {"type": "MAIN_FUNDING", "asset": asset, "amount": amount_text},
            )
            result["success"] = True
            result["message"] = "归集成功"
            return result
        except Exception as exc:
            rollback_message = await self._rollback_collect(
                main_credentials=main_credentials,
                child_credentials=child_credentials,
                child_email=child_email,
                asset=asset,
                amount_text=amount_text,
                moved_child_to_spot=moved_child_to_spot,
                moved_child_to_main=moved_child_to_main,
            )
            result["success"] = False
            result["message"] = f"归集失败：{exc}{rollback_message}"
            return result

    async def _rollback_distribution(
        self,
        *,
        main_credentials: BinanceCredentials,
        child_email: str,
        asset: str,
        amount_text: str,
        moved_main_to_spot: bool,
        moved_main_to_child: bool,
    ) -> str:
        try:
            if moved_main_to_child:
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
            if moved_main_to_spot:
                await self._signed_request(
                    main_credentials,
                    "POST",
                    "/sapi/v1/asset/transfer",
                    {"type": "MAIN_FUNDING", "asset": asset, "amount": amount_text},
                )
        except Exception as exc:
            return f"；回滚失败，资金可能停留在中间账户：{exc}"
        return "；已尝试回滚到主账号 Funding"

    async def _rollback_collect(
        self,
        *,
        main_credentials: BinanceCredentials,
        child_credentials: BinanceCredentials,
        child_email: str,
        asset: str,
        amount_text: str,
        moved_child_to_spot: bool,
        moved_child_to_main: bool,
    ) -> str:
        try:
            if moved_child_to_main:
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
            if moved_child_to_spot:
                await self._signed_request(
                    child_credentials,
                    "POST",
                    "/sapi/v1/asset/transfer",
                    {"type": "MAIN_FUNDING", "asset": asset, "amount": amount_text},
                )
        except Exception as exc:
            return f"；回滚失败，资金可能已留在主账号现货或子账号现货：{exc}"
        return "；已尝试回滚到子账号 Funding"

    async def _get_sub_account_email_map(self, main_account: MainAccountConfig) -> dict[str, str]:
        credentials = self._main_credentials(main_account)
        page = 1
        result: dict[str, str] = {}
        while True:
            payload = await self._signed_request(
                credentials,
                "GET",
                "/sapi/v1/sub-account/list",
                {"page": page, "limit": 200},
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

    async def _fetch_funding_assets(self, credentials: BinanceCredentials) -> list[dict[str, str]]:
        payload = await self._signed_request(
            credentials,
            "POST",
            "/sapi/v1/asset/get-funding-asset",
            {"needBtcValuation": "false"},
        )
        rows = payload if isinstance(payload, list) else []
        assets: list[dict[str, str]] = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            asset = str(item.get("asset") or "").strip().upper()
            if not asset:
                continue
            free = Decimal(str(item.get("free") or "0"))
            locked = Decimal(str(item.get("locked") or "0"))
            freeze = Decimal(str(item.get("freeze") or "0"))
            withdrawing = Decimal(str(item.get("withdrawing") or "0"))
            total = free + locked + freeze + withdrawing
            if total <= Decimal("0"):
                continue
            assets.append(
                {
                    "asset": asset,
                    "free": self._format_decimal(free),
                    "locked": self._format_decimal(locked),
                    "freeze": self._format_decimal(freeze),
                    "withdrawing": self._format_decimal(withdrawing),
                    "total": self._format_decimal(total),
                }
            )
        assets.sort(key=lambda entry: entry["asset"])
        return assets

    async def _signed_request(
        self,
        credentials: BinanceCredentials,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
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
        timeout_s = max(self._settings.binance_secondary_timeout_ms, 1) / 1000
        try:
            response = await client.request(method, f"{path}?{query}&signature={signature}", timeout=timeout_s)
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise FundingTransferError(f"Binance 网络请求失败：{exc}") from exc
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            detail = self._extract_error_message(exc.response)
            raise FundingTransferError(detail) from exc
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

    def _get_main_account(self, main_id: str) -> MainAccountConfig:
        normalized = str(main_id or "").strip().lower()
        main_account = self._settings.monitor_main_accounts.get(normalized)
        if main_account is None:
            raise FundingTransferError(f"未找到分组：{normalized}")
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
            raise FundingTransferError(f"子账号 {child.account_id} 未配置 UID")
        child_email = email_by_uid.get(child.uid)
        if not child_email:
            raise FundingTransferError(f"主账号 API 未找到 UID {child.uid} 对应的子账号邮箱")
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

    def _funding_available_map(self, assets: list[dict[str, str]]) -> dict[str, str]:
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
                return f"Binance 返回错误：{message}"
        return f"Binance 返回错误：HTTP {response.status_code}"

    def _summarize_operation(self, action_label: str, results: list[dict[str, Any]]) -> str:
        success_count = sum(1 for result in results if result["success"])
        total_count = len(results)
        if success_count == total_count:
            return f"{action_label}成功，共处理 {total_count} 个子账号"
        if success_count == 0:
            return f"{action_label}失败，未成功处理任何子账号"
        return f"{action_label}部分成功，成功 {success_count} 个，失败 {total_count - success_count} 个"
