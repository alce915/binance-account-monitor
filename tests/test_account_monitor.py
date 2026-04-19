from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from pathlib import Path

import pytest

from monitor_app.account_monitor import AccountMonitorController
from monitor_app.config import MainAccountConfig, MonitorAccountConfig, Settings


class FakeMonitorGateway:
    call_counts: dict[str, int] = {}

    def __init__(self, account: MonitorAccountConfig) -> None:
        self.account = account

    async def get_unified_account_snapshot(
        self,
        *,
        history_window_days: int = 7,
        income_limit: int = 100,
        interest_limit: int = 100,
        previous_snapshot: dict | None = None,
        mark_price_provider=None,
        refresh_id: str | None = None,
    ) -> dict:
        FakeMonitorGateway.call_counts[self.account.account_id] = FakeMonitorGateway.call_counts.get(self.account.account_id, 0) + 1
        return {
            "status": "ok",
            "source": "papi",
            "account_id": self.account.account_id,
            "account_name": self.account.display_name,
            "main_account_id": self.account.main_account_id,
            "main_account_name": self.account.main_account_name,
            "child_account_id": self.account.child_account_id,
            "child_account_name": self.account.child_account_name,
            "account_status": "NORMAL",
            "updated_at": "2026-03-25T00:00:00+00:00",
            "message": "ok",
            "totals": {
                "equity": Decimal("1200"),
                "margin": Decimal("200"),
                "available_balance": Decimal("950"),
                "unrealized_pnl": Decimal("18"),
                "total_income": Decimal("7.5"),
                "total_commission": Decimal("-1.8"),
                "total_distribution": Decimal("1.5"),
                "distribution_apy_7d": Decimal("0.06517857142857142857142857143"),
                "total_interest": Decimal("1.2"),
            },
            "positions": [],
            "assets": [],
            "income_summary": {
                "window_days": history_window_days,
                "records": 1,
                "total_income": Decimal("7.5"),
                "total_commission": Decimal("-1.8"),
                "by_type": {"COMMISSION": Decimal("-1.8")},
                "by_asset": {},
            },
            "distribution_summary": {
                "window_days": history_window_days,
                "records": 1,
                "total_distribution": Decimal("1.5"),
                "by_type": {"RWUSD rewards distribution": Decimal("1.5")},
                "by_asset": {"RWUSD": Decimal("1.5")},
            },
            "distribution_profit_summary": {
                "today": {"label": "今日收益丨收益率", "amount": Decimal("0.2"), "rate": Decimal("0.0001666667"), "start_at": "2026-03-25T16:00:00+00:00", "complete": True},
                "week": {"label": "本周收益丨收益率", "amount": Decimal("0.8"), "rate": Decimal("0.0006666667"), "start_at": "2026-03-23T16:00:00+00:00", "complete": True},
                "month": {"label": "本月收益丨收益率", "amount": Decimal("1.2"), "rate": Decimal("0.001"), "start_at": "2026-02-28T16:00:00+00:00", "complete": True},
                "year": {"label": "年度收益丨收益率", "amount": Decimal("1.4"), "rate": Decimal("0.0011666667"), "start_at": "2025-12-31T16:00:00+00:00", "complete": True},
                "all": {"label": "全部收益丨收益率", "amount": Decimal("1.5"), "rate": Decimal("0.00125"), "start_at": "2025-03-01T00:00:00+00:00", "complete": True},
                "backfill_complete": True,
            },
            "interest_summary": {"window_days": history_window_days, "records": 1, "margin_interest_total": Decimal("1.2"), "negative_balance_interest_total": Decimal("0"), "total_interest": Decimal("1.2")},
            "section_errors": {},
            "diagnostics": {
                "refresh_id": refresh_id,
                "timings": {"gateway_total_ms": 12},
                "fallback_sections": [],
            },
        }

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_account_monitor_controller_groups_and_filters_accounts(tmp_path: Path) -> None:
    FakeMonitorGateway.call_counts = {}
    settings = Settings(
        _env_file=None,
        monitor_refresh_interval_ms=50,
        monitor_history_window_days=3,
        monitor_history_db_path=tmp_path / "history.db",
    )
    account1 = MonitorAccountConfig(account_id="group_a.sub1", child_account_id="sub1", child_account_name="Sub One", main_account_id="group_a", main_account_name="Group A", api_key="k1", api_secret="s1")
    account2 = MonitorAccountConfig(account_id="group_a.sub2", child_account_id="sub2", child_account_name="Sub Two", main_account_id="group_a", main_account_name="Group A", api_key="k2", api_secret="s2")
    account3 = MonitorAccountConfig(account_id="group_b.sub1", child_account_id="sub1", child_account_name="Sub Three", main_account_id="group_b", main_account_name="Group B", api_key="k3", api_secret="s3")
    settings.monitor_accounts = {account.account_id: account for account in (account1, account2, account3)}
    settings.monitor_main_accounts = {
        "group_a": MainAccountConfig(main_id="group_a", name="Group A", children=(account1, account2)),
        "group_b": MainAccountConfig(main_id="group_b", name="Group B", children=(account3,)),
    }
    controller = AccountMonitorController(settings, gateway_factory=lambda account: FakeMonitorGateway(account))

    queue = await controller.subscribe(["group_a.sub1", "group_b.sub1"])
    try:
        initial = await asyncio.wait_for(queue.get(), timeout=1)
        refreshed = await asyncio.wait_for(queue.get(), timeout=1)
    finally:
        controller.unsubscribe(queue)
        await controller.close()

    assert initial["event"] == "monitor_snapshot"
    assert refreshed["data"]["summary"]["account_count"] == 2
    assert len(refreshed["data"]["groups"]) == 2
    assert refreshed["data"]["groups"][0]["main_account_id"] == "group_a"
    assert refreshed["data"]["summary"]["equity"] == "2400"
    assert refreshed["data"]["summary"]["total_commission"] == "-3.6"
    assert refreshed["data"]["summary"]["total_distribution"] == "3.0"
    assert refreshed["data"]["groups"][0]["profit_summary"]["all"]["amount"] == "3.0"
    assert Decimal(refreshed["data"]["summary"]["distribution_apy_7d"]).quantize(Decimal("0.00000001")) == Decimal("0.06517857")
    assert refreshed["data"]["profit_summary"]["today"]["amount"] == "0.4"
    assert refreshed["data"]["profit_summary"]["all"]["complete"] is True
    assert refreshed["data"]["profit_summary"]["backfill_complete"] is True
    assert Decimal(refreshed["data"]["profit_summary"]["all"]["rate"]).quantize(Decimal("0.00000001")) == Decimal("0.00125000")
    assert refreshed["data"]["service"]["monitor_enabled"] is True


@pytest.mark.asyncio
async def test_account_monitor_background_refresh_runs_without_subscribers_after_start(tmp_path: Path) -> None:
    FakeMonitorGateway.call_counts = {}
    settings = Settings(
        _env_file=None,
        monitor_refresh_interval_ms=50,
        monitor_history_window_days=3,
        monitor_history_db_path=tmp_path / "history.db",
        unimmr_alerts_enabled=True,
    )
    account = MonitorAccountConfig(
        account_id="group_a.sub1",
        child_account_id="sub1",
        child_account_name="Sub One",
        main_account_id="group_a",
        main_account_name="Group A",
        api_key="k1",
        api_secret="s1",
    )
    settings.monitor_accounts = {account.account_id: account}
    settings.monitor_main_accounts = {
        "group_a": MainAccountConfig(main_id="group_a", name="Group A", children=(account,)),
    }
    controller = AccountMonitorController(settings, gateway_factory=lambda selected: FakeMonitorGateway(selected))
    try:
        await controller.start()
        await asyncio.sleep(0.12)
    finally:
        await controller.close()

    assert FakeMonitorGateway.call_counts["group_a.sub1"] >= 2


@pytest.mark.asyncio
async def test_account_monitor_controller_can_disable_and_enable_monitoring(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        monitor_refresh_interval_ms=50,
        monitor_history_window_days=3,
        monitor_history_db_path=tmp_path / "history.db",
    )
    account = MonitorAccountConfig(
        account_id="group_a.sub1",
        child_account_id="sub1",
        child_account_name="Sub One",
        main_account_id="group_a",
        main_account_name="Group A",
        api_key="k1",
        api_secret="s1",
    )
    settings.monitor_accounts = {account.account_id: account}
    settings.monitor_main_accounts = {
        "group_a": MainAccountConfig(main_id="group_a", name="Group A", children=(account,)),
    }
    controller = AccountMonitorController(settings, gateway_factory=lambda selected: FakeMonitorGateway(selected))

    queue = await controller.subscribe()
    try:
        await asyncio.wait_for(queue.get(), timeout=1)
        await asyncio.wait_for(queue.get(), timeout=1)

        disabled = await controller.set_monitor_enabled(False)
        disabled_event = await asyncio.wait_for(queue.get(), timeout=1)
        enabled = await controller.set_monitor_enabled(True)
        enabled_event = await asyncio.wait_for(queue.get(), timeout=1)
        refreshed_event = await asyncio.wait_for(queue.get(), timeout=1)
    finally:
        controller.unsubscribe(queue)
        await controller.close()

    assert disabled["status"] == "disabled"
    assert disabled["message"] == "监控已禁用"
    assert disabled["service"]["monitor_enabled"] is False
    assert disabled_event["data"]["status"] == "disabled"
    assert disabled_event["data"]["message"] == "监控已禁用"
    assert disabled_event["data"]["service"]["monitor_enabled"] is False

    assert enabled["service"]["monitor_enabled"] is True
    assert enabled_event["data"]["service"]["monitor_enabled"] is True
    assert refreshed_event["data"]["summary"]["account_count"] == 1


class SlowMonitorGateway(FakeMonitorGateway):
    call_count = 0
    release_event = asyncio.Event()
    started_event = asyncio.Event()

    async def get_unified_account_snapshot(
        self,
        *,
        history_window_days: int = 7,
        income_limit: int = 100,
        interest_limit: int = 100,
        previous_snapshot: dict | None = None,
        mark_price_provider=None,
        refresh_id: str | None = None,
    ) -> dict:
        SlowMonitorGateway.call_count += 1
        if SlowMonitorGateway.call_count > 1:
            SlowMonitorGateway.started_event.set()
            await SlowMonitorGateway.release_event.wait()
        return await super().get_unified_account_snapshot(
            history_window_days=history_window_days,
            income_limit=income_limit,
            interest_limit=interest_limit,
            previous_snapshot=previous_snapshot,
            mark_price_provider=mark_price_provider,
            refresh_id=refresh_id,
        )


@pytest.mark.asyncio
async def test_refresh_keeps_existing_payload_until_new_cycle_completes(tmp_path: Path) -> None:
    SlowMonitorGateway.call_count = 0
    SlowMonitorGateway.release_event = asyncio.Event()
    SlowMonitorGateway.started_event = asyncio.Event()
    settings = Settings(
        _env_file=None,
        monitor_refresh_interval_ms=999999,
        monitor_history_window_days=3,
        monitor_history_db_path=tmp_path / "history.db",
    )
    account = MonitorAccountConfig(
        account_id="group_a.sub1",
        child_account_id="sub1",
        child_account_name="Sub One",
        main_account_id="group_a",
        main_account_name="Group A",
        api_key="k1",
        api_secret="s1",
    )
    settings.monitor_accounts = {account.account_id: account}
    settings.monitor_main_accounts = {
        "group_a": MainAccountConfig(main_id="group_a", name="Group A", children=(account,)),
    }
    controller = AccountMonitorController(settings, gateway_factory=lambda selected: SlowMonitorGateway(selected))
    await controller.refresh_now()
    baseline = controller.current_snapshot()

    refresh_task = asyncio.create_task(controller.refresh_now())
    await asyncio.wait_for(SlowMonitorGateway.started_event.wait(), timeout=1)
    assert controller.current_snapshot()["updated_at"] == baseline["updated_at"]
    assert controller.current_snapshot()["summary"] == baseline["summary"]
    SlowMonitorGateway.release_event.set()
    await asyncio.wait_for(refresh_task, timeout=1)
    await controller.close()


class FailAfterFirstGateway(FakeMonitorGateway):
    call_counts: dict[str, int] = {}

    async def get_unified_account_snapshot(
        self,
        *,
        history_window_days: int = 7,
        income_limit: int = 100,
        interest_limit: int = 100,
        previous_snapshot: dict | None = None,
        mark_price_provider=None,
        refresh_id: str | None = None,
    ) -> dict:
        count = FailAfterFirstGateway.call_counts.get(self.account.account_id, 0) + 1
        FailAfterFirstGateway.call_counts[self.account.account_id] = count
        if count >= 2:
            raise RuntimeError("temporary refresh failure")
        return await super().get_unified_account_snapshot(
            history_window_days=history_window_days,
            income_limit=income_limit,
            interest_limit=interest_limit,
            previous_snapshot=previous_snapshot,
            mark_price_provider=mark_price_provider,
            refresh_id=refresh_id,
        )


@pytest.mark.asyncio
async def test_refresh_failure_preserves_previous_payload(tmp_path: Path) -> None:
    FailAfterFirstGateway.call_counts = {}
    settings = Settings(
        _env_file=None,
        monitor_refresh_interval_ms=999999,
        monitor_history_window_days=3,
        monitor_history_db_path=tmp_path / "history.db",
    )
    account = MonitorAccountConfig(
        account_id="group_a.sub1",
        child_account_id="sub1",
        child_account_name="Sub One",
        main_account_id="group_a",
        main_account_name="Group A",
        api_key="k1",
        api_secret="s1",
    )
    settings.monitor_accounts = {account.account_id: account}
    settings.monitor_main_accounts = {
        "group_a": MainAccountConfig(main_id="group_a", name="Group A", children=(account,)),
    }
    controller = AccountMonitorController(settings, gateway_factory=lambda selected: FailAfterFirstGateway(selected))
    try:
        first = await controller.refresh_now()
        first_updated_at = first["updated_at"]
        second = await controller.refresh_now()
    finally:
        await controller.close()

    assert first["refresh_result"]["success"] is True
    assert first["refresh_result"]["refresh_id"]
    assert first["refresh_result"]["failed_accounts"] == []
    assert first["refresh_result"]["fallback_sections"] == []
    assert second["refresh_result"]["success"] is False
    assert second["refresh_result"]["message"].startswith("刷新失败，已保留当前数据：")
    assert second["updated_at"] == first_updated_at
    assert second["summary"]["account_count"] == 1
    assert second["summary"]["success_count"] == 1
    assert "duration_ms" in second["refresh_result"]
    assert second["refresh_result"]["failed_accounts"][0]["account_id"] == "group_a.sub1"


@pytest.mark.asyncio
async def test_auto_refresh_failure_logs_and_broadcasts_warning(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    FailAfterFirstGateway.call_counts = {}
    settings = Settings(
        _env_file=None,
        monitor_refresh_interval_ms=50,
        monitor_history_window_days=3,
        monitor_history_db_path=tmp_path / "history.db",
    )
    account = MonitorAccountConfig(
        account_id="group_a.sub1",
        child_account_id="sub1",
        child_account_name="Sub One",
        main_account_id="group_a",
        main_account_name="Group A",
        api_key="k1",
        api_secret="s1",
    )
    settings.monitor_accounts = {account.account_id: account}
    settings.monitor_main_accounts = {
        "group_a": MainAccountConfig(main_id="group_a", name="Group A", children=(account,)),
    }
    controller = AccountMonitorController(settings, gateway_factory=lambda selected: FailAfterFirstGateway(selected))
    await controller.refresh_now()

    async def failing_refresh_once(*, refresh_id: str, reason: str) -> tuple[dict, bool]:
        raise RuntimeError("temporary loop failure")

    controller._refresh_once = failing_refresh_once  # type: ignore[method-assign]
    caplog.set_level("ERROR", logger="uvicorn.error")
    queue = await controller.subscribe()
    try:
        initial = await asyncio.wait_for(queue.get(), timeout=1)
        warning = await asyncio.wait_for(queue.get(), timeout=1)
    finally:
        controller.unsubscribe(queue)
        await controller.close()

    assert initial["data"]["status"] == "ok"
    assert warning["data"]["status"] == "error"
    assert "temporary loop failure" in warning["data"]["message"]
    assert warning["data"]["message"].startswith("自动刷新失败，已保留当前数据：")
    assert any("Auto refresh failed refresh_id=" in record.getMessage() for record in caplog.records)


class PartialFailureGateway(FakeMonitorGateway):
    call_counts: dict[str, int] = {}

    async def get_unified_account_snapshot(
        self,
        *,
        history_window_days: int = 7,
        income_limit: int = 100,
        interest_limit: int = 100,
        previous_snapshot: dict | None = None,
        mark_price_provider=None,
        refresh_id: str | None = None,
    ) -> dict:
        count = PartialFailureGateway.call_counts.get(self.account.account_id, 0) + 1
        PartialFailureGateway.call_counts[self.account.account_id] = count
        if self.account.account_id == "group_a.sub2" and count >= 2:
            raise RuntimeError("temporary refresh failure")
        return await super().get_unified_account_snapshot(
            history_window_days=history_window_days,
            income_limit=income_limit,
            interest_limit=interest_limit,
            previous_snapshot=previous_snapshot,
            mark_price_provider=mark_price_provider,
            refresh_id=refresh_id,
        )


@pytest.mark.asyncio
async def test_refresh_partial_failure_commits_partial_payload(tmp_path: Path) -> None:
    PartialFailureGateway.call_counts = {}
    settings = Settings(
        _env_file=None,
        monitor_refresh_interval_ms=999999,
        monitor_history_window_days=3,
        monitor_history_db_path=tmp_path / "history.db",
    )
    account1 = MonitorAccountConfig(
        account_id="group_a.sub1",
        child_account_id="sub1",
        child_account_name="Sub One",
        main_account_id="group_a",
        main_account_name="Group A",
        api_key="k1",
        api_secret="s1",
    )
    account2 = MonitorAccountConfig(
        account_id="group_a.sub2",
        child_account_id="sub2",
        child_account_name="Sub Two",
        main_account_id="group_a",
        main_account_name="Group A",
        api_key="k2",
        api_secret="s2",
    )
    settings.monitor_accounts = {
        account.account_id: account
        for account in (account1, account2)
    }
    settings.monitor_main_accounts = {
        "group_a": MainAccountConfig(main_id="group_a", name="Group A", children=(account1, account2)),
    }
    controller = AccountMonitorController(settings, gateway_factory=lambda selected: PartialFailureGateway(selected))
    try:
        first = await controller.refresh_now()
        second = await controller.refresh_now()
    finally:
        await controller.close()

    assert first["refresh_result"]["success"] is True
    assert second["refresh_result"]["success"] is True
    assert second["status"] == "partial"
    assert second["message"] == "部分账号刷新失败"
    assert second["summary"]["account_count"] == 2
    assert second["summary"]["success_count"] == 1
    assert second["summary"]["error_count"] == 1
    assert second["refresh_result"]["failed_accounts"] == [
        {"account_id": "group_a.sub2", "message": "temporary refresh failure"}
    ]
    failing_account = next(
        account
        for group in second["groups"]
        for account in group["accounts"]
        if account["account_id"] == "group_a.sub2"
    )
    assert failing_account["status"] == "error"
    assert failing_account["message"] == "temporary refresh failure"


class RecordingUniMmrAlerts:
    def __init__(self) -> None:
        self.payloads: list[dict] = []

    async def evaluate_payload(self, payload: dict) -> dict:
        self.payloads.append(payload)
        return {"triggered": 1, "has_danger": True, "queued": True}

    async def simulate_payload(self, payload: dict) -> dict:
        self.payloads.append(payload)
        return {"triggered": 1, "has_danger": True, "queued": True, "simulated": True}

    async def status_summary(self, *, monitor_enabled: bool | None = None) -> dict:
        return {
            "enabled": True if monitor_enabled is None else bool(monitor_enabled),
            "configured": True,
            "monitor_enabled": True if monitor_enabled is None else bool(monitor_enabled),
            "telegram": {},
            "accounts": [],
        }

    async def close(self) -> None:
        return None

    def has_danger_accounts(self) -> bool:
        return False


@pytest.mark.asyncio
async def test_simulate_unimmr_alerts_overrides_current_snapshot_values(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        monitor_refresh_interval_ms=50,
        monitor_history_window_days=3,
        monitor_history_db_path=tmp_path / "history.db",
        unimmr_alerts_enabled=True,
    )
    account = MonitorAccountConfig(
        account_id="group_a.sub1",
        child_account_id="sub1",
        child_account_name="Sub One",
        main_account_id="group_a",
        main_account_name="Group A",
        api_key="k1",
        api_secret="s1",
    )
    account2 = MonitorAccountConfig(
        account_id="group_a.sub2",
        child_account_id="sub2",
        child_account_name="Sub Two",
        main_account_id="group_a",
        main_account_name="Group A",
        api_key="k2",
        api_secret="s2",
    )
    settings.monitor_accounts = {
        account.account_id: account
        for account in (account, account2)
    }
    settings.monitor_main_accounts = {
        "group_a": MainAccountConfig(main_id="group_a", name="Group A", children=(account, account2)),
    }
    controller = AccountMonitorController(settings, gateway_factory=lambda selected: FakeMonitorGateway(selected))
    fake_alerts = RecordingUniMmrAlerts()
    controller._unimmr_alerts = fake_alerts
    controller._last_payload = controller._build_payload(
        [
            {
                "status": "ok",
                "account_id": "group_a.sub1",
                "account_name": "Group A / Sub One",
                "main_account_id": "group_a",
                "main_account_name": "Group A",
                "child_account_id": "sub1",
                "child_account_name": "Sub One",
                "uni_mmr": Decimal("2.80"),
            },
            {
                "status": "ok",
                "account_id": "group_a.sub2",
                "account_name": "Group A / Sub Two",
                "main_account_id": "group_a",
                "main_account_name": "Group A",
                "child_account_id": "sub2",
                "child_account_name": "Sub Two",
                "uni_mmr": Decimal("2.70"),
            },
        ]
    )
    try:
        result = await controller.simulate_unimmr_alerts(
            [{"account_id": "group_a.sub1", "uni_mmr": Decimal("1.18")}]
        )
    finally:
        await controller.close()

    assert result["triggered"] == 1
    assert result["simulated"] is True
    assert len(fake_alerts.payloads[0]["accounts"]) == 1
    assert fake_alerts.payloads[0]["accounts"][0]["account_id"] == "group_a.sub1"
    assert fake_alerts.payloads[0]["accounts"][0]["uni_mmr"] == Decimal("1.18")


@pytest.mark.asyncio
async def test_simulate_unimmr_alerts_rejects_unknown_account(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        monitor_refresh_interval_ms=50,
        monitor_history_window_days=3,
        monitor_history_db_path=tmp_path / "history.db",
        unimmr_alerts_enabled=True,
    )
    account = MonitorAccountConfig(
        account_id="group_a.sub1",
        child_account_id="sub1",
        child_account_name="Sub One",
        main_account_id="group_a",
        main_account_name="Group A",
        api_key="k1",
        api_secret="s1",
    )
    settings.monitor_accounts = {account.account_id: account}
    settings.monitor_main_accounts = {
        "group_a": MainAccountConfig(main_id="group_a", name="Group A", children=(account,)),
    }
    controller = AccountMonitorController(settings, gateway_factory=lambda selected: FakeMonitorGateway(selected))
    controller._unimmr_alerts = RecordingUniMmrAlerts()
    controller._last_payload = controller._build_payload(
        [
            {
                "status": "ok",
                "account_id": "group_a.sub1",
                "account_name": "Group A / Sub One",
                "main_account_id": "group_a",
                "main_account_name": "Group A",
                "child_account_id": "sub1",
                "child_account_name": "Sub One",
                "uni_mmr": Decimal("2.80"),
            }
        ]
    )
    try:
        with pytest.raises(ValueError, match="Unknown UniMMR simulation account_id"):
            await controller.simulate_unimmr_alerts(
                [{"account_id": "group_a.unknown", "uni_mmr": Decimal("1.18")}]
            )
    finally:
        await controller.close()


@pytest.mark.asyncio
async def test_unimmr_alert_status_reports_disabled_when_monitor_is_off(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        monitor_refresh_interval_ms=50,
        monitor_history_window_days=3,
        monitor_history_db_path=tmp_path / "history.db",
        unimmr_alerts_enabled=True,
    )
    account = MonitorAccountConfig(
        account_id="group_a.sub1",
        child_account_id="sub1",
        child_account_name="Sub One",
        main_account_id="group_a",
        main_account_name="Group A",
        api_key="k1",
        api_secret="s1",
    )
    settings.monitor_accounts = {account.account_id: account}
    settings.monitor_main_accounts = {
        "group_a": MainAccountConfig(main_id="group_a", name="Group A", children=(account,)),
    }
    controller = AccountMonitorController(settings, gateway_factory=lambda selected: FakeMonitorGateway(selected))
    try:
        await controller.start()
        enabled_status = await controller.unimmr_alert_status()
        await controller.set_monitor_enabled(False)
        disabled_status = await controller.unimmr_alert_status()
    finally:
        await controller.close()

    assert enabled_status["enabled"] is True
    assert disabled_status["enabled"] is False
    assert disabled_status["monitor_enabled"] is False


class TrackingGateway(FakeMonitorGateway):
    close_calls: dict[str, int] = {}

    async def close(self) -> None:
        TrackingGateway.close_calls[self.account.account_id] = TrackingGateway.close_calls.get(self.account.account_id, 0) + 1


@pytest.mark.asyncio
async def test_reload_accounts_closes_removed_gateways_and_updates_service_ids(tmp_path: Path) -> None:
    TrackingGateway.close_calls = {}
    config_path = tmp_path / "config" / "binance_monitor_accounts.json"
    settings = Settings(
        _env_file=None,
        monitor_accounts_file=config_path,
        monitor_refresh_interval_ms=999999,
        monitor_history_window_days=3,
        monitor_history_db_path=tmp_path / "history.db",
    )
    _write_accounts_file(
        config_path,
        {
            "main_accounts": [
                {
                    "main_id": "group_a",
                    "name": "Group A",
                    "children": [
                        {"account_id": "sub1", "name": "Sub One", "api_key": "k1", "api_secret": "s1"},
                        {"account_id": "sub2", "name": "Sub Two", "api_key": "k2", "api_secret": "s2"},
                    ],
                }
            ]
        },
    )
    settings.load_monitor_accounts()

    def gateway_factory(account: MonitorAccountConfig) -> TrackingGateway:
        return TrackingGateway(account)

    controller = AccountMonitorController(settings, gateway_factory=gateway_factory)
    try:
        await controller.refresh_now()
        first_gateway = controller._gateways["group_a.sub1"]
        second_gateway = controller._gateways["group_a.sub2"]

        _write_accounts_file(
            config_path,
            {
                "main_accounts": [
                    {
                        "main_id": "group_a",
                        "name": "Group A",
                        "children": [
                            {"account_id": "sub1", "name": "Sub One", "api_key": "k1", "api_secret": "s1"},
                        ],
                    },
                    {
                        "main_id": "group_b",
                        "name": "Group B",
                        "children": [
                            {"account_id": "sub1", "name": "Sub Three", "api_key": "k3", "api_secret": "s3"},
                        ],
                    },
                ]
            },
        )

        reloaded = await controller.reload_accounts()
        await controller.refresh_now()
        assert reloaded["service"]["account_ids"] == ["group_a.sub1", "group_b.sub1"]
        assert reloaded["service"]["main_account_ids"] == ["group_a", "group_b"]
        assert controller._gateways["group_a.sub1"] is first_gateway
        assert "group_a.sub2" not in controller._gateways
        assert controller._gateways["group_b.sub1"].account.account_id == "group_b.sub1"
        assert second_gateway.account.account_id == "group_a.sub2"
        assert TrackingGateway.close_calls == {"group_a.sub2": 1}
    finally:
        await controller.close()


@pytest.mark.asyncio
async def test_reload_accounts_recreates_gateway_when_account_config_changes(tmp_path: Path) -> None:
    TrackingGateway.close_calls = {}
    config_path = tmp_path / "config" / "binance_monitor_accounts.json"
    settings = Settings(
        _env_file=None,
        monitor_accounts_file=config_path,
        monitor_refresh_interval_ms=999999,
        monitor_history_window_days=3,
        monitor_history_db_path=tmp_path / "history.db",
    )
    _write_accounts_file(
        config_path,
        {
            "main_accounts": [
                {
                    "main_id": "group_a",
                    "name": "Group A",
                    "children": [
                        {"account_id": "sub1", "name": "Sub One", "api_key": "k1", "api_secret": "s1"},
                    ],
                }
            ]
        },
    )
    settings.load_monitor_accounts()

    factory_calls: list[str] = []

    def gateway_factory(account: MonitorAccountConfig) -> TrackingGateway:
        factory_calls.append(account.api_key)
        return TrackingGateway(account)

    controller = AccountMonitorController(settings, gateway_factory=gateway_factory)
    try:
        await controller.refresh_now()
        original_gateway = controller._gateways["group_a.sub1"]

        _write_accounts_file(
            config_path,
            {
                "main_accounts": [
                    {
                        "main_id": "group_a",
                        "name": "Group A",
                        "children": [
                            {"account_id": "sub1", "name": "Sub One", "api_key": "k1-updated", "api_secret": "s1-updated"},
                        ],
                    }
                ]
            },
        )

        await controller.reload_accounts()
        await controller.refresh_now()
        assert TrackingGateway.close_calls == {"group_a.sub1": 1}
        assert controller._gateways["group_a.sub1"] is not original_gateway
        assert controller._gateways["group_a.sub1"].account.api_key == "k1-updated"
        assert factory_calls == ["k1", "k1-updated"]
    finally:
        await controller.close()


def _write_accounts_file(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


class FallbackGateway(FakeMonitorGateway):
    async def get_unified_account_snapshot(
        self,
        *,
        history_window_days: int = 7,
        income_limit: int = 100,
        interest_limit: int = 100,
        previous_snapshot: dict | None = None,
        mark_price_provider=None,
        refresh_id: str | None = None,
    ) -> dict:
        payload = await super().get_unified_account_snapshot(
            history_window_days=history_window_days,
            income_limit=income_limit,
            interest_limit=interest_limit,
            previous_snapshot=previous_snapshot,
            mark_price_provider=mark_price_provider,
            refresh_id=refresh_id,
        )
        payload["section_errors"] = {
            "distribution_history": {
                "message": "temporary network error",
                "attempts": 3,
                "used_fallback": True,
                "stale": True,
                "source": "network",
            }
        }
        payload["diagnostics"]["fallback_sections"] = ["distribution_history"]
        return payload


@pytest.mark.asyncio
async def test_refresh_result_includes_fallback_sections(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        monitor_refresh_interval_ms=999999,
        monitor_history_window_days=3,
        monitor_history_db_path=tmp_path / "history.db",
    )
    account = MonitorAccountConfig(
        account_id="group_a.sub1",
        child_account_id="sub1",
        child_account_name="Sub One",
        main_account_id="group_a",
        main_account_name="Group A",
        api_key="k1",
        api_secret="s1",
    )
    settings.monitor_accounts = {account.account_id: account}
    settings.monitor_main_accounts = {
        "group_a": MainAccountConfig(main_id="group_a", name="Group A", children=(account,)),
    }
    controller = AccountMonitorController(settings, gateway_factory=lambda selected: FallbackGateway(selected))
    try:
        refreshed = await controller.refresh_now()
    finally:
        await controller.close()

    assert refreshed["refresh_result"]["success"] is True
    assert refreshed["refresh_result"]["refresh_id"]
    assert refreshed["refresh_result"]["failed_accounts"] == []
    assert refreshed["refresh_result"]["fallback_sections"] == [
        {"account_id": "group_a.sub1", "sections": ["distribution_history"]}
    ]


class DiagnosticGateway(FakeMonitorGateway):
    async def get_unified_account_snapshot(
        self,
        *,
        history_window_days: int = 7,
        income_limit: int = 100,
        interest_limit: int = 100,
        previous_snapshot: dict | None = None,
        mark_price_provider=None,
        refresh_id: str | None = None,
    ) -> dict:
        payload = await super().get_unified_account_snapshot(
            history_window_days=history_window_days,
            income_limit=income_limit,
            interest_limit=interest_limit,
            previous_snapshot=previous_snapshot,
            mark_price_provider=mark_price_provider,
            refresh_id=refresh_id,
        )
        if self.account.account_id == "group_a.sub1":
            payload["diagnostics"]["timings"] = {"gateway_total_ms": 17, "spot_query_ms": 9}
            payload["section_errors"] = {
                "distribution_history": {
                    "message": "temporary network error",
                    "attempts": 3,
                    "used_fallback": True,
                    "stale": True,
                    "source": "network",
                }
            }
            payload["diagnostics"]["fallback_sections"] = ["distribution_history"]
            return payload
        payload["diagnostics"]["timings"] = {"gateway_total_ms": 41, "spot_query_ms": 6}
        return payload


@pytest.mark.asyncio
async def test_refresh_result_includes_timings_and_slow_accounts(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        monitor_refresh_interval_ms=999999,
        monitor_history_window_days=3,
        monitor_history_db_path=tmp_path / "history.db",
    )
    account1 = MonitorAccountConfig(
        account_id="group_a.sub1",
        child_account_id="sub1",
        child_account_name="Sub One",
        main_account_id="group_a",
        main_account_name="Group A",
        api_key="k1",
        api_secret="s1",
    )
    account2 = MonitorAccountConfig(
        account_id="group_a.sub2",
        child_account_id="sub2",
        child_account_name="Sub Two",
        main_account_id="group_a",
        main_account_name="Group A",
        api_key="k2",
        api_secret="s2",
    )
    settings.monitor_accounts = {account.account_id: account for account in (account1, account2)}
    settings.monitor_main_accounts = {
        "group_a": MainAccountConfig(main_id="group_a", name="Group A", children=(account1, account2)),
    }
    controller = AccountMonitorController(settings, gateway_factory=lambda selected: DiagnosticGateway(selected))
    try:
        refreshed = await controller.refresh_now()
    finally:
        await controller.close()

    refresh_result = refreshed["refresh_result"]
    assert refresh_result["success"] is True
    assert refresh_result["fallback_section_count"] == 1
    assert refresh_result["slow_accounts"] == [{"account_id": "group_a.sub2", "duration_ms": 41}, {"account_id": "group_a.sub1", "duration_ms": 17}]
    assert refresh_result["timings"]["accounts"]["group_a.sub1"]["gateway_total_ms"] == 17
    assert refresh_result["timings"]["accounts"]["group_a.sub2"]["gateway_total_ms"] == 41
    assert refresh_result["timings"]["slowest_account_ms"] == 41
    assert refresh_result["timings"]["collect_payload_ms"] >= 0
    assert refresh_result["timings"]["broadcast_ms"] >= 0


class TimeoutAfterFirstGateway(FakeMonitorGateway):
    call_counts: dict[str, int] = {}

    async def get_unified_account_snapshot(
        self,
        *,
        history_window_days: int = 7,
        income_limit: int = 100,
        interest_limit: int = 100,
        previous_snapshot: dict | None = None,
        mark_price_provider=None,
        refresh_id: str | None = None,
    ) -> dict:
        count = TimeoutAfterFirstGateway.call_counts.get(self.account.account_id, 0) + 1
        TimeoutAfterFirstGateway.call_counts[self.account.account_id] = count
        if count >= 2:
            await asyncio.sleep(0.2)
        return await super().get_unified_account_snapshot(
            history_window_days=history_window_days,
            income_limit=income_limit,
            interest_limit=interest_limit,
            previous_snapshot=previous_snapshot,
            mark_price_provider=mark_price_provider,
            refresh_id=refresh_id,
        )


@pytest.mark.asyncio
async def test_refresh_timeout_preserves_previous_payload(tmp_path: Path) -> None:
    TimeoutAfterFirstGateway.call_counts = {}
    settings = Settings(
        _env_file=None,
        monitor_refresh_interval_ms=999999,
        monitor_refresh_timeout_ms=50,
        monitor_history_window_days=3,
        monitor_history_db_path=tmp_path / "history.db",
    )
    account = MonitorAccountConfig(
        account_id="group_a.sub1",
        child_account_id="sub1",
        child_account_name="Sub One",
        main_account_id="group_a",
        main_account_name="Group A",
        api_key="k1",
        api_secret="s1",
    )
    settings.monitor_accounts = {account.account_id: account}
    settings.monitor_main_accounts = {
        "group_a": MainAccountConfig(main_id="group_a", name="Group A", children=(account,)),
    }
    controller = AccountMonitorController(settings, gateway_factory=lambda selected: TimeoutAfterFirstGateway(selected))
    try:
        first = await controller.refresh_now()
        second = await controller.refresh_now()
    finally:
        await controller.close()

    assert first["refresh_result"]["success"] is True
    assert second["refresh_result"]["success"] is False
    assert second["refresh_result"]["timeout"] is True
    assert second["refresh_result"]["message"] == "刷新超时，已保留当前数据"
    assert second["updated_at"] == first["updated_at"]
    assert second["summary"]["account_count"] == 1
    assert second["summary"]["success_count"] == 1
