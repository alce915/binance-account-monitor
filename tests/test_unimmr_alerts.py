from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from monitor_app.config import Settings
from monitor_app.unimmr_alerts import DEDUPE_RETENTION_MS, UniMmrAlertService


def _build_settings(tmp_path: Path, **overrides) -> Settings:
    overrides.setdefault("allow_plaintext_secrets", True)
    settings = Settings(_env_file=None, monitor_history_db_path=tmp_path / "history.db", **overrides)
    if "unimmr_alerts_enabled" in overrides:
        settings.unimmr_alerts_enabled = bool(overrides["unimmr_alerts_enabled"])
    return settings


class RecordingNotifier:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []

    async def send_telegram_notification(
        self,
        text: str,
        *,
        severity: str = "info",
        category: str = "generic",
        dedupe_key: str | None = None,
        dedupe_window_ms: int = 0,
        on_sent=None,
        on_failed=None,
    ) -> dict[str, object]:
        self.messages.append(
            {
                "text": text,
                "severity": severity,
                "category": category,
                "dedupe_key": dedupe_key,
                "dedupe_window_ms": dedupe_window_ms,
            }
        )
        if on_sent is not None:
            await on_sent()
        return {"status": "queued"}

    def stats(self) -> dict[str, object]:
        return {
            "queued": len(self.messages),
            "sent": len(self.messages),
            "dropped": 0,
            "failed": 0,
            "last_error": "",
            "last_sent_at": None,
        }


class Clock:
    def __init__(self, start_ms: int) -> None:
        self.now_ms = start_ms

    def advance(self, delta_ms: int) -> None:
        self.now_ms += delta_ms

    def __call__(self) -> int:
        return self.now_ms


def _build_payload(value: str, *, account_id: str = "group_a.sub1") -> dict:
    return {
        "status": "ok",
        "updated_at": "2026-04-19T10:00:00+08:00",
        "message": "ok",
        "accounts": [
            {
                "status": "ok",
                "account_id": account_id,
                "account_name": "Group A / Sub One",
                "main_account_id": "group_a",
                "main_account_name": "Group A",
                "child_account_id": "sub1",
                "child_account_name": "Sub One",
                "uni_mmr": Decimal(value),
            }
        ],
    }


@pytest.mark.asyncio
async def test_unimmr_warning_entry_and_step_drop_alerts(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path, unimmr_alerts_enabled=True)
    clock = Clock(1_000_000)
    notifier = RecordingNotifier()
    service = UniMmrAlertService(settings, notifier=notifier, now_ms=clock)
    try:
        await service.evaluate_payload(_build_payload("1.50"))
        assert len(notifier.messages) == 1
        assert "进入 UniMMR 预警区间" in str(notifier.messages[0]["text"])

        clock.advance(60_000)
        await service.evaluate_payload(_build_payload("1.45"))
        assert len(notifier.messages) == 1

        clock.advance(60_000)
        await service.evaluate_payload(_build_payload("1.40"))
        assert len(notifier.messages) == 2
        assert "0.1" in str(notifier.messages[1]["text"])
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_unimmr_warning_reentry_cooldown_blocks_entry_but_not_new_step(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path, unimmr_alerts_enabled=True)
    clock = Clock(2_000_000)
    notifier = RecordingNotifier()
    service = UniMmrAlertService(settings, notifier=notifier, now_ms=clock)
    try:
        await service.evaluate_payload(_build_payload("1.50"))
        assert len(notifier.messages) == 1

        clock.advance(60_000)
        await service.evaluate_payload(_build_payload("1.60"))
        clock.advance(60_000)
        await service.evaluate_payload(_build_payload("1.60"))
        assert len(notifier.messages) == 2
        assert "UniMMR 已从预警区间恢复到安全区间" in str(notifier.messages[1]["text"])

        clock.advance(60_000)
        await service.evaluate_payload(_build_payload("1.50"))
        assert len(notifier.messages) == 2

        clock.advance(60_000)
        await service.evaluate_payload(_build_payload("1.40"))
        assert len(notifier.messages) == 3
        assert "0.1" in str(notifier.messages[2]["text"])
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_unimmr_danger_repeat_and_step_drop_alerts(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path, unimmr_alerts_enabled=True)
    clock = Clock(3_000_000)
    notifier = RecordingNotifier()
    service = UniMmrAlertService(settings, notifier=notifier, now_ms=clock)
    try:
        await service.evaluate_payload(_build_payload("1.20"))
        assert len(notifier.messages) == 1
        assert "进入 UniMMR 危险区间" in str(notifier.messages[0]["text"])

        clock.advance(4 * 60_000)
        await service.evaluate_payload(_build_payload("1.19"))
        assert len(notifier.messages) == 1

        clock.advance(60_000)
        await service.evaluate_payload(_build_payload("1.19"))
        assert len(notifier.messages) == 2
        assert "UniMMR 危险区间 5 分钟续报" in str(notifier.messages[1]["text"])

        clock.advance(60_000)
        await service.evaluate_payload(_build_payload("1.15"))
        assert len(notifier.messages) == 3
        assert "0.05" in str(notifier.messages[2]["text"])
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_unimmr_recovery_requires_two_refreshes(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path, unimmr_alerts_enabled=True)
    clock = Clock(4_000_000)
    notifier = RecordingNotifier()
    service = UniMmrAlertService(settings, notifier=notifier, now_ms=clock)
    try:
        await service.evaluate_payload(_build_payload("1.20"))
        assert len(notifier.messages) == 1

        clock.advance(60_000)
        await service.evaluate_payload(_build_payload("1.30"))
        assert len(notifier.messages) == 1

        clock.advance(60_000)
        await service.evaluate_payload(_build_payload("1.31"))
        assert len(notifier.messages) == 2
        assert "UniMMR 已从危险区间恢复到预警区间" in str(notifier.messages[1]["text"])
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_unimmr_recovery_does_not_fire_warning_drop_before_recovery_confirmation(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path, unimmr_alerts_enabled=True)
    clock = Clock(5_000_000)
    notifier = RecordingNotifier()
    service = UniMmrAlertService(settings, notifier=notifier, now_ms=clock)
    try:
        await service.evaluate_payload(_build_payload("1.20"))
        assert len(notifier.messages) == 1
        assert "进入 UniMMR 危险区间" in str(notifier.messages[0]["text"])

        clock.advance(60_000)
        await service.evaluate_payload(_build_payload("1.30"))
        assert len(notifier.messages) == 1

        clock.advance(60_000)
        await service.evaluate_payload(_build_payload("1.30"))
        assert len(notifier.messages) == 2
        assert "UniMMR 已从危险区间恢复到预警区间" in str(notifier.messages[1]["text"])

        summary = await service.status_summary()
        account = summary["accounts"][0]
        assert account["current_band"] == "warning"
        assert account["last_reason"] == "recovery"
        assert account["warning_step"] == 2
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_unimmr_simulation_does_not_mutate_live_state(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path, unimmr_alerts_enabled=True)
    clock = Clock(6_000_000)
    notifier = RecordingNotifier()
    service = UniMmrAlertService(settings, notifier=notifier, now_ms=clock)
    try:
        await service.evaluate_payload(_build_payload("1.60"))
        before = await service.status_summary()
        result = await service.simulate_payload(_build_payload("1.50"))
        after = await service.status_summary()
    finally:
        await service.close()

    assert result["triggered"] == 1
    assert result["simulated"] is True
    assert len(notifier.messages) == 1
    assert before["accounts"] == after["accounts"]


@pytest.mark.asyncio
async def test_unimmr_simulation_ignores_existing_live_state(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path, unimmr_alerts_enabled=True)
    clock = Clock(6_500_000)
    notifier = RecordingNotifier()
    service = UniMmrAlertService(settings, notifier=notifier, now_ms=clock)
    try:
        await service.evaluate_payload(_build_payload("1.50"))
        assert len(notifier.messages) == 1

        result = await service.simulate_payload(_build_payload("1.50"))
    finally:
        await service.close()

    assert result["triggered"] == 1
    assert result["simulated"] is True
    assert len(notifier.messages) == 2
    assert "进入 UniMMR 预警区间" in str(notifier.messages[1]["text"])


@pytest.mark.asyncio
async def test_unimmr_dedupe_state_prunes_expired_rows(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path, unimmr_alerts_enabled=True)
    clock = Clock(7_000_000)
    notifier = RecordingNotifier()
    service = UniMmrAlertService(settings, notifier=notifier, now_ms=clock)
    try:
        await service._store.mark_dedupe_sent("old-key", now_ms=clock.now_ms - DEDUPE_RETENTION_MS - 1)
        await service._store.mark_dedupe_sent("new-key", now_ms=clock.now_ms)
        async with service._store._lock:
            rows = service._store._conn.execute(
                "SELECT dedupe_key FROM telegram_dedupe_state ORDER BY dedupe_key ASC"
            ).fetchall()
    finally:
        await service.close()

    assert [str(row["dedupe_key"]) for row in rows] == ["new-key"]


@pytest.mark.asyncio
async def test_unimmr_event_table_prunes_old_rows(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        monitor_history_db_path=tmp_path / "history.db",
        unimmr_alerts_enabled=True,
        unimmr_alert_event_max_rows=3,
    )
    settings.unimmr_alerts_enabled = True
    clock = Clock(8_000_000)
    notifier = RecordingNotifier()
    service = UniMmrAlertService(settings, notifier=notifier, now_ms=clock)
    try:
        for index in range(5):
            await service._store.record_event(
                created_at_ms=clock.now_ms + index,
                account_id=f"group_a.sub{index}",
                main_account_id="group_a",
                band="warning",
                severity="warn",
                reason_code="warning_entry",
                reason_text="进入 UniMMR 预警区间",
                uni_mmr=Decimal("1.50"),
                sent=True,
                detail="sent",
            )
        async with service._store._lock:
            rows = service._store._conn.execute(
                "SELECT account_id FROM unimmr_alert_events ORDER BY created_at_ms ASC, id ASC"
            ).fetchall()
    finally:
        await service.close()

    assert [str(row["account_id"]) for row in rows] == ["group_a.sub2", "group_a.sub3", "group_a.sub4"]
