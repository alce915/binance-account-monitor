from __future__ import annotations

from pathlib import Path

import pytest

from monitor_app.config import Settings
from monitor_app.telegram_notifications import TelegramNotificationService


class RecordingSender:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def __call__(self, text: str, *, proxy_url: str | None = None) -> dict[str, object]:
        self.calls.append({"text": text, "proxy_url": proxy_url})
        return {"ok": True, "status_code": 200}


class FlakyCallback:
    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self) -> None:
        self.calls += 1
        raise RuntimeError("callback write failed")


@pytest.mark.asyncio
async def test_telegram_notification_service_dry_run_records_without_sending(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        monitor_history_db_path=tmp_path / "history.db",
        tg_enabled=True,
        tg_bot_token="token",
        tg_chat_id="chat",
        tg_dry_run=True,
    )
    sender = RecordingSender()
    service = TelegramNotificationService(settings, sender=sender)
    await service.start()
    try:
        result = await service.send_telegram_notification("hello")
        assert result["status"] == "queued"
        await service.drain()
        stats = service.stats()
        assert stats["queued"] == 1
        assert stats["sent"] == 1
        assert sender.calls == []
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_telegram_notification_service_survives_callback_failures(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        monitor_history_db_path=tmp_path / "history.db",
        tg_enabled=True,
        tg_bot_token="token",
        tg_chat_id="chat",
        tg_dry_run=False,
    )
    sender = RecordingSender()
    callback = FlakyCallback()
    service = TelegramNotificationService(settings, sender=sender)
    await service.start()
    try:
        result1 = await service.send_telegram_notification("first", on_sent=callback)
        result2 = await service.send_telegram_notification("second")
        assert result1["status"] == "queued"
        assert result2["status"] == "queued"
        await service.drain()
        stats = service.stats()
        assert callback.calls == 1
        assert len(sender.calls) == 2
        assert stats["sent"] == 2
        assert stats["worker_alive"] is True
        assert "callback write failed" in str(stats["last_error"])
    finally:
        await service.close()
