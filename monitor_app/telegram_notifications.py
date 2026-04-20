from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from time import perf_counter
from typing import Any, Awaitable, Callable

import httpx

from monitor_app.config import Settings
from monitor_app.security import sanitize_error_summary

logger = logging.getLogger("uvicorn.error")

_BACKOFF_SECONDS = (1.0, 2.0, 3.0)
_MAX_MESSAGE_LENGTH = 3500
_SEVERITY_ORDER = {"info": 0, "warn": 1, "critical": 2}


@dataclass(slots=True)
class TelegramNotificationItem:
    text: str
    severity: str
    category: str
    created_at_ms: int
    dedupe_key: str | None = None
    dedupe_window_ms: int = 0
    on_sent: Callable[[], Awaitable[None]] | None = None
    on_failed: Callable[[str], Awaitable[None]] | None = None


@dataclass(slots=True)
class TelegramNotificationStats:
    queued: int = 0
    sent: int = 0
    dropped: int = 0
    failed: int = 0
    last_error: str = ""
    last_sent_at: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "queued": self.queued,
            "sent": self.sent,
            "dropped": self.dropped,
            "failed": self.failed,
            "last_error": self.last_error,
            "last_sent_at": self.last_sent_at,
        }


class TelegramNotificationService:
    def __init__(
        self,
        settings: Settings,
        *,
        sender: Callable[[str], Awaitable[dict[str, Any]]] | None = None,
        now_ms: Callable[[], int] | None = None,
    ) -> None:
        self._settings = settings
        self._sender = sender
        self._now_ms = now_ms or (lambda: int(datetime.now(UTC).timestamp() * 1000))
        self._bot_token = settings.resolved_tg_bot_token()
        self._chat_id = settings.resolved_tg_chat_id()
        self._queue: deque[TelegramNotificationItem] = deque()
        self._queue_lock = asyncio.Lock()
        self._queue_event = asyncio.Event()
        self._worker_task: asyncio.Task[None] | None = None
        self._closed = False
        self._stats = TelegramNotificationStats()
        self._inflight = 0

    @property
    def enabled(self) -> bool:
        return bool(self._settings.tg_enabled and self._bot_token and self._chat_id)

    def reload_credentials(self) -> None:
        self._bot_token = self._settings.resolved_tg_bot_token()
        self._chat_id = self._settings.resolved_tg_chat_id()

    async def start(self) -> None:
        if self._worker_task is None or self._worker_task.done():
            self._worker_task = asyncio.create_task(self._worker_loop())
        async with self._queue_lock:
            if self._queue:
                self._queue_event.set()

    async def close(self) -> None:
        self._closed = True
        self._queue_event.set()
        if self._worker_task is not None:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            self._worker_task = None

    def stats(self) -> dict[str, Any]:
        payload = self._stats.as_dict()
        payload["queue_depth"] = len(self._queue)
        payload["worker_alive"] = bool(self._worker_task is not None and not self._worker_task.done())
        return payload

    async def drain(self) -> None:
        await self.start()
        while True:
            async with self._queue_lock:
                if not self._queue and self._inflight == 0:
                    return
            await asyncio.sleep(0.01)

    async def send_telegram_notification(
        self,
        text: str,
        *,
        severity: str = "info",
        category: str = "generic",
        dedupe_key: str | None = None,
        dedupe_window_ms: int = 0,
        on_sent: Callable[[], Awaitable[None]] | None = None,
        on_failed: Callable[[str], Awaitable[None]] | None = None,
    ) -> dict[str, Any]:
        await self.start()
        normalized_text = self._normalize_text(text)
        if not self.enabled:
            logger.info("Telegram notification skipped category=%s severity=%s reason=disabled", category, severity)
            return {"status": "disabled", "reason": "disabled"}

        item = TelegramNotificationItem(
            text=normalized_text,
            severity=severity,
            category=category,
            created_at_ms=self._now_ms(),
            dedupe_key=dedupe_key,
            dedupe_window_ms=max(int(dedupe_window_ms or 0), 0),
            on_sent=on_sent,
            on_failed=on_failed,
        )
        enqueue_result = await self._enqueue(item)
        logger.info(
            "Telegram notification enqueue status=%s category=%s severity=%s length=%s",
            enqueue_result["status"],
            category,
            severity,
            len(normalized_text),
        )
        return enqueue_result

    def _normalize_text(self, text: str) -> str:
        normalized = str(text or "").strip()
        if len(normalized) <= _MAX_MESSAGE_LENGTH:
            return normalized
        return normalized[: _MAX_MESSAGE_LENGTH - len("... [truncated]")] + "... [truncated]"

    async def _enqueue(self, item: TelegramNotificationItem) -> dict[str, Any]:
        async with self._queue_lock:
            max_queue_size = max(int(self._settings.tg_max_queue_size or 0), 1)
            if len(self._queue) >= max_queue_size:
                if item.severity != "critical":
                    self._stats.dropped += 1
                    return {"status": "dropped", "reason": "queue_full"}
                replaced = False
                for index, queued in enumerate(self._queue):
                    if _SEVERITY_ORDER.get(queued.severity, 0) < _SEVERITY_ORDER["critical"]:
                        del self._queue[index]
                        replaced = True
                        break
                if not replaced:
                    self._stats.dropped += 1
                    return {"status": "dropped", "reason": "queue_full"}
            self._queue.append(item)
            self._stats.queued += 1
            self._queue_event.set()
            return {"status": "queued"}

    async def _worker_loop(self) -> None:
        while True:
            await self._queue_event.wait()
            while True:
                async with self._queue_lock:
                    if not self._queue:
                        self._queue_event.clear()
                        break
                    item = self._queue.popleft()
                    self._inflight += 1
                try:
                    await self._process_item(item)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # pragma: no cover - hard to hit intentionally
                    error_message = sanitize_error_summary(exc, fallback="Telegram worker processing failed")
                    self._stats.last_error = error_message
                    logger.exception(
                        "Telegram worker processing failed category=%s severity=%s message=%s",
                        item.category,
                        item.severity,
                        error_message,
                    )
                finally:
                    async with self._queue_lock:
                        self._inflight = max(self._inflight - 1, 0)

    async def _process_item(self, item: TelegramNotificationItem) -> None:
        started_at = perf_counter()
        if self._settings.tg_dry_run:
            self._stats.sent += 1
            self._stats.last_sent_at = self._now_ms()
            logger.info(
                "Telegram dry-run message category=%s severity=%s length=%s",
                item.category,
                item.severity,
                len(item.text),
            )
            await self._run_callback(item.on_sent, label="on_sent")
            return

        success = False
        error_message = ""
        for index, backoff_s in enumerate(_BACKOFF_SECONDS, start=1):
            try:
                response = await self._send_message(item.text)
                if response.get("ok") is True:
                    success = True
                    break
                if int(response.get("status_code") or 0) == 429 and response.get("retry_after"):
                    await asyncio.sleep(float(response["retry_after"]))
                elif index < len(_BACKOFF_SECONDS):
                    await asyncio.sleep(backoff_s)
                error_message = str(response.get("description") or f"HTTP {response.get('status_code')}")
            except Exception as exc:  # pragma: no cover - exercised indirectly
                error_message = sanitize_error_summary(exc, fallback="Telegram send failed")
                if index < len(_BACKOFF_SECONDS):
                    await asyncio.sleep(backoff_s)
        if success:
            self._stats.sent += 1
            self._stats.last_sent_at = self._now_ms()
            logger.info(
                "Telegram message sent category=%s severity=%s duration_ms=%s",
                item.category,
                item.severity,
                int((perf_counter() - started_at) * 1000),
            )
            await self._run_callback(item.on_sent, label="on_sent")
            return

        self._stats.failed += 1
        self._stats.last_error = error_message
        logger.warning(
            "Telegram message failed category=%s severity=%s message=%s",
            item.category,
            item.severity,
            error_message,
        )
        await self._run_callback(item.on_failed, error_message, label="on_failed")

    async def _run_callback(self, callback: Callable[..., Awaitable[None]] | None, *args: Any, label: str) -> None:
        if callback is None:
            return
        try:
            await callback(*args)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - exercised indirectly
            error_message = sanitize_error_summary(exc, fallback=f"Telegram callback failed: {label}")
            self._stats.last_error = error_message
            logger.exception("Telegram callback failed label=%s message=%s", label, error_message)

    async def _send_message(self, text: str) -> dict[str, Any]:
        if self._sender is not None:
            return await self._sender(text, proxy_url=self._settings.tg_proxy_url or None)  # type: ignore[misc]
        endpoint = f"https://api.telegram.org/bot{self._bot_token}/sendMessage"
        transport_kwargs: dict[str, Any] = {}
        if self._settings.tg_proxy_url:
            transport_kwargs["proxy"] = self._settings.tg_proxy_url
        timeout = httpx.Timeout(10.0)
        async with httpx.AsyncClient(timeout=timeout, **transport_kwargs) as client:
            response = await client.post(
                endpoint,
                json={"chat_id": self._chat_id, "text": text},
            )
        payload: dict[str, Any]
        try:
            payload = response.json()
        except Exception:
            payload = {"ok": response.is_success, "description": response.text}
        if response.status_code == 429:
            retry_after = payload.get("parameters", {}).get("retry_after")
            payload["retry_after"] = retry_after
        payload["status_code"] = response.status_code
        payload.setdefault("ok", response.is_success)
        return payload
