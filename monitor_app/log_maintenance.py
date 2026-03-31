from __future__ import annotations

import asyncio
from collections import deque
from pathlib import Path
from typing import Iterable


def _tail_lines(path: Path, max_lines: int) -> list[str]:
    if max_lines <= 0:
        return []
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        return list(deque(handle, maxlen=max_lines))


def trim_log_file(path: Path, max_lines: int) -> bool:
    if max_lines <= 0 or not path.exists():
        return False
    lines = _tail_lines(path, max_lines)
    try:
        current_lines = sum(1 for _ in path.open("r", encoding="utf-8", errors="ignore"))
    except OSError:
        current_lines = len(lines)
    if current_lines <= max_lines:
        return False
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text("".join(lines), encoding="utf-8")
    tmp_path.replace(path)
    return True


async def log_trim_loop(
    paths: Iterable[Path],
    max_lines: int,
    interval_s: int,
    stop_event: asyncio.Event,
) -> None:
    while not stop_event.is_set():
        for path in paths:
            try:
                trim_log_file(path, max_lines)
            except OSError:
                continue
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=max(5, interval_s))
        except asyncio.TimeoutError:
            continue
