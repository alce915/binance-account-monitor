from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, Response, StreamingResponse
from pydantic import BaseModel

from monitor_app.account_monitor import AccountMonitorController
from monitor_app.account_import import (
    AccountImportError,
    build_accounts_excel_template,
    parse_accounts_excel,
    write_monitor_accounts_payload,
)
from monitor_app.config import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    monitor = AccountMonitorController(settings)
    app.state.monitor = monitor
    try:
        yield
    finally:
        await monitor.close()


app = FastAPI(title=settings.monitor_app_name, lifespan=lifespan)


class MonitorControlRequest(BaseModel):
    enabled: bool


@app.get("/", include_in_schema=False)
async def index() -> FileResponse:
    return FileResponse(Path(__file__).with_name("static").joinpath("monitor.html"))


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/monitor/summary")
async def get_summary(account_ids: str | None = Query(default=None)) -> dict:
    monitor: AccountMonitorController = app.state.monitor
    return monitor.current_summary(_parse_account_ids(account_ids))


@app.get("/api/monitor/groups")
async def get_groups(account_ids: str | None = Query(default=None)) -> dict:
    monitor: AccountMonitorController = app.state.monitor
    return monitor.current_groups(_parse_account_ids(account_ids))


@app.get("/api/monitor/accounts")
async def get_accounts(account_ids: str | None = Query(default=None)) -> dict:
    monitor: AccountMonitorController = app.state.monitor
    return monitor.current_accounts(_parse_account_ids(account_ids))


@app.post("/api/monitor/control")
async def set_monitor_control(payload: MonitorControlRequest) -> dict:
    monitor: AccountMonitorController = app.state.monitor
    return await monitor.set_monitor_enabled(payload.enabled)


@app.post("/api/monitor/refresh")
async def refresh_monitor() -> dict:
    monitor: AccountMonitorController = app.state.monitor
    return await monitor.refresh_now()


@app.post("/api/config/import/excel")
async def import_monitor_accounts_excel(file: UploadFile = File(...)) -> dict:
    filename = file.filename or "accounts.xlsx"
    if Path(filename).suffix.lower() != ".xlsx":
        raise HTTPException(status_code=400, detail="Only .xlsx files are supported")

    try:
        payload, import_result = parse_accounts_excel(await file.read(), filename=filename)
        write_monitor_accounts_payload(settings.monitor_accounts_file, payload)
        monitor: AccountMonitorController = app.state.monitor
        await monitor.reload_accounts()
        response = await monitor.refresh_now()
    except AccountImportError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        await file.close()

    response["import_result"] = import_result.as_dict()
    if response.get("refresh_result", {}).get("success", False):
        response["message"] = "Excel 导入成功，数据已刷新"
    else:
        response["message"] = "Excel 导入成功，但刷新失败"
    return response


@app.get("/api/config/import/excel-template")
async def download_monitor_accounts_excel_template() -> Response:
    content = build_accounts_excel_template()
    return Response(
        content=content,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": 'attachment; filename="monitor_accounts_template.xlsx"',
        },
    )


@app.get("/stream/monitor")
async def stream_monitor(account_ids: str | None = Query(default=None)) -> StreamingResponse:
    monitor: AccountMonitorController = app.state.monitor
    selected_ids = _parse_account_ids(account_ids)

    async def event_generator():
        queue = await monitor.subscribe(selected_ids)
        try:
            while True:
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=15)
                    yield format_sse(message["event"], message["data"])
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        finally:
            monitor.unsubscribe(queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


def _parse_account_ids(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    account_ids = [item.strip().lower() for item in raw.split(",") if item.strip()]
    return account_ids or None


def format_sse(event: str, payload: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"
