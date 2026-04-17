from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from monitor_app.account_monitor import AccountMonitorController
from monitor_app.account_import import (
    AccountImportError,
    build_accounts_excel_template,
    parse_accounts_excel,
    write_monitor_accounts_payload,
)
from monitor_app.config import settings
from monitor_app.funding_transfer import FundingTransferError, FundingTransferService
from monitor_app.log_maintenance import log_trim_loop
from monitor_app.security import (
    is_loopback_client,
    is_trusted_loopback_host,
    sanitize_error_summary,
    sanitize_funding_payload,
    sanitize_monitor_payload,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    monitor = AccountMonitorController(settings)
    funding_transfer = FundingTransferService(settings)
    log_trim_stop = asyncio.Event()
    log_trim_task = asyncio.create_task(
        log_trim_loop(
            [settings.monitor_runtime_log_path],
            settings.monitor_runtime_log_max_lines,
            settings.monitor_runtime_log_trim_interval_s,
            log_trim_stop,
        )
    )
    app.state.monitor = monitor
    app.state.funding_transfer = funding_transfer
    app.state.allow_test_non_loopback = False
    try:
        yield
    finally:
        log_trim_stop.set()
        log_trim_task.cancel()
        try:
            await log_trim_task
        except asyncio.CancelledError:
            pass
        await funding_transfer.close()
        await monitor.close()


app = FastAPI(
    title=settings.monitor_app_name,
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)
app.add_middleware(TrustedHostMiddleware, allowed_hosts=["127.0.0.1", "localhost", "::1", "[::1]", "testserver"])
app.mount("/static", StaticFiles(directory=Path(__file__).with_name("static")), name="static")


@app.middleware("http")
async def enforce_loopback_only(request: Request, call_next):
    if getattr(request.app.state, "allow_test_non_loopback", False):
        return await call_next(request)
    if not is_trusted_loopback_host(request.headers.get("host")):
        return JSONResponse(status_code=403, content={"detail": "Loopback access only"})
    client_host = request.client.host if request.client else None
    if not is_loopback_client(client_host):
        return JSONResponse(status_code=403, content={"detail": "Loopback access only"})
    return await call_next(request)


class MonitorControlRequest(BaseModel):
    enabled: bool


class FundingDistributeItem(BaseModel):
    account_id: str
    amount: str


class FundingDistributeRequest(BaseModel):
    asset: str
    transfers: list[FundingDistributeItem]


class FundingCollectItem(BaseModel):
    account_id: str
    amount: str


class FundingCollectRequest(BaseModel):
    asset: str
    transfers: list[FundingCollectItem] = Field(default_factory=list)
    account_ids: list[str] = Field(default_factory=list)


@app.get("/", include_in_schema=False)
async def index() -> FileResponse:
    return FileResponse(
        Path(__file__).with_name("static").joinpath("monitor_v2.html"),
        headers={"Cache-Control": "no-store, max-age=0"},
    )


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/monitor/summary")
async def get_summary(account_ids: str | None = Query(default=None)) -> dict:
    monitor: AccountMonitorController = app.state.monitor
    return sanitize_monitor_payload(monitor.current_summary(_parse_account_ids(account_ids)))


@app.get("/api/monitor/groups")
async def get_groups(account_ids: str | None = Query(default=None)) -> dict:
    monitor: AccountMonitorController = app.state.monitor
    return sanitize_monitor_payload(monitor.current_groups(_parse_account_ids(account_ids)))


@app.get("/api/monitor/accounts")
async def get_accounts(account_ids: str | None = Query(default=None)) -> dict:
    monitor: AccountMonitorController = app.state.monitor
    return sanitize_monitor_payload(monitor.current_accounts(_parse_account_ids(account_ids)))


@app.post("/api/monitor/control")
async def set_monitor_control(payload: MonitorControlRequest) -> dict:
    monitor: AccountMonitorController = app.state.monitor
    return sanitize_monitor_payload(await monitor.set_monitor_enabled(payload.enabled))


@app.post("/api/monitor/refresh")
async def refresh_monitor() -> dict:
    monitor: AccountMonitorController = app.state.monitor
    return sanitize_monitor_payload(await monitor.refresh_now())


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
        raise HTTPException(status_code=400, detail=sanitize_error_summary(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=sanitize_error_summary(exc)) from exc
    finally:
        await file.close()

    response["import_result"] = import_result.as_dict()
    if response.get("refresh_result", {}).get("success", False):
        response["message"] = "Excel 导入成功，数据已刷新"
    else:
        response["message"] = "Excel 导入成功，但刷新失败"
    return sanitize_monitor_payload(response)


@app.get("/api/funding/groups/{main_id}")
async def get_funding_group(main_id: str) -> dict:
    funding_transfer: FundingTransferService = app.state.funding_transfer
    try:
        return sanitize_funding_payload(await funding_transfer.get_group_overview(main_id))
    except FundingTransferError as exc:
        raise HTTPException(status_code=400, detail=sanitize_error_summary(exc)) from exc


@app.post("/api/funding/groups/{main_id}/distribute")
async def distribute_group_funding(main_id: str, payload: FundingDistributeRequest) -> dict:
    funding_transfer: FundingTransferService = app.state.funding_transfer
    try:
        return sanitize_funding_payload(await funding_transfer.distribute(
            main_id,
            asset=payload.asset,
            transfers=[item.model_dump() for item in payload.transfers],
        ))
    except FundingTransferError as exc:
        raise HTTPException(status_code=400, detail=sanitize_error_summary(exc)) from exc


@app.post("/api/funding/groups/{main_id}/collect")
async def collect_group_funding(main_id: str, payload: FundingCollectRequest) -> dict:
    funding_transfer: FundingTransferService = app.state.funding_transfer
    try:
        return sanitize_funding_payload(await funding_transfer.collect(
            main_id,
            asset=payload.asset,
            transfers=[item.model_dump() for item in payload.transfers],
            account_ids=payload.account_ids,
        ))
    except FundingTransferError as exc:
        raise HTTPException(status_code=400, detail=sanitize_error_summary(exc)) from exc


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
                    yield format_sse(message["event"], sanitize_monitor_payload(message["data"]))
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
