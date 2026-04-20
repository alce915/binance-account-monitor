from __future__ import annotations

import asyncio
import html
import json
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from monitor_app.account_monitor import AccountMonitorController
from monitor_app.access_control import (
    AUTH_CROSS_ORIGIN_FORBIDDEN,
    AUTH_ROLE_FORBIDDEN,
    AccessControlService,
    route_capability_snapshot,
)
from monitor_app.access_control.config import default_access_control_payload
from monitor_app.account_import import (
    AccountImportError,
    apply_settings_secret_updates,
    build_accounts_excel_template,
    collect_monitor_account_secret_refs,
    materialize_monitor_accounts_secret_refs,
    parse_accounts_excel,
    write_text_atomic,
    write_monitor_accounts_payload,
)
from monitor_app.config import settings
from monitor_app.funding_transfer import FundingTransferError, FundingTransferRequestRejected, FundingTransferService
from monitor_app.i18n import (
    excel_import_refresh_failed_message,
    excel_import_refresh_success_message,
    excel_import_security_notice,
    excel_import_settings_success_message,
    login_page_texts,
)
from monitor_app.log_maintenance import log_trim_loop
from monitor_app.secrets import verify_secret_store_consistency
from monitor_app.security import (
    is_loopback_client,
    sanitize_error_summary,
    sanitize_funding_payload,
    sanitize_monitor_payload,
)


def _verify_project_secret_consistency_if_ready() -> None:
    access_control_path = settings.access_control_config_file if settings.access_control_config_file.exists() else None
    monitor_accounts_path = settings.monitor_accounts_file if settings.monitor_accounts_file.exists() else None
    env_path_candidate = Path(settings.env_file_path)
    env_path = env_path_candidate if env_path_candidate.exists() else None
    if access_control_path is None and monitor_accounts_path is None and env_path is None:
        return
    provider = settings.build_secret_provider(required=False)
    if provider is None:
        return
    result = verify_secret_store_consistency(
        access_control_path=access_control_path,
        monitor_accounts_path=monitor_accounts_path,
        env_path=env_path,
        provider=provider,
    )
    if result["missing_refs"]:
        raise RuntimeError(f"Secret store is missing refs: {', '.join(result['missing_refs'])}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    monitor = AccountMonitorController(settings)
    funding_transfer = FundingTransferService(settings)
    access_control = AccessControlService(settings)
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
    app.state.access_control = access_control
    app.state.allow_test_non_loopback = False
    try:
        missing_policies = route_capability_snapshot(app)
        if missing_policies:
            raise RuntimeError(f"Missing route capability policy declarations: {', '.join(missing_policies)}")
        _verify_project_secret_consistency_if_ready()
        await monitor.start()
        yield
    finally:
        log_trim_stop.set()
        log_trim_task.cancel()
        try:
            await log_trim_task
        except asyncio.CancelledError:
            pass
        await funding_transfer.close()
        await access_control.close()
        await monitor.close()


app = FastAPI(
    title=settings.monitor_app_name,
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)
app.mount("/static", StaticFiles(directory=Path(__file__).with_name("static")), name="static")
app.mount(
    "/public/login",
    StaticFiles(directory=Path(__file__).with_name("static").joinpath("login_public"), check_dir=False),
    name="public-login",
)


@app.middleware("http")
async def enforce_access_control(request: Request, call_next):
    access_control: AccessControlService = request.app.state.access_control
    context = await access_control.build_context(request)
    request.state.auth = context
    denial = await access_control.authorize_request(request, context)
    if denial is not None:
        if context.clear_session_cookie:
            access_control.clear_session_cookie(denial, request=request)
        access_control.apply_security_headers(denial)
        return denial
    response = await call_next(request)
    if (
        getattr(request.state, "auth", None)
        and request.state.auth.session_cookie
        and not getattr(request.state, "skip_auth_cookie_refresh", False)
    ):
        access_control.apply_session_cookie(response, request.state.auth.session_cookie, request=request)
    elif getattr(request.state, "auth", None) and request.state.auth.clear_session_cookie:
        access_control.clear_session_cookie(response, request=request)
    access_control.apply_security_headers(response)
    return response


class MonitorControlRequest(BaseModel):
    enabled: bool


class LoginRequest(BaseModel):
    password: str = ""


class BreakGlassDisableRequest(BaseModel):
    nonce: str = ""


class FundingDistributeItem(BaseModel):
    account_id: str
    amount: str


class FundingDistributeRequest(BaseModel):
    asset: str
    operation_id: str
    transfers: list[FundingDistributeItem]


class FundingCollectItem(BaseModel):
    account_id: str
    amount: str


class FundingCollectRequest(BaseModel):
    asset: str
    operation_id: str
    transfers: list[FundingCollectItem] = Field(default_factory=list)
    account_ids: list[str] = Field(default_factory=list)


class TelegramTestRequest(BaseModel):
    message: str = ""


class UniMmrSimulateItem(BaseModel):
    account_id: str
    uni_mmr: str


class UniMmrSimulateRequest(BaseModel):
    updates: list[UniMmrSimulateItem] = Field(default_factory=list)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    path = request.url.path
    if path.startswith("/api/funding/groups/") and (path.endswith("/distribute") or path.endswith("/collect")):
        for error in exc.errors():
            location = error.get("loc") or ()
            if "operation_id" in location:
                return JSONResponse(
                    status_code=400,
                    content={
                        "detail": "operation_id is required",
                        "error": {"code": "OPERATION_ID_REQUIRED", "message": "operation_id is required"},
                    },
                )
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


def funding_error_response(exc: FundingTransferError) -> JSONResponse:
    detail = sanitize_error_summary(exc)
    error_payload = {"code": "PRECHECK_UNAVAILABLE", "message": detail}
    headers = {}
    if isinstance(exc, FundingTransferRequestRejected):
        error_payload["code"] = exc.code
        if exc.operation_id:
            error_payload["operation_id"] = exc.operation_id
            headers["X-Funding-Operation-Id"] = exc.operation_id
    return JSONResponse(status_code=400, content={"detail": detail, "error": error_payload}, headers=headers)


def funding_success_response(payload: dict) -> JSONResponse:
    public_payload = sanitize_funding_payload(payload)
    headers = {}
    operation_id = str(public_payload.get("operation_id") or "").strip()
    if operation_id:
        headers["X-Funding-Operation-Id"] = operation_id
    return JSONResponse(status_code=200, content=public_payload, headers=headers)


@app.get("/", include_in_schema=False)
async def index() -> FileResponse:
    return FileResponse(
        Path(__file__).with_name("static").joinpath("monitor_v2.html"),
        headers={"Cache-Control": "no-store, max-age=0"},
    )


def _render_login_page_html() -> str:
    template = Path(__file__).with_name("static").joinpath("login.html").read_text(encoding="utf-8")
    texts = login_page_texts()
    replacements = {
        "__LOGIN_TITLE__": html.escape(texts["title"]),
        "__LOGIN_DESCRIPTION__": html.escape(texts["description"]),
        "__LOGIN_PASSWORD_LABEL__": html.escape(texts["password_label"]),
        "__LOGIN_PASSWORD_PLACEHOLDER__": html.escape(texts["password_placeholder"]),
        "__LOGIN_SUBMIT_BUTTON__": html.escape(texts["submit_button"]),
        "__LOGIN_I18N__": json.dumps(texts, ensure_ascii=False),
    }
    content = template
    for marker, value in replacements.items():
        content = content.replace(marker, value)
    return content


@app.get("/login", include_in_schema=False)
async def login_page() -> HTMLResponse:
    return HTMLResponse(
        _render_login_page_html(),
        headers={"Cache-Control": "no-store, max-age=0"},
    )


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/auth/login")
async def login(payload: LoginRequest, request: Request) -> JSONResponse:
    access_control: AccessControlService = app.state.access_control
    result, cookie_value = await access_control.login(request, payload.password)
    if cookie_value is None:
        status_code = 503 if result.get("error", {}).get("code") == "AUTH_NOT_INITIALIZED" else 429 if result.get("error", {}).get("code") == "AUTH_RATE_LIMITED" else 401
        return JSONResponse(status_code=status_code, content=result, headers={"Cache-Control": "no-store"})
    response = JSONResponse(status_code=200, content=result, headers={"Cache-Control": "no-store"})
    access_control.apply_session_cookie(response, cookie_value, request=request)
    return response


@app.post("/api/auth/logout")
async def logout(request: Request) -> JSONResponse:
    access_control: AccessControlService = app.state.access_control
    request.state.skip_auth_cookie_refresh = True
    response = JSONResponse(status_code=200, content=await access_control.logout(request), headers={"Cache-Control": "no-store"})
    access_control.clear_session_cookie(response, request=request)
    return response


@app.get("/api/auth/session")
async def auth_session(request: Request) -> JSONResponse:
    access_control: AccessControlService = app.state.access_control
    context = await access_control.build_context(request)
    payload = access_control.session_payload_for_response(context)
    status_code = 503 if payload.get("error", {}).get("code") == "AUTH_NOT_INITIALIZED" else 200
    request.state.skip_auth_cookie_refresh = True
    response = JSONResponse(status_code=status_code, content=payload, headers={"Cache-Control": "no-store"})
    if context.session_cookie:
        access_control.apply_session_cookie(response, context.session_cookie, request=request)
    elif context.clear_session_cookie:
        access_control.clear_session_cookie(response, request=request)
    return response


@app.get("/api/auth/audit")
async def auth_audit(
    request: Request,
    limit: int = Query(default=50, ge=1, le=200),
    result: str = Query(default=""),
    reason_code: str = Query(default=""),
) -> JSONResponse:
    access_control: AccessControlService = app.state.access_control
    events = await access_control.list_audit_events(limit=limit, result=result, reason_code=reason_code)
    return JSONResponse(
        status_code=200,
        content={"items": events, "limit": limit, "result": result, "reason_code": reason_code},
        headers={"Cache-Control": "no-store"},
    )


@app.get("/api/auth/break-glass/challenge")
async def break_glass_challenge(request: Request) -> JSONResponse:
    access_control: AccessControlService = app.state.access_control
    result = await access_control.issue_break_glass_challenge(request)
    if result is None:
        return access_control.auth_error_response(403, AUTH_ROLE_FORBIDDEN, message="\u4ec5\u672c\u673a\u53ef\u7528")
    return JSONResponse(status_code=200, content=result, headers={"Cache-Control": "no-store"})


@app.post("/api/auth/break-glass/disable")
async def break_glass_disable(payload: BreakGlassDisableRequest, request: Request) -> JSONResponse:
    access_control: AccessControlService = app.state.access_control
    if not is_loopback_client(access_control.resolve_client_ip(request)):
        return access_control.auth_error_response(403, AUTH_ROLE_FORBIDDEN, message="\u4ec5\u672c\u673a\u53ef\u7528")
    if not access_control.is_same_origin_request(request):
        return access_control.auth_error_response(403, AUTH_CROSS_ORIGIN_FORBIDDEN)
    result = await access_control.break_glass_disable(request, nonce=payload.nonce)
    if result is None:
        return access_control.auth_error_response(403, "AUTH_INVALID")
    return JSONResponse(status_code=200, content=result, headers={"Cache-Control": "no-store"})


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
async def import_monitor_accounts_excel(request: Request, file: UploadFile = File(...)) -> dict:
    filename = file.filename or "accounts.xlsx"
    if Path(filename).suffix.lower() != ".xlsx":
        raise HTTPException(status_code=400, detail="Only .xlsx files are supported")

    access_control: AccessControlService = app.state.access_control
    monitor: AccountMonitorController = app.state.monitor
    updated_settings_keys: list[str] = []
    updated_secret_refs: list[str] = []
    accounts_provided = False
    monitor_reload_attempted = False
    access_control_settings_updated = False
    telegram_settings_updated = False
    runtime_env_snapshot: dict[str, str] | None = None
    try:
        parsed = parse_accounts_excel(await file.read(), filename=filename)
        secret_provider = settings.build_secret_provider(required=True)
        assert secret_provider is not None
        monitor_accounts_path = settings.monitor_accounts_file
        access_control_path = settings.access_control_config_file
        env_path = Path(settings.env_file_path)
        access_control_exists = access_control_path.exists()
        env_exists = env_path.exists()
        previous_store = secret_provider.dump_store()
        existing_payload = (
            json.loads(monitor_accounts_path.read_text(encoding="utf-8-sig"))
            if monitor_accounts_path.exists()
            else {"main_accounts": []}
        )
        existing_monitor_accounts_text = (
            monitor_accounts_path.read_text(encoding="utf-8")
            if monitor_accounts_path.exists()
            else None
        )
        existing_access_control_text = access_control_path.read_text(encoding="utf-8-sig") if access_control_exists else "{}"
        existing_env_text = env_path.read_text(encoding="utf-8") if env_exists else ""
        access_control_payload = (
            json.loads(existing_access_control_text or "{}")
            if access_control_exists
            else default_access_control_payload()
        )
        accounts_provided = bool(parsed.payload.get("main_accounts"))

        if accounts_provided:
            secret_payload, used_secret_refs, next_store = materialize_monitor_accounts_secret_refs(
                parsed.payload,
                secret_store=previous_store,
            )
            previous_monitor_refs = collect_monitor_account_secret_refs(existing_payload)
            for secret_ref in sorted(previous_monitor_refs - used_secret_refs):
                next_store.pop(secret_ref, None)
        else:
            secret_payload = existing_payload
            next_store = dict(previous_store)

        updated_settings_keys, next_access_control_payload, next_env_text, next_store = apply_settings_secret_updates(
            settings_updates=parsed.settings_updates,
            current_store=next_store,
            access_control_payload=access_control_payload,
            env_content=existing_env_text,
            master_key_file=str(settings.monitor_master_key_file or "").strip(),
        )
        updated_secret_refs = sorted(
            secret_ref
            for secret_ref, secret_value in next_store.items()
            if previous_store.get(secret_ref) != secret_value
        )
        access_control_settings_updated = any(key.startswith("access_control.") for key in updated_settings_keys)
        telegram_settings_updated = any(key.startswith("telegram.") for key in updated_settings_keys)
        should_write_access_control = access_control_exists or any(
            key.startswith("access_control.") for key in updated_settings_keys
        )
        should_write_env = env_exists or any(key.startswith("telegram.") for key in updated_settings_keys) or bool(
            str(settings.monitor_master_key_file or "").strip()
        )
        runtime_env_snapshot = settings.capture_runtime_env_overrides_snapshot()
        try:
            secret_provider.replace_store(next_store)
            if accounts_provided:
                write_monitor_accounts_payload(monitor_accounts_path, secret_payload)
            if should_write_access_control:
                write_text_atomic(
                    access_control_path,
                    json.dumps(next_access_control_payload, ensure_ascii=False, indent=2) + "\n",
                )
            if should_write_env:
                write_text_atomic(env_path, next_env_text)
            verification = verify_secret_store_consistency(
                access_control_path=access_control_path if should_write_access_control or access_control_exists else None,
                monitor_accounts_path=monitor_accounts_path if accounts_provided or monitor_accounts_path.exists() else None,
                env_path=env_path if should_write_env or env_exists else None,
                provider=secret_provider,
            )
            if verification["missing_refs"]:
                raise ValueError(f"Secret store consistency check failed: {', '.join(verification['missing_refs'])}")
            if should_write_env:
                settings.reload_runtime_env_overrides(env_content=next_env_text)
            if telegram_settings_updated:
                monitor.reload_telegram_credentials()
            if accounts_provided:
                monitor_reload_attempted = True
                await monitor.reload_accounts()
                response = await monitor.refresh_now()
            else:
                response = monitor.current_groups()
                response["refresh_result"] = {"success": True, "timeout": False}
            if should_write_access_control:
                access_control.reload()
            if access_control_settings_updated and getattr(request.state, "auth", None):
                request.state.skip_auth_cookie_refresh = True
                request.state.auth.session_cookie = None
                request.state.auth.clear_session_cookie = True
        except Exception:
            try:
                secret_provider.replace_store(previous_store)
                if existing_monitor_accounts_text is None:
                    monitor_accounts_path.unlink(missing_ok=True)
                else:
                    write_text_atomic(monitor_accounts_path, existing_monitor_accounts_text)
                if access_control_exists:
                    write_text_atomic(access_control_path, existing_access_control_text if existing_access_control_text.endswith("\n") else f"{existing_access_control_text}\n")
                else:
                    access_control_path.unlink(missing_ok=True)
                if env_exists:
                    write_text_atomic(env_path, existing_env_text if existing_env_text.endswith("\n") else f"{existing_env_text}\n")
                else:
                    env_path.unlink(missing_ok=True)
                if should_write_env:
                    settings.restore_runtime_env_overrides_snapshot(runtime_env_snapshot)
                if telegram_settings_updated:
                    monitor.reload_telegram_credentials()
                if monitor_reload_attempted:
                    await monitor.reload_accounts()
            except Exception:
                pass
            raise
    except AccountImportError as exc:
        raise HTTPException(status_code=400, detail=sanitize_error_summary(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=sanitize_error_summary(exc)) from exc
    finally:
        await file.close()

    response["import_result"] = parsed.import_result.as_dict()
    response["import_result"]["updated_settings_keys"] = updated_settings_keys
    response["import_result"]["updated_secret_refs"] = sorted(
        {
            secret_ref
            for secret_ref in updated_secret_refs
            if str(secret_ref or "").strip()
        }
    )
    response["template_version"] = parsed.template_version
    response["security_notice"] = excel_import_security_notice()
    if parsed.import_result.mode == "settings_only":
        response["refresh_result"] = {"success": True, "skipped": True}
        response["message"] = excel_import_settings_success_message()
    elif response.get("refresh_result", {}).get("success", False):
        response["message"] = excel_import_refresh_success_message()
    else:
        response["message"] = excel_import_refresh_failed_message()
    return sanitize_monitor_payload(response)


@app.get("/api/funding/groups/{main_id}")
async def get_funding_group(main_id: str) -> dict:
    funding_transfer: FundingTransferService = app.state.funding_transfer
    try:
        return sanitize_funding_payload(await funding_transfer.get_group_overview(main_id))
    except FundingTransferError as exc:
        raise HTTPException(status_code=400, detail=sanitize_error_summary(exc)) from exc


@app.get("/api/funding/groups/{main_id}/audit")
async def get_funding_group_audit(main_id: str) -> dict:
    funding_transfer: FundingTransferService = app.state.funding_transfer
    try:
        return sanitize_funding_payload(await funding_transfer.get_audit_entries(main_id))
    except FundingTransferError as exc:
        return funding_error_response(exc)


@app.get("/api/funding/groups/{main_id}/audit/{operation_id}")
async def get_funding_group_audit_detail(main_id: str, operation_id: str, direction: str = Query(...)) -> dict:
    funding_transfer: FundingTransferService = app.state.funding_transfer
    try:
        return sanitize_funding_payload(await funding_transfer.get_audit_entry_detail(main_id, operation_id, direction=direction))
    except FundingTransferError as exc:
        return funding_error_response(exc)


@app.post("/api/funding/groups/{main_id}/distribute")
async def distribute_group_funding(main_id: str, payload: FundingDistributeRequest) -> dict:
    funding_transfer: FundingTransferService = app.state.funding_transfer
    try:
        result = await funding_transfer.distribute(
            main_id,
            asset=payload.asset,
            operation_id=payload.operation_id,
            transfers=[item.model_dump() for item in payload.transfers],
        )
        return funding_success_response(result)
    except FundingTransferError as exc:
        return funding_error_response(exc)


@app.post("/api/funding/groups/{main_id}/collect")
async def collect_group_funding(main_id: str, payload: FundingCollectRequest) -> dict:
    funding_transfer: FundingTransferService = app.state.funding_transfer
    try:
        result = await funding_transfer.collect(
            main_id,
            asset=payload.asset,
            operation_id=payload.operation_id,
            transfers=[item.model_dump() for item in payload.transfers],
            account_ids=payload.account_ids,
        )
        return funding_success_response(result)
    except FundingTransferError as exc:
        return funding_error_response(exc)


@app.post("/api/alerts/telegram/test")
async def test_telegram_alert(payload: TelegramTestRequest) -> dict:
    monitor: AccountMonitorController = app.state.monitor
    return {
        "result": await monitor.send_test_telegram_notification(payload.message),
        "stats": await monitor.unimmr_alert_status(),
    }


@app.get("/api/alerts/unimmr/status")
async def get_unimmr_alert_status() -> dict:
    monitor: AccountMonitorController = app.state.monitor
    return await monitor.unimmr_alert_status()


@app.post("/api/alerts/unimmr/simulate")
async def simulate_unimmr_alert(payload: UniMmrSimulateRequest) -> dict:
    monitor: AccountMonitorController = app.state.monitor
    try:
        result = await monitor.simulate_unimmr_alerts([item.model_dump() for item in payload.updates])
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=sanitize_error_summary(exc)) from exc
    return {
        "result": result,
        "stats": await monitor.unimmr_alert_status(),
    }


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
