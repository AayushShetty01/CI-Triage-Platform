import asyncio
import hashlib
import io
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncGenerator
from xml.etree.ElementTree import ParseError

from defusedxml import ElementTree as SafeET
from defusedxml.common import DefusedXmlException
from fastapi import FastAPI, HTTPException, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from sqlalchemy import create_engine, func, insert, select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from starlette.datastructures import UploadFile
from starlette.requests import Request as StarletteRequest

from app.database import outbox, runs
from app.relay import (
    refresh_outbox_metrics,
    run_outbox_relay,
    set_outbox_metric_values,
)

logger = logging.getLogger(__name__)

health_checks = Counter(
    "ingestion_health_checks_total",
    "Number of successful ingestion service health checks.",
)
ingest_requests = Counter(
    "ingest_requests_total",
    "Number of ingestion HTTP requests.",
    ("method", "path", "status"),
)
ingest_latency = Histogram(
    "ingest_latency_seconds",
    "Duration of ingestion HTTP requests.",
    ("method", "path"),
)
outbox_pending = Gauge(
    "outbox_pending",
    "Number of unpublished ingestion outbox events.",
)
outbox_oldest_pending_seconds = Gauge(
    "outbox_oldest_pending_seconds",
    "Age in seconds of the oldest unpublished ingestion outbox event.",
)
relay_publish_errors = Counter(
    "relay_publish_errors_total",
    "Number of failed outbox relay iterations.",
)
relay_consecutive_failures = Gauge(
    "relay_consecutive_failures",
    "Number of consecutive failed outbox relay iterations.",
)

DEFAULT_MAX_REPORT_SIZE_BYTES = 10 * 1024 * 1024
DEFAULT_DATABASE_URL = (
    "postgresql+psycopg://ingestion:ingestion_dev@localhost:5432/triage"
)
DEFAULT_RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"


def _create_engine(database_url: str) -> Engine:
    connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
    engine = create_engine(database_url, pool_pre_ping=True, connect_args=connect_args)
    if engine.dialect.name == "sqlite":
        engine = engine.execution_options(schema_translate_map={"ingestion": None})
    return engine


async def _read_limited_body(request: Request, max_bytes: int) -> bytes:
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            if int(content_length) > max_bytes:
                raise HTTPException(status_code=413, detail="Report exceeds the size limit")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Invalid Content-Length header") from exc

    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > max_bytes:
            raise HTTPException(status_code=413, detail="Report exceeds the size limit")
    return bytes(body)


async def _request_from_body(request: Request, body: bytes) -> StarletteRequest:
    sent = False

    async def receive():
        nonlocal sent
        if sent:
            return {"type": "http.disconnect"}
        sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    return StarletteRequest(request.scope, receive)


def _xml_local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def validate_junit_xml(raw_xml: bytes) -> None:
    root_name: str | None = None
    found_suite = False
    try:
        for _, element in SafeET.iterparse(io.BytesIO(raw_xml), events=("start",)):
            name = _xml_local_name(element.tag)
            if root_name is None:
                root_name = name
                if root_name not in {"testsuite", "testsuites"}:
                    raise HTTPException(
                        status_code=400,
                        detail="XML root must be testsuite or testsuites",
                    )
            if name == "testsuite":
                found_suite = True
                if not element.attrib.get("name", "").strip():
                    raise HTTPException(
                        status_code=400,
                        detail="Each testsuite must have a name",
                    )
            elif name == "testcase" and not element.attrib.get("name", "").strip():
                raise HTTPException(
                    status_code=400,
                    detail="Each testcase must have a name",
                )
    except HTTPException:
        raise
    except (ParseError, DefusedXmlException) as exc:
        raise HTTPException(status_code=400, detail="Invalid or unsafe JUnit XML report") from exc

    if root_name is None or not found_suite:
        raise HTTPException(status_code=400, detail="JUnit XML must contain a testsuite")


async def _extract_submission(request: Request, max_report_size_bytes: int) -> tuple[bytes, str, str]:
    content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    commit_sha: str | None
    branch: str | None

    if content_type == "multipart/form-data":
        maximum_body_bytes = max_report_size_bytes + 64 * 1024
        body = await _read_limited_body(request, maximum_body_bytes)
        form_request = await _request_from_body(request, body)
        form = await form_request.form(max_files=1, max_fields=4)
        try:
            files = [
                value
                for key, value in form.multi_items()
                if key in {"file", "report"} and isinstance(value, UploadFile)
            ]
            if len(files) != 1:
                raise HTTPException(
                    status_code=400,
                    detail="Multipart form must include exactly one file or report field",
                )
            raw_xml = await files[0].read(max_report_size_bytes + 1)
            commit_sha_value = form.get("commit_sha")
            branch_value = form.get("branch")
            commit_sha = commit_sha_value if isinstance(commit_sha_value, str) else None
            branch = branch_value if isinstance(branch_value, str) else None
        finally:
            await form.close()
    elif content_type in {"application/xml", "text/xml", "application/junit+xml"}:
        raw_xml = await _read_limited_body(request, max_report_size_bytes)
        commit_sha = None
        branch = None
    else:
        raise HTTPException(
            status_code=415,
            detail="Content-Type must be multipart/form-data or an XML media type",
        )

    if len(raw_xml) > max_report_size_bytes:
        raise HTTPException(status_code=413, detail="Report exceeds the size limit")

    commit_sha = (
        commit_sha
        or request.headers.get("x-commit-sha")
        or request.query_params.get("commit_sha")
    )
    branch = branch or request.headers.get("x-branch") or request.query_params.get("branch")
    if not commit_sha or not commit_sha.strip() or len(commit_sha.strip()) > 128:
        raise HTTPException(status_code=400, detail="commit_sha is required and must be at most 128 characters")
    if not branch or not branch.strip() or len(branch.strip()) > 255:
        raise HTTPException(status_code=400, detail="branch is required and must be at most 255 characters")

    commit_sha = commit_sha.strip()
    branch = branch.strip()
    validate_junit_xml(raw_xml)
    return raw_xml, commit_sha, branch


def _insert_for_dialect(connection, table, values):
    primary_key = next(iter(table.primary_key.columns))
    if connection.dialect.name == "postgresql":
        statement = (
            postgresql_insert(table)
            .values(**values)
            .on_conflict_do_nothing()
            .returning(primary_key)
        )
    elif connection.dialect.name == "sqlite":
        statement = (
            sqlite_insert(table)
            .values(**values)
            .on_conflict_do_nothing()
            .returning(primary_key)
        )
    else:
        statement = insert(table).values(**values).returning(primary_key)
    return connection.execute(statement)


def _store_run(
    engine: Engine,
    raw_xml: bytes,
    commit_sha: str,
    branch: str,
) -> tuple[str, int, datetime | None]:
    run_id = hashlib.sha256(commit_sha.encode("utf-8") + raw_xml).hexdigest()
    received_at = datetime.now(timezone.utc)
    event_id = str(uuid.uuid4())
    payload = {
        "event_id": event_id,
        "event_type": "RunReceived",
        "occurred_at": received_at.isoformat(),
        "schema_version": "1.0",
        "run_id": run_id,
        "payload": {
            "commit_sha": commit_sha,
            "branch": branch,
            "received_at": received_at.isoformat(),
            "raw_ref": f"/internal/runs/{run_id}/raw",
        },
    }
    with engine.begin() as connection:
        result = _insert_for_dialect(
            connection,
            runs,
            {
                "run_id": run_id,
                "commit_sha": commit_sha,
                "branch": branch,
                "received_at": received_at,
                "raw_xml": raw_xml,
                "status": "received",
            },
        )
        created = result.scalar_one_or_none() is not None
        if created:
            connection.execute(
                insert(outbox).values(
                    event_type="RunReceived",
                    payload=payload,
                    created_at=received_at,
                    published_at=None,
                )
            )
        pending_count, oldest_pending_created_at = connection.execute(
            select(func.count(), func.min(outbox.c.created_at)).where(
                outbox.c.published_at.is_(None)
            )
        ).one()
    return run_id, int(pending_count), oldest_pending_created_at


def create_app(
    database_url: str | None = None,
    rabbitmq_url: str | None = None,
    enable_relay: bool = True,
    max_report_size_bytes: int | None = None,
) -> FastAPI:
    engine = _create_engine(database_url or os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL))
    broker_url = rabbitmq_url or os.getenv("RABBITMQ_URL", DEFAULT_RABBITMQ_URL)
    poll_interval_seconds = float(
        os.getenv(
            "RELAY_POLL_INTERVAL_SECONDS",
            os.getenv("OUTBOX_POLL_INTERVAL_SECONDS", "1"),
        )
    )
    backoff_cap_seconds = float(os.getenv("RELAY_BACKOFF_CAP_SECONDS", "30"))
    retry_after_seconds = int(os.getenv("INGESTION_RETRY_AFTER_SECONDS", "5"))
    batch_size = int(os.getenv("OUTBOX_BATCH_SIZE", "50"))
    if max_report_size_bytes is None:
        max_report_size_bytes = int(
            os.getenv("MAX_REPORT_SIZE_BYTES", str(DEFAULT_MAX_REPORT_SIZE_BYTES))
        )
    if (
        max_report_size_bytes < 1
        or poll_interval_seconds <= 0
        or backoff_cap_seconds <= 0
        or retry_after_seconds <= 0
        or batch_size < 1
    ):
        raise ValueError("Report size, relay intervals, retry-after, and batch size must be positive")

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncGenerator[None, None]:
        relay_task: asyncio.Task[None] | None = None
        if enable_relay:
            relay_task = asyncio.create_task(
                run_outbox_relay(
                    engine,
                    broker_url,
                    poll_interval_seconds,
                    batch_size,
                    outbox_pending,
                    outbox_oldest_pending_seconds,
                    relay_publish_errors,
                    relay_consecutive_failures,
                    backoff_cap_seconds,
                )
            )
        try:
            yield
        finally:
            if relay_task is not None:
                relay_task.cancel()
                try:
                    await relay_task
                except asyncio.CancelledError:
                    pass
            engine.dispose()

    application = FastAPI(title="CI Triage Platform - Ingestion", lifespan=lifespan)
    application.state.engine = engine
    application.state.max_report_size_bytes = max_report_size_bytes
    application.state.retry_after_seconds = retry_after_seconds

    @application.middleware("http")
    async def track_ingestion_requests(request: Request, call_next):
        started_at = time.perf_counter()
        status_code = 500
        route = request.scope.get("route")
        path = getattr(route, "path", request.url.path)
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            ingest_requests.labels(request.method, path, str(status_code)).inc()
            ingest_latency.labels(request.method, path).observe(time.perf_counter() - started_at)

    @application.get("/health")
    def health() -> dict[str, str]:
        health_checks.inc()
        return {"status": "ok"}

    @application.get("/metrics", include_in_schema=False)
    def metrics(request: Request) -> Response:
        try:
            refresh_outbox_metrics(
                request.app.state.engine,
                outbox_pending,
                outbox_oldest_pending_seconds,
            )
        except SQLAlchemyError:
            logger.exception("Failed to refresh outbox metrics")
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @application.post("/runs", status_code=202)
    async def submit_run(request: Request) -> dict[str, str]:
        raw_xml, commit_sha, branch = await _extract_submission(
            request,
            request.app.state.max_report_size_bytes,
        )
        try:
            run_id, pending_count, oldest_pending_created_at = _store_run(
                request.app.state.engine,
                raw_xml,
                commit_sha,
                branch,
            )
        except SQLAlchemyError as exc:
            logger.exception("Failed to store ingested run")
            raise HTTPException(
                status_code=503,
                detail="Ingestion storage is unavailable",
                headers={"Retry-After": str(request.app.state.retry_after_seconds)},
            ) from exc
        set_outbox_metric_values(
            pending_count,
            oldest_pending_created_at,
            outbox_pending,
            outbox_oldest_pending_seconds,
        )
        return {"run_id": run_id, "status": "accepted"}

    @application.get("/internal/runs/{run_id}/raw")
    def get_raw_run(run_id: str, request: Request) -> Response:
        try:
            with request.app.state.engine.connect() as connection:
                raw_xml = connection.execute(
                    select(runs.c.raw_xml).where(runs.c.run_id == run_id)
                ).scalar_one_or_none()
        except SQLAlchemyError as exc:
            logger.exception("Failed to retrieve raw report")
            raise HTTPException(
                status_code=503,
                detail="Ingestion storage is unavailable",
                headers={"Retry-After": str(request.app.state.retry_after_seconds)},
            ) from exc
        if raw_xml is None:
            raise HTTPException(status_code=404, detail="Run not found")
        return Response(
            content=raw_xml,
            media_type="application/xml",
            headers={"X-Content-Type-Options": "nosniff"},
        )

    return application


app = create_app()
