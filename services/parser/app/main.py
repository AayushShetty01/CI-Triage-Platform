import asyncio
import logging
import os
import re
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator

from fastapi import FastAPI, HTTPException, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from sqlalchemy import create_engine, select
from sqlalchemy.engine import Engine

from app.consumer import run_consumer
from app.database import metadata, outbox, parsed_runs, test_results
from app.ingestion_client import IngestionClient
from app.logging_config import (
    configure_json_logging,
    request_id_context,
    run_id_context,
)
from app.relay import (
    refresh_outbox_metrics,
    run_outbox_relay,
    set_outbox_metric_values,
)

logger = logging.getLogger(__name__)
REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")

# Metrics
health_checks = Counter(
    "parser_health_checks_total",
    "Number of successful parser service health checks.",
)
events_consumed = Counter(
    "parser_events_consumed_total",
    "Number of RunReceived events consumed from RabbitMQ.",
)
events_processed = Counter(
    "parser_events_processed_total",
    "Number of RunReceived events processed.",
    ("status",),
)
tests_total = Counter(
    "parser_tests_total",
    "Total number of individual test cases parsed.",
)
failures_total = Counter(
    "parser_failures_extracted_total",
    "Total number of failed test cases extracted.",
)
poison_messages = Counter(
    "parser_poison_messages_total",
    "Number of poison or unprocessable messages rejected to DLQ.",
)
outbox_pending = Gauge(
    "parser_outbox_pending",
    "Number of unpublished parser outbox events.",
)
outbox_oldest_pending_seconds = Gauge(
    "parser_outbox_oldest_pending_seconds",
    "Age in seconds of the oldest unpublished parser outbox event.",
)
relay_publish_errors = Counter(
    "parser_relay_publish_errors_total",
    "Number of failed parser outbox relay iterations.",
)
relay_consecutive_failures = Gauge(
    "parser_relay_consecutive_failures",
    "Number of consecutive failed parser outbox relay iterations.",
)

DEFAULT_DATABASE_URL = (
    "postgresql+psycopg://parser:parser_dev@localhost:5432/triage"
)
DEFAULT_RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"
DEFAULT_INGESTION_BASE_URL = "http://localhost:8001"
DEFAULT_INTERNAL_API_KEY = "dev-internal-key-change-me"


def _create_engine(database_url: str) -> Engine:
    connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
    engine = create_engine(database_url, pool_pre_ping=True, connect_args=connect_args)
    if engine.dialect.name == "sqlite":
        engine = engine.execution_options(schema_translate_map={"parser": None})
    return engine


def create_app(
    database_url: str | None = None,
    rabbitmq_url: str | None = None,
    ingestion_base_url: str | None = None,
    internal_api_key: str | None = None,
    enable_consumer: bool = True,
    enable_relay: bool = True,
    outbox_poll_interval_seconds: float = 1.0,
    relay_backoff_cap_seconds: float = 30.0,
    outbox_batch_size: int = 50,
) -> FastAPI:
    configure_json_logging()
    db_url = database_url or os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
    rb_url = rabbitmq_url or os.getenv("RABBITMQ_URL", DEFAULT_RABBITMQ_URL)
    ing_url = ingestion_base_url or os.getenv("INGESTION_BASE_URL", DEFAULT_INGESTION_BASE_URL)
    int_key = internal_api_key or os.getenv("INTERNAL_API_KEY", DEFAULT_INTERNAL_API_KEY)

    engine = _create_engine(db_url)
    ingestion_client = IngestionClient(base_url=ing_url, internal_key=int_key)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        app.state.engine = engine
        app.state.ingestion_client = ingestion_client

        # In sqlite testing, create tables automatically
        if engine.dialect.name == "sqlite":
            metadata.create_all(engine)

        stop_consumer_event = asyncio.Event()
        consumer_task: asyncio.Task[None] | None = None
        relay_task: asyncio.Task[None] | None = None

        metrics_map = {
            "consumed_total": events_consumed,
            "processed_total": events_processed,
            "poison_messages_total": poison_messages,
        }

        if enable_consumer:
            consumer_task = asyncio.create_task(
                run_consumer(
                    engine=engine,
                    rabbitmq_url=rb_url,
                    ingestion_client=ingestion_client,
                    metrics=metrics_map,
                    stop_event=stop_consumer_event,
                )
            )

        if enable_relay:
            relay_task = asyncio.create_task(
                run_outbox_relay(
                    engine=engine,
                    rabbitmq_url=rb_url,
                    poll_interval_seconds=outbox_poll_interval_seconds,
                    batch_size=outbox_batch_size,
                    pending_gauge=outbox_pending,
                    oldest_pending_gauge=outbox_oldest_pending_seconds,
                    publish_errors=relay_publish_errors,
                    consecutive_failures_gauge=relay_consecutive_failures,
                    backoff_cap_seconds=relay_backoff_cap_seconds,
                )
            )

        try:
            yield
        finally:
            stop_consumer_event.set()
            if consumer_task is not None:
                consumer_task.cancel()
                try:
                    await consumer_task
                except asyncio.CancelledError:
                    pass
            if relay_task is not None:
                relay_task.cancel()
                try:
                    await relay_task
                except asyncio.CancelledError:
                    pass
            engine.dispose()

    app = FastAPI(title="CI Triage Platform - Parser Service", lifespan=lifespan)
    app.state.engine = engine
    app.state.ingestion_client = ingestion_client

    @app.middleware("http")
    async def context_and_timing_middleware(request: Request, call_next):
        req_id = request.headers.get("x-request-id")
        if req_id and REQUEST_ID_PATTERN.match(req_id):
            request_id_context.set(req_id)
        else:
            request_id_context.set(None)
        run_id_context.set(None)
        return await call_next(request)

    @app.get("/health")
    async def health(request: Request) -> dict[str, str]:
        eng: Engine = getattr(request.app.state, "engine", engine)
        try:
            with eng.connect() as conn:
                conn.execute(select(1))
        except Exception as err:
            logger.error("Database health check failed: %s", err)
            raise HTTPException(status_code=503, detail="Database unhealthy")
        health_checks.inc()
        return {"status": "healthy", "service": "parser"}

    @app.get("/metrics")
    async def metrics(request: Request) -> Response:
        eng: Engine = getattr(request.app.state, "engine", engine)
        refresh_outbox_metrics(eng, outbox_pending, outbox_oldest_pending_seconds)
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/runs/{run_id}")
    async def get_parsed_run(run_id: str, request: Request) -> dict[str, Any]:
        eng: Engine = getattr(request.app.state, "engine", engine)
        with eng.connect() as conn:
            row = conn.execute(
                select(parsed_runs).where(parsed_runs.c.run_id == run_id)
            ).mappings().one_or_none()
            if row is None:
                raise HTTPException(status_code=404, detail="Run not found")
            return dict(row)

    @app.get("/runs/{run_id}/results")
    async def get_test_results(run_id: str, request: Request) -> list[dict[str, Any]]:
        eng: Engine = getattr(request.app.state, "engine", engine)
        with eng.connect() as conn:
            rows = conn.execute(
                select(test_results).where(test_results.c.run_id == run_id)
            ).mappings().all()
            return [dict(r) for r in rows]

    @app.get("/runs/{run_id}/failures")
    async def get_run_failures(run_id: str, request: Request) -> list[dict[str, Any]]:
        eng: Engine = getattr(request.app.state, "engine", engine)
        with eng.connect() as conn:
            rows = conn.execute(
                select(test_results)
                .where(
                    test_results.c.run_id == run_id,
                    test_results.c.status.in_(["failed", "error"]),
                )
            ).mappings().all()
            return [dict(r) for r in rows]

    return app


app = create_app()
