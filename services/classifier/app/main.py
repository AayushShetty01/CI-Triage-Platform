import asyncio
import logging
import re
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import Any, AsyncGenerator

import httpx
from fastapi import FastAPI, HTTPException, Query, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import create_engine, func, select
from sqlalchemy.engine import Engine

from app import metrics
from app.consumer import run_consumer
from app.database import classifications, failure_groups, llm_jobs, metadata
from app.llm_client import PROMPT_HASH, PROMPT_VERSION, OllamaClient
from app.llm_worker import LLMWorkerPool
from app.logging_config import configure_json_logging, request_id_context, run_id_context
from app.relay import refresh_outbox_metrics, run_outbox_relay
from app.rules import RULES_VERSION
from app.settings import Settings

logger = logging.getLogger(__name__)
REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


def _create_engine(database_url: str) -> Engine:
    connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
    engine = create_engine(database_url, pool_pre_ping=True, connect_args=connect_args)
    if engine.dialect.name == "sqlite":
        engine = engine.execution_options(schema_translate_map={"classifier": None})
    return engine


def _serialize(row: Any) -> dict[str, Any]:
    return {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in dict(row).items()}


def create_app(
    settings: Settings | None = None,
    llm_transport: httpx.AsyncBaseTransport | None = None,
    **overrides: Any,
) -> FastAPI:
    configure_json_logging()
    cfg = replace(settings or Settings.from_env(), **overrides)
    engine = _create_engine(cfg.database_url)
    client = OllamaClient(
        base_url=cfg.ollama_base_url,
        model=cfg.ollama_model,
        timeout_seconds=cfg.llm_timeout_seconds,
        max_input_chars=cfg.llm_max_input_chars,
        max_output_tokens=cfg.llm_max_output_tokens,
        transport=llm_transport,
    )
    pool = LLMWorkerPool(engine, client, cfg)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        if engine.dialect.name == "sqlite":
            metadata.create_all(engine)
        stop = asyncio.Event()
        tasks: list[asyncio.Task[None]] = []
        if cfg.enable_consumer:
            tasks.append(asyncio.create_task(run_consumer(engine, cfg, stop)))
        if cfg.enable_relay:
            tasks.append(
                asyncio.create_task(
                    run_outbox_relay(
                        engine=engine,
                        rabbitmq_url=cfg.rabbitmq_url,
                        poll_interval_seconds=cfg.outbox_poll_interval_seconds,
                        batch_size=cfg.outbox_batch_size,
                        pending_gauge=metrics.outbox_pending,
                        oldest_pending_gauge=metrics.outbox_oldest_pending_seconds,
                        publish_errors=metrics.relay_publish_errors,
                        consecutive_failures_gauge=metrics.relay_consecutive_failures,
                        backoff_cap_seconds=cfg.relay_backoff_cap_seconds,
                    )
                )
            )
        if cfg.enable_llm_worker and cfg.llm_enabled:
            tasks.extend(pool.start(stop))
        try:
            yield
        finally:
            stop.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            engine.dispose()

    app = FastAPI(title="CI Triage Platform - Classifier Service", lifespan=lifespan)
    app.state.engine = engine
    app.state.settings = cfg
    app.state.worker_pool = pool

    @app.middleware("http")
    async def context_middleware(request: Request, call_next):
        req_id = request.headers.get("x-request-id")
        request_id_context.set(req_id if req_id and REQUEST_ID_PATTERN.match(req_id) else None)
        run_id_context.set(None)
        return await call_next(request)

    @app.get("/health")
    async def health() -> dict[str, str]:
        try:
            with engine.connect() as conn:
                conn.execute(select(1))
        except Exception as err:
            logger.error("Database health check failed: %s", err)
            raise HTTPException(status_code=503, detail="Database unhealthy")
        metrics.health_checks.inc()
        return {"status": "healthy", "service": "classifier"}

    @app.get("/metrics")
    async def metrics_endpoint() -> Response:
        refresh_outbox_metrics(engine, metrics.outbox_pending, metrics.outbox_oldest_pending_seconds)
        pool.refresh_pending_gauge()
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/runs/{run_id}/classifications")
    async def run_classifications(run_id: str) -> dict[str, Any]:
        with engine.connect() as conn:
            rows = conn.execute(
                select(classifications)
                .where(classifications.c.run_id == run_id)
                .order_by(classifications.c.test_id)
            ).mappings().all()
        if not rows:
            raise HTTPException(status_code=404, detail="No classifications for this run")
        items = [_serialize(r) for r in rows]
        for item in items:
            item.pop("llm_job_id", None)
        counts: dict[str, int] = {}
        for item in items:
            counts[item["status"]] = counts.get(item["status"], 0) + 1
        return {"run_id": run_id, "status_counts": counts, "classifications": items}

    @app.get("/groups")
    async def groups(limit: int = Query(50, ge=1, le=500)) -> list[dict[str, Any]]:
        """Failure groups (shared fingerprint), most frequent first, with their latest category."""
        with engine.connect() as conn:
            group_rows = conn.execute(
                select(failure_groups)
                .order_by(failure_groups.c.occurrence_count.desc(), failure_groups.c.last_seen_at.desc())
                .limit(limit)
            ).mappings().all()
            fingerprints = [g["fingerprint"] for g in group_rows]
            latest: dict[str, dict[str, Any]] = {}
            if fingerprints:
                for row in conn.execute(
                    select(
                        classifications.c.fingerprint,
                        classifications.c.category,
                        classifications.c.status,
                        classifications.c.source,
                    )
                    .where(classifications.c.fingerprint.in_(fingerprints))
                    .order_by(classifications.c.id)
                ).mappings():
                    latest[row["fingerprint"]] = dict(row)
        result = []
        for g in group_rows:
            item = _serialize(g)
            last = latest.get(g["fingerprint"], {})
            item.update(
                category=last.get("category"),
                status=last.get("status"),
                source=last.get("source"),
            )
            result.append(item)
        return result

    @app.get("/llm/status")
    async def llm_status() -> dict[str, Any]:
        with engine.connect() as conn:
            job_counts = {
                status: count
                for status, count in conn.execute(
                    select(llm_jobs.c.status, func.count()).group_by(llm_jobs.c.status)
                ).all()
            }
        return {
            "llm_enabled": cfg.llm_enabled,
            "worker_enabled_here": cfg.enable_llm_worker,
            "circuit_state_here": pool.breaker.state,
            "model": cfg.ollama_model,
            "prompt_version": PROMPT_VERSION,
            "prompt_hash": PROMPT_HASH,
            "rules_version": RULES_VERSION,
            "jobs": job_counts,
        }

    return app


app = create_app()
