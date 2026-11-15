# Query & Insights owns a read projection; it never reads another service's database.

import asyncio
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import create_engine, func, select
from sqlalchemy.engine import Engine

from app.consumer import run_consumer
from app.database import failures, metadata
from app.metrics import metrics
from app.queries import flaky_candidates, get_run, insights, list_runs

STATIC = Path(__file__).parent / "static"


def create_app(database_url: str | None = None, enable_consumer: bool = True) -> FastAPI:
    url = database_url or os.getenv("DATABASE_URL", "postgresql+psycopg://query:query_dev@localhost:5432/triage")
    options = {"check_same_thread": False} if url.startswith("sqlite") else {}
    engine: Engine = create_engine(url, pool_pre_ping=True, connect_args=options)
    if engine.dialect.name == "sqlite":
        engine = engine.execution_options(schema_translate_map={"query": None})

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if engine.dialect.name == "sqlite":
            metadata.create_all(engine)
        ready = asyncio.Event()
        app.state.consumer_ready = ready
        task = None
        if enable_consumer:
            task = asyncio.create_task(run_consumer(
                engine, os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
                os.getenv("PARSER_BASE_URL", "http://localhost:8002"), metrics, ready,
            ))
        try:
            yield
        finally:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            engine.dispose()

    app = FastAPI(title="CI Triage - Query & Insights", lifespan=lifespan)
    app.state.engine = engine

    @app.middleware("http")
    async def measure(request: Request, call_next):
        started = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        finally:
            route = request.scope.get("route")
            path = route.path if route else request.url.path
            metrics.requests.labels(request.method, path, str(status)).inc()
            metrics.latency.labels(request.method, path).observe(time.perf_counter() - started)

    @app.get("/")
    def dashboard():
        return FileResponse(STATIC / "index.html")

    @app.get("/app.js")
    def javascript():
        return FileResponse(STATIC / "app.js", media_type="text/javascript")

    @app.get("/style.css")
    def stylesheet():
        return FileResponse(STATIC / "style.css", media_type="text/css")

    @app.get("/health")
    def health():
        try:
            with engine.connect() as conn:
                conn.execute(select(1))
        except Exception as exc:
            raise HTTPException(status_code=503, detail="Query database unavailable") from exc
        if enable_consumer and not app.state.consumer_ready.is_set():
            raise HTTPException(status_code=503, detail="Query queue not ready")
        return {"status": "healthy", "service": "query"}

    @app.get("/metrics")
    def prometheus():
        with engine.connect() as conn:
            count = conn.execute(select(func.count()).select_from(failures).where(
                failures.c.status == "pending"
            )).scalar_one()
        metrics.pending.set(count)
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/api/runs")
    def api_runs(limit: int = Query(50, ge=1, le=200),
                 days: int = Query(7, ge=1, le=90), branch: str | None = None):
        return list_runs(engine, limit, days, branch)

    @app.get("/api/runs/{run_id}")
    def api_run(run_id: str):
        result = get_run(engine, run_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Run not found")
        return result

    @app.get("/api/insights")
    def api_insights(days: int = Query(7, ge=1, le=90), branch: str | None = None):
        return insights(engine, days, branch)

    @app.get("/api/flaky")
    def api_flaky(days: int = Query(14, ge=1, le=90), branch: str | None = None):
        return {"days": days, "branch": branch, "definition":
                "Tests with both passed and failed/error runs in the selected window; candidate only.",
                "candidates": flaky_candidates(engine, days, branch)}

    return app


app = create_app()
