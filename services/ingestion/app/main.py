from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest

app = FastAPI(title="CI Triage Platform - Ingestion")
health_checks = Counter(
    "ingestion_health_checks_total",
    "Number of successful ingestion service health checks.",
)


@app.get("/health")
def health() -> dict[str, str]:
    health_checks.inc()
    return {"status": "ok"}


@app.get("/metrics", include_in_schema=False)
def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
