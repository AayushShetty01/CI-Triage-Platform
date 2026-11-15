# Query & Insights runbook

## Start and inspect

From the repository root, copy `.env.example` to `.env` and run `docker compose up -d --build`. Open the triage UI at `http://localhost:8005`, Query health at `http://localhost:8005/health`, Prometheus at `http://localhost:9090`, and Grafana at `http://localhost:3000` (default development login `admin` / `triage_dev`). Grafana provisions **CI Triage Operations** with request rate, HTTP errors, p95 latency, pending work, LLM tokens and model calls.

Query owns the `query` PostgreSQL schema and subscribes to `run.received`, `failures.extracted`, and `failure.classified` via the durable `query.events` queue. It is healthy only after its queue is bound. Wait for `/health` to return 200 before uploading the first report. On a PostgreSQL volume created before Query existed, run `docker compose exec postgres sh /docker-entrypoint-initdb.d/40-query.sh` once to create the role and schema.

The API endpoints are `GET /api/runs`, `GET /api/runs/{run_id}`, `GET /api/insights?days=7&branch=main`, and `GET /api/flaky?days=14&branch=main`. The dashboard polls every ten seconds. `pending` means Parser found a failure but its final classification has not arrived. `unknown` is a terminal category; `degraded` is a terminal status produced by the classifier's fallback. A received run awaiting Parser is `accepted`.

## Demonstrate the path

1. Upload a synthetic JUnit XML report to Ingestion using the command in the root README. Keep the returned `run_id`.
2. Open `/api/runs/{run_id}` and observe `accepted` or `pending`, then `complete` or `degraded`. The UI shows the same state.
3. Check `/api/insights` for category counts and common fingerprints. Submit a second synthetic run with the same `test_id` passing and check `/api/flaky` for its candidate signal.
4. Inspect `query_events_total`, `query_pending_failures`, `query_consumer_ready`, `query_http_requests_total`, and `query_http_latency_seconds` at `/metrics` or in Grafana.

## Break and recover

After Query has started once, stop it with `docker compose stop query`, upload another run, and then `docker compose start query`. RabbitMQ retains its events; the projection catches up and the UI shows the run. For a Parser outage, Query retries `FailuresExtracted` after a delay and continues serving previously stored insights. An invalid event is rejected to `query.events.dlq`; it does not block later messages. A duplicate event ID changes no projection row or insight count. The local test suite verifies these state and idempotency rules with an isolated database.

## Limits

This is a local demonstration deployment. The Query schema is created on service startup, not migrated independently; schema changes require a migration before using a persistent deployment. Query's first queue declaration cannot replay events published before it existed. Its `FailureClassified` stream contains terminal results only; pending is inferred from `FailuresExtracted`. The flaky signal can include code or environment changes and should be investigated by a developer. The dashboard does not expose raw XML or stack traces; it shows a truncated error message. No personal data should be uploaded.
