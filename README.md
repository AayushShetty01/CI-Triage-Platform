# CI-Triage-Platform

CI Test-Failure Triage Platform is an event-driven distributed system that
automates CI test analysis. It uses FastAPI, RabbitMQ, PostgreSQL, and a local
LLM to ingest JUnit reports, classify recurring failures, detect flaky tests,
and provide actionable insights through APIs and dashboards.

## Ingestion

Ingestion accepts JUnit XML reports up to `MAX_REPORT_SIZE_BYTES` (10 MiB by
default), stores each run and its `RunReceived` event in PostgreSQL in one
transaction, and relays events asynchronously to RabbitMQ. Duplicate
submissions with the same commit SHA and report bytes return the existing
`run_id`. The transaction and API behavior are implemented in
[`main.py`](services/ingestion/app/main.py), with the database schema in
[`database.py`](services/ingestion/app/database.py); relay behavior is in
[`relay.py`](services/ingestion/app/relay.py).

### Start the local service

Use Docker Compose and Python 3.11 for the local test tools:

```bash
cp .env.example .env
docker compose up -d --build
```

Ingestion listens on port `8001`. The supported endpoints are:

- `POST /runs`: upload a raw XML report (`application/xml`, `text/xml`, or
  `application/junit+xml`) or a multipart form with `file`/`report`,
  `commit_sha`, and `branch`.
- `GET /internal/runs/{run_id}/raw`: retrieve the original uploaded XML bytes.
- `GET /health`: health response.
- `GET /metrics`: Prometheus metrics.

For raw XML, submit required metadata and authentication headers:

```bash
curl -X POST http://localhost:8001/runs \
  -H 'Content-Type: application/xml' \
  -H 'X-API-Key: dev-ingestion-key-change-me' \
  -H 'X-Commit-Sha: 0123456789abcdef' \
  -H 'X-Branch: main' \
  --data-binary @junit.xml
```

`POST /runs` requires `X-API-Key`, matching `INGESTION_API_KEY`.
`GET /internal/runs/{run_id}/raw` requires `X-Internal-Key`, matching
`INTERNAL_API_KEY`. The Parser service must send `X-Internal-Key` and read
`INGESTION_BASE_URL` and `INTERNAL_API_KEY` from its configuration. `/health`
and `/metrics` remain open so health checks and Prometheus can access them.

Authentication is deliberately minimal (static shared keys), not a production
design. Development values are defined in `.env.example` and Compose; configure
non-development values outside local development.
The authentication and Compose environment settings are defined in
[`main.py`](services/ingestion/app/main.py) and
[`.env.example`](.env.example).

### Configuration, events, and logs

The relevant settings are:

- `DATABASE_URL` and `RABBITMQ_URL` (Compose builds these from its credentials)
- `INGESTION_API_KEY` and `INTERNAL_API_KEY`
- `MAX_REPORT_SIZE_BYTES`
- `OUTBOX_POLL_INTERVAL_SECONDS`, `RELAY_POLL_INTERVAL_SECONDS`,
  `RELAY_BACKOFF_CAP_SECONDS`, and `OUTBOX_BATCH_SIZE`
- `INGESTION_RETRY_AFTER_SECONDS`
- `INGESTION_BASE_URL` for the Parser's Ingestion endpoint

The relay publishes persistent messages to the durable `triage.events` topic
exchange with publisher confirms. It marks an outbox row published only after
the broker confirms. Delivery is at least once: a retry can redeliver the same
event, so consumers should be idempotent and use the stable `event_id`.
Ingestion logs are JSON lines with request IDs and run/event IDs when known.
API keys, request bodies, and XML are not logged.

The application metrics include `ingest_requests_total`,
`ingest_latency_seconds`, `ingest_auth_failures_total`, `outbox_pending`,
`outbox_oldest_pending_seconds`, `relay_publish_errors_total`, and
`relay_consecutive_failures`. The oldest-pending gauge is zero when no
unpublished outbox row remains. Metric definitions are in
[`main.py`](services/ingestion/app/main.py); the relay refreshes outbox gauges
in [`relay.py`](services/ingestion/app/relay.py).

### Tests

Install development dependencies:

```bash
python -m pip install -r services/ingestion/requirements-dev.txt
```

Run the default unit and integration test suite:

```bash
python -m pytest -c services/ingestion/pytest.ini services/ingestion/tests
```

The tests named `test_contracts.py` validate the shared schema, samples,
producer output, relay transformation, negative cases, and—when the configured
integration services are available—a message received from RabbitMQ:

```bash
python -m pytest -c services/ingestion/pytest.ini \
  services/ingestion/tests/test_contracts.py -q
```

  The test cases are in
  [`test_contracts.py`](services/ingestion/tests/test_contracts.py) and the
  other Ingestion tests are under
  [`services/ingestion/tests/`](services/ingestion/tests/).

The real failure-injection suite is marked `failure` and excluded from the
default run. It stops, starts, or kills Docker Compose services and always
attempts to restore the stack:

```bash
make failure-tests
```

It exercises broker/database outages, crash recovery, in-flight uploads,
broker flapping, and concurrent duplicate submissions. Scenario summaries,
timestamped service logs, and drained messages are written under
`services/ingestion/failure_simulation/evidence/`; generated folders are
ignored by Git except for the committed `example/` reference. See the
[Ingestion runbook](docs/runbook/ingestion.md) and
[mutation results](services/ingestion/failure_simulation/MUTATION_CHECK.md).

Run the 50-request load smoke:

```bash
make load-test
```

Reproduce the full measured matrix (the broker-down run stops and restores
RabbitMQ during measurement):

```bash
python tools/load/ingestion_load.py --requests 500 --concurrency 1 --report-size small --label small-concurrency-1 --output-dir docs/evidence/ingestion-load
python tools/load/ingestion_load.py --requests 500 --concurrency 5 --report-size small --label small-concurrency-5 --output-dir docs/evidence/ingestion-load
python tools/load/ingestion_load.py --requests 500 --concurrency 20 --report-size small --label small-concurrency-20 --output-dir docs/evidence/ingestion-load
python tools/load/ingestion_load.py --requests 500 --concurrency 50 --report-size small --label small-concurrency-50 --output-dir docs/evidence/ingestion-load
python tools/load/ingestion_load.py --requests 500 --concurrency 20 --report-size small --label small-concurrency-20-repeat-1 --output-dir docs/evidence/ingestion-load
python tools/load/ingestion_load.py --requests 500 --concurrency 20 --report-size small --label small-concurrency-20-repeat-2 --output-dir docs/evidence/ingestion-load
python tools/load/ingestion_load.py --requests 500 --concurrency 20 --report-size small --label small-concurrency-20-repeat-3 --output-dir docs/evidence/ingestion-load
python tools/load/ingestion_load.py --requests 100 --concurrency 5 --report-size large --label large-concurrency-5 --output-dir docs/evidence/ingestion-load
python tools/load/ingestion_load.py --requests 100 --concurrency 20 --report-size large --label large-concurrency-20 --output-dir docs/evidence/ingestion-load
python tools/load/ingestion_load.py --requests 500 --concurrency 20 --report-size small --label small-broker-down-concurrency-20 --output-dir docs/evidence/ingestion-load --broker-down
```

For the full load commands and saved measurements, see
[`docs/evidence/ingestion-load/SUMMARY.md`](docs/evidence/ingestion-load/SUMMARY.md).
Those results are from one laptop and are not production capacity figures.

The `RunReceived` event contract is owned by Ingestion. See the
[schema](contracts/events/run_received.schema.json),
[consumer samples](contracts/events/samples/), and
[contract documentation](contracts/README.md).
