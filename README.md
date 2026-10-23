# CI-Triage-Platform
CI Test-Failure Triage Platform is an event-driven distributed system that automates CI test analysis. Built with FastAPI, RabbitMQ, PostgreSQL, and a local LLM, it ingests JUnit reports, classifies recurring failures, detects flaky tests, and provides actionable insights through APIs and dashboards.

## Ingestion service

The Ingestion service accepts JUnit XML reports, stores each run and a
`RunReceived` event atomically, then relays events to RabbitMQ. Report uploads
are limited to 10 MiB by default. Duplicate submissions with the same commit
SHA and report bytes return the same `run_id` without creating another run.

Start Ingestion and its PostgreSQL and RabbitMQ dependencies with Docker Compose:

```bash
cp .env.example .env
docker compose up --build ingestion
```

The service listens on port `8001`. Submit raw XML using headers:

```bash
curl -X POST http://localhost:8001/runs \
  -H 'Content-Type: application/xml' \
  -H 'X-Commit-Sha: 0123456789abcdef' \
  -H 'X-Branch: main' \
  --data-binary @junit.xml
```

Multipart uploads are also accepted, using `file` (or `report`), `commit_sha`,
and `branch` fields. Both formats return HTTP `202` with the stable `run_id`.
Parser retrieves stored report bytes from
`GET /internal/runs/{run_id}/raw`. Check `http://localhost:8001/health` for the
health response and `http://localhost:8001/metrics` for Prometheus metrics.

The relay publishes persistent messages to the durable `triage.events` topic
exchange with publisher confirms. A pending event is marked published only
after confirmation. Delivery is at least once: if the relay stops after a
broker confirmation but before the database update, it can publish again with
the same `event_id`, so consumers should be idempotent. Events follow
[`RunReceived.schema.json`](contracts/events/RunReceived.schema.json).

Compose initializes a dedicated `ingestion` database role and schema on the
first start. Set `INGESTION_DB_PASSWORD`, `POSTGRES_ADMIN_PASSWORD`,
`RABBITMQ_USER`, and `RABBITMQ_PASSWORD` in the environment to override local
development credentials. `MAX_REPORT_SIZE_BYTES`, `OUTBOX_POLL_INTERVAL_SECONDS`,
and `OUTBOX_BATCH_SIZE` configure upload and relay limits.

To run its tests locally from the repository root:

Use Python 3.11, as required by the project:

```bash
python -m pip install -r services/ingestion/requirements-dev.txt
python -m pytest -c services/ingestion/pytest.ini services/ingestion/tests
```

The same test suite runs automatically in GitHub Actions on pushes and pull
requests, or manually from the repository's **Actions → Ingestion tests** page.
Each workflow run includes a per-test-case pass/fail summary; its downloadable
JUnit XML report is attached as the `ingestion-junit-report` artifact.
