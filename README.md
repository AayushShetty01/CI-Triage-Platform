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

To run the fast tests locally from the repository root:

Use Python 3.11, as required by the project:

```bash
python -m pip install -r services/ingestion/requirements-dev.txt
mkdir -p artifacts
python -m pytest \
  -c services/ingestion/pytest.ini \
  services/ingestion/tests \
  --junitxml=artifacts/junit-report.xml
```

The test run writes `artifacts/ingestion-test.log` with timestamped API
requests, sanitized payload snippets, response codes, error response details,
timings, outcomes, and failure tracebacks, plus `artifacts/ingestion-summary.txt`
with case counts, execution percentages, failed test IDs, and failure messages.
Headers and query values are not logged, and credential-like values are
redacted. The JUnit XML records the actual pytest outcomes. These files are
uploaded as the `ingestion-test-artifacts` artifact by GitHub Actions, including
when tests fail.

The named pytest cases inventory `/health` and `/metrics`; raw and multipart
JUnit uploads, accepted XML media types, metadata sources and validation,
malformed/unsafe XML, size and content-type limits, duplicate/idempotent
submissions, persisted report retrieval, and outbox/relay success and failure.
The integration case also verifies persistence and the delivered event against
the configured PostgreSQL database and RabbitMQ broker. JUnit case names are
the pytest node IDs; the execution log and text summary also include stable
`ING-...` identifiers for each test.

The integration test also runs when `INGESTION_TEST_DATABASE_URL` and
`INGESTION_TEST_RABBITMQ_URL` point to an isolated PostgreSQL database and
RabbitMQ broker. It applies the migrations, uploads a representative report
through the API, verifies the stored report and duplicate behavior, and checks
that the actual broker receives the `RunReceived` event. GitHub Actions starts
dedicated PostgreSQL and RabbitMQ services for this test. Avoid pointing the
integration test at a shared or production database.

The healthy ingestion tests are independent from a controlled failure
simulation suite. Run the simulation locally with:

```bash
python -m pytest \
  -c services/ingestion/pytest.ini \
  services/ingestion/failure_simulation/test_ingestion_failures.py
```

The simulation executes deterministic checks against a mock payment
application, generates a 15-case JUnit report from those check results, and
submits it to the real ingestion API using an isolated SQLite database. Its
sample intentionally contains 9 passing and 6 failing simulated cases; these
failures are not ingestion API test failures and do not affect the healthy
suite. The generated `artifacts/failure-simulation-junit.xml`,
`artifacts/failure-simulation-execution.log`, and
`artifacts/failure-simulation-summary.txt` are uploaded by the separately
selectable **Ingestion failure simulation** workflow
(`workflow_dispatch`). The summary identifies the failures as controlled
payment-application simulations.

The workflow runs automatically on pushes and pull requests, or manually from
the repository's **Actions → Ingestion tests** page. Each workflow run includes
a per-test-case pass/fail summary. Download the test files from **Actions →
select the workflow run → Artifacts → ingestion-test-artifacts**.

GitLab CI is also configured in `.gitlab-ci.yml`. It runs the same suite on
branch and merge-request pipelines using isolated PostgreSQL and RabbitMQ
service containers. GitLab publishes `artifacts/junit-report.xml` as a test
report and uploads the log and text summary for one week even when the test job
fails. This requires a GitLab runner that supports service containers. In
GitLab, download the files from **Build → Pipelines → select the pipeline →
ingestion-tests → Browse artifacts**. Test failures remain failures; the normal
pipeline does not force a target pass/fail ratio or inject test faults.
