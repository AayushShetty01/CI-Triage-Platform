# CI-Triage-Platform
CI Test-Failure Triage Platform is an event-driven distributed system that automates CI test analysis. Built with FastAPI, RabbitMQ, PostgreSQL, and a local LLM, it ingests JUnit reports, classifies recurring failures, detects flaky tests, and provides actionable insights through APIs and dashboards.

## Ingestion service

Run the initial Ingestion service skeleton with Docker Compose:

```bash
docker compose up --build ingestion
```

The service listens on port `8001`. Check `http://localhost:8001/health` for its
health response and `http://localhost:8001/metrics` for Prometheus metrics.

To run its tests locally from the repository root:

Use Python 3.11, as required by the project:

```bash
python -m pip install -r services/ingestion/requirements-dev.txt
python -m pytest -c services/ingestion/pytest.ini services/ingestion/tests
```
