# Query integration evidence — 15 November 2026

These checks used the local Docker Compose stack on a development laptop with synthetic JUnit reports only. Query and the three upstream business services used PostgreSQL and RabbitMQ containers. The LLM test used an existing host Ollama `llama3.2:1b` model; no new model was downloaded. These are integration observations, not production capacity figures.

| Check | Observation |
|---|---|
| Four-service rule path | Run `fc951a88671b` was accepted by Ingestion, parsed, classified by a rule as `assertion`, and exposed by Query as `complete` with `rules_version=1`. |
| Host LLM path | Run `3208f874630e` reached Query with `source=llm`, `model=llama3.2:1b`, `prompt_version=v1`, and a terminal category. |
| Flaky candidate | A later passing run of `demo.Checkout::rejects_bad_card` gave Query one passed and one failed run; `/api/flaky?days=7&branch=main` returned one `potentially_flaky` candidate. |
| Query outage | Query was stopped, run `2f766b74fd12` was uploaded, and Query was restarted. The durable queue delivered the run and its final `assertion` category after recovery. |
| Pending to final result | With the LLM worker stopped, run `7d2132dafa01` showed `state=pending`, `pending=1`, and `category=null` in Query. After the worker restarted, the same run showed `state=complete`, `pending=0`, and `source=llm`. |
| Operations metrics | Prometheus reported `up` for Ingestion, Parser, Classifier, Classifier worker, and Query. Grafana API returned the provisioned `ci-triage-ops` dashboard. |
| Error-case contract | After rebuilding Query with the upstream services, run `25f01a223d88` containing one JUnit `<error>` and one passed test showed `total_tests=2`, `failed_tests=1`, `passed_tests=1`, and a terminal `timeout` category. |
| Startup ordering | Compose created and marked Query healthy before starting Ingestion. Query health requires its durable queue to be bound, so a fresh stack cannot accept a report before Query subscribes. |

The local automated Query suite passed 15 tests covering projection idempotency, out-of-order classification, pending/terminal/degraded states, flaky candidates, version mixing, invalid categories and payloads, Parser history lookup, retry on Parser outage, malformed Parser responses, and poison events. `docker compose config --quiet`, JavaScript syntax checking and Grafana JSON validation also passed.

To reproduce, start the stack as in the root README and wait for Query `/health`. Upload synthetic JUnit XML through `POST /runs`, save the returned `run_id`, and inspect `/api/runs/{run_id}`. For the pending case, stop `classifier-llm-worker` before uploading an unknown failure, inspect the Query API, then start the existing worker container. For the outage case, stop Query after it has bound its durable queue, upload a report, then restart Query and inspect the result. The [runbook](../runbook/query.md) gives the relevant endpoints and metrics.
