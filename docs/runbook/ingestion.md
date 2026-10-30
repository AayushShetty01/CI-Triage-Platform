# Ingestion runbook

This runbook covers the Ingestion service in the repository's Docker Compose
stack. It assumes Docker Compose, Python 3.11, and the development dependencies
are installed. See the [root Ingestion instructions](../../README.md#ingestion)
for setup and test prerequisites.

## Observe

Start or inspect the stack:

```bash
docker compose up -d --build
docker compose ps
curl -fsS http://localhost:8001/health
```

`/health` and `/metrics` are open endpoints. A healthy service returns
`{"status":"ok"}` from `/health`. For a recovered broker, the useful outbox
signals are `outbox_pending 0`, `outbox_oldest_pending_seconds 0`, and
`relay_consecutive_failures 0`. During an outage, pending rows and consecutive
failures can rise; the relay retries after backoff.

Inspect the principal metrics:

```bash
curl -fsS http://localhost:8001/metrics \
  | grep -E '^(outbox_pending|outbox_oldest_pending_seconds|relay_consecutive_failures|relay_publish_errors_total|ingest_requests_total|ingest_auth_failures_total|ingest_latency_seconds)'
```

`outbox_pending` is the number of unpublished rows. The oldest-pending gauge is
zero when no row is pending and otherwise measures age from the oldest row's
creation time using the application UTC clock. `relay_publish_errors_total`
counts failed relay iterations; `relay_consecutive_failures` should return to
zero after a successful recovery. The request counters, authentication-failure
counter, and latency histogram help distinguish request failures from relay
backlog. Metric definitions and updates are in
[`main.py`](../../services/ingestion/app/main.py) and
[`relay.py`](../../services/ingestion/app/relay.py).

Ingestion application logs are one JSON object per line. Docker Compose adds a
service/timestamp prefix, which this command strips before passing JSON to
`jq`:

```bash
docker compose logs --no-color --timestamps ingestion \
  | sed -E 's/^[^|]*\| [^ ]+ //' \
  | jq -c 'select(.service == "ingestion")'
```

Filter for a run, publish failure, or request:

```bash
docker compose logs --no-color --timestamps ingestion \
  | sed -E 's/^[^|]*\| [^ ]+ //' \
  | jq -c 'select(.event == "run_accepted" or .event == "relay_publish_failed")'
```

Never add API-key values, request bodies, or XML to logs or evidence.

RabbitMQ's management UI is at <http://localhost:15672>; use the credentials
configured by `RABBITMQ_USER` and `RABBITMQ_PASSWORD` (the Compose development
defaults are in `.env.example`). Inspect ready and unacknowledged message counts
under **Queues and Streams** for the consumer queue. RabbitMQ exchanges route
messages but do not themselves have a queue depth. The failure harness creates
a durable, randomly named queue bound to `triage.events` with `run.received`;
its exact queue name is in each scenario's `summary.json`.

## Break

Run one real scenario at a time. These tests manipulate Docker Compose
containers and should not be run against a shared or production stack. Each
command writes timestamped service logs, a summary, and drained messages under
the named evidence folder.

### RabbitMQ unavailable

```bash
python -m pytest -c services/ingestion/pytest.ini \
  services/ingestion/failure_simulation/test_failure_scenarios.py \
  -m failure -k test_broker_down -v
```

The test stops the RabbitMQ container, submits 20 unique runs, checks that the
uploads are accepted and remain pending, then restores RabbitMQ and checks that
the queue receives the corresponding events. The passing run is recorded under
`services/ingestion/failure_simulation/evidence/broker_down/`.
The committed example is
[`evidence/example/`](../../services/ingestion/failure_simulation/evidence/example/).

The relay's real `relay_publish_failed` log line below was captured from
Compose logs while RabbitMQ was SIGKILLed during an in-flight publish in a
supplemental interruption experiment. That extra kill caused at-least-once
redelivery and the strict 20-message assertion timed out after observing 26
messages; it is recorded separately in the
[supplemental result](../evidence/broker-down-publish-failure-summary.json)
and [saved log excerpt](../evidence/broker-down-publish-failure.jsonl).
It is not the passing single-outage scenario above.

```json
{"timestamp":"2026-10-30T14:14:01.419+00:00","level":"error","service":"ingestion","message":"Outbox event publish failed","event":"relay_publish_failed","run_id":"e14ecc8da135fdaa1fa01a34ad19d9224479358315186dfe90529eea98d78c03","event_id":"f038bfd0-0c89-4a7c-927e-80e10aa5218d","endpoint":"run.received","exception_type":"AMQPConnectionError","traceback":"  File \"/app/app/relay.py\", line 112, in publish_pending_batch\n    await exchange.publish(\n  File \"/usr/local/lib/python3.11/site-packages/aio_pika/exchange.py\", line 203, in publish\n    return await channel.basic_publish(\n           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/usr/local/lib/python3.11/site-packages/aiormq/channel.py\", line 699, in basic_publish\n    return await countdown(confirmation)\n           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/usr/local/lib/python3.11/site-packages/aiormq/tools.py\", line 96, in __call__\n    return await coro\n           ^^^^^^^^^^\n  File \"/usr/local/lib/python3.11/asyncio/tasks.py\", line 694, in _wrap_awaitable\n    return (yield from awaitable.__await__())\n            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/usr/local/lib/python3.11/site-packages/aiormq/abc.py\", line 44, in __inner\n    return await self.task\n           ^^^^^^^^^^^^^^^\n  File \"/usr/local/lib/python3.11/site-packages/aiormq/connection.py\", line 727, in __reader\n    async for weight, channel, frame in frame_receiver:\n  File \"/usr/local/lib/python3.11/site-packages/aiormq/connection.py\", line 230, in __anext__\n    return await self.get_frame()\n           ^^^^^^^^^^^^^^^^^^^^^^\n  File \"/usr/local/lib/python3.11/site-packages/aiormq/connection.py\", line 215, in get_frame\n    raise AMQPConnectionError(\n"}
```

No credential is present in this captured log entry; its traceback contains
only the publish call path and broker-connection exception.

### PostgreSQL unavailable

```bash
python -m pytest -c services/ingestion/pytest.ini \
  services/ingestion/failure_simulation/test_failure_scenarios.py \
  -m failure -k test_database_down -v
```

Uploads during the database outage should not receive `202`; after PostgreSQL
returns, Ingestion should recover without being restarted. Evidence is written
under `services/ingestion/failure_simulation/evidence/database_down/`.

### Ingestion crash with unpublished rows

```bash
python -m pytest -c services/ingestion/pytest.ini \
  services/ingestion/failure_simulation/test_failure_scenarios.py \
  -m failure -k test_crash_with_unpublished_rows -v
```

The test SIGKILLs Ingestion while RabbitMQ is down, then verifies that the
pending rows publish after both services recover and that raw reports remain
byte-identical. Evidence is under
`services/ingestion/failure_simulation/evidence/crash_with_unpublished_rows/`.

### Ingestion killed during uploads

```bash
python -m pytest -c services/ingestion/pytest.ini \
  services/ingestion/failure_simulation/test_failure_scenarios.py \
  -m failure -k test_kill_during_uploads -v
```

The test kills Ingestion during bounded concurrent uploads. Every request
acknowledged with `202` must be stored and eventually published; connection
errors are recorded without asserting whether those requests were stored.
Evidence is under
`services/ingestion/failure_simulation/evidence/kill_during_uploads/`.

### RabbitMQ flapping

```bash
python -m pytest -c services/ingestion/pytest.ini \
  services/ingestion/failure_simulation/test_failure_scenarios.py \
  -m failure -k test_broker_flapping -v
```

The test repeatedly stops and starts RabbitMQ's application inside the
container, rather than restarting the container, while uploads continue. It
checks the acknowledged-event set, the pending-or-queued durability invariant,
and any duplicate payloads. Evidence is under
`services/ingestion/failure_simulation/evidence/broker_flapping/`.

### Concurrent duplicate uploads

```bash
python -m pytest -c services/ingestion/pytest.ini \
  services/ingestion/failure_simulation/test_failure_scenarios.py \
  -m failure -k test_concurrent_duplicates -v
```

All submissions of the same report should resolve to one run and one delivered
event. Evidence is under
`services/ingestion/failure_simulation/evidence/concurrent_duplicates/`.

## Recover

After an interruption, bring the Compose services up and wait for health:

```bash
docker compose up -d
docker compose ps
curl -fsS http://localhost:8001/health
curl -fsS http://localhost:8001/metrics \
  | grep -E '^(outbox_pending|outbox_oldest_pending_seconds|relay_consecutive_failures)'
```

Confirm RabbitMQ is healthy, pending rows drain, the oldest-pending gauge
returns to zero, and relay consecutive failures return to zero. If PostgreSQL
was stopped, the recovery scenario specifically verifies Ingestion recovers
without a process restart. If recovery does not happen, preserve the logs and
summary first; do not hide the failure by restarting Ingestion.

## Known limits

- Authentication uses static shared API keys. It is intentionally minimal,
  not a production authentication design; see the [API documentation](../../README.md#ingestion).
- The Compose deployment runs one Ingestion service instance. Multi-relay
  throughput and global event ordering are not established by the current
  tests.
- Raw JUnit XML is stored in PostgreSQL (`ingestion.runs.raw_xml`), so database
  storage and backup policy apply to report retention.
- The outbox is at-least-once. Consumers must deduplicate or otherwise make
  effects idempotent using the stable event identifier.
- Load results are from one laptop and are not production capacity figures.
  The saved matrix reports small-report p95 of 10.32608574969345 ms at
  concurrency 1, 32.08916665080324 ms at concurrency 5,
  156.82045060257224 ms at concurrency 20, and 1396.2393774014345 ms at
  concurrency 50. The large-report p95 at concurrency 20 was
  1172.43011904975 ms. See the
  [saved load results](../evidence/ingestion-load/SUMMARY.md) and linked raw
  JSON outputs.
