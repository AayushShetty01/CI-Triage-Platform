# ADR-003: Transactional outbox for Ingestion events

- **Status:** Accepted
- **Owner:** Aayush

## Context

Ingestion accepts JUnit XML, stores the report and its `RunReceived` event in
PostgreSQL, and asynchronously publishes the event to RabbitMQ. The report row
and outbox row are written in one database transaction; see
[`main.py`](../../services/ingestion/app/main.py) and the schema in
[`database.py`](../../services/ingestion/app/database.py). The relay publishes
persistent messages with publisher confirms before setting `published_at`; see
[`relay.py`](../../services/ingestion/app/relay.py).

The stated constraint is **zero cost, runs on a laptop**. The project already
uses PostgreSQL and RabbitMQ in Docker Compose, so the design must use those
services without requiring a paid broker, hosted database, or separate
stream-processing platform.

## Decision

Use the PostgreSQL transactional outbox. The API transaction stores both the
run and its event. A background relay reads unpublished rows, publishes to the
durable `triage.events` exchange with publisher confirms, and then marks the
row published. If RabbitMQ is unavailable, the rows remain pending and the
relay retries with backoff.

The failure scenarios exercise broker and database outages, process crashes,
in-flight uploads, concurrent duplicates, and broker flapping. The saved
[example failure evidence](../../services/ingestion/failure_simulation/evidence/example/)
and [mutation results](../../services/ingestion/failure_simulation/MUTATION_CHECK.md)
record the durability checks, including tests that fail when the outbox insert
or transaction ordering is removed.

## Alternatives considered

| Alternative | Assessment against “zero cost, runs on a laptop” |
|---|---|
| Dual write (commit the run, then publish directly) | Requires no extra platform, but leaves a failure window between the database commit and publish. A successful upload could have no event. The transactional outbox closes that gap using the existing database. |
| Publish, then write the run | Requires no extra platform, but a database failure after publish can leave consumers with an event for a run that does not exist. Reversing operation order does not make the two systems atomic. |
| Change data capture / logical replication | Can provide a database change stream, but adds replication configuration and a connector or consumer to operate locally. That is more infrastructure than the current laptop-scale stack needs. |
| Skip the queue and call Parser synchronously | Avoids a broker, but couples upload acceptance to Parser availability and request latency. It also removes the asynchronous recovery boundary. |

## Delivery semantics and consequences

Delivery is **at least once**, not exactly once. If RabbitMQ confirms a message
and the relay stops before the database marks the row published, the relay can
send that event again. The event has a stable `event_id`, and consumers must be
idempotent so retries do not duplicate downstream effects. The relay's
publish-confirm/update order is visible in
[`relay.py`](../../services/ingestion/app/relay.py); failure and mutation cases
are recorded in the
[failure evidence](../../services/ingestion/failure_simulation/evidence/example/)
and [mutation check](../../services/ingestion/failure_simulation/MUTATION_CHECK.md).

This design keeps upload acceptance independent of RabbitMQ availability, at
the cost of retaining unpublished rows and operating a relay. PostgreSQL and
RabbitMQ remain local Compose dependencies; no paid service is required.

## What would change our mind

- A database already selected for the project provides a durable built-in
  change stream that can replace the relay without adding paid or separately
  operated infrastructure.
- The system requires exactly-once ordering across services. The current
  at-least-once contract and idempotent-consumer requirement would not satisfy
  that requirement.
- Measurements from representative production workloads show that polling the
  outbox cannot meet an agreed delivery-latency or throughput target.
