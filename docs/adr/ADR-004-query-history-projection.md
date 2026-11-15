# ADR-004: Query owns the test history projection

- **Status:** Accepted for the group project
- **Owner:** Mu Sicheng

## Context

The UI must answer “which tests are potentially flaky?” using outcomes across CI runs. Parser owns raw test history. Query owns the dashboard and aggregate API. The system already uses RabbitMQ with at-least-once delivery, and must run locally at zero cost. A read-time call from Query to Parser would make every flaky request depend on Parser's availability and latency.

## Decision

Query consumes `FailuresExtracted` and fetches that run's complete results from Parser once, after Parser has committed them. It stores only the fields needed for insights in its own schema. Query also consumes `RunReceived` and `FailureClassified`, allowing the UI to show accepted, pending, classified and degraded states. The event ID is recorded in the same transaction as projection updates, and `(run_id, test_id)` is unique. Classifications arriving before `FailuresExtracted` are retained. Query never reads Parser's database directly.

A test is labelled **potentially flaky** when it has at least one passed run and one failed or error run within the selected time window and branch filter. This is a candidate signal; it does not prove nondeterminism, because environment or code changes may explain the difference.

## Alternative rejected

Query could call Parser's `/runs/{run_id}/results` or a new history endpoint on every dashboard read. It avoids a copied history table, but makes read availability and latency depend on Parser. It also forces Parser to serve aggregate queries outside its parsing responsibility. The chosen projection costs storage and eventual consistency; a transient Parser outage pauses consumption of `FailuresExtracted` until the read succeeds.

## Consequences and change trigger

The Query service can continue serving previously projected insights when Parser is down. During an outage, its queue and pending states can lag; metrics reveal that. Duplicate deliveries are harmless. Compose waits for Query to bind its queue before starting Ingestion, so new deployments do not accept reports before the queue exists. Events from a deployment that predates Query still require explicit backfill.

We would reconsider the projection if measured history storage or recovery lag exceeded the project's laptop budget, or if the course scenario demanded a strongly consistent flaky answer immediately after report acceptance. In that case, a Parser history API with a documented availability and latency budget would be the simpler trade-off.
