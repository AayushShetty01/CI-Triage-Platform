# Ingestion Failure-Test Mutation Checks

The mutations below were temporary and were reverted after each run. Each
restoration was checked with `git diff --exit-code` for the mutated production
file.

| Mutation | File changed | Scenario run | Failed? | Assertion that caught it |
|---|---|---|---|---|
| A: skip the outbox insert while returning 202 | `services/ingestion/app/main.py` | `test_broker_down` | Yes | `pending outbox increased by 20`: expected 20, observed 0 |
| B, initial attempt: commit `published_at` before each publish call | `services/ingestion/app/relay.py` | `test_crash_with_unpublished_rows`, then `test_broker_flapping` | No | The broker-down crash scenario could not enter the batch-publish function; with the broker up, the fast publish batch did not expose a loss in the original checks. |
| B, after strengthening the test: same pre-confirm marking mutation | `services/ingestion/app/relay.py` | `test_broker_flapping` | Yes | `every acknowledged run remains pending or queued until confirmed`: observed acknowledged runs with neither a pending row nor a corresponding queued message. |
| C: return 202 before calling the storage transaction | `services/ingestion/app/main.py` | `test_kill_during_uploads` | Yes | `every run acknowledged with 202 is stored`: the acknowledged run ID was absent from the database after recovery. |

The initial B attempt exposed a coverage gap, so `test_broker_flapping` now
continuously checks that acknowledged runs remain represented by a pending
outbox row or a queued message. The strengthened assertion caught the same
pre-confirm marking mutation during the real broker-flapping run.
