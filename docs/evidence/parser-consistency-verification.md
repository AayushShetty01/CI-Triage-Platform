# Parser Failure Simulation Consistency Verification

- **Date:** 2026-10-31 12:32:13 UTC
- **Report Source:** `artifacts/failure-simulation-junit.xml` (from `services/ingestion/failure_simulation/test_ingestion_failures.py`)
- **Total Iterations Tested:** 3 consecutive runs + 1 duplicate idempotency re-submission
- **Result:** **PASSED (100% Consistent)**

## Consistency Matrix

| Metric | Iteration 1 | Iteration 2 | Iteration 3 | Consistent? |
|---|---|---|---|---|
| **Run ID** | `79b48d04131467ea...` | `8eb47f77a4b3b0b7...` | `9048b589d5d8fd58...` | Unique per commit |
| **Commit SHA** | `sim-commit-001-alpha` | `sim-commit-002-alpha` | `sim-commit-003-alpha` | Distinct |
| **Total Tests** | 15 | 15 | 15 | **YES (15)** |
| **Passed Tests** | 9 | 9 | 9 | **YES (9)** |
| **Failed Tests** | 6 | 6 | 6 | **YES (6)** |
| **Failures Extracted** | 6 | 6 | 6 | **YES (6)** |
| **Idempotency Test** | Validated | N/A | N/A | **Identical Run ID returned** |

## Extracted Failures (Consistently Extracted in All Runs)

| # | Test ID | Error Type | Error Message |
|---|---|---|---|
| 1 | `simulation.checkout.CheckoutTests::duplicate_checkout_returns_conflict` | `AssertionError` | Expected HTTP 409, received HTTP 201 |
| 2 | `simulation.checkout.CheckoutTests::expired_card_requires_payment` | `AssertionError` | Expected HTTP 402, received HTTP 200 |
| 3 | `simulation.checkout.CheckoutTests::payment_response_is_authorized` | `AssertionError` | Expected response status 'authorized', received 'pending' |
| 4 | `simulation.payment.PaymentTests::tax_amount_matches_invoice` | `AssertionError` | Expected tax_amount 0.8, received 0.0 |
| 5 | `simulation.payment.PaymentTests::timed_out_request_returns_response` | `AssertionError` | Expected HTTP 200, but the request timed out and returned no response |
| 6 | `simulation.payment.PersistenceTests::authorized_payment_is_persisted` | `AssertionError` | Expected authorized payment txn-missing to be stored, but no record was found |

## Verification Details

1. **Failure Simulation**: The mock payment application was executed to generate realistic failures across 3 test suites (`checkout-api`, `payment-api`, and `payment-persistence`).
2. **Ingestion Service**: Each run accepted the XML payload and replied `202 Accepted` within milliseconds, atomically staging the `RunReceived` outbox event.
3. **Event Transport & Parser**: RabbitMQ delivered each event to Parser. Parser fetched the raw XML report using `INTERNAL_API_KEY`, parsed all 15 tests, persisted them to PostgreSQL with unique constraint `(run_id, test_id)`, and staged the `FailuresExtracted` event.
4. **Idempotency**: Submitting an identical report with the same commit returned the existing `run_id` and did not produce duplicate test entries or duplicate outbox events.
