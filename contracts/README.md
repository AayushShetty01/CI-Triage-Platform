# Event contracts

Ingestion owns the `RunReceived` event schema in
[`events/run_received.schema.json`](./events/run_received.schema.json).

Parser owns the `FailuresExtracted` event schema in
[`events/failures_extracted.schema.json`](./events/failures_extracted.schema.json).

Classifier owns the `FailureClassified` event schema in
[`events/failure_classified.schema.json`](./events/failure_classified.schema.json).
It is published once per failed test case (routing key `failure.classified`)
when that failure reaches a final state: `classified` (by a rule or the LLM) or
`degraded` (LLM unavailable; category `unknown`). A failure listed in
`FailuresExtracted` with no `FailureClassified` yet is still pending.

Consumers must ignore unknown fields in the event payload so producers can add
compatible data without breaking existing consumers.

A schema change requires approval from the producing service and every
consumer. The sample envelopes in
[`events/samples/`](./events/samples/) are the contract fixtures services
should validate in their own tests.
