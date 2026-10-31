# Event contracts

Ingestion owns the `RunReceived` event schema in
[`events/run_received.schema.json`](./events/run_received.schema.json).

Parser owns the `FailuresExtracted` event schema in
[`events/failures_extracted.schema.json`](./events/failures_extracted.schema.json).

Consumers must ignore unknown fields in the event payload so producers can add
compatible data without breaking existing consumers.

A schema change requires approval from the producing service and every
consumer. The sample envelopes in
[`events/samples/`](./events/samples/) are the contract fixtures services
should validate in their own tests.
