# Ingestion load run: small-broker-down-concurrency-20

- Report size: small
- Concurrency: 20
- Requests: 500
- Success rate (202): 100.0000%
- Non-202 counts: none: 0
- Throughput: 250.152 requests/s

| 202 latency | ms |
|---|---:|
| p50 | 55.67410399999062 |
| p95 | 209.54104199954634 |
| p99 | 281.3502914192577 |
| max | 305.98675000146613 |

- End `outbox_pending`: 0.0
- End `outbox_oldest_pending_seconds`: 0.0
- Broker recovery to pending zero: 131.493479 seconds
