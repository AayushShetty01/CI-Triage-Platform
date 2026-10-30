# Ingestion load test results

Measured outputs with raw per-request statuses, latencies, run IDs, machine facts, metrics, and sampled container stats are saved as the linked JSON files. Markdown per-run summaries are alongside them.

Single-laptop measurements only; not production capacity figures.

The raw output records base commit `0fc7bb71cf39db7934fb333c111bda8ef994d6de`; Task 7's tool and other working-tree changes were not committed during measurement, so this is not a clean-tree fingerprint ([run output](small-concurrency-1.json)). The end-of-run outbox gauges cover the entire database, not just requests from that load run; the raw outputs do not record a pre-measurement baseline.

The captured machine facts were OS `macOS-27.2-arm64-arm-64bit-Mach-O`, CPU `Apple A18 Pro`, 6 logical cores, 8589934592 bytes RAM, and Docker `29.7.2` ([run output](small-concurrency-1.json)).

| Run (raw output) | Requests | HTTP 202 | Non-202 counts | Throughput (req/s) | p50 / p95 / p99 / max latency (ms; 202 only) | Pending / oldest age (seconds) | Recovery to pending 0 (seconds) | Max sampled CPU (Ingestion / Postgres) |
|---|---:|---|---|---:|---|---:|---:|---|
| [small / 1](small-concurrency-1.json) | 500 | 500/500 (100.0%) | 0 | 216.85578840692438 | 3.269312499469379 / 10.32608574969345 / 23.77961874793982 / 26.168583000981016 | 451.0 / 2.017547 | — | Ingestion 46.41% / Postgres 12.86% |
| [small / 5](small-concurrency-5.json) | 500 | 500/500 (100.0%) | 0 | 325.5875601296298 | 11.249125498579815 / 32.08916665080324 / 39.92056216786294 / 44.527000001835404 | 902.0 / 5.180986 | — | Ingestion 55.28% / Postgres 22.29% |
| [small / 20](small-concurrency-20.json) | 500 | 500/500 (100.0%) | 0 | 257.36928140043506 | 65.74579199877917 / 156.82045060257224 / 191.47923757860545 / 256.01620900124544 | 1303.0 / 8.562697 | — | Ingestion 65.08% / Postgres 13.14% |
| [small / 50](small-concurrency-50.json) | 500 | 500/500 (100.0%) | 0 | 99.62569462897177 | 312.9918334998365 / 1396.2393774014345 / 2071.106911911338 / 2835.7997499988414 | 1704.0 / 14.276339 | — | Ingestion 35.98% / Postgres 8.35% |
| [small / 20 repeat 1](small-concurrency-20-repeat-1.json) | 500 | 500/500 (100.0%) | 0 | 261.00637795192944 | 65.11225000031118 / 147.14098569893397 / 205.97918966715952 / 232.35908300193842 | 2105.0 / 17.377801 | — | Ingestion 59.36% / Postgres 18.97% |
| [small / 20 repeat 2](small-concurrency-20-repeat-2.json) | 500 | 500/500 (100.0%) | 0 | 258.60968764104365 | 62.214270499680424 / 149.17493750053836 / 238.51564733304258 / 258.6648750002496 | 2556.0 / 20.607035 | — | Ingestion 61.78% / Postgres 21.02% |
| [small / 20 repeat 3](small-concurrency-20-repeat-3.json) | 500 | 500/500 (100.0%) | 0 | 196.98265576629169 | 92.27472900238354 / 169.5138000019142 / 248.58923999865507 / 284.7946670008241 | 3007.0 / 25.455557 | — | Ingestion 57.25% / Postgres 30.87% |
| [large / 5](large-concurrency-5.json) | 100 | 100/100 (100.0%) | 0 | 18.96863440907486 | 236.26912500003527 / 370.9006874978513 / 572.3935777407315 / 590.1634580004611 | 3008.0 / 30.780453 | — | Ingestion 74.63% / Postgres 21.28% |
| [large / 20](large-concurrency-20.json) | 100 | 100/100 (100.0%) | 0 | 21.245872481763108 | 865.8379794997018 / 1172.43011904975 / 1221.6805228309022 / 1223.8813749972905 | 3009.0 / 37.884927 | — | Ingestion 78.65% / Postgres 29.09% |
| [small / 20 broker down](small-broker-down-concurrency-20.json) | 500 | 500/500 (100.0%) | 0 | 250.15198822028574 | 55.67410399999062 / 209.54104199954634 / 281.3502914192577 / 305.98675000146613 | 0.0 / 0.0 | 131.49347874999876 | Ingestion 48.08% / Postgres 20.55% |

## Observations

These measurements are from a single laptop and are not production capacity figures. Each statement below is derived from and linked to saved JSON output.

- For small reports, measured p95 was 10.32608574969345 ms at concurrency 1, 32.08916665080324 ms at concurrency 5, 156.82045060257224 ms at concurrency 20, and 1396.2393774014345 ms at concurrency 50. The first tested concurrency increase over concurrency 1 was concurrency 5. Sources: [concurrency 1](small-concurrency-1.json), [concurrency 5](small-concurrency-5.json), [concurrency 20](small-concurrency-20.json), [concurrency 50](small-concurrency-50.json).
- The three repeat concurrency-20 p95 samples were 147.14098569893397 ms, 149.17493750053836 ms, and 169.5138000019142 ms. Sources: [small-concurrency-20-repeat-1.json](small-concurrency-20-repeat-1.json), [small-concurrency-20-repeat-2.json](small-concurrency-20-repeat-2.json), [small-concurrency-20-repeat-3.json](small-concurrency-20-repeat-3.json).
- All measured requests were HTTP 202. Large-report p95 was 370.9006874978513 ms at concurrency 5 and 1172.43011904975 ms at concurrency 20. Sources: [large-concurrency-5.json](large-concurrency-5.json), [large-concurrency-20.json](large-concurrency-20.json).
- The largest sampled container CPU value for Ingestion was 78.65% in [large-concurrency-20.json](large-concurrency-20.json); for Postgres it was 30.87% in [small-concurrency-20-repeat-3.json](small-concurrency-20-repeat-3.json). Ingestion had the larger sampled maximum in the saved runs; these are intermittent Docker stats samples, not continuous profiling.
