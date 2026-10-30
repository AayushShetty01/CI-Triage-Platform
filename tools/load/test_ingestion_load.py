import pytest

from ingestion_load import percentile, summarize_requests


def test_percentile_interpolates_known_values() -> None:
    values = [10.0, 20.0, 30.0, 40.0]

    assert percentile(values, 0) == 10.0
    assert percentile(values, 50) == 25.0
    assert percentile(values, 95) == 38.5
    assert percentile(values, 100) == 40.0
    assert percentile([], 50) is None


def test_percentile_rejects_out_of_range_value() -> None:
    with pytest.raises(ValueError, match="between 0 and 100"):
        percentile([1.0], 101)


def test_summary_counts_statuses_and_summarizes_only_202_latencies() -> None:
    results = [
        {"status": 202, "latency_ms": 10.0, "run_id": "run-one"},
        {"status": 202, "latency_ms": 30.0, "run_id": "run-two"},
        {"status": 503, "latency_ms": 50.0, "run_id": None},
        {"status": "connection_error", "latency_ms": 60.0, "run_id": None},
    ]

    summary = summarize_requests(results, elapsed_seconds=2.0)

    assert summary["requests"] == 4
    assert summary["success_202"] == 2
    assert summary["success_rate"] == 0.5
    assert summary["non_202_counts"] == {"503": 1, "connection_error": 1}
    assert summary["throughput_requests_per_second"] == 2.0
    assert summary["latency_202_ms"] == {
        "p50": 20.0,
        "p95": 29.0,
        "p99": 29.8,
        "max": 30.0,
        "mean": 20.0,
    }
