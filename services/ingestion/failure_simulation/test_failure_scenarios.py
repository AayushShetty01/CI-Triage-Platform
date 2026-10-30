from __future__ import annotations

import asyncio
import uuid
from collections.abc import Iterator
from typing import Any

import httpx
import pytest

from failure_simulation.harness import (
    ComposeHarness,
    DatabaseHelper,
    IngestionClient,
    RabbitMQHelper,
    ScenarioEvidence,
    metric_value,
    synthetic_junit_report,
    wait_until,
)

pytestmark = pytest.mark.failure


@pytest.fixture
def failure_stack(request: pytest.FixtureRequest) -> Iterator[dict[str, Any]]:
    compose = ComposeHarness()
    compose.up()
    for service in ("postgres", "rabbitmq", "ingestion"):
        compose.wait_healthy(service)

    ingestion = IngestionClient()
    database = DatabaseHelper(compose.database_url())
    rabbitmq = RabbitMQHelper(compose.rabbitmq_url())
    rabbitmq.prepare()
    initial_counts = wait_until(
        database.counts,
        timeout=60,
        interval=1,
        description="initial outbox queue to drain",
        condition=lambda counts: counts["pending"] == 0,
    )
    scenario = request.node.name.removeprefix("test_")
    evidence = ScenarioEvidence(scenario, rabbitmq.queue_name)
    evidence.measurements["initial_database_counts"] = initial_counts
    stack = {
        "compose": compose,
        "database": database,
        "evidence": evidence,
        "ingestion": ingestion,
        "rabbitmq": rabbitmq,
    }

    yield stack

    cleanup_errors = []
    try:
        compose.up()
        for service in ("postgres", "rabbitmq", "ingestion"):
            compose.wait_healthy(service)
        wait_until(
            database.counts,
            timeout=90,
            interval=1,
            description="outbox to drain during stack recovery",
            condition=lambda counts: counts["pending"] == 0,
        )
    except Exception as exc:
        cleanup_errors.append(f"Stack recovery: {type(exc).__name__}: {exc}")

    try:
        evidence.drained_messages.extend(rabbitmq.drain_messages())
        evidence.measurements["drained_message_count"] = len(evidence.drained_messages)
    except Exception as exc:
        cleanup_errors.append(f"RabbitMQ message drain: {type(exc).__name__}: {exc}")

    try:
        evidence.measurements["final_database_counts"] = database.counts()
    except Exception as exc:
        cleanup_errors.append(f"Final database counts: {type(exc).__name__}: {exc}")

    evidence.measurements["cleanup_errors"] = cleanup_errors
    test_report = getattr(request.node, "rep_call", None)
    scenario_passed = bool(test_report and test_report.passed and not cleanup_errors)
    try:
        evidence.write(passed=scenario_passed, compose=compose)
    finally:
        ingestion.close()
        database.close()
    if cleanup_errors:
        pytest.fail("; ".join(cleanup_errors), pytrace=False)


def _assert(evidence: ScenarioEvidence, name: str, expected: Any, actual: Any) -> None:
    evidence.check(name, actual == expected, expected, actual)


def test_broker_down(failure_stack: dict[str, Any]) -> None:
    compose: ComposeHarness = failure_stack["compose"]
    database: DatabaseHelper = failure_stack["database"]
    evidence: ScenarioEvidence = failure_stack["evidence"]
    ingestion: IngestionClient = failure_stack["ingestion"]
    rabbitmq: RabbitMQHelper = failure_stack["rabbitmq"]
    baseline = database.counts()
    reports = [
        (synthetic_junit_report(index), f"broker-down-{uuid.uuid4().hex}")
        for index in range(20)
    ]

    compose.stop("rabbitmq")
    responses = [
        ingestion.post_report(report, commit_sha)
        for report, commit_sha in reports
    ]
    statuses = [response.status_code for response in responses]
    run_ids = [
        response.json()["run_id"]
        for response in responses
        if response.status_code == 202
    ]
    evidence.measurements.update(
        {
            "http_statuses": statuses,
            "accepted_run_ids": run_ids,
            "runs_after_upload": database.counts()["runs"],
        }
    )
    for index, status in enumerate(statuses):
        evidence.check(f"upload_{index + 1}_accepted", status == 202, 202, status)
    _assert(evidence, "accepted run IDs are distinct", 20, len(set(run_ids)))
    _assert(
        evidence,
        "runs increased by 20",
        baseline["runs"] + 20,
        database.counts()["runs"],
    )
    _assert(
        evidence,
        "pending outbox increased by 20",
        baseline["pending"] + 20,
        database.counts()["pending"],
    )

    metrics = wait_until(
        lambda: {
            "outbox_pending": metric_value(ingestion.metrics(), "outbox_pending"),
            "relay_consecutive_failures": metric_value(
                ingestion.metrics(), "relay_consecutive_failures"
            ),
        },
        timeout=60,
        interval=1,
        description="relay to report pending rows and broker failures",
        condition=lambda values: values["outbox_pending"] == 20
        and values["relay_consecutive_failures"] > 0,
    )
    evidence.measurements["metrics_while_broker_down"] = metrics
    _assert(evidence, "metrics pending gauge", 20, metrics["outbox_pending"])
    evidence.check(
        "relay failure gauge is positive",
        metrics["relay_consecutive_failures"] > 0,
        "> 0",
        metrics["relay_consecutive_failures"],
    )

    compose.start("rabbitmq")
    compose.wait_healthy("rabbitmq")
    pending_after_recovery = wait_until(
        lambda: database.counts()["pending"],
        timeout=120,
        interval=1,
        description="pending outbox to reach zero after RabbitMQ recovery",
        condition=lambda pending: pending == 0,
    )
    evidence.measurements["pending_after_recovery"] = pending_after_recovery
    wait_until(
        rabbitmq.count_messages,
        timeout=30,
        interval=1,
        description="test queue to contain 20 delivered events",
        condition=lambda count: count == 20,
    )
    messages = rabbitmq.drain_messages()
    evidence.drained_messages = messages
    message_run_ids = [message["run_id"] for message in messages]
    _assert(evidence, "delivered message count", 20, len(messages))
    _assert(
        evidence,
        "delivered run IDs are distinct",
        20,
        len(set(message_run_ids)),
    )
    _assert(evidence, "delivered IDs match accepted IDs", set(run_ids), set(message_run_ids))
    evidence.check(
        "every event includes raw_ref",
        all(
            message.get("payload", {}).get("raw_ref")
            == f"/internal/runs/{message['run_id']}/raw"
            for message in messages
        ),
        "raw_ref for each run",
        [message.get("payload", {}).get("raw_ref") for message in messages],
    )


def test_database_down(failure_stack: dict[str, Any]) -> None:
    compose: ComposeHarness = failure_stack["compose"]
    database: DatabaseHelper = failure_stack["database"]
    evidence: ScenarioEvidence = failure_stack["evidence"]
    ingestion: IngestionClient = failure_stack["ingestion"]
    report = synthetic_junit_report(301)
    commit_sha = f"database-down-{uuid.uuid4().hex}"
    baseline = database.counts()

    compose.stop("postgres")
    responses = [
        ingestion.post_report(report, commit_sha)
        for _ in range(4)
    ]
    statuses = [response.status_code for response in responses]
    retry_after_headers = [response.headers.get("retry-after") for response in responses]
    evidence.measurements.update(
        {
            "http_statuses_while_database_down": statuses,
            "retry_after_headers": retry_after_headers,
        }
    )
    for index, (status, retry_after) in enumerate(zip(statuses, retry_after_headers)):
        evidence.check(f"database-down request {index + 1} is not accepted", status != 202, "not 202", status)
        evidence.check(
            f"database-down request {index + 1} has Retry-After",
            status == 503 and retry_after is not None,
            "HTTP 503 with Retry-After",
            {"status": status, "retry_after": retry_after},
        )

    compose.start("postgres")
    compose.wait_healthy("postgres")
    wait_until(
        database.can_query,
        timeout=60,
        interval=1,
        description="PostgreSQL to accept queries after restart",
    )
    _assert(evidence, "ingestion health after PostgreSQL restart", 200, ingestion.health_status())

    accepted = ingestion.post_report(report, commit_sha)
    evidence.measurements["recovery_http_status"] = accepted.status_code
    _assert(evidence, "same report accepted after database recovery", 202, accepted.status_code)
    run_id = accepted.json()["run_id"]
    row_counts = wait_until(
        lambda: database.counts_for_run(run_id),
        timeout=30,
        interval=1,
        description="recovered report to have one run and one outbox row",
        condition=lambda counts: counts == {"runs": 1, "outbox": 1},
    )
    evidence.measurements["recovered_run_row_counts"] = row_counts
    evidence.measurements["baseline_database_counts"] = baseline


def test_concurrent_duplicates(failure_stack: dict[str, Any]) -> None:
    database: DatabaseHelper = failure_stack["database"]
    evidence: ScenarioEvidence = failure_stack["evidence"]
    ingestion: IngestionClient = failure_stack["ingestion"]
    rabbitmq: RabbitMQHelper = failure_stack["rabbitmq"]
    report = synthetic_junit_report(401)
    commit_sha = f"concurrent-duplicate-{uuid.uuid4().hex}"
    count = 20

    async def upload_together() -> list[httpx.Response]:
        start = asyncio.Event()
        async with httpx.AsyncClient(base_url=ingestion.base_url, timeout=30) as client:
            async def upload_one() -> httpx.Response:
                await start.wait()
                return await ingestion.post_report_async(client, report, commit_sha)

            tasks = [
                asyncio.create_task(upload_one())
                for _ in range(count)
            ]
            start.set()
            return await asyncio.gather(*tasks)

    responses = asyncio.run(upload_together())
    statuses = [response.status_code for response in responses]
    run_ids = [
        response.json()["run_id"]
        for response in responses
        if response.status_code == 202
    ]
    evidence.measurements.update(
        {
            "request_count": count,
            "http_statuses": statuses,
            "returned_run_ids": run_ids,
        }
    )
    _assert(evidence, "all concurrent uploads accepted", count, statuses.count(202))
    _assert(evidence, "all uploads return one run ID", 1, len(set(run_ids)))
    run_id = run_ids[0]
    row_counts = wait_until(
        lambda: database.counts_for_run(run_id),
        timeout=30,
        interval=1,
        description="one run and outbox row for concurrent duplicate uploads",
        condition=lambda counts: counts == {"runs": 1, "outbox": 1},
    )
    evidence.measurements["duplicate_row_counts"] = row_counts
    wait_until(
        rabbitmq.count_messages,
        timeout=60,
        interval=1,
        description="exactly one event delivered for concurrent duplicate uploads",
        condition=lambda messages: messages == 1,
    )
    messages = rabbitmq.drain_messages()
    evidence.drained_messages = messages
    _assert(evidence, "duplicate event count", 1, len(messages))
    _assert(evidence, "published event run ID", run_id, messages[0]["run_id"])
