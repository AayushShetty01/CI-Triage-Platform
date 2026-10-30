from __future__ import annotations

import asyncio
import time
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
    run_id_for_submission,
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
    first_age = metric_value(
        ingestion.metrics(),
        "outbox_oldest_pending_seconds",
    )
    time.sleep(2)
    second_age = metric_value(
        ingestion.metrics(),
        "outbox_oldest_pending_seconds",
    )
    evidence.measurements["oldest_pending_seconds_samples"] = [first_age, second_age]
    evidence.check("oldest pending age is positive", first_age > 0, "> 0", first_age)
    evidence.check(
        "oldest pending age increases while broker is down",
        second_age > first_age,
        f"> {first_age}",
        second_age,
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
    recovered_age = wait_until(
        lambda: metric_value(
            ingestion.metrics(),
            "outbox_oldest_pending_seconds",
        ),
        timeout=30,
        interval=1,
        description="oldest pending age metric to reset after recovery",
        condition=lambda age: age == 0,
    )
    evidence.measurements["oldest_pending_seconds_after_recovery"] = recovered_age
    _assert(evidence, "oldest pending age after recovery", 0, recovered_age)
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


def test_crash_with_unpublished_rows(failure_stack: dict[str, Any]) -> None:
    compose: ComposeHarness = failure_stack["compose"]
    database: DatabaseHelper = failure_stack["database"]
    evidence: ScenarioEvidence = failure_stack["evidence"]
    ingestion: IngestionClient = failure_stack["ingestion"]
    rabbitmq: RabbitMQHelper = failure_stack["rabbitmq"]
    reports = [
        (synthetic_junit_report(501 + index), f"crash-before-publish-{uuid.uuid4().hex}")
        for index in range(10)
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
            "acknowledged_run_ids": run_ids,
            "acknowledged_count": len(run_ids),
        }
    )
    _assert(evidence, "all uploads accepted before crash", 10, statuses.count(202))
    _assert(evidence, "accepted run IDs are distinct", 10, len(set(run_ids)))

    outage_state = wait_until(
        lambda: {
            "pending": database.counts()["pending"],
            "relay_failures": metric_value(
                ingestion.metrics(), "relay_consecutive_failures"
            ),
        },
        timeout=45,
        interval=1,
        description="relay to fail against RabbitMQ while retaining all pending rows",
        condition=lambda state: state["relay_failures"] > 0,
    )
    evidence.measurements["before_crash"] = outage_state
    _assert(evidence, "ten outbox rows remain pending before crash", 10, outage_state["pending"])
    stored_run_ids = database.stored_run_ids(set(run_ids))
    evidence.measurements["stored_count"] = len(stored_run_ids)
    evidence.check(
        "all acknowledged runs remain stored before crash",
        set(run_ids).issubset(stored_run_ids),
        set(run_ids),
        stored_run_ids,
    )

    compose.kill("ingestion")
    compose.start("rabbitmq")
    compose.wait_healthy("rabbitmq")
    rabbitmq.ensure_queue()
    compose.start("ingestion")
    compose.wait_healthy("ingestion")
    recovery_started = time.monotonic()
    pending = wait_until(
        lambda: database.counts()["pending"],
        timeout=120,
        interval=1,
        description="crash-leftover outbox rows to publish after restart",
        condition=lambda count: count == 0,
    )
    recovery_seconds = time.monotonic() - recovery_started
    evidence.measurements["recovery_to_pending_zero_seconds"] = recovery_seconds
    _assert(evidence, "pending rows after recovery", 0, pending)

    wait_until(
        rabbitmq.count_messages,
        timeout=30,
        interval=1,
        description="ten crash-recovery messages to arrive",
        condition=lambda count: count == 10,
    )
    messages = rabbitmq.drain_messages()
    evidence.drained_messages = messages
    message_run_ids = [message["run_id"] for message in messages]
    _assert(evidence, "published message count", 10, len(messages))
    _assert(evidence, "published run IDs are distinct", 10, len(set(message_run_ids)))
    _assert(evidence, "published IDs match acknowledged IDs", set(run_ids), set(message_run_ids))

    recovered_raw_reports = {
        run_id: ingestion.get_raw_report(run_id)
        for run_id in run_ids
    }
    original_by_run_id = {
        response.json()["run_id"]: report
        for (report, _), response in zip(reports, responses)
    }
    _assert(
        evidence,
        "all acknowledged raw reports match uploaded bytes",
        original_by_run_id,
        recovered_raw_reports,
    )
    evidence.measurements.update(
        {
            "published_count": len(messages),
            "duplicate_count": len(messages) - len(set(message_run_ids)),
            "duplicated_run_ids": [],
        }
    )


class _HeldRequestBody(httpx.AsyncByteStream):
    def __init__(
        self,
        body: bytes,
        started: asyncio.Event,
        release: asyncio.Event,
    ) -> None:
        self.body = body
        self.started = started
        self.release = release

    async def __aiter__(self):
        midpoint = len(self.body) // 2
        yield self.body[:midpoint]
        self.started.set()
        await self.release.wait()
        yield self.body[midpoint:]


def test_kill_during_uploads(failure_stack: dict[str, Any]) -> None:
    compose: ComposeHarness = failure_stack["compose"]
    database: DatabaseHelper = failure_stack["database"]
    evidence: ScenarioEvidence = failure_stack["evidence"]
    ingestion: IngestionClient = failure_stack["ingestion"]
    rabbitmq: RabbitMQHelper = failure_stack["rabbitmq"]
    request_count = 50
    concurrency = 5
    submissions = [
        (
            synthetic_junit_report(601 + index),
            f"kill-during-upload-{uuid.uuid4().hex}",
        )
        for index in range(request_count)
    ]

    async def upload_and_kill() -> tuple[list[dict[str, Any]], float, float, int, int]:
        release_upload_bodies = asyncio.Event()
        held_started = [asyncio.Event() for _ in range(concurrency - 1)]
        outcomes: list[dict[str, Any]] = []
        kill_requested = False

        async with httpx.AsyncClient(base_url=ingestion.base_url, timeout=30) as client:

            async def post_one(index: int, stream: httpx.AsyncByteStream | None = None) -> dict[str, Any]:
                report, commit_sha = submissions[index]
                try:
                    if stream is None:
                        response = await ingestion.post_report_async(client, report, commit_sha)
                    else:
                        response = await client.post(
                            "/runs",
                            content=stream,
                            headers={
                                **ingestion.headers,
                                "Content-Type": "application/xml",
                                "X-Commit-Sha": commit_sha,
                                "X-Branch": "failure-test",
                            },
                        )
                    return {
                        "index": index,
                        "commit_sha": commit_sha,
                        "run_id": (
                            response.json().get("run_id")
                            if response.status_code == 202
                            else run_id_for_submission(report, commit_sha)
                        ),
                        "status": response.status_code,
                        "connection_error": False,
                        "error": None,
                        "before_kill": not kill_requested,
                    }
                except httpx.HTTPError as exc:
                    return {
                        "index": index,
                        "commit_sha": commit_sha,
                        "run_id": run_id_for_submission(report, commit_sha),
                        "status": None,
                        "connection_error": True,
                        "error": f"{type(exc).__name__}: {exc}",
                        "before_kill": not kill_requested,
                    }

            initial_limit = asyncio.Semaphore(concurrency)

            async def upload_initial(index: int) -> dict[str, Any]:
                async with initial_limit:
                    result = await post_one(index)
                    result["before_kill"] = True
                    return result

            initial_outcomes = await asyncio.gather(
                *(upload_initial(index) for index in range(20))
            )
            outcomes.extend(initial_outcomes)
            initial_acknowledged_count = sum(
                result["status"] == 202 for result in initial_outcomes
            )

            queue: asyncio.Queue[int] = asyncio.Queue()
            for index in range(20, request_count):
                queue.put_nowait(index)

            async def worker() -> None:
                while True:
                    try:
                        index = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    try:
                        if index < 20 + len(held_started):
                            stream = _HeldRequestBody(
                                submissions[index][0],
                                held_started[index - 20],
                                release_upload_bodies,
                            )
                            outcome = await post_one(index, stream)
                        else:
                            outcome = await post_one(index)
                        outcomes.append(outcome)
                    finally:
                        queue.task_done()

            workers = [
                asyncio.create_task(worker())
                for _ in range(concurrency)
            ]
            await asyncio.wait_for(
                asyncio.gather(*(event.wait() for event in held_started)),
                timeout=60,
            )
            held_upload_count = sum(event.is_set() for event in held_started)
            kill_requested = True
            kill_started = time.monotonic()
            await asyncio.to_thread(compose.kill, "ingestion")
            kill_completed = time.monotonic()
            release_upload_bodies.set()
            await asyncio.gather(*workers)
            return (
                outcomes,
                kill_started,
                kill_completed,
                held_upload_count,
                initial_acknowledged_count,
            )

    (
        outcomes,
        kill_started,
        kill_completed,
        held_upload_count,
        initial_acknowledged_count,
    ) = asyncio.run(upload_and_kill())
    outcomes.sort(key=lambda result: result["index"])
    acknowledged = [
        outcome for outcome in outcomes if outcome["status"] == 202
    ]
    connection_errors = [
        outcome for outcome in outcomes if outcome["connection_error"]
    ]
    unexpected_responses = [
        {"index": outcome["index"], "status": outcome["status"]}
        for outcome in outcomes
        if not outcome["connection_error"] and outcome["status"] != 202
    ]
    accepted_before_kill = [
        outcome for outcome in acknowledged if outcome["before_kill"]
    ]
    evidence.measurements.update(
        {
            "request_count": request_count,
            "concurrency": concurrency,
            "initial_20_acknowledged_count": initial_acknowledged_count,
            "acknowledged_count": len(acknowledged),
            "acknowledged_run_ids": [outcome["run_id"] for outcome in acknowledged],
            "acknowledged_before_kill_count": len(accepted_before_kill),
            "connection_error_count": len(connection_errors),
            "http_statuses": [outcome["status"] for outcome in outcomes],
            "connection_errors": [
                {"index": outcome["index"], "error": outcome["error"]}
                for outcome in connection_errors
            ],
            "kill_requested_at_monotonic": kill_started,
            "kill_completed_at_monotonic": kill_completed,
            "uploads_active_at_kill": held_upload_count,
        }
    )
    _assert(evidence, "all 50 uploads were attempted", request_count, len(outcomes))
    _assert(
        evidence,
        "uploads returned 202 or lost their connection during the kill",
        [],
        unexpected_responses,
    )
    _assert(
        evidence,
        "first 20 uploads acknowledged before kill",
        20,
        initial_acknowledged_count,
    )
    evidence.check(
        "at least 20 uploads received 202 before the kill",
        len(accepted_before_kill) >= 20,
        ">= 20",
        len(accepted_before_kill),
    )
    _assert(
        evidence,
        "four uploads remain in flight when Ingestion is killed",
        concurrency - 1,
        held_upload_count,
    )

    compose.start("ingestion")
    compose.wait_healthy("ingestion")
    recovery_started = time.monotonic()
    acknowledged_run_ids = {outcome["run_id"] for outcome in acknowledged}
    attempted_run_ids = {outcome["run_id"] for outcome in outcomes}
    stored_run_ids = database.stored_run_ids(attempted_run_ids)
    evidence.measurements["stored_count"] = len(stored_run_ids)
    stored_connection_errors = {
        outcome["run_id"]
        for outcome in connection_errors
        if outcome["run_id"] in stored_run_ids
    }
    evidence.measurements.update(
        {
            "connection_error_stored_count": len(stored_connection_errors),
            "connection_error_not_stored_count": len(connection_errors)
            - len(stored_connection_errors),
        }
    )
    evidence.check(
        "every run acknowledged with 202 is stored",
        acknowledged_run_ids.issubset(stored_run_ids),
        acknowledged_run_ids,
        stored_run_ids,
    )

    pending = wait_until(
        lambda: database.counts()["pending"],
        timeout=120,
        interval=1,
        description="accepted outbox rows to publish after Ingestion recovery",
        condition=lambda count: count == 0,
    )
    evidence.measurements["recovery_to_pending_zero_seconds"] = (
        time.monotonic() - recovery_started
    )
    _assert(evidence, "pending rows after Ingestion recovery", 0, pending)
    wait_until(
        rabbitmq.count_messages,
        timeout=60,
        interval=1,
        description="acknowledged events to arrive after Ingestion recovery",
        condition=lambda count: count >= len(acknowledged_run_ids),
    )
    messages = rabbitmq.drain_messages()
    evidence.drained_messages = messages
    published_run_ids = {message["run_id"] for message in messages}
    evidence.check(
        "every acknowledged run is published",
        acknowledged_run_ids.issubset(published_run_ids),
        acknowledged_run_ids,
        published_run_ids,
    )
    evidence.check(
        "all published runs were among attempted uploads",
        published_run_ids.issubset(attempted_run_ids),
        attempted_run_ids,
        published_run_ids,
    )
    raw_match_count = sum(
        ingestion.get_raw_report(outcome["run_id"])
        == submissions[outcome["index"]][0]
        for outcome in acknowledged
    )
    _assert(
        evidence,
        "all acknowledged raw reports match uploaded bytes",
        len(acknowledged),
        raw_match_count,
    )
    evidence.measurements.update(
        {
            "published_count": len(messages),
            "published_run_id_count": len(published_run_ids),
            "duplicate_count": len(messages) - len(published_run_ids),
            "duplicated_run_ids": sorted(
                run_id
                for run_id in published_run_ids
                if sum(message["run_id"] == run_id for message in messages) > 1
            ),
        }
    )


def test_broker_flapping(failure_stack: dict[str, Any]) -> None:
    compose: ComposeHarness = failure_stack["compose"]
    database: DatabaseHelper = failure_stack["database"]
    evidence: ScenarioEvidence = failure_stack["evidence"]
    ingestion: IngestionClient = failure_stack["ingestion"]
    rabbitmq: RabbitMQHelper = failure_stack["rabbitmq"]
    request_count = 60
    submissions = [
        (
            synthetic_junit_report(701 + index),
            f"broker-flapping-{uuid.uuid4().hex}",
        )
        for index in range(request_count)
    ]

    async def upload_while_flapping() -> tuple[
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[dict[str, Any]],
        float,
    ]:
        flapping_started = asyncio.Event()
        uploads_finished = asyncio.Event()
        final_recovery_ready = asyncio.Event()
        final_recovery_monotonic: list[float] = []
        acknowledged_run_ids: set[str] = set()
        delivery_invariant_violations: list[dict[str, Any]] = []

        async def flap_broker() -> list[dict[str, Any]]:
            recoveries: list[dict[str, Any]] = []
            flapping_started.set()
            for attempt in range(4):
                queued_before = await asyncio.to_thread(rabbitmq.count_messages)
                stop_started = time.monotonic()
                await asyncio.to_thread(compose.stop_rabbitmq_app)
                await asyncio.to_thread(rabbitmq.wait_unavailable)
                outage_started = time.monotonic()
                await asyncio.sleep(3)
                await asyncio.to_thread(compose.start_rabbitmq_app)
                await asyncio.to_thread(rabbitmq.wait_available)
                outage_seconds = time.monotonic() - outage_started
                await asyncio.to_thread(rabbitmq.ensure_queue)
                queued_after = await asyncio.to_thread(rabbitmq.count_messages)
                recovered_at = time.monotonic()
                recoveries.append(
                    {
                        "attempt": attempt + 1,
                        "outage_seconds": outage_seconds,
                        "stop_command_seconds": outage_started - stop_started,
                        "queued_before": queued_before,
                        "queued_after": queued_after,
                    }
                )
                if attempt == 3:
                    final_recovery_monotonic.append(recovered_at)
                    final_recovery_ready.set()
                if attempt < 3:
                    await asyncio.sleep(3)
            return recoveries

        async def upload_reports() -> list[dict[str, Any]]:
            await flapping_started.wait()
            results: list[dict[str, Any]] = []
            async with httpx.AsyncClient(base_url=ingestion.base_url, timeout=30) as client:
                for index, (report, commit_sha) in enumerate(submissions):
                    try:
                        response = await ingestion.post_report_async(client, report, commit_sha)
                        results.append(
                            {
                                "index": index,
                                "commit_sha": commit_sha,
                                "run_id": (
                                    response.json().get("run_id")
                                    if response.status_code == 202
                                    else run_id_for_submission(report, commit_sha)
                                ),
                                "status": response.status_code,
                                "error": None,
                            }
                        )
                        if response.status_code == 202:
                            acknowledged_run_ids.add(response.json()["run_id"])
                    except httpx.HTTPError as exc:
                        results.append(
                            {
                                "index": index,
                                "commit_sha": commit_sha,
                                "run_id": run_id_for_submission(report, commit_sha),
                                "status": None,
                                "error": f"{type(exc).__name__}: {exc}",
                            }
                        )
                    await asyncio.sleep(0.45)
            uploads_finished.set()
            return results

        async def monitor_durability_invariant() -> None:
            while not uploads_finished.is_set():
                run_ids = set(acknowledged_run_ids)
                if run_ids:
                    queued = await asyncio.to_thread(rabbitmq.try_count_messages)
                    if queued is not None:
                        pending = await asyncio.to_thread(
                            database.pending_count_for_run_ids,
                            run_ids,
                        )
                        if pending + queued < len(run_ids):
                            delivery_invariant_violations.append(
                                {
                                    "acknowledged": len(run_ids),
                                    "pending": pending,
                                    "queued": queued,
                                }
                            )
                await asyncio.sleep(0.05)

        async def wait_for_final_drain() -> float:
            await final_recovery_ready.wait()
            await uploads_finished.wait()
            recovery_started = final_recovery_monotonic[0]
            await asyncio.to_thread(
                wait_until,
                lambda: database.counts()["pending"],
                timeout=180,
                interval=1,
                description="pending outbox to drain after final RabbitMQ recovery",
                condition=lambda count: count == 0,
            )
            return time.monotonic() - recovery_started

        flap_task = asyncio.create_task(flap_broker())
        uploads_task = asyncio.create_task(upload_reports())
        monitor_task = asyncio.create_task(monitor_durability_invariant())
        drain_task = asyncio.create_task(wait_for_final_drain())
        results, recoveries, _, recovery_to_drain_seconds = await asyncio.gather(
            uploads_task,
            flap_task,
            monitor_task,
            drain_task,
        )
        return results, recoveries, delivery_invariant_violations, recovery_to_drain_seconds

    results, recoveries, invariant_violations, recovery_to_drain_seconds = asyncio.run(
        upload_while_flapping()
    )
    accepted = [result for result in results if result["status"] == 202]
    accepted_run_ids = {result["run_id"] for result in accepted}
    evidence.measurements.update(
        {
            "request_count": request_count,
            "http_statuses": [result["status"] for result in results],
            "acknowledged_count": len(accepted),
            "acknowledged_run_ids": sorted(accepted_run_ids),
            "broker_recoveries": recoveries,
            "broker_outage_seconds": [item["outage_seconds"] for item in recoveries],
            "delivery_invariant_violations": invariant_violations,
            "recovery_to_pending_zero_seconds": recovery_to_drain_seconds,
        }
    )
    _assert(evidence, "all uploads accepted while broker flapped", request_count, len(accepted))
    _assert(evidence, "four broker outages completed", 4, len(recoveries))
    evidence.check(
        "queued messages were retained across each restart",
        all(item["queued_after"] >= item["queued_before"] for item in recoveries),
        "queue count does not shrink across broker restart",
        [
            {"before": item["queued_before"], "after": item["queued_after"]}
            for item in recoveries
        ],
    )
    _assert(
        evidence,
        "every acknowledged run remains pending or queued until confirmed",
        [],
        invariant_violations,
    )

    pending = database.counts()["pending"]
    _assert(evidence, "pending rows after final broker recovery", 0, pending)
    stored_run_ids = {
        result["run_id"]
        for result in results
        if result["status"] == 202
    }
    actual_stored_run_ids = database.stored_run_ids(stored_run_ids)
    evidence.measurements["stored_count"] = len(actual_stored_run_ids)
    evidence.check(
        "all acknowledged runs remain stored after broker flapping",
        stored_run_ids.issubset(actual_stored_run_ids),
        stored_run_ids,
        actual_stored_run_ids,
    )
    wait_until(
        rabbitmq.count_messages,
        timeout=60,
        interval=1,
        description="flapping test queue to contain all acknowledged events",
        condition=lambda count: count >= len(stored_run_ids),
    )
    messages = rabbitmq.drain_messages()
    evidence.drained_messages = messages
    message_run_ids = [message["run_id"] for message in messages]
    published_run_ids = set(message_run_ids)
    _assert(evidence, "published run IDs match acknowledged IDs", stored_run_ids, published_run_ids)
    duplicate_count = len(message_run_ids) - len(published_run_ids)
    grouped_messages: dict[str, list[dict[str, Any]]] = {}
    for message in messages:
        grouped_messages.setdefault(message["run_id"], []).append(message)
    duplicates_are_identical = all(
        all(message["payload"] == copies[0]["payload"] for message in copies[1:])
        for copies in grouped_messages.values()
    )
    evidence.check(
        "duplicate event payloads are identical",
        duplicates_are_identical,
        "identical payload for each repeated run_id",
        {
            run_id: [message["payload"] for message in copies]
            for run_id, copies in grouped_messages.items()
            if len(copies) > 1
        },
    )
    evidence.measurements.update(
        {
            "published_count": len(messages),
            "duplicate_count": duplicate_count,
            "duplicated_run_ids": sorted(
                run_id
                for run_id, copies in grouped_messages.items()
                if len(copies) > 1
            ),
        }
    )
