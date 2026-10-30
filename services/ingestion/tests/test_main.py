import asyncio
import json

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, func, select, update
from sqlalchemy.exc import SQLAlchemyError

from app.database import metadata, outbox, runs
from app.main import create_app
from app.relay import _retry_delay, publish_pending_batch, run_outbox_relay

JUNIT_XML = b"""<?xml version="1.0"?>
<testsuite name="sample">
  <testcase classname="example.Sample" name="passes" />
</testsuite>
"""


@pytest.fixture
def ingestion_client(tmp_path):
    database_url = f"sqlite:///{tmp_path / 'ingestion-test.db'}"
    engine = create_engine(
        database_url,
        connect_args={"check_same_thread": False},
        execution_options={"schema_translate_map": {"ingestion": None}},
    )
    metadata.create_all(engine)
    app = create_app(
        database_url=database_url,
        enable_relay=False,
        max_report_size_bytes=1024,
    )
    with TestClient(app) as client:
        yield client, app.state.engine
    engine.dispose()
    app.state.engine.dispose()


def _raw_upload(
    client: TestClient,
    body: bytes = JUNIT_XML,
    commit_sha: str = "0123456789abcdef",
    branch: str = "main",
):
    return client.post(
        "/runs",
        content=body,
        headers={
            "Content-Type": "application/xml",
            "X-Commit-Sha": commit_sha,
            "X-Branch": branch,
        },
    )


def test_health_and_metrics_endpoints(ingestion_client) -> None:
    client, _ = ingestion_client

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    metrics_response = client.get("/metrics")
    assert metrics_response.status_code == 200
    assert "ingestion_health_checks_total" in metrics_response.text
    assert "ingest_requests_total" in metrics_response.text
    assert "outbox_pending" in metrics_response.text
    assert "relay_consecutive_failures" in metrics_response.text


def test_post_returns_503_with_retry_after_when_database_is_unavailable(
    ingestion_client,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _ = ingestion_client

    def database_unavailable(*args, **kwargs):
        raise SQLAlchemyError("database unavailable")

    monkeypatch.setattr("app.main._store_run", database_unavailable)
    response = _raw_upload(client)

    assert response.status_code == 503
    assert response.status_code != 202
    assert response.headers["retry-after"] == str(client.app.state.retry_after_seconds)


def test_raw_report_returns_503_with_retry_after_when_database_is_unavailable(
    ingestion_client,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, engine = ingestion_client

    def database_unavailable():
        raise SQLAlchemyError("database unavailable")

    monkeypatch.setattr(engine, "connect", database_unavailable)
    response = client.get(f"/internal/runs/{'1' * 64}/raw")

    assert response.status_code == 503
    assert response.headers["retry-after"] == str(client.app.state.retry_after_seconds)


def test_retry_after_setting_can_be_configured_from_environment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("INGESTION_RETRY_AFTER_SECONDS", "9")
    app = create_app(
        database_url=f"sqlite:///{tmp_path / 'retry-after-setting.db'}",
        enable_relay=False,
    )
    try:
        assert app.state.retry_after_seconds == 9
    finally:
        app.state.engine.dispose()


def test_retry_delay_grows_to_cap_and_stays_within_full_jitter_bounds() -> None:
    bounds = []

    def use_upper_bound(lower: float, upper: float) -> float:
        bounds.append((lower, upper))
        return upper

    delays = [_retry_delay(2.0, 10.0, attempt, use_upper_bound) for attempt in range(5)]

    assert [upper for _, upper in bounds] == [2.0, 4.0, 8.0, 10.0, 10.0]
    assert delays == [upper for _, upper in bounds]
    assert all(lower <= delay <= upper for (lower, upper), delay in zip(bounds, delays))
    assert all(first <= second for first, second in zip(delays, delays[1:]))
    assert max(delays) <= 10.0


def test_retry_delay_can_select_lower_jitter_bound() -> None:
    assert _retry_delay(2.0, 30.0, 3, lambda lower, upper: lower) == 0.0


def test_raw_report_is_stored_with_outbox_event_in_same_submission(ingestion_client) -> None:
    client, engine = ingestion_client

    response = _raw_upload(client)

    assert response.status_code == 202
    run_id = response.json()["run_id"]
    assert len(run_id) == 64
    with engine.connect() as connection:
        run = connection.execute(
            select(runs).where(runs.c.run_id == run_id)
        ).mappings().one()
        event = connection.execute(select(outbox)).mappings().one()
    assert run["raw_xml"] == JUNIT_XML
    assert run["commit_sha"] == "0123456789abcdef"
    assert run["branch"] == "main"
    assert run["status"] == "received"
    assert event["event_type"] == "RunReceived"
    assert event["payload"]["run_id"] == run_id
    assert event["payload"]["payload"]["commit_sha"] == "0123456789abcdef"
    assert event["payload"]["payload"]["raw_ref"] == f"/internal/runs/{run_id}/raw"
    assert event["published_at"] is None


def test_duplicate_submission_returns_same_run_without_duplicate_rows(ingestion_client) -> None:
    client, engine = ingestion_client

    first = _raw_upload(client)
    second = _raw_upload(client)

    assert first.status_code == second.status_code == 202
    assert first.json()["run_id"] == second.json()["run_id"]
    with engine.connect() as connection:
        assert connection.execute(select(func.count()).select_from(runs)).scalar_one() == 1
        assert connection.execute(select(func.count()).select_from(outbox)).scalar_one() == 1


def test_same_report_for_different_commits_creates_separate_runs(ingestion_client) -> None:
    client, engine = ingestion_client

    first = _raw_upload(client, commit_sha="commit-one")
    second = _raw_upload(client, commit_sha="commit-two")

    assert first.status_code == second.status_code == 202
    assert first.json()["run_id"] != second.json()["run_id"]
    with engine.connect() as connection:
        assert connection.execute(select(func.count()).select_from(runs)).scalar_one() == 2
        assert connection.execute(select(func.count()).select_from(outbox)).scalar_one() == 2


def test_multiple_suites_and_failure_details_are_preserved(ingestion_client) -> None:
    client, _ = ingestion_client
    report = b"""<?xml version="1.0"?>
<testsuites name="ci-run" tests="2" failures="1">
  <testsuite name="unit" tests="1" failures="1">
    <testcase classname="payments.CheckoutTests" name="rejects_expired_card">
      <failure message="expected HTTP 402, got 200">AssertionError</failure>
    </testcase>
  </testsuite>
  <testsuite name="integration" tests="1">
    <testcase classname="payments.ApiTests" name="creates_invoice" time="0.42" />
  </testsuite>
</testsuites>
"""

    response = _raw_upload(client, body=report)

    assert response.status_code == 202
    raw_response = client.get(f"/internal/runs/{response.json()['run_id']}/raw")
    assert raw_response.status_code == 200
    assert raw_response.content == report


def test_multipart_report_submission(ingestion_client) -> None:
    client, _ = ingestion_client

    response = client.post(
        "/runs",
        data={"commit_sha": "abc123", "branch": "feature/test"},
        files={"file": ("junit.xml", JUNIT_XML, "application/xml")},
    )

    assert response.status_code == 202
    assert len(response.json()["run_id"]) == 64


def test_missing_metadata_is_rejected(ingestion_client) -> None:
    client, _ = ingestion_client

    response = client.post(
        "/runs",
        content=JUNIT_XML,
        headers={"Content-Type": "application/xml", "X-Commit-Sha": "abc123"},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "branch is required and must be at most 255 characters"


def test_missing_commit_sha_is_rejected(ingestion_client) -> None:
    client, _ = ingestion_client

    response = client.post(
        "/runs",
        content=JUNIT_XML,
        headers={"Content-Type": "application/xml", "X-Branch": "main"},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "commit_sha is required and must be at most 128 characters"


@pytest.mark.parametrize(
    "content_type",
    ["text/xml", "application/junit+xml"],
    ids=["text-xml", "junit-xml"],
)
def test_alternate_xml_content_types_are_accepted(
    ingestion_client,
    content_type: str,
) -> None:
    client, _ = ingestion_client

    response = client.post(
        "/runs",
        content=JUNIT_XML,
        headers={
            "Content-Type": content_type,
            "X-Commit-Sha": "alternate-content-type",
            "X-Branch": "main",
        },
    )

    assert response.status_code == 202
    raw_response = client.get(f"/internal/runs/{response.json()['run_id']}/raw")
    assert raw_response.status_code == 200
    assert raw_response.content == JUNIT_XML


def test_metadata_can_be_supplied_as_query_parameters(ingestion_client) -> None:
    client, _ = ingestion_client

    response = client.post(
        "/runs",
        params={"commit_sha": "query-commit", "branch": "query-branch"},
        content=JUNIT_XML,
        headers={"Content-Type": "application/xml"},
    )

    assert response.status_code == 202
    assert response.json()["status"] == "accepted"


def test_multipart_report_field_alias_is_accepted(ingestion_client) -> None:
    client, _ = ingestion_client

    response = client.post(
        "/runs",
        data={"commit_sha": "report-alias", "branch": "main"},
        files={"report": ("junit.xml", JUNIT_XML, "application/xml")},
    )

    assert response.status_code == 202
    raw_response = client.get(f"/internal/runs/{response.json()['run_id']}/raw")
    assert raw_response.status_code == 200
    assert raw_response.content == JUNIT_XML


def test_unsupported_content_type_is_rejected(ingestion_client) -> None:
    client, _ = ingestion_client

    response = client.post(
        "/runs",
        content=JUNIT_XML,
        headers={
            "Content-Type": "application/json",
            "X-Commit-Sha": "unsupported-content",
            "X-Branch": "main",
        },
    )

    assert response.status_code == 415
    assert response.json()["detail"] == (
        "Content-Type must be multipart/form-data or an XML media type"
    )


@pytest.mark.parametrize(
    ("commit_sha", "branch", "expected_detail"),
    [
        pytest.param(
            "c" * 129,
            "main",
            "commit_sha is required and must be at most 128 characters",
            id="commit-sha-too-long",
        ),
        pytest.param(
            "valid-commit",
            "b" * 256,
            "branch is required and must be at most 255 characters",
            id="branch-too-long",
        ),
    ],
)
def test_metadata_over_maximum_length_is_rejected(
    ingestion_client,
    commit_sha: str,
    branch: str,
    expected_detail: str,
) -> None:
    client, _ = ingestion_client

    response = _raw_upload(client, commit_sha=commit_sha, branch=branch)

    assert response.status_code == 400
    assert response.json()["detail"] == expected_detail


@pytest.mark.parametrize(
    "body",
    [
        b"<testsuite name='unfinished'>",
        b"<not-junit />",
        b"<testsuite><testcase name='missing suite name' /></testsuite>",
        b"<testsuite name='suite'><testcase /></testsuite>",
        b'<!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>'
        b'<testsuite name="suite"><testcase name="&xxe;" /></testsuite>',
    ],
    ids=[
        "malformed-xml",
        "non-junit-root",
        "suite-name-required",
        "testcase-name-required",
        "unsafe-doctype-rejected",
    ],
)
def test_invalid_or_unsafe_junit_xml_is_rejected(ingestion_client, body: bytes) -> None:
    client, engine = ingestion_client

    response = _raw_upload(client, body)

    assert response.status_code == 400
    with engine.connect() as connection:
        assert connection.execute(select(func.count()).select_from(runs)).scalar_one() == 0


def test_report_size_limit_is_enforced(ingestion_client) -> None:
    client, _ = ingestion_client

    response = _raw_upload(client, b"x" * 1025)

    assert response.status_code == 413


def test_internal_raw_endpoint_returns_stored_xml(ingestion_client) -> None:
    client, _ = ingestion_client
    run_id = _raw_upload(client).json()["run_id"]

    response = client.get(f"/internal/runs/{run_id}/raw")

    assert response.status_code == 200
    assert response.content == JUNIT_XML
    assert response.headers["content-type"] == "application/xml"


def test_internal_raw_endpoint_returns_404_for_unknown_run(ingestion_client) -> None:
    client, _ = ingestion_client

    response = client.get(f"/internal/runs/{'0' * 64}/raw")

    assert response.status_code == 404


class ConfirmingExchange:
    def __init__(self) -> None:
        self.messages: list[tuple[str, dict[str, object]]] = []

    async def publish(self, message, routing_key: str, mandatory: bool) -> None:
        assert mandatory is True
        assert message.content_type == "application/json"
        assert routing_key == "run.received"
        self.messages.append((routing_key, json.loads(message.body)))


class FailingExchange:
    async def publish(self, message, routing_key: str, mandatory: bool) -> None:
        assert message.content_type == "application/json"
        assert routing_key == "run.received"
        assert mandatory is True
        raise RuntimeError("broker unavailable")


class FailingThenConfirmingExchange:
    def __init__(self) -> None:
        self.publish_attempts = 0
        self.messages: list[dict[str, object]] = []

    async def publish(self, message, routing_key: str, mandatory: bool) -> None:
        assert message.content_type == "application/json"
        assert routing_key == "run.received"
        assert mandatory is True
        self.publish_attempts += 1
        if self.publish_attempts in {1, 3}:
            raise RuntimeError("temporary broker failure")
        self.messages.append(json.loads(message.body))


class RecordingMetric:
    def __init__(self) -> None:
        self.values: list[int] = []
        self.increment_count = 0

    def set(self, value: int) -> None:
        self.values.append(value)

    def inc(self) -> None:
        self.increment_count += 1


def test_legacy_outbox_event_gets_raw_ref_when_published(ingestion_client) -> None:
    client, engine = ingestion_client
    response = _raw_upload(client)
    run_id = response.json()["run_id"]

    with engine.begin() as connection:
        stored_event = connection.execute(select(outbox)).mappings().one()
        legacy_payload = dict(stored_event["payload"])
        legacy_body = dict(legacy_payload["payload"])
        legacy_body.pop("raw_ref")
        legacy_payload["payload"] = legacy_body
        connection.execute(
            update(outbox)
            .where(outbox.c.id == stored_event["id"])
            .values(payload=legacy_payload)
        )

    exchange = ConfirmingExchange()
    published = asyncio.run(publish_pending_batch(engine, exchange, batch_size=50))

    assert published == 1
    message = exchange.messages[0][1]
    assert message["run_id"] == run_id
    assert message["payload"]["raw_ref"] == f"/internal/runs/{run_id}/raw"
    assert message["payload"]["commit_sha"] == "0123456789abcdef"


def test_relay_recovers_from_publish_error_and_resets_failure_gauge(
    ingestion_client,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, engine = ingestion_client
    _raw_upload(client)
    exchange = FailingThenConfirmingExchange()
    pending_gauge = RecordingMetric()
    errors = RecordingMetric()
    failures = RecordingMetric()
    connections = []

    class FakeChannel:
        async def declare_exchange(self, *args, **kwargs):
            return exchange

        async def close(self) -> None:
            return None

    class FakeConnection:
        def __init__(self) -> None:
            self.is_closed = False

        async def channel(self, **kwargs):
            return FakeChannel()

        async def close(self) -> None:
            self.is_closed = True

    async def connect(_rabbitmq_url: str):
        connection = FakeConnection()
        connections.append(connection)
        return connection

    monkeypatch.setattr("app.relay.aio_pika.connect_robust", connect)
    sleeps = []
    random_bounds = []

    async def controlled_sleep(delay: float) -> None:
        sleeps.append(delay)
        if len(sleeps) == 2:
            _raw_upload(client, commit_sha="relay-second-run")
        elif len(sleeps) == 4:
            raise asyncio.CancelledError

    def use_upper_bound(lower: float, upper: float) -> float:
        random_bounds.append((lower, upper))
        return upper

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            run_outbox_relay(
                engine=engine,
                rabbitmq_url="amqp://unused",
                poll_interval_seconds=0.25,
                batch_size=50,
                pending_gauge=pending_gauge,
                publish_errors=errors,
                consecutive_failures_gauge=failures,
                backoff_cap_seconds=0.4,
                sleep=controlled_sleep,
                random_uniform=use_upper_bound,
            )
        )

    assert exchange.publish_attempts == 4
    assert len(exchange.messages) == 2
    assert len(connections) == 3
    assert sleeps == [0.25, 0.25, 0.25, 0.25]
    assert random_bounds == [(0.0, 0.25), (0.0, 0.25)]
    assert errors.increment_count == 2
    assert failures.values == [1, 0, 1, 0]
    assert pending_gauge.values[-1] == 0


def test_relay_marks_event_published_only_after_confirmation(ingestion_client) -> None:
    client, engine = ingestion_client
    client.post(
        "/runs",
        content=JUNIT_XML,
        headers={
            "Content-Type": "application/xml",
            "X-Commit-Sha": "0123456789abcdef",
            "X-Branch": "main",
        },
    )
    exchange = ConfirmingExchange()

    published = asyncio.run(publish_pending_batch(engine, exchange, batch_size=50))

    assert published == 1
    assert len(exchange.messages) == 1
    routing_key, message = exchange.messages[0]
    assert routing_key == "run.received"
    assert message["event_type"] == "RunReceived"
    with engine.connect() as connection:
        stored_event = connection.execute(select(outbox)).mappings().one()
    assert stored_event["published_at"] is not None


def test_relay_failure_leaves_event_pending(ingestion_client) -> None:
    client, engine = ingestion_client
    _raw_upload(client)

    with pytest.raises(RuntimeError, match="broker unavailable"):
        asyncio.run(publish_pending_batch(engine, FailingExchange(), batch_size=50))

    with engine.connect() as connection:
        stored_event = connection.execute(select(outbox)).mappings().one()
    assert stored_event["published_at"] is None
