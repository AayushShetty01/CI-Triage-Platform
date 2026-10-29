import asyncio
import json

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, func, select

from app.database import metadata, outbox, runs
from app.main import create_app
from app.relay import publish_pending_batch

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
