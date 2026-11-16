import asyncio
import copy
import json
import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from jsonschema import Draft202012Validator, FormatChecker, ValidationError
from sqlalchemy import select, update

from app.database import outbox
from app.main import create_app
from app.relay import publish_pending_batch
from tests.conftest import prepare_test_database

SERVICE_DIR = Path(__file__).resolve().parents[1]
CONTRACTS_DIR = SERVICE_DIR.parents[1] / "contracts" / "events"
SCHEMA_PATH = CONTRACTS_DIR / "run_received.schema.json"
SAMPLES_DIR = CONTRACTS_DIR / "samples"
SCHEMA = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
VALIDATOR = Draft202012Validator(SCHEMA, format_checker=FormatChecker())
JUNIT_XML = b'<testsuite name="contract"><testcase name="passes" /></testsuite>'
INGESTION_API_KEY = "test-ingestion-api-key"


@pytest.fixture
def contract_client():
    database_url = os.environ["DATABASE_URL"]
    engine = prepare_test_database()
    app = create_app(database_url=database_url, enable_relay=False)
    with TestClient(app) as client:
        yield client, app.state.engine
    engine.dispose()
    app.state.engine.dispose()


class ConfirmingExchange:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []

    async def publish(self, message, routing_key: str, mandatory: bool) -> None:
        assert message.content_type == "application/json"
        assert routing_key == "run.received"
        assert mandatory is True
        self.messages.append(json.loads(message.body))


@pytest.mark.parametrize(
    "sample_name",
    ["run_received.json", "run_received.legacy-publish.json"],
)
def test_shared_samples_validate(sample_name: str) -> None:
    sample = json.loads((SAMPLES_DIR / sample_name).read_text(encoding="utf-8"))

    VALIDATOR.validate(sample)


def test_event_from_post_and_outbox_validates(contract_client) -> None:
    client, engine = contract_client
    response = client.post(
        "/runs",
        content=JUNIT_XML,
        headers={
            "X-API-Key": INGESTION_API_KEY,
            "Content-Type": "application/xml",
            "X-Commit-Sha": "contract-commit",
            "X-Branch": "contract",
        },
    )
    assert response.status_code == 202

    with engine.connect() as connection:
        event = connection.execute(select(outbox.c.payload)).scalar_one()

    VALIDATOR.validate(event)


def test_ci_metadata_is_included_in_the_run_received_contract(contract_client) -> None:
    client, engine = contract_client
    response = client.post(
        "/runs",
        content=JUNIT_XML,
        headers={
            "X-API-Key": INGESTION_API_KEY,
            "Content-Type": "application/xml",
            "X-Commit-Sha": "ci-contract-commit",
            "X-Branch": "main",
            "X-CI-Repository": "example/project",
            "X-CI-Workflow": "CI",
            "X-CI-Run-Id": "12345-1-ingestion",
            "X-CI-Run-Attempt": "1",
            "X-CI-Service": "ingestion",
            "X-CI-Actor": "example",
            "X-CI-Run-Url": "https://github.com/example/project/actions/runs/12345",
            "X-CI-Timestamp": "2026-04-20T12:00:00Z",
        },
    )
    assert response.status_code == 202
    with engine.connect() as connection:
        event = connection.execute(select(outbox.c.payload)).scalar_one()

    VALIDATOR.validate(event)
    assert event["payload"]["ci"]["run_id"] == "12345-1-ingestion"


def test_partial_ci_metadata_is_rejected(contract_client) -> None:
    client, _ = contract_client
    response = client.post(
        "/runs",
        content=JUNIT_XML,
        headers={
            "X-API-Key": INGESTION_API_KEY,
            "Content-Type": "application/xml",
            "X-Commit-Sha": "ci-contract-commit",
            "X-Branch": "main",
            "X-CI-Service": "ingestion",
        },
    )

    assert response.status_code == 400


def test_legacy_outbox_event_validates_after_relay_transformation(contract_client) -> None:
    client, engine = contract_client
    response = client.post(
        "/runs",
        content=JUNIT_XML,
        headers={
            "X-API-Key": INGESTION_API_KEY,
            "Content-Type": "application/xml",
            "X-Commit-Sha": "legacy-contract-commit",
            "X-Branch": "contract",
        },
    )
    assert response.status_code == 202

    with engine.begin() as connection:
        stored = connection.execute(select(outbox)).mappings().one()
        legacy_event = copy.deepcopy(stored["payload"])
        legacy_event["payload"].pop("raw_ref")
        connection.execute(
            update(outbox)
            .where(outbox.c.id == stored["id"])
            .values(payload=legacy_event)
        )

    exchange = ConfirmingExchange()
    assert asyncio.run(publish_pending_batch(engine, exchange, batch_size=10)) == 1
    assert len(exchange.messages) == 1
    VALIDATOR.validate(exchange.messages[0])


@pytest.mark.parametrize(
    "invalid_change",
    [
        "missing_raw_ref",
        "malformed_run_id",
        "wrong_event_type",
    ],
    ids=["missing-raw-ref", "malformed-run-id", "wrong-event-type"],
)
def test_invalid_envelopes_fail_validation(invalid_change: str) -> None:
    event = json.loads((SAMPLES_DIR / "run_received.json").read_text(encoding="utf-8"))
    if invalid_change == "missing_raw_ref":
        event["payload"].pop("raw_ref")
    elif invalid_change == "malformed_run_id":
        event["run_id"] = "not-a-run-id"
    else:
        event["event_type"] = "RunCompleted"

    with pytest.raises(ValidationError):
        VALIDATOR.validate(event)
