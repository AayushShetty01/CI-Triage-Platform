import asyncio
import copy
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from jsonschema import Draft202012Validator, FormatChecker, ValidationError
from sqlalchemy import create_engine, select, update

from app.database import metadata, outbox
from app.main import create_app
from app.relay import publish_pending_batch

SERVICE_DIR = Path(__file__).resolve().parents[1]
CONTRACTS_DIR = SERVICE_DIR.parents[1] / "contracts" / "events"
SCHEMA_PATH = CONTRACTS_DIR / "run_received.schema.json"
SAMPLES_DIR = CONTRACTS_DIR / "samples"
SCHEMA = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
VALIDATOR = Draft202012Validator(SCHEMA, format_checker=FormatChecker())
JUNIT_XML = b'<testsuite name="contract"><testcase name="passes" /></testsuite>'


@pytest.fixture
def contract_client(tmp_path):
    database_url = f"sqlite:///{tmp_path / 'contract-test.db'}"
    engine = create_engine(
        database_url,
        connect_args={"check_same_thread": False},
        execution_options={"schema_translate_map": {"ingestion": None}},
    )
    metadata.create_all(engine)
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
            "Content-Type": "application/xml",
            "X-Commit-Sha": "contract-commit",
            "X-Branch": "contract",
        },
    )
    assert response.status_code == 202

    with engine.connect() as connection:
        event = connection.execute(select(outbox.c.payload)).scalar_one()

    VALIDATOR.validate(event)


def test_legacy_outbox_event_validates_after_relay_transformation(contract_client) -> None:
    client, engine = contract_client
    response = client.post(
        "/runs",
        content=JUNIT_XML,
        headers={
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
