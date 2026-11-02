import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from jsonschema import Draft202012Validator, FormatChecker, ValidationError
from sqlalchemy import select

from app.consumer import process_run_received_event
from app.database import outbox
from app.ingestion_client import IngestionClient
from tests.conftest import SAMPLE_FAILURES_XML

SERVICE_DIR = Path(__file__).resolve().parents[1]
REPO_DIR = SERVICE_DIR.parents[1]
CONTRACTS_DIR = REPO_DIR / "contracts" / "events"
SAMPLES_DIR = CONTRACTS_DIR / "samples"

RUN_RECEIVED_SCHEMA = json.loads((CONTRACTS_DIR / "run_received.schema.json").read_text(encoding="utf-8"))
RUN_RECEIVED_VALIDATOR = Draft202012Validator(RUN_RECEIVED_SCHEMA, format_checker=FormatChecker())

FAILURES_EXTRACTED_SCHEMA = json.loads((CONTRACTS_DIR / "failures_extracted.schema.json").read_text(encoding="utf-8"))
FAILURES_EXTRACTED_VALIDATOR = Draft202012Validator(FAILURES_EXTRACTED_SCHEMA, format_checker=FormatChecker())


@pytest.mark.parametrize(
    "sample_name",
    ["run_received.json", "run_received.legacy-publish.json"],
)
def test_incoming_run_received_samples_validate(sample_name: str):
    sample = json.loads((SAMPLES_DIR / sample_name).read_text(encoding="utf-8"))
    RUN_RECEIVED_VALIDATOR.validate(sample)


def test_outgoing_failures_extracted_sample_validates():
    sample = json.loads((SAMPLES_DIR / "failures_extracted.json").read_text(encoding="utf-8"))
    FAILURES_EXTRACTED_VALIDATOR.validate(sample)


def test_failures_extracted_schema_rejects_invalid_payload():
    invalid_sample = {
        "event_id": "not-a-uuid",
        "event_type": "WrongEvent",
        "occurred_at": "invalid-date",
        "schema_version": "1.0",
        "run_id": "short-id",
        "payload": {},
    }
    with pytest.raises(ValidationError):
        FAILURES_EXTRACTED_VALIDATOR.validate(invalid_sample)


@pytest.mark.asyncio
async def test_parser_produced_event_conforms_to_schema(parser_test_db):
    engine, _ = parser_test_db
    mock_ingestion = IngestionClient()
    mock_ingestion.get_raw_xml = AsyncMock(return_value=SAMPLE_FAILURES_XML)

    run_id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    incoming_event = {
        "event_id": "2e9c2a85-3c72-4dfb-9f62-ef143c77f58d",
        "event_type": "RunReceived",
        "occurred_at": "2026-01-15T12:30:45Z",
        "schema_version": "1.0",
        "run_id": run_id,
        "payload": {
            "commit_sha": "abcdef0123456789",
            "branch": "main",
            "raw_ref": f"/internal/runs/{run_id}/raw",
            "received_at": "2026-01-15T12:30:45Z",
        },
    }

    success = await process_run_received_event(incoming_event, engine, mock_ingestion)
    assert success is True

    # Retrieve generated outbox event
    with engine.connect() as conn:
        row = conn.execute(select(outbox)).mappings().one()
        payload = row["payload"]
        assert row["event_type"] == "FailuresExtracted"

    # Validate generated payload against FailuresExtracted schema!
    FAILURES_EXTRACTED_VALIDATOR.validate(payload)
    assert payload["run_id"] == run_id
    assert payload["payload"]["failed_tests"] == 2
    assert len(payload["payload"]["failures"]) == 2
