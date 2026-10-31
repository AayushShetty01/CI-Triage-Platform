import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import select

from app.consumer import handle_message, process_run_received_event
from app.database import outbox, parsed_runs, test_results
from app.ingestion_client import (
    IngestionClient,
    IngestionPermanentError,
    IngestionTransientError,
)
from tests.conftest import (
    MALFORMED_XML,
    SAMPLE_FAILURES_XML,
    SAMPLE_PASS_XML,
)

SAMPLE_RUN_ID = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"


def _make_event(run_id: str = SAMPLE_RUN_ID) -> dict:
    return {
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


@pytest.mark.asyncio
async def test_process_run_received_success(parser_test_db):
    engine, _ = parser_test_db
    mock_ingestion = IngestionClient()
    mock_ingestion.get_raw_xml = AsyncMock(return_value=SAMPLE_FAILURES_XML)

    event = _make_event()
    success = await process_run_received_event(event, engine, mock_ingestion)
    assert success is True

    # Verify parsed_runs
    with engine.connect() as conn:
        run = conn.execute(
            select(parsed_runs).where(parsed_runs.c.run_id == SAMPLE_RUN_ID)
        ).mappings().one()
        assert run["total_tests"] == 4
        assert run["passed_tests"] == 1
        assert run["failed_tests"] == 2
        assert run["status"] == "completed"

        # Verify test_results
        tests = conn.execute(
            select(test_results).where(test_results.c.run_id == SAMPLE_RUN_ID)
        ).mappings().all()
        assert len(tests) == 4

        # Verify outbox event
        outbox_events = conn.execute(
            select(outbox).where(outbox.c.event_type == "FailuresExtracted")
        ).mappings().all()
        assert len(outbox_events) == 1
        payload = outbox_events[0]["payload"]
        assert payload["run_id"] == SAMPLE_RUN_ID
        assert len(payload["payload"]["failures"]) == 2


@pytest.mark.asyncio
async def test_idempotency_duplicate_run_received(parser_test_db):
    engine, _ = parser_test_db
    mock_ingestion = IngestionClient()
    mock_ingestion.get_raw_xml = AsyncMock(return_value=SAMPLE_FAILURES_XML)

    event = _make_event()

    # Process first time
    await process_run_received_event(event, engine, mock_ingestion)

    # Process second time (simulating at-least-once duplicate delivery)
    await process_run_received_event(event, engine, mock_ingestion)

    # Check database: no duplicates in parsed_runs or test_results
    with engine.connect() as conn:
        runs = conn.execute(
            select(parsed_runs).where(parsed_runs.c.run_id == SAMPLE_RUN_ID)
        ).mappings().all()
        assert len(runs) == 1

        tests = conn.execute(
            select(test_results).where(test_results.c.run_id == SAMPLE_RUN_ID)
        ).mappings().all()
        assert len(tests) == 4

        outbox_events = conn.execute(
            select(outbox).where(outbox.c.event_type == "FailuresExtracted")
        ).mappings().all()
        assert len(outbox_events) == 1


@pytest.mark.asyncio
async def test_handle_message_poison_bad_json(parser_test_db):
    engine, _ = parser_test_db
    mock_ingestion = IngestionClient()

    # Create mock aio_pika message with invalid JSON
    message = MagicMock()
    message.body = b"not-a-valid-json-string{["
    message.process = MagicMock()
    message.process.return_value.__aenter__ = AsyncMock()
    message.process.return_value.__aexit__ = AsyncMock()
    message.reject = AsyncMock()
    message.ack = AsyncMock()
    message.nack = AsyncMock()

    await handle_message(message, engine, mock_ingestion)

    # Must reject without requeue (send to DLQ)
    message.reject.assert_awaited_once_with(requeue=False)
    message.ack.assert_not_awaited()


@pytest.mark.asyncio
async def test_handle_message_poison_corrupt_xml(parser_test_db):
    engine, _ = parser_test_db
    mock_ingestion = IngestionClient()
    mock_ingestion.get_raw_xml = AsyncMock(return_value=MALFORMED_XML)

    event = _make_event()
    message = MagicMock()
    message.body = json.dumps(event).encode("utf-8")
    message.process = MagicMock()
    message.process.return_value.__aenter__ = AsyncMock()
    message.process.return_value.__aexit__ = AsyncMock()
    message.reject = AsyncMock()
    message.ack = AsyncMock()
    message.nack = AsyncMock()

    await handle_message(message, engine, mock_ingestion)

    # Corrupt XML is an unrecoverable poison message -> DLQ
    message.reject.assert_awaited_once_with(requeue=False)
    message.ack.assert_not_awaited()


@pytest.mark.asyncio
async def test_handle_message_transient_network_error(parser_test_db):
    engine, _ = parser_test_db
    mock_ingestion = IngestionClient()
    mock_ingestion.get_raw_xml = AsyncMock(side_effect=IngestionTransientError("503 Service Unavailable"))

    event = _make_event()
    message = MagicMock()
    message.body = json.dumps(event).encode("utf-8")
    message.process = MagicMock()
    message.process.return_value.__aenter__ = AsyncMock()
    message.process.return_value.__aexit__ = AsyncMock()
    message.reject = AsyncMock()
    message.ack = AsyncMock()
    message.nack = AsyncMock()

    await handle_message(message, engine, mock_ingestion)

    # Transient error must be nacked with requeue=True for retry
    message.nack.assert_awaited_once_with(requeue=True)
    message.reject.assert_not_awaited()
    message.ack.assert_not_awaited()


@pytest.mark.asyncio
async def test_handle_message_permanent_404_error(parser_test_db):
    engine, _ = parser_test_db
    mock_ingestion = IngestionClient()
    mock_ingestion.get_raw_xml = AsyncMock(side_effect=IngestionPermanentError("Run not found 404"))

    event = _make_event()
    message = MagicMock()
    message.body = json.dumps(event).encode("utf-8")
    message.process = MagicMock()
    message.process.return_value.__aenter__ = AsyncMock()
    message.process.return_value.__aexit__ = AsyncMock()
    message.reject = AsyncMock()
    message.ack = AsyncMock()
    message.nack = AsyncMock()

    await handle_message(message, engine, mock_ingestion)

    # Permanent 404 error -> reject without requeue (DLQ)
    message.reject.assert_awaited_once_with(requeue=False)
    message.ack.assert_not_awaited()
