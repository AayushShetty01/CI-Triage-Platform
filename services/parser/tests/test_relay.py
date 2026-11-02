import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from prometheus_client import Gauge
from sqlalchemy import insert, select

from app.database import outbox
from app.relay import (
    _retry_delay,
    publish_pending_batch,
    refresh_outbox_metrics,
)


@pytest.mark.asyncio
async def test_publish_pending_batch_marks_published(parser_test_db):
    engine, _ = parser_test_db
    now = datetime.now(timezone.utc)
    event_payload = {
        "event_id": "test-uuid-1234",
        "event_type": "FailuresExtracted",
        "run_id": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "payload": {"failed_tests": 1},
    }

    with engine.begin() as conn:
        conn.execute(
            insert(outbox).values(
                event_type="FailuresExtracted",
                payload=event_payload,
                created_at=now,
            )
        )

    # Mock exchange
    mock_exchange = MagicMock()
    mock_exchange.publish = AsyncMock()

    published = await publish_pending_batch(engine, mock_exchange, batch_size=10)
    assert published == 1

    # Verify message was published to exchange
    mock_exchange.publish.assert_awaited_once()
    call_args = mock_exchange.publish.call_args
    assert call_args.kwargs["routing_key"] == "failures.extracted"

    # Verify outbox row now has published_at set
    with engine.connect() as conn:
        row = conn.execute(select(outbox)).mappings().one()
        assert row["published_at"] is not None


def test_refresh_outbox_metrics(parser_test_db):
    engine, _ = parser_test_db
    pending_gauge = Gauge("test_pending", "desc")
    oldest_pending_gauge = Gauge("test_oldest", "desc")

    # When no rows
    count = refresh_outbox_metrics(engine, pending_gauge, oldest_pending_gauge)
    assert count == 0

    # Insert pending row
    with engine.begin() as conn:
        conn.execute(
            insert(outbox).values(
                event_type="FailuresExtracted",
                payload={},
                created_at=datetime.now(timezone.utc),
            )
        )

    count = refresh_outbox_metrics(engine, pending_gauge, oldest_pending_gauge)
    assert count == 1


def test_retry_delay_calculation():
    # Attempt 0: base_seconds * 1
    delay = _retry_delay(
        base_seconds=1.0,
        cap_seconds=30.0,
        attempt=0,
        random_uniform=lambda a, b: b,
    )
    assert delay == 1.0

    # Attempt 3: base_seconds * 8
    delay = _retry_delay(
        base_seconds=1.0,
        cap_seconds=30.0,
        attempt=3,
        random_uniform=lambda a, b: b,
    )
    assert delay == 8.0

    # Attempt 10: capped at 30.0
    delay = _retry_delay(
        base_seconds=1.0,
        cap_seconds=30.0,
        attempt=10,
        random_uniform=lambda a, b: b,
    )
    assert delay == 30.0
