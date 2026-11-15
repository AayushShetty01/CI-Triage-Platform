import uuid
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from sqlalchemy import insert
from sqlalchemy.engine import Connection

from app.database import outbox

EVENT_TYPE = "FailureClassified"
SCHEMA_VERSION = "1.0"


def build_failure_classified(row: Mapping[str, Any], now: datetime) -> dict[str, Any]:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": EVENT_TYPE,
        "occurred_at": now.isoformat(),
        "schema_version": SCHEMA_VERSION,
        "run_id": row["run_id"],
        "payload": {
            "test_id": row["test_id"],
            "test_name": row["test_name"],
            "class_name": row["class_name"],
            "commit_sha": row["commit_sha"],
            "branch": row["branch"],
            "fingerprint": row["fingerprint"],
            "category": row["category"],
            "status": row["status"],
            "source": row["source"],
            "confidence": row.get("confidence"),
            "reason": row.get("reason"),
            "rule_id": row.get("rule_id"),
            "versions": {
                "rules_version": row["rules_version"],
                "model": row.get("model"),
                "prompt_version": row.get("prompt_version"),
                "prompt_hash": row.get("prompt_hash"),
            },
        },
    }


def stage_failure_classified(conn: Connection, row: Mapping[str, Any], now: datetime) -> dict[str, Any]:
    """Write a FailureClassified event to the outbox inside the caller's transaction."""
    event = build_failure_classified(row, now)
    conn.execute(insert(outbox).values(event_type=EVENT_TYPE, payload=event, created_at=now))
    return event
