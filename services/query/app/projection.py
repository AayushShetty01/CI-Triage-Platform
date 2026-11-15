# Maintains the Query service's local projection from at-least-once events.
# All changes for one event, including its deduplication record, commit together.

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Connection, Engine

from app.database import failures, processed_events, runs, tests

EVENT_TYPES = {"RunReceived", "FailuresExtracted", "FailureClassified"}
TERMINAL_STATUSES = {"classified", "degraded"}
CATEGORIES = {"assertion", "timeout", "infrastructure", "flaky", "unknown"}


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def insert_for(conn: Connection, table):
    return pg_insert(table) if conn.dialect.name == "postgresql" else sqlite_insert(table)


def validate_event(event: dict[str, Any]) -> None:
    if not isinstance(event, dict) or event.get("event_type") not in EVENT_TYPES:
        raise ValueError("Unsupported event type")
    for field in ("event_id", "run_id", "occurred_at"):
        if not isinstance(event.get(field), str) or not event[field]:
            raise ValueError(f"Missing {field}")
    if not isinstance(event.get("payload"), dict):
        raise ValueError("Missing payload")
    parse_time(event["occurred_at"])
    payload = event["payload"]
    if event["event_type"] == "FailuresExtracted":
        if not isinstance(payload.get("failures"), list):
            raise ValueError("Missing failures list")
        if any(not isinstance(row, dict) or not row.get("test_id") for row in payload["failures"]):
            raise ValueError("Invalid failure")
    if event["event_type"] == "FailureClassified":
        if not payload.get("test_id") or payload.get("status") not in TERMINAL_STATUSES:
            raise ValueError("Invalid classification status or test_id")
        if payload.get("category") not in CATEGORIES or not isinstance(payload.get("versions"), dict):
            raise ValueError("Invalid category or versions")


def apply_event(engine: Engine, event: dict[str, Any], test_results: list[dict[str, Any]] | None = None) -> bool:
    validate_event(event)
    kind = event["event_type"]
    payload = event["payload"]
    run_id = event["run_id"]
    occurred_at = parse_time(event["occurred_at"])
    with engine.begin() as conn:
        marker = insert_for(conn, processed_events).values(
            event_id=event["event_id"], event_type=kind, processed_at=datetime.now(timezone.utc)
        ).on_conflict_do_nothing(index_elements=["event_id"])
        if conn.execute(marker).rowcount == 0:
            return False

        run_values: dict[str, Any] = {"run_id": run_id}
        if kind in {"RunReceived", "FailuresExtracted"}:
            run_values.update(commit_sha=payload.get("commit_sha"), branch=payload.get("branch"))
        if kind == "RunReceived":
            run_values["received_at"] = parse_time(payload.get("received_at", event["occurred_at"]))
        elif kind == "FailuresExtracted":
            run_values.update(
                parsed_at=occurred_at,
                total_tests=payload.get("total_tests"),
                failed_tests=payload.get("failed_tests"),
                passed_tests=payload.get("passed_tests"),
                skipped_tests=payload.get("skipped_tests"),
            )
        elif kind == "FailureClassified":
            run_values.update(commit_sha=payload.get("commit_sha"), branch=payload.get("branch"))
        update_values = {k: v for k, v in run_values.items() if k != "run_id" and v is not None}
        conn.execute(insert_for(conn, runs).values(**run_values).on_conflict_do_update(
            index_elements=["run_id"], set_=update_values or {"run_id": run_id}
        ))

        if kind == "FailuresExtracted":
            for row in payload["failures"]:
                values = {
                    "run_id": run_id, "test_id": row["test_id"],
                    "test_name": row.get("test_name") or row["test_id"],
                    "class_name": row.get("class_name") or "",
                    "error_type": row.get("error_type"),
                    "error_message": (row.get("error_message") or "")[:2000],
                    "status": "pending",
                }
                conn.execute(insert_for(conn, failures).values(**values).on_conflict_do_nothing(
                    index_elements=["run_id", "test_id"]
                ))
            for row in test_results or []:
                if row.get("status") not in {"passed", "failed", "error", "skipped"}:
                    continue
                values = {
                    "run_id": run_id, "test_id": row["test_id"],
                    "test_name": row.get("test_name") or row["test_id"],
                    "class_name": row.get("class_name") or "",
                    "status": row["status"],
                    "duration_seconds": str(row.get("duration_seconds", "")),
                }
                conn.execute(insert_for(conn, tests).values(**values).on_conflict_do_nothing(
                    index_elements=["run_id", "test_id"]
                ))
        elif kind == "FailureClassified":
            versions = payload["versions"]
            values = {
                "run_id": run_id, "test_id": payload["test_id"],
                "test_name": payload.get("test_name") or payload["test_id"],
                "class_name": payload.get("class_name") or "",
                "fingerprint": payload.get("fingerprint"),
                "category": payload["category"], "status": payload["status"],
                "source": payload.get("source"),
                "rules_version": versions.get("rules_version"),
                "model": versions.get("model"),
                "prompt_version": versions.get("prompt_version"),
                "prompt_hash": versions.get("prompt_hash"),
                "classified_at": occurred_at,
            }
            stmt = insert_for(conn, failures).values(**values)
            conn.execute(stmt.on_conflict_do_update(
                index_elements=["run_id", "test_id"],
                set_={key: value for key, value in values.items() if key not in {"run_id", "test_id"}},
            ))
    return True


def has_event(engine: Engine, event_id: str) -> bool:
    with engine.connect() as conn:
        return conn.execute(select(processed_events.c.event_id).where(
            processed_events.c.event_id == event_id
        )).scalar_one_or_none() is not None
