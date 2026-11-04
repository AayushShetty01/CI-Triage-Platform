"""FailuresExtracted consumer.

The consumer never calls the LLM. For each failure it either:
- classifies it with a rule (status=classified, source=rule), or
- reuses a finished LLM result for the same fingerprint (source=llm_cache), or
- attaches it to an LLM job for that fingerprint (status=pending), or
- when the LLM is disabled, marks it degraded (category=unknown, source=fallback).

Everything for one event is written in one transaction, together with the
processed_events row and any outbox events, so a redelivered event is a no-op.
"""

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import aio_pika
from sqlalchemy import insert, select, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Connection, Engine

from app import metrics
from app.database import classifications, failure_groups, llm_jobs, processed_events
from app.events import stage_failure_classified
from app.fingerprint import compute_fingerprint
from app.llm_client import PROMPT_HASH, PROMPT_VERSION
from app.logging_config import event_id_context, run_id_context
from app.rules import RULES_VERSION, classify_by_rules, has_no_signal
from app.settings import Settings

logger = logging.getLogger(__name__)

EXCHANGE_NAME = "triage.events"
ROUTING_KEY_FAILURES_EXTRACTED = "failures.extracted"
QUEUE_NAME = "classifier.failures_extracted"
DLX_NAME = "triage.dlx"
DLQ_NAME = "classifier.failures_extracted.dlq"
TRANSIENT_RETRY_DELAY_SECONDS = 1.0


class InvalidEvent(ValueError):
    """The message can never be processed; it belongs in the DLQ."""


@dataclass
class ProcessResult:
    duplicate: bool = False
    rule: int = 0
    cached: int = 0
    pending: int = 0
    degraded: int = 0


def _insert_fn(conn: Connection):
    return postgresql_insert if conn.dialect.name == "postgresql" else sqlite_insert


def validate_event(event: Any) -> None:
    if not isinstance(event, dict):
        raise InvalidEvent("event is not a JSON object")
    if event.get("event_type") != "FailuresExtracted":
        raise InvalidEvent(f"unexpected event_type {event.get('event_type')!r}")
    for key in ("event_id", "run_id"):
        if not isinstance(event.get(key), str) or not event[key]:
            raise InvalidEvent(f"missing {key}")
    payload = event.get("payload")
    if not isinstance(payload, dict) or not isinstance(payload.get("failures"), list):
        raise InvalidEvent("payload.failures must be a list")
    for failure in payload["failures"]:
        if not isinstance(failure, dict) or not failure.get("test_id") or not failure.get("test_name"):
            raise InvalidEvent("each failure needs test_id and test_name")


def _upsert_group(conn: Connection, fp, error_type: str | None, now: datetime) -> None:
    stmt = _insert_fn(conn)(failure_groups).values(
        fingerprint=fp.value,
        error_type=(error_type or None) and error_type[:255],
        normalized_message=fp.normalized_message,
        top_frame=fp.top_frame and fp.top_frame[:512],
        occurrence_count=1,
        first_seen_at=now,
        last_seen_at=now,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["fingerprint"],
        set_={
            "occurrence_count": failure_groups.c.occurrence_count + 1,
            "last_seen_at": now,
        },
    )
    conn.execute(stmt)


def _attach_llm_job(
    conn: Connection,
    fingerprint: str,
    failure: dict[str, Any],
    settings: Settings,
    now: datetime,
) -> dict[str, Any]:
    """Get (creating if needed) the job for this fingerprint, locked for this transaction.

    The row lock matters: the worker takes the same lock when it finishes a job,
    so a pending classification is either visible to the worker's completion
    step, or sees the job already done here.
    """
    conn.execute(
        _insert_fn(conn)(llm_jobs)
        .values(
            fingerprint=fingerprint,
            model=settings.ollama_model,
            prompt_version=PROMPT_VERSION,
            status="pending",
            attempts=0,
            next_attempt_at=now,
            queued_at=now,
            error_type=failure.get("error_type"),
            error_message=failure.get("error_message"),
            stack_trace=failure.get("stack_trace"),
            created_at=now,
        )
        .on_conflict_do_nothing(index_elements=["fingerprint", "model", "prompt_version"])
    )
    job = (
        conn.execute(
            select(llm_jobs)
            .where(
                llm_jobs.c.fingerprint == fingerprint,
                llm_jobs.c.model == settings.ollama_model,
                llm_jobs.c.prompt_version == PROMPT_VERSION,
            )
            .with_for_update()
        )
        .mappings()
        .one()
    )
    if job["status"] == "failed":
        # The LLM gave up on this fingerprint earlier; a new occurrence gets a fresh try.
        conn.execute(
            update(llm_jobs)
            .where(llm_jobs.c.id == job["id"])
            .values(status="pending", attempts=0, next_attempt_at=now, queued_at=now, last_error=None)
        )
        job = {**job, "status": "pending"}
    return dict(job)


def classify_failures_in_tx(
    conn: Connection,
    event: dict[str, Any],
    settings: Settings,
    now: datetime,
) -> ProcessResult:
    result = ProcessResult()
    run_id = event["run_id"]
    payload = event["payload"]

    inserted = conn.execute(
        _insert_fn(conn)(processed_events)
        .values(event_id=event["event_id"], run_id=run_id, processed_at=now)
        .on_conflict_do_nothing(index_elements=["event_id"])
        .returning(processed_events.c.event_id)
    ).first()
    if inserted is None:
        result.duplicate = True
        return result

    existing_tests = set(
        conn.execute(
            select(classifications.c.test_id).where(classifications.c.run_id == run_id)
        ).scalars()
    )

    for failure in payload["failures"]:
        test_id = failure["test_id"]
        if test_id in existing_tests:
            continue  # same run delivered under a different event_id
        existing_tests.add(test_id)

        error_type = failure.get("error_type")
        error_message = failure.get("error_message")
        stack_trace = failure.get("stack_trace")
        fp = compute_fingerprint(error_type, error_message, stack_trace)
        _upsert_group(conn, fp, error_type, now)

        row: dict[str, Any] = {
            "run_id": run_id,
            "test_id": test_id,
            "test_name": failure["test_name"],
            "class_name": failure.get("class_name") or "",
            "commit_sha": payload.get("commit_sha") or "unknown",
            "branch": payload.get("branch") or "unknown",
            "fingerprint": fp.value,
            "rules_version": RULES_VERSION,
            "created_at": now,
            "category": None,
            "source": None,
            "confidence": None,
            "reason": None,
            "rule_id": None,
            "model": None,
            "prompt_version": None,
            "prompt_hash": None,
            "llm_job_id": None,
            "classified_at": None,
        }

        match = classify_by_rules(error_type, error_message, stack_trace)
        if has_no_signal(error_type, error_message, stack_trace):
            row.update(
                status="classified",
                category="unknown",
                source="rule",
                confidence=1.0,
                rule_id="no_signal",
                reason="Report has no error type, message or stack trace",
                classified_at=now,
            )
            result.rule += 1
        elif match is not None:
            row.update(
                status="classified",
                category=match.category,
                source="rule",
                confidence=1.0,
                rule_id=match.rule_id,
                reason=f"Matched rule {match.rule_id}",
                classified_at=now,
            )
            result.rule += 1
        elif not settings.llm_enabled:
            row.update(
                status="degraded",
                category="unknown",
                source="fallback",
                reason="No rule matched and the LLM is disabled",
                classified_at=now,
            )
            result.degraded += 1
        else:
            job = _attach_llm_job(conn, fp.value, failure, settings, now)
            row.update(
                llm_job_id=job["id"],
                model=settings.ollama_model,
                prompt_version=PROMPT_VERSION,
                prompt_hash=PROMPT_HASH,
            )
            if job["status"] == "done":
                row.update(
                    status="classified",
                    category=job["result_category"],
                    source="llm_cache",
                    confidence=job["result_confidence"],
                    reason=job["result_reason"],
                    classified_at=now,
                )
                result.cached += 1
            else:
                row.update(status="pending")
                result.pending += 1

        conn.execute(insert(classifications).values(**row))
        if row["status"] != "pending":
            stage_failure_classified(conn, row, now)
            metrics.classifications_total.labels(
                source=row["source"], category=row["category"], status=row["status"]
            ).inc()

    return result


async def process_failures_extracted_event(
    event: dict[str, Any],
    engine: Engine,
    settings: Settings,
) -> ProcessResult:
    validate_event(event)
    event_id_context.set(event["event_id"])
    run_id_context.set(event["run_id"])
    now = datetime.now(timezone.utc)
    with engine.begin() as conn:
        result = classify_failures_in_tx(conn, event, settings, now)
    log_event = "classifier_duplicate_event_ignored" if result.duplicate else "classifier_event_processed"
    logger.info(
        "FailuresExtracted handled: rule=%d cached=%d pending=%d degraded=%d",
        result.rule,
        result.cached,
        result.pending,
        result.degraded,
        extra={"event": log_event, "event_id": event["event_id"], "run_id": event["run_id"]},
    )
    return result


async def handle_message(
    message: aio_pika.abc.AbstractIncomingMessage,
    engine: Engine,
    settings: Settings,
) -> None:
    async with message.process(requeue=False, ignore_processed=True):
        metrics.events_consumed.inc()
        try:
            event = json.loads(message.body.decode("utf-8"))
            await process_failures_extracted_event(event, engine, settings)
        except (UnicodeDecodeError, json.JSONDecodeError, InvalidEvent) as err:
            logger.error(
                "Poison message rejected to DLQ: %s",
                err,
                extra={"event": "classifier_poison_message"},
            )
            await message.reject(requeue=False)
            metrics.poison_messages.inc()
            metrics.events_processed.labels(status="poison").inc()
            return
        except Exception as err:
            # Database or other transient failure: back off briefly, then redeliver.
            logger.warning(
                "Transient error processing event; requeueing: %s",
                err,
                extra={"event": "classifier_transient_error"},
            )
            await asyncio.sleep(TRANSIENT_RETRY_DELAY_SECONDS)
            await message.nack(requeue=True)
            metrics.events_processed.labels(status="retry").inc()
            return
        await message.ack()
        metrics.events_processed.labels(status="success").inc()


async def run_consumer(
    engine: Engine,
    settings: Settings,
    stop_event: asyncio.Event | None = None,
) -> None:
    connection: aio_pika.abc.AbstractRobustConnection | None = None
    while stop_event is None or not stop_event.is_set():
        try:
            connection = await aio_pika.connect_robust(settings.rabbitmq_url)
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=10)
            exchange = await channel.declare_exchange(
                EXCHANGE_NAME, aio_pika.ExchangeType.TOPIC, durable=True
            )
            dlx = await channel.declare_exchange(DLX_NAME, aio_pika.ExchangeType.TOPIC, durable=True)
            dlq = await channel.declare_queue(DLQ_NAME, durable=True)
            await dlq.bind(dlx, routing_key=DLQ_NAME)
            queue = await channel.declare_queue(
                QUEUE_NAME,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": DLX_NAME,
                    "x-dead-letter-routing-key": DLQ_NAME,
                },
            )
            await queue.bind(exchange, routing_key=ROUTING_KEY_FAILURES_EXTRACTED)
            logger.info(
                "Consuming %s from %s",
                ROUTING_KEY_FAILURES_EXTRACTED,
                QUEUE_NAME,
                extra={"event": "classifier_consumer_started"},
            )
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    if stop_event is not None and stop_event.is_set():
                        break
                    await handle_message(message, engine, settings)
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception(
                "Consumer error, reconnecting in 5s", extra={"event": "classifier_consumer_error"}
            )
            await asyncio.sleep(5.0)
        finally:
            if connection is not None and not connection.is_closed:
                await connection.close()
