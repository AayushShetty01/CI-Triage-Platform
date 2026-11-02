import asyncio
import json
import logging
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

import aio_pika
from sqlalchemy import create_engine, insert, select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine

from app.database import outbox, parsed_runs, test_results
from app.ingestion_client import (
    IngestionClient,
    IngestionPermanentError,
    IngestionTransientError,
)
from app.logging_config import event_id_context, run_id_context
from app.xml_parser import XmlParseError, parse_junit_xml

logger = logging.getLogger(__name__)

EXCHANGE_NAME = "triage.events"
ROUTING_KEY_RUN_RECEIVED = "run.received"
QUEUE_NAME = "parser.run_received"
DLX_NAME = "triage.dlx"
DLQ_NAME = "parser.run_received.dlq"


async def process_run_received_event(
    event_data: dict[str, Any],
    engine: Engine,
    ingestion_client: IngestionClient,
) -> bool:
    """Process a single RunReceived event idempotently.
    
    Returns True if processed or already processed, False if permanent failure.
    """
    event_id = event_data.get("event_id")
    run_id = event_data.get("run_id")
    event_type = event_data.get("event_type")
    payload = event_data.get("payload") or {}

    if not run_id or event_type != "RunReceived":
        logger.error(
            "Invalid event format or unexpected event_type: %s",
            event_type,
            extra={"event": "parser_invalid_event", "event_id": event_id, "run_id": run_id},
        )
        raise ValueError(f"Invalid RunReceived event: run_id={run_id}, event_type={event_type}")

    commit_sha = payload.get("commit_sha", "")
    branch = payload.get("branch", "")
    raw_ref = payload.get("raw_ref") or f"/internal/runs/{run_id}/raw"

    event_id_context.set(event_id)
    run_id_context.set(run_id)

    log_ctx = {
        "event_id": event_id,
        "run_id": run_id,
        "commit_sha": commit_sha,
        "branch": branch,
    }

    # 1. Idempotency check: Has this run_id already been completed?
    with engine.connect() as conn:
        existing = conn.execute(
            select(parsed_runs.c.run_id, parsed_runs.c.status).where(
                parsed_runs.c.run_id == run_id
            )
        ).one_or_none()
        if existing is not None and existing.status == "completed":
            logger.info(
                "Run already processed, skipping duplicate event",
                extra={**log_ctx, "event": "parser_duplicate_run_ignored"},
            )
            return True

    # 2. Fetch raw XML from Ingestion service
    logger.info("Fetching raw report XML from Ingestion", extra={**log_ctx, "event": "parser_fetch_xml"})
    raw_xml = await ingestion_client.get_raw_xml(raw_ref)

    # 3. Parse XML report
    logger.info("Parsing JUnit XML report", extra={**log_ctx, "event": "parser_parse_xml"})
    summary = parse_junit_xml(raw_xml)

    now = datetime.now(timezone.utc)

    # 4. Save parsed results & outbox event in one database transaction
    with engine.begin() as conn:
        # Check again under lock/transaction
        existing = conn.execute(
            select(parsed_runs.c.run_id).where(parsed_runs.c.run_id == run_id)
        ).scalar_one_or_none()

        if existing is None:
            conn.execute(
                insert(parsed_runs).values(
                    run_id=run_id,
                    commit_sha=commit_sha,
                    branch=branch,
                    total_tests=summary.total_tests,
                    passed_tests=summary.passed_tests,
                    failed_tests=summary.failed_tests,
                    skipped_tests=summary.skipped_tests,
                    error_tests=summary.error_tests,
                    status="completed",
                    parsed_at=now,
                )
            )

        # Idempotent insert of test cases using ON CONFLICT DO NOTHING
        is_postgres = conn.dialect.name == "postgresql"
        insert_fn = postgresql_insert if is_postgres else sqlite_insert

        test_records = [
            {
                "run_id": run_id,
                "test_id": tc.test_id,
                "test_name": tc.test_name,
                "class_name": tc.class_name,
                "file_path": tc.file_path,
                "duration_seconds": tc.duration_seconds,
                "status": tc.status,
                "error_type": tc.error_type,
                "error_message": tc.error_message,
                "stack_trace": tc.stack_trace,
                "created_at": now,
            }
            for tc in summary.test_cases
        ]

        if test_records:
            # Insert in chunks if needed
            stmt = insert_fn(test_results).values(test_records)
            stmt = stmt.on_conflict_do_nothing(index_elements=["run_id", "test_id"])
            conn.execute(stmt)

        # 5. Build and store FailuresExtracted outbox event
        failures_payload = [
            {
                "test_id": tc.test_id,
                "test_name": tc.test_name,
                "class_name": tc.class_name,
                "file_path": tc.file_path,
                "duration_seconds": tc.duration_seconds,
                "error_type": tc.error_type,
                "error_message": tc.error_message,
                "stack_trace": tc.stack_trace,
            }
            for tc in summary.failures
        ]

        event_payload = {
            "event_id": str(uuid.uuid4()),
            "event_type": "FailuresExtracted",
            "occurred_at": now.isoformat(),
            "schema_version": "1.0",
            "run_id": run_id,
            "payload": {
                "commit_sha": commit_sha,
                "branch": branch,
                "total_tests": summary.total_tests,
                "failed_tests": summary.failed_tests,
                "passed_tests": summary.passed_tests,
                "skipped_tests": summary.skipped_tests,
                "failures": failures_payload,
            },
        }

        # Check if outbox event already exists for this run_id
        # We store it only once per run
        existing_outbox = conn.execute(
            select(outbox.c.id).where(
                outbox.c.event_type == "FailuresExtracted",
                outbox.c.payload["run_id"].as_string() == run_id
                if is_postgres
                else outbox.c.payload.like(f"%{run_id}%"),
            )
        ).first()

        if existing_outbox is None:
            conn.execute(
                insert(outbox).values(
                    event_type="FailuresExtracted",
                    payload=event_payload,
                    created_at=now,
                )
            )

    logger.info(
        "Successfully parsed run report and staged FailuresExtracted event",
        extra={
            **log_ctx,
            "event": "parser_run_completed",
            "test_count": summary.total_tests,
            "failure_count": summary.failed_tests,
        },
    )
    return True


async def handle_message(
    message: aio_pika.abc.AbstractIncomingMessage,
    engine: Engine,
    ingestion_client: IngestionClient,
    metrics: dict[str, Any] | None = None,
    max_retries: int = 3,
) -> None:
    async with message.process(requeue=False, ignore_processed=True):
        if metrics and "consumed_total" in metrics:
            metrics["consumed_total"].inc()

        try:
            body = message.body.decode("utf-8")
            event_data = json.loads(body)
        except Exception as err:
            logger.exception(
                "Poison message: failed to decode JSON body. Sending to DLQ.",
                extra={"event": "parser_poison_message_json"},
            )
            # Message rejected without requeue -> routes to dead letter exchange
            await message.reject(requeue=False)
            if metrics and "poison_messages_total" in metrics:
                metrics["poison_messages_total"].inc()
            return

        try:
            await process_run_received_event(event_data, engine, ingestion_client)
            await message.ack()
            if metrics and "processed_total" in metrics:
                metrics["processed_total"].labels(status="success").inc()
        except XmlParseError as err:
            # Corrupted / invalid XML report in Ingestion is a poison payload
            logger.error(
                "Poison message: invalid JUnit XML. Rejecting to DLQ: %s",
                err,
                extra={"event": "parser_poison_xml"},
            )
            await message.reject(requeue=False)
            if metrics and "poison_messages_total" in metrics:
                metrics["poison_messages_total"].inc()
        except IngestionPermanentError as err:
            # 404 or unrecoverable client error
            logger.error(
                "Permanent Ingestion error. Rejecting to DLQ: %s",
                err,
                extra={"event": "parser_permanent_ingestion_error"},
            )
            await message.reject(requeue=False)
            if metrics and "poison_messages_total" in metrics:
                metrics["poison_messages_total"].inc()
        except (IngestionTransientError, Exception) as err:
            # Transient network/broker/DB error: requeue message
            logger.warning(
                "Transient error processing event; requeueing: %s",
                err,
                extra={"event": "parser_transient_error"},
            )
            await message.nack(requeue=True)
            if metrics and "processed_total" in metrics:
                metrics["processed_total"].labels(status="retry").inc()


async def run_consumer(
    engine: Engine,
    rabbitmq_url: str,
    ingestion_client: IngestionClient,
    metrics: dict[str, Any] | None = None,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Connect to RabbitMQ and continuously consume RunReceived messages."""
    connection: aio_pika.abc.AbstractRobustConnection | None = None
    while stop_event is None or not stop_event.is_set():
        try:
            logger.info("Connecting consumer to RabbitMQ at %s", rabbitmq_url)
            connection = await aio_pika.connect_robust(rabbitmq_url)
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=10)

            # Declare topic exchange
            exchange = await channel.declare_exchange(
                EXCHANGE_NAME,
                aio_pika.ExchangeType.TOPIC,
                durable=True,
            )

            # Declare Dead Letter Exchange and Queue
            dlx = await channel.declare_exchange(
                DLX_NAME,
                aio_pika.ExchangeType.TOPIC,
                durable=True,
            )
            dlq = await channel.declare_queue(DLQ_NAME, durable=True)
            await dlq.bind(dlx, routing_key="#")

            # Declare parser queue with dead-letter routing to DLX
            queue = await channel.declare_queue(
                QUEUE_NAME,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": DLX_NAME,
                    "x-dead-letter-routing-key": DLQ_NAME,
                },
            )

            await queue.bind(exchange, routing_key=ROUTING_KEY_RUN_RECEIVED)
            logger.info("Bound queue %s to %s with routing_key %s", QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY_RUN_RECEIVED)

            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    if stop_event is not None and stop_event.is_set():
                        break
                    await handle_message(message, engine, ingestion_client, metrics=metrics)

        except asyncio.CancelledError:
            logger.info("Consumer cancelled, shutting down")
            break
        except Exception:
            logger.exception("Consumer encountered unexpected error, reconnecting in 5s...")
            await asyncio.sleep(5.0)
        finally:
            if connection is not None and not connection.is_closed:
                await connection.close()
