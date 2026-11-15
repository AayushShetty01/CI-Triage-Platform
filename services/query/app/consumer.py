# Consumes three event types so the read model can show accepted, pending,
# and terminal classification states even when messages arrive out of order.

import asyncio
import json
import logging
from typing import Any

import aio_pika
import httpx
from sqlalchemy.engine import Engine

from app.projection import apply_event, has_event, validate_event

logger = logging.getLogger(__name__)
EXCHANGE = "triage.events"
QUEUE = "query.events"
DLX = "triage.dlx"
DLQ = "query.events.dlq"
ROUTING_KEYS = ("run.received", "failures.extracted", "failure.classified")


def validate_parser_results(rows: Any) -> None:
    if not isinstance(rows, list):
        raise ValueError("Parser returned non-list results")
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Parser returned invalid test result")
        for field in ("test_id", "test_name", "class_name"):
            if not isinstance(row.get(field), str) or len(row[field]) > 255:
                raise ValueError(f"Parser returned invalid {field}")
        if not row["test_id"] or not row["test_name"]:
            raise ValueError("Parser returned empty test identity")
        if not isinstance(row.get("status"), str) or row["status"] not in {"passed", "failed", "error", "skipped"}:
            raise ValueError("Parser returned invalid test status")


async def handle_message(message: Any, engine: Engine, parser_url: str, metrics: Any) -> None:
    try:
        event = json.loads(message.body.decode("utf-8"))
        validate_event(event)
    except (UnicodeError, json.JSONDecodeError, ValueError, TypeError) as exc:
        logger.warning("Rejecting invalid event: %s", exc)
        metrics.events.labels(result="poison").inc()
        await message.reject(requeue=False)
        return

    if has_event(engine, event["event_id"]):
        metrics.events.labels(result="duplicate").inc()
        await message.ack()
        return

    test_results = None
    if event["event_type"] == "FailuresExtracted":
        try:
            async with httpx.AsyncClient(base_url=parser_url, timeout=5.0) as client:
                response = await client.get(f"/runs/{event['run_id']}/results")
                response.raise_for_status()
                test_results = response.json()
                validate_parser_results(test_results)
        except ValueError as exc:
            logger.error("Rejecting malformed Parser response for %s: %s", event["run_id"], exc)
            metrics.events.labels(result="poison").inc()
            await message.reject(requeue=False)
            return
        except httpx.HTTPError as exc:
            logger.warning("Parser history unavailable for %s: %s", event["run_id"], exc)
            metrics.events.labels(result="retry").inc()
            await asyncio.sleep(2)
            await message.nack(requeue=True)
            return

    try:
        applied = apply_event(engine, event, test_results)
    except Exception:
        logger.exception("Projection failed for event %s", event["event_id"])
        metrics.events.labels(result="retry").inc()
        await asyncio.sleep(2)
        await message.nack(requeue=True)
        return
    metrics.events.labels(result="applied" if applied else "duplicate").inc()
    await message.ack()


async def run_consumer(engine: Engine, rabbitmq_url: str, parser_url: str,
                       metrics: Any, ready: asyncio.Event) -> None:
    while True:
        try:
            connection = await aio_pika.connect_robust(rabbitmq_url)
            async with connection:
                channel = await connection.channel()
                await channel.set_qos(prefetch_count=10)
                exchange = await channel.declare_exchange(EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)
                dlx = await channel.declare_exchange(DLX, aio_pika.ExchangeType.TOPIC, durable=True)
                dead = await channel.declare_queue(DLQ, durable=True)
                await dead.bind(dlx, routing_key=DLQ)
                queue = await channel.declare_queue(QUEUE, durable=True, arguments={
                    "x-dead-letter-exchange": DLX, "x-dead-letter-routing-key": DLQ,
                })
                for key in ROUTING_KEYS:
                    await queue.bind(exchange, routing_key=key)
                ready.set()
                metrics.consumer_ready.set(1)
                async with queue.iterator() as messages:
                    async for message in messages:
                        await handle_message(message, engine, parser_url, metrics)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Query consumer disconnected; reconnecting")
            await asyncio.sleep(5)
        finally:
            ready.clear()
            metrics.consumer_ready.set(0)
