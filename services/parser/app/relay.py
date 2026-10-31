import asyncio
import json
import logging
import random
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

import aio_pika
from sqlalchemy import func, select, update
from sqlalchemy.engine import Engine

from app.database import outbox

logger = logging.getLogger(__name__)
EXCHANGE_NAME = "triage.events"
ROUTING_KEYS = {"FailuresExtracted": "failures.extracted"}


def set_outbox_metric_values(
    pending_count: int,
    oldest_pending_created_at: datetime | None,
    pending_gauge: Any,
    oldest_pending_gauge: Any,
    clock: Callable[[], datetime] | None = None,
) -> None:
    pending_gauge.set(pending_count)
    if oldest_pending_created_at is None:
        oldest_pending_gauge.set(0)
        return

    now = clock() if clock is not None else datetime.now(timezone.utc)
    if oldest_pending_created_at.tzinfo is None:
        oldest_pending_created_at = oldest_pending_created_at.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    age_seconds = max(0.0, (now - oldest_pending_created_at).total_seconds())
    oldest_pending_gauge.set(age_seconds)


def refresh_outbox_metrics(
    engine: Engine,
    pending_gauge: Any,
    oldest_pending_gauge: Any,
    clock: Callable[[], datetime] | None = None,
) -> int:
    with engine.connect() as connection:
        pending_count, oldest_pending_created_at = connection.execute(
            select(func.count(), func.min(outbox.c.created_at)).where(
                outbox.c.published_at.is_(None)
            )
        ).one()
    pending_count = int(pending_count or 0)
    set_outbox_metric_values(
        pending_count,
        oldest_pending_created_at,
        pending_gauge,
        oldest_pending_gauge,
        clock,
    )
    return pending_count


def _retry_delay(
    base_seconds: float,
    cap_seconds: float,
    attempt: int,
    random_uniform: Callable[[float, float], float],
) -> float:
    upper_bound = min(cap_seconds, base_seconds * 2**attempt)
    return random_uniform(0.0, upper_bound)


async def publish_pending_batch(
    engine: Engine,
    exchange: Any,
    batch_size: int,
) -> int:
    published = 0
    with engine.begin() as connection:
        pending_events = (
            connection.execute(
                select(outbox)
                .where(outbox.c.published_at.is_(None))
                .order_by(outbox.c.id)
                .with_for_update(skip_locked=True)
                .limit(batch_size)
            )
            .mappings()
            .all()
        )

        for event in pending_events:
            routing_key = ROUTING_KEYS.get(event["event_type"])
            if routing_key is None:
                raise ValueError(f"Unsupported outbox event type: {event['event_type']}")
            payload = dict(event["payload"])
            log_context = {
                "event_id": payload.get("event_id"),
                "run_id": payload.get("run_id"),
                "endpoint": routing_key,
            }
            logger.info(
                "Publishing FailuresExtracted outbox event",
                extra={**log_context, "event": "parser_relay_publish_attempt"},
            )
            try:
                await exchange.publish(
                    aio_pika.Message(
                        body=json.dumps(payload).encode("utf-8"),
                        content_type="application/json",
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        message_id=payload.get("event_id"),
                    ),
                    routing_key=routing_key,
                    mandatory=True,
                )
            except Exception:
                logger.exception(
                    "Parser outbox event publish failed",
                    extra={**log_context, "event": "parser_relay_publish_failed"},
                )
                raise
            logger.info(
                "Parser outbox event publish confirmed",
                extra={
                    **log_context,
                    "event": "parser_relay_publish_confirmed",
                },
            )
            connection.execute(
                update(outbox)
                .where(outbox.c.id == event["id"])
                .values(published_at=datetime.now(timezone.utc))
            )
            published += 1
    return published


async def run_outbox_relay(
    engine: Engine,
    rabbitmq_url: str,
    poll_interval_seconds: float,
    batch_size: int,
    pending_gauge: Any,
    oldest_pending_gauge: Any,
    publish_errors: Any,
    consecutive_failures_gauge: Any,
    backoff_cap_seconds: float = 30.0,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    random_uniform: Callable[[float, float], float] = random.uniform,
) -> None:
    connection: aio_pika.abc.AbstractRobustConnection | None = None
    attempt = 0
    try:
        while True:
            delay = poll_interval_seconds
            try:
                pending_count = refresh_outbox_metrics(
                    engine,
                    pending_gauge,
                    oldest_pending_gauge,
                )
                if pending_count:
                    if connection is None or connection.is_closed:
                        connection = await aio_pika.connect_robust(rabbitmq_url)
                    channel = await connection.channel(publisher_confirms=True)
                    exchange = await channel.declare_exchange(
                        EXCHANGE_NAME,
                        aio_pika.ExchangeType.TOPIC,
                        durable=True,
                    )
                    await publish_pending_batch(engine, exchange, batch_size)
                    attempt = 0
                    consecutive_failures_gauge.set(0)
                    refresh_outbox_metrics(
                        engine,
                        pending_gauge,
                        oldest_pending_gauge,
                    )
                    await channel.close()
            except asyncio.CancelledError:
                raise
            except Exception:
                delay = _retry_delay(
                    poll_interval_seconds,
                    backoff_cap_seconds,
                    attempt,
                    random_uniform,
                )
                attempt += 1
                publish_errors.inc()
                consecutive_failures_gauge.set(attempt)
                logger.exception(
                    "Parser outbox relay iteration failed",
                    extra={
                        "event": "parser_relay_cycle_failed",
                        "consecutive_failures": attempt,
                        "backoff_seconds": delay,
                    },
                )
            await sleep(delay)
    finally:
        if connection is not None and not connection.is_closed:
            await connection.close()
