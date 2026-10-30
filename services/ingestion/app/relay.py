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
ROUTING_KEYS = {"RunReceived": "run.received"}


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

    # Age is row-created UTC time compared with an injectable application UTC clock.
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
    pending_count = int(pending_count)
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
            payload_body = dict(payload.get("payload", {}))
            payload_body["raw_ref"] = f"/internal/runs/{payload['run_id']}/raw"
            payload["payload"] = payload_body
            log_context = {
                "event_id": payload["event_id"],
                "run_id": payload["run_id"],
                "endpoint": routing_key,
            }
            logger.info(
                "Publishing outbox event",
                extra={**log_context, "event": "relay_publish_attempt"},
            )
            try:
                await exchange.publish(
                    aio_pika.Message(
                        body=json.dumps(payload).encode("utf-8"),
                        content_type="application/json",
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        message_id=payload["event_id"],
                    ),
                    routing_key=routing_key,
                    mandatory=True,
                )
            except Exception:
                logger.exception(
                    "Outbox event publish failed",
                    extra={**log_context, "event": "relay_publish_failed"},
                )
                raise
            logger.info(
                "Outbox event publish confirmed",
                extra={
                    **log_context,
                    "event": "relay_publish_confirmed",
                },
            )
            connection.execute(
                update(outbox)
                .where(outbox.c.id == event["id"])
                .values(published_at=datetime.now(timezone.utc))
            )
            published += 1
    return published


def count_pending(engine: Engine) -> int:
    with engine.connect() as connection:
        result = connection.execute(
            select(func.count()).select_from(outbox).where(outbox.c.published_at.is_(None))
        )
        return int(result.scalar_one())


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
                consecutive_failures_gauge.set(attempt)
                publish_errors.inc()
                logger.exception(
                    "Outbox relay iteration failed; pending events will be retried",
                    extra={
                        "event": "relay_backoff",
                        "consecutive_failures": attempt,
                        "backoff_seconds": delay,
                    },
                )
                if connection is not None and not connection.is_closed:
                    try:
                        await connection.close()
                    except Exception:
                        logger.exception(
                            "Failed to close RabbitMQ connection after relay failure",
                            extra={"event": "relay_connection_close_failed"},
                        )
                connection = None
            await sleep(delay)
    finally:
        if connection is not None and not connection.is_closed:
            await connection.close()
