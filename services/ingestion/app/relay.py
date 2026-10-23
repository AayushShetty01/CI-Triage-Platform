import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

import aio_pika
from sqlalchemy import func, select, update
from sqlalchemy.engine import Engine

from app.database import outbox

logger = logging.getLogger(__name__)
EXCHANGE_NAME = "triage.events"
ROUTING_KEYS = {"RunReceived": "run.received"}


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
            await exchange.publish(
                aio_pika.Message(
                    body=json.dumps(event["payload"]).encode("utf-8"),
                    content_type="application/json",
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    message_id=event["payload"]["event_id"],
                ),
                routing_key=routing_key,
                mandatory=True,
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
    publish_errors: Any,
) -> None:
    connection: aio_pika.abc.AbstractRobustConnection | None = None
    try:
        while True:
            try:
                pending_gauge.set(count_pending(engine))
                if connection is None or connection.is_closed:
                    connection = await aio_pika.connect_robust(rabbitmq_url)
                channel = await connection.channel(publisher_confirms=True)
                exchange = await channel.declare_exchange(
                    EXCHANGE_NAME,
                    aio_pika.ExchangeType.TOPIC,
                    durable=True,
                )
                await publish_pending_batch(engine, exchange, batch_size)
                pending_gauge.set(count_pending(engine))
                await channel.close()
            except asyncio.CancelledError:
                raise
            except Exception:
                publish_errors.inc()
                logger.exception("Outbox relay iteration failed; pending events will be retried")
                if connection is not None and not connection.is_closed:
                    try:
                        await connection.close()
                    except Exception:
                        logger.exception("Failed to close RabbitMQ connection after relay failure")
                connection = None
            await asyncio.sleep(poll_interval_seconds)
    finally:
        if connection is not None and not connection.is_closed:
            await connection.close()
