import asyncio
import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from threading import Event

import aio_pika
import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from jsonschema import Draft202012Validator, FormatChecker
from sqlalchemy import create_engine, delete, insert, select
from sqlalchemy.engine import Engine

from app.database import outbox, runs
from app.main import create_app

SERVICE_DIR = Path(__file__).resolve().parents[1]
EVENT_SCHEMA_PATH = (
    SERVICE_DIR.parents[1] / "contracts" / "events" / "run_received.schema.json"
)
EVENT_SCHEMA = json.loads(EVENT_SCHEMA_PATH.read_text(encoding="utf-8"))
EVENT_VALIDATOR = Draft202012Validator(
    EVENT_SCHEMA,
    format_checker=FormatChecker(),
)
REALISTIC_JUNIT_XML = b"""<?xml version="1.0"?>
<testsuites name="payments-ci" tests="2" failures="1">
  <testsuite name="checkout" tests="2" failures="1">
    <testcase classname="payments.CheckoutTests" name="rejects_expired_card">
      <failure message="expected HTTP 402, got 200">AssertionError</failure>
    </testcase>
    <testcase classname="payments.CheckoutTests" name="accepts_valid_card" />
  </testsuite>
</testsuites>
"""


def _prepare_postgres_database(database_url: str) -> Engine:
    engine = create_engine(database_url, pool_pre_ping=True)
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS ingestion")

    config = Config(str(SERVICE_DIR / "alembic.ini"))
    config.set_main_option("script_location", str(SERVICE_DIR / "migrations"))
    config.config_file_name = None
    previous_database_url = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = database_url
    try:
        command.upgrade(config, "head")
    finally:
        if previous_database_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous_database_url
    return engine


@pytest.fixture(scope="session")
def postgres_engine() -> Iterator[Engine]:
    database_url = os.getenv("INGESTION_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("Set INGESTION_TEST_DATABASE_URL to run PostgreSQL integration tests")

    engine = _prepare_postgres_database(database_url)
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def integration_services() -> Iterator[tuple[Engine, str, str]]:
    database_url = os.getenv("INGESTION_TEST_DATABASE_URL")
    rabbitmq_url = os.getenv("INGESTION_TEST_RABBITMQ_URL")
    if not database_url or not rabbitmq_url:
        pytest.skip("Set integration database and RabbitMQ URLs to run service integration tests")
    assert database_url is not None
    assert rabbitmq_url is not None

    engine = _prepare_postgres_database(database_url)
    yield engine, database_url, rabbitmq_url
    engine.dispose()


async def _create_event_queue(rabbitmq_url: str, queue_name: str) -> None:
    connection = await aio_pika.connect_robust(rabbitmq_url)
    try:
        channel = await connection.channel()
        exchange = await channel.declare_exchange(
            "triage.events",
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )
        queue = await channel.declare_queue(queue_name, durable=False)
        await queue.bind(exchange, routing_key="run.received")
    finally:
        await connection.close()


async def _receive_event(rabbitmq_url: str, queue_name: str) -> dict[str, object]:
    connection = await aio_pika.connect_robust(rabbitmq_url)
    try:
        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name, durable=False)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            message = await queue.get(fail=False)
            if message is not None:
                async with message.process():
                    return json.loads(message.body)
            await asyncio.sleep(0.1)
        pytest.fail("The RunReceived event was not delivered to RabbitMQ within 15 seconds")
    finally:
        await connection.close()


async def _delete_event_queue(rabbitmq_url: str, queue_name: str) -> None:
    connection = await aio_pika.connect_robust(rabbitmq_url)
    try:
        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name, durable=False)
        await queue.delete()
    finally:
        await connection.close()


def _wait_for_published_event(engine: Engine, run_id: str) -> None:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        with engine.connect() as connection:
            events = (
                connection.execute(
                    select(outbox).where(outbox.c.payload["run_id"].as_string() == run_id)
                )
                .mappings()
                .all()
            )
        if len(events) == 1 and events[0]["published_at"] is not None:
            return
        time.sleep(0.1)
    pytest.fail("The outbox event was not marked published within 15 seconds")


def test_upload_persists_report_and_publishes_confirmed_event(
    integration_services: tuple[Engine, str, str],
) -> None:
    engine, database_url, rabbitmq_url = integration_services
    queue_name = f"ingestion-integration-{uuid.uuid4().hex}"
    asyncio.run(_create_event_queue(rabbitmq_url, queue_name))

    try:
        app = create_app(
            database_url=database_url,
            rabbitmq_url=rabbitmq_url,
            max_report_size_bytes=1024 * 1024,
        )
        commit_sha = uuid.uuid4().hex
        with TestClient(app) as client:
            response = client.post(
                "/runs",
                content=REALISTIC_JUNIT_XML,
                headers={
                    "Content-Type": "application/xml",
                    "X-Commit-Sha": commit_sha,
                    "X-Branch": "feature/checkout",
                },
            )
            assert response.status_code == 202
            run_id = response.json()["run_id"]

            duplicate = client.post(
                "/runs",
                content=REALISTIC_JUNIT_XML,
                headers={
                    "Content-Type": "application/xml",
                    "X-Commit-Sha": commit_sha,
                    "X-Branch": "feature/checkout",
                },
            )
            assert duplicate.status_code == 202
            assert duplicate.json()["run_id"] == run_id

            raw_response = client.get(f"/internal/runs/{run_id}/raw")
            assert raw_response.status_code == 200
            assert raw_response.content == REALISTIC_JUNIT_XML

            event = asyncio.run(_receive_event(rabbitmq_url, queue_name))
            EVENT_VALIDATOR.validate(event)
            assert event["event_type"] == "RunReceived"
            assert event["run_id"] == run_id
            assert event["payload"]["commit_sha"] == commit_sha
            assert event["payload"]["branch"] == "feature/checkout"
            assert event["payload"]["raw_ref"] == f"/internal/runs/{run_id}/raw"

            with engine.connect() as connection:
                stored_run = connection.execute(
                    select(runs).where(runs.c.run_id == run_id)
                ).mappings().one()
            _wait_for_published_event(engine, run_id)
            with engine.connect() as connection:
                stored_events = connection.execute(
                    select(outbox).where(outbox.c.payload["run_id"].as_string() == run_id)
                ).mappings().all()
            assert stored_run["raw_xml"] == REALISTIC_JUNIT_XML
            assert stored_run["status"] == "received"
            assert len(stored_events) == 1
            assert stored_events[0]["published_at"] is not None
    finally:
        asyncio.run(_delete_event_queue(rabbitmq_url, queue_name))


def test_postgres_skip_locked_prevents_overlapping_relay_batches(
    postgres_engine: Engine,
) -> None:
    engine = postgres_engine
    now = datetime.now(timezone.utc)
    test_run_ids = [uuid.uuid4().hex for _ in range(10)]
    with engine.begin() as connection:
        row_ids = connection.execute(
            insert(outbox)
            .values(
                [
                    {
                        "event_type": "RunReceived",
                        "payload": {
                            "event_id": str(uuid.uuid4()),
                            "event_type": "RunReceived",
                            "occurred_at": now.isoformat(),
                            "schema_version": "1.0",
                            "run_id": run_id,
                            "payload": {},
                        },
                        "created_at": now,
                        "published_at": None,
                    }
                    for run_id in test_run_ids
                ]
            )
            .returning(outbox.c.id)
        ).scalars().all()

    locked_rows_selected = Event()
    release_first_batch = Event()

    def select_batch(hold_locks: bool) -> list[int]:
        with engine.connect() as connection:
            with connection.begin():
                selected = connection.execute(
                    select(outbox.c.id)
                    .where(outbox.c.id.in_(row_ids), outbox.c.published_at.is_(None))
                    .order_by(outbox.c.id)
                    .with_for_update(skip_locked=True)
                    .limit(10)
                ).scalars().all()
                if hold_locks:
                    locked_rows_selected.set()
                    if not release_first_batch.wait(timeout=10):
                        raise TimeoutError("Timed out waiting to release the first relay batch")
                return list(selected)

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            first_batch = executor.submit(select_batch, True)
            assert locked_rows_selected.wait(timeout=10)
            second_batch = executor.submit(select_batch, False)
            second_ids = second_batch.result(timeout=10)
            release_first_batch.set()
            first_ids = first_batch.result(timeout=10)

        assert set(first_ids).isdisjoint(second_ids)
        assert set(first_ids) | set(second_ids) == set(row_ids)
    finally:
        release_first_batch.set()
        with engine.begin() as connection:
            connection.execute(delete(outbox).where(outbox.c.id.in_(row_ids)))
