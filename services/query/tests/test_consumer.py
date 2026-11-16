import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import httpx
from sqlalchemy import func, select

from app import consumer
from app.database import tests
from tests.conftest import create_test_engine


class Message:
    def __init__(self, value):
        self.body = json.dumps(value).encode() if isinstance(value, dict) else value
        self.acked = self.rejected = self.nacked = False

    async def ack(self):
        self.acked = True

    async def reject(self, requeue):
        self.rejected = True
        assert requeue is False

    async def nack(self, requeue):
        self.nacked = True
        assert requeue is True


def engine():
    return create_test_engine()


def metrics():
    counter = SimpleNamespace(inc=Mock())
    return SimpleNamespace(events=SimpleNamespace(labels=Mock(return_value=counter)))


def extracted():
    return {"event_id": str(uuid4()), "event_type": "FailuresExtracted",
            "schema_version": "1.0",
            "occurred_at": datetime.now(timezone.utc).isoformat(), "run_id": "a" * 64,
            "payload": {"commit_sha": "abc", "branch": "main", "total_tests": 1,
                        "failed_tests": 0, "passed_tests": 1, "skipped_tests": 0, "failures": []}}


def test_consumer_fetches_parser_history_once_and_deduplicates(monkeypatch):
    db = engine()
    calls = []

    class Client:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get(self, path):
            calls.append(path)
            return httpx.Response(200, json=[{"test_id": "pkg.Test::case", "test_name": "case",
                                              "class_name": "pkg.Test", "status": "passed"}],
                                  request=httpx.Request("GET", "http://parser" + path))

    monkeypatch.setattr(consumer.httpx, "AsyncClient", Client)
    value = extracted()
    first, second = Message(value), Message(value)
    asyncio.run(consumer.handle_message(first, db, "http://parser", metrics()))
    asyncio.run(consumer.handle_message(second, db, "http://parser", metrics()))
    assert first.acked and second.acked
    assert len(calls) == 1
    with db.connect() as conn:
        assert conn.scalar(select(func.count()).select_from(tests)) == 1
    db.dispose()


def test_consumer_requeues_when_parser_unavailable(monkeypatch):
    db = engine()

    class Client:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get(self, path):
            raise httpx.ConnectError("parser down")

    async def no_sleep(seconds):
        assert seconds == 2

    monkeypatch.setattr(consumer.httpx, "AsyncClient", Client)
    monkeypatch.setattr(consumer.asyncio, "sleep", no_sleep)
    message = Message(extracted())
    asyncio.run(consumer.handle_message(message, db, "http://parser", metrics()))
    assert message.nacked and not message.acked
    db.dispose()


def test_consumer_sends_invalid_event_to_dlq():
    db = engine()
    message = Message(b"not json")
    asyncio.run(consumer.handle_message(message, db, "http://parser", metrics()))
    assert message.rejected and not message.acked
    db.dispose()


def test_consumer_sends_invalid_payload_to_dlq():
    db = engine()
    value = extracted()
    value["payload"]["failed_tests"] = "one"
    message = Message(value)
    asyncio.run(consumer.handle_message(message, db, "http://parser", metrics()))
    assert message.rejected and not message.nacked
    db.dispose()


def test_consumer_sends_invalid_parser_result_to_dlq(monkeypatch):
    db = engine()

    class Client:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get(self, path):
            return httpx.Response(200, json=[{"status": "passed"}],
                                  request=httpx.Request("GET", "http://parser" + path))

    monkeypatch.setattr(consumer.httpx, "AsyncClient", Client)
    message = Message(extracted())
    asyncio.run(consumer.handle_message(message, db, "http://parser", metrics()))
    assert message.rejected and not message.nacked
    db.dispose()
