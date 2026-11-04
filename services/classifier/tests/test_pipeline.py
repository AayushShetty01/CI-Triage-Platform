"""Consumer + LLM worker behaviour against SQLite, with a fake Ollama."""

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import httpx
from jsonschema import Draft202012Validator, FormatChecker
from sqlalchemy import select, update

from app.circuit_breaker import OPEN
from app.consumer import handle_message, process_failures_extracted_event
from app.database import classifications, failure_groups, llm_jobs, outbox
from app.llm_client import OllamaClient
from app.llm_worker import LLMWorkerPool
from tests.conftest import CONTRACTS_DIR, RUN_A, RUN_B, failure, failures_extracted

VALIDATOR = Draft202012Validator(
    json.loads((CONTRACTS_DIR / "failure_classified.schema.json").read_text()),
    format_checker=FormatChecker(),
)
EXTRACTED_VALIDATOR = Draft202012Validator(
    json.loads((CONTRACTS_DIR / "failures_extracted.schema.json").read_text()),
    format_checker=FormatChecker(),
)

UNKNOWN_1 = failure("orders.Tests::creates_order", "KeyError", "'customer_id' missing for order 17")
UNKNOWN_2 = failure("orders.Tests::updates_order", "KeyError", "'customer_id' missing for order 99")


def rows(engine, table, *where):
    with engine.connect() as conn:
        return [dict(r) for r in conn.execute(select(table).where(*where)).mappings()]


def events(engine):
    return [r["payload"] for r in rows(engine, outbox)]


def make_pool(engine, settings, fake_ollama, clock=None):
    client = OllamaClient("http://ollama", settings.ollama_model, transport=fake_ollama.transport)
    kwargs = {"clock": clock} if clock else {}
    return LLMWorkerPool(engine, client, settings, **kwargs)


async def test_rule_matches_are_classified_immediately(engine, settings):
    event = failures_extracted(
        RUN_A,
        [
            failure("p.T::a", "AssertionError", "expected HTTP 402, got 200"),
            failure("db.T::b", "TimeoutError", "DB connection timed out after 30s"),
            failure("x.T::c", None, None),
        ],
    )
    EXTRACTED_VALIDATOR.validate(event)
    result = await process_failures_extracted_event(event, engine, settings)
    assert (result.rule, result.pending) == (3, 0)

    by_test = {r["test_id"]: r for r in rows(engine, classifications)}
    assert by_test["p.T::a"]["category"] == "assertion"
    assert by_test["db.T::b"]["category"] == "timeout"
    assert by_test["x.T::c"]["rule_id"] == "no_signal"
    assert all(r["status"] == "classified" and r["source"] == "rule" for r in by_test.values())

    published = events(engine)
    assert len(published) == 3
    for e in published:
        VALIDATOR.validate(e)
    assert rows(engine, llm_jobs) == []


async def test_duplicate_event_is_a_no_op(engine, settings):
    event = failures_extracted(RUN_A, [UNKNOWN_1, failure("p.T::a", "AssertionError", "boom")])
    await process_failures_extracted_event(event, engine, settings)
    again = await process_failures_extracted_event(event, engine, settings)
    assert again.duplicate
    # Same run under a new event_id is also not double counted.
    await process_failures_extracted_event({**event, "event_id": "other-id"}, engine, settings)

    assert len(rows(engine, classifications)) == 2
    assert len(events(engine)) == 1
    assert len(rows(engine, llm_jobs)) == 1
    assert rows(engine, failure_groups, failure_groups.c.error_type == "KeyError")[0]["occurrence_count"] == 1


async def test_similar_failures_share_one_llm_call(engine, settings, fake_ollama):
    await process_failures_extracted_event(failures_extracted(RUN_A, [UNKNOWN_1, UNKNOWN_2]), engine, settings)
    jobs = rows(engine, llm_jobs)
    assert len(jobs) == 1
    assert {r["status"] for r in rows(engine, classifications)} == {"pending"}
    assert events(engine) == []

    pool = make_pool(engine, settings, fake_ollama)
    assert await pool.process_one()
    assert not await pool.process_one()  # nothing left
    assert len(fake_ollama.calls) == 1

    done = rows(engine, classifications)
    assert {(r["status"], r["category"], r["source"]) for r in done} == {("classified", "flaky", "llm")}
    assert all(r["model"] == "test-model" and r["prompt_version"] == "v1" and r["prompt_hash"] for r in done)
    published = events(engine)
    assert len(published) == 2
    for e in published:
        VALIDATOR.validate(e)

    # A later run with the same fingerprint is answered from the cache: no LLM call.
    third = failure("orders.Tests::deletes_order", "KeyError", "'customer_id' missing for order 5")
    result = await process_failures_extracted_event(failures_extracted(RUN_B, [third]), engine, settings)
    assert result.cached == 1
    cached = rows(engine, classifications, classifications.c.run_id == RUN_B)[0]
    assert (cached["status"], cached["source"], cached["category"]) == ("classified", "llm_cache", "flaky")
    assert len(fake_ollama.calls) == 1


async def test_llm_down_retries_then_degrades(engine, settings, fake_ollama):
    settings.breaker_failure_threshold = 100  # isolate retry behaviour from the breaker
    fake_ollama.fail_with = httpx.ConnectError("connection refused")
    await process_failures_extracted_event(failures_extracted(RUN_A, [UNKNOWN_1]), engine, settings)
    pool = make_pool(engine, settings, fake_ollama)

    for attempt in range(1, settings.llm_max_attempts + 1):
        assert await pool.process_one()
        job = rows(engine, llm_jobs)[0]
        assert job["attempts"] == attempt

    job = rows(engine, llm_jobs)[0]
    assert job["status"] == "failed"
    row = rows(engine, classifications)[0]
    assert (row["status"], row["category"], row["source"]) == ("degraded", "unknown", "fallback")
    assert "LLM unavailable after 3 attempts" in row["reason"]
    VALIDATOR.validate(events(engine)[0])

    # When the same failure shows up again, the LLM gets a fresh chance.
    fake_ollama.fail_with = None
    again = failure("orders.Tests::creates_order", "KeyError", "'customer_id' missing for order 3")
    await process_failures_extracted_event(failures_extracted(RUN_B, [again]), engine, settings)
    assert rows(engine, llm_jobs)[0]["status"] == "pending"
    assert await pool.process_one()
    assert rows(engine, classifications, classifications.c.run_id == RUN_B)[0]["category"] == "flaky"


async def test_open_breaker_stops_calls_and_stale_jobs_degrade(engine, settings, fake_ollama):
    now = [datetime(2026, 1, 1, tzinfo=timezone.utc)]
    fake_ollama.fail_with = httpx.ReadTimeout("slow")
    pool = make_pool(engine, settings, fake_ollama, clock=lambda: now[0])

    other = failure("pay.T::refund", "ValueError", "refund amount mismatch")
    await process_failures_extracted_event(failures_extracted(RUN_A, [UNKNOWN_1, other]), engine, settings)
    with engine.begin() as conn:  # jobs were queued "now" by the consumer's real clock
        conn.execute(update(llm_jobs).values(queued_at=now[0], next_attempt_at=now[0]))

    assert await pool.process_one()
    now[0] += timedelta(seconds=1)
    assert await pool.process_one()
    assert pool.breaker.state == OPEN  # threshold is 2
    calls_when_opened = len(fake_ollama.calls)

    now[0] += timedelta(seconds=5)
    assert not await pool.process_one()
    assert len(fake_ollama.calls) == calls_when_opened

    # Not stale yet: nothing degraded.
    assert pool.degrade_stale_jobs() == 0
    now[0] += timedelta(seconds=settings.llm_degrade_after_seconds)
    assert pool.degrade_stale_jobs() == 2
    assert {r["status"] for r in rows(engine, classifications)} == {"degraded"}


async def test_busy_but_healthy_llm_does_not_degrade(engine, settings, fake_ollama):
    now = [datetime.now(timezone.utc) + timedelta(hours=1)]
    pool = make_pool(engine, settings, fake_ollama, clock=lambda: now[0])
    await process_failures_extracted_event(failures_extracted(RUN_A, [UNKNOWN_1]), engine, settings)
    assert pool.degrade_stale_jobs() == 0
    assert rows(engine, classifications)[0]["status"] == "pending"


async def test_invalid_model_output_becomes_unknown(engine, settings, fake_ollama):
    fake_ollama.reply = '{"category": "run_shell", "reason": "pwned"}'
    await process_failures_extracted_event(failures_extracted(RUN_A, [UNKNOWN_1]), engine, settings)
    pool = make_pool(engine, settings, fake_ollama)
    assert await pool.process_one()
    row = rows(engine, classifications)[0]
    assert (row["status"], row["category"], row["source"]) == ("classified", "unknown", "llm")
    assert row["reason"] == "Model output failed validation"
    assert pool.breaker.state == "closed"


async def test_crashed_worker_lease_is_reclaimed(engine, settings, fake_ollama):
    await process_failures_extracted_event(failures_extracted(RUN_A, [UNKNOWN_1]), engine, settings)
    now = [datetime.now(timezone.utc)]
    pool = make_pool(engine, settings, fake_ollama, clock=lambda: now[0])
    claimed = pool.claim_job()  # simulate a worker that claims and then dies
    assert claimed is not None
    assert pool.claim_job() is None  # lease still held

    now[0] += timedelta(seconds=settings.llm_timeout_seconds + 11)
    assert await pool.process_one()
    assert rows(engine, classifications)[0]["status"] == "classified"
    assert rows(engine, llm_jobs)[0]["attempts"] == 2


async def test_reconcile_finishes_orphaned_pending_rows(engine, settings, fake_ollama):
    await process_failures_extracted_event(failures_extracted(RUN_A, [UNKNOWN_1]), engine, settings)
    with engine.begin() as conn:
        conn.execute(update(llm_jobs).values(status="done", result_category="timeout"))
    pool = make_pool(engine, settings, fake_ollama)
    assert pool.reconcile_orphans() == 1
    row = rows(engine, classifications)[0]
    assert (row["status"], row["category"], row["source"]) == ("classified", "timeout", "llm_cache")


async def test_llm_disabled_marks_unmatched_as_degraded(engine, settings):
    settings.llm_enabled = False
    await process_failures_extracted_event(failures_extracted(RUN_A, [UNKNOWN_1]), engine, settings)
    row = rows(engine, classifications)[0]
    assert (row["status"], row["category"], row["source"]) == ("degraded", "unknown", "fallback")
    assert rows(engine, llm_jobs) == []


def _message(body: bytes) -> MagicMock:
    msg = MagicMock()
    msg.body = body
    msg.process.return_value.__aenter__ = AsyncMock(return_value=None)
    msg.process.return_value.__aexit__ = AsyncMock(return_value=False)
    msg.ack, msg.reject, msg.nack = AsyncMock(), AsyncMock(), AsyncMock()
    return msg


async def test_poison_messages_go_to_dlq(engine, settings):
    for body in (b"not json", b'{"event_type": "Other"}', json.dumps({**failures_extracted(RUN_A, []), "payload": {}}).encode()):
        msg = _message(body)
        await handle_message(msg, engine, settings)
        msg.reject.assert_awaited_once_with(requeue=False)
        msg.ack.assert_not_awaited()


async def test_valid_message_is_acked(engine, settings):
    msg = _message(json.dumps(failures_extracted(RUN_A, [UNKNOWN_1])).encode())
    await handle_message(msg, engine, settings)
    msg.ack.assert_awaited_once()


def test_contract_sample_validates():
    sample = json.loads((CONTRACTS_DIR / "samples" / "failure_classified.json").read_text())
    VALIDATOR.validate(sample)
