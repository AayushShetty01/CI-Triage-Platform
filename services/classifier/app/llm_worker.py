"""LLM worker pool.

Workers claim jobs from classifier.llm_jobs (one job per fingerprint), call the
LLM through the circuit breaker, then finish every pending classification
attached to the job in one transaction with its outbox events.

Failure handling:
- LLM timeout / connection error / HTTP error: retry with exponential backoff,
  up to llm_max_attempts; then the job fails and its classifications are
  marked degraded (category unknown, source fallback).
- Breaker open: no call is made; the job is put back without using an attempt.
- A job that has waited longer than llm_degrade_after_seconds is degraded, so
  results never stay pending indefinitely while the LLM is down.
- Invalid model output: the job finishes as category unknown (the LLM is up,
  so this does not trip the breaker).
- Worker crash: a claimed job carries a lease (locked_until); once it expires
  another worker reclaims it.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.engine import Connection, Engine

from app import metrics
from app.circuit_breaker import OPEN, STATE_VALUES, CircuitBreaker
from app.database import classifications, llm_jobs
from app.events import stage_failure_classified
from app.llm_client import (
    FailureInput,
    LLMInvalidOutput,
    LLMUnavailable,
    OllamaClient,
    looks_like_injection,
)
from app.settings import Settings

logger = logging.getLogger(__name__)

LEASE_MARGIN_SECONDS = 10.0


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class LLMWorkerPool:
    def __init__(
        self,
        engine: Engine,
        client: OllamaClient,
        settings: Settings,
        breaker: CircuitBreaker | None = None,
        clock=_utcnow,
        sleep=asyncio.sleep,
    ) -> None:
        self.engine = engine
        self.client = client
        self.settings = settings
        self.clock = clock
        self.sleep = sleep
        self.breaker = breaker or CircuitBreaker(
            failure_threshold=settings.breaker_failure_threshold,
            reset_timeout_seconds=settings.breaker_reset_seconds,
            on_state_change=self._on_breaker_change,
        )
        metrics.llm_circuit_state.set(STATE_VALUES[self.breaker.state])

    def _on_breaker_change(self, state: str) -> None:
        metrics.llm_circuit_state.set(STATE_VALUES[state])
        logger.warning(
            "LLM circuit breaker is now %s", state, extra={"event": "classifier_breaker_state", "outcome": state}
        )

    # ---- job lifecycle -------------------------------------------------

    def claim_job(self) -> dict[str, Any] | None:
        now = self.clock()
        lease = timedelta(seconds=self.settings.llm_timeout_seconds + LEASE_MARGIN_SECONDS)
        with self.engine.begin() as conn:
            job = (
                conn.execute(
                    select(llm_jobs)
                    .where(
                        or_(
                            and_(llm_jobs.c.status == "pending", llm_jobs.c.next_attempt_at <= now),
                            and_(llm_jobs.c.status == "in_progress", llm_jobs.c.locked_until < now),
                        )
                    )
                    .order_by(llm_jobs.c.next_attempt_at, llm_jobs.c.id)
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                .mappings()
                .first()
            )
            if job is None:
                return None
            attempts = job["attempts"] + 1
            conn.execute(
                update(llm_jobs)
                .where(llm_jobs.c.id == job["id"])
                .values(status="in_progress", attempts=attempts, locked_until=now + lease)
            )
            return {**dict(job), "status": "in_progress", "attempts": attempts}

    def release_job(self, job: dict[str, Any], retry_at: datetime, *, count_attempt: bool, error: str | None) -> None:
        values: dict[str, Any] = {
            "status": "pending",
            "next_attempt_at": retry_at,
            "locked_until": None,
        }
        if not count_attempt:
            values["attempts"] = max(0, job["attempts"] - 1)
        if error:
            values["last_error"] = error[:300]
        with self.engine.begin() as conn:
            conn.execute(update(llm_jobs).where(llm_jobs.c.id == job["id"]).values(**values))

    def _finish_classifications(
        self,
        conn: Connection,
        job_id: int,
        now: datetime,
        values: dict[str, Any],
    ) -> int:
        rows = (
            conn.execute(
                select(classifications).where(
                    classifications.c.llm_job_id == job_id,
                    classifications.c.status == "pending",
                )
            )
            .mappings()
            .all()
        )
        for row in rows:
            conn.execute(
                update(classifications)
                .where(classifications.c.id == row["id"])
                .values(**values, classified_at=now)
            )
            finished = {**dict(row), **values}
            stage_failure_classified(conn, finished, now)
            metrics.classifications_total.labels(
                source=finished["source"], category=finished["category"], status=finished["status"]
            ).inc()
        return len(rows)

    def complete_job(self, job_id: int, category: str, confidence: float | None, reason: str | None) -> int:
        now = self.clock()
        with self.engine.begin() as conn:
            # Lock the job row first; the consumer takes the same lock when attaching.
            conn.execute(select(llm_jobs.c.id).where(llm_jobs.c.id == job_id).with_for_update()).first()
            conn.execute(
                update(llm_jobs)
                .where(llm_jobs.c.id == job_id)
                .values(
                    status="done",
                    result_category=category,
                    result_confidence=confidence,
                    result_reason=reason,
                    locked_until=None,
                    completed_at=now,
                    last_error=None,
                )
            )
            return self._finish_classifications(
                conn,
                job_id,
                now,
                {
                    "status": "classified",
                    "category": category,
                    "source": "llm",
                    "confidence": confidence,
                    "reason": reason,
                },
            )

    def fail_job(self, job_id: int, reason: str, only_if_status: str | None = None) -> int:
        now = self.clock()
        with self.engine.begin() as conn:
            status = conn.execute(
                select(llm_jobs.c.status).where(llm_jobs.c.id == job_id).with_for_update()
            ).scalar_one_or_none()
            if status is None or (only_if_status is not None and status != only_if_status):
                return 0  # another worker picked it up in the meantime
            conn.execute(
                update(llm_jobs)
                .where(llm_jobs.c.id == job_id)
                .values(status="failed", locked_until=None, completed_at=now, last_error=reason[:300])
            )
            return self._finish_classifications(
                conn,
                job_id,
                now,
                {
                    "status": "degraded",
                    "category": "unknown",
                    "source": "fallback",
                    "confidence": None,
                    "reason": reason[:300],
                },
            )

    # ---- sweeps --------------------------------------------------------

    def degrade_stale_jobs(self) -> int:
        """Degrade jobs that have waited too long because the LLM is failing.

        A job that is merely queued behind others (no error, breaker closed) is
        left alone: a busy LLM is slow, not unavailable.
        """
        cutoff = self.clock() - timedelta(seconds=self.settings.llm_degrade_after_seconds)
        conditions = [llm_jobs.c.status == "pending", llm_jobs.c.queued_at < cutoff]
        if self.breaker.state == "closed":
            conditions.append(llm_jobs.c.last_error.is_not(None))
        with self.engine.connect() as conn:
            stale_ids = list(conn.execute(select(llm_jobs.c.id).where(*conditions)).scalars())
        degraded = 0
        for job_id in stale_ids:
            degraded += self.fail_job(
                job_id,
                f"LLM unavailable for more than {int(self.settings.llm_degrade_after_seconds)}s; "
                "rule-based result only",
                only_if_status="pending",
            )
        return degraded

    def reconcile_orphans(self, limit: int = 100) -> int:
        """Finish pending classifications whose job already ended (safety net)."""
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(llm_jobs.c.id, llm_jobs.c.status, llm_jobs.c.result_category,
                       llm_jobs.c.result_confidence, llm_jobs.c.result_reason, llm_jobs.c.last_error)
                .join(classifications, classifications.c.llm_job_id == llm_jobs.c.id)
                .where(classifications.c.status == "pending", llm_jobs.c.status.in_(["done", "failed"]))
                .distinct()
                .limit(limit)
            ).mappings().all()
        fixed = 0
        for job in rows:
            with self.engine.begin() as conn:
                conn.execute(select(llm_jobs.c.id).where(llm_jobs.c.id == job["id"]).with_for_update()).first()
                if job["status"] == "done":
                    values = {
                        "status": "classified",
                        "category": job["result_category"],
                        "source": "llm_cache",
                        "confidence": job["result_confidence"],
                        "reason": job["result_reason"],
                    }
                else:
                    values = {
                        "status": "degraded",
                        "category": "unknown",
                        "source": "fallback",
                        "confidence": None,
                        "reason": (job["last_error"] or "LLM unavailable")[:300],
                    }
                fixed += self._finish_classifications(conn, job["id"], self.clock(), values)
        return fixed

    def refresh_pending_gauge(self) -> None:
        with self.engine.connect() as conn:
            count = conn.execute(
                select(func.count()).where(llm_jobs.c.status.in_(["pending", "in_progress"]))
            ).scalar_one()
        metrics.llm_jobs_pending.set(count)

    # ---- one unit of work ---------------------------------------------

    async def process_one(self) -> bool:
        """Claim and process one job. Returns True if a job was handled."""
        if self.breaker.state == OPEN:
            return False
        job = self.claim_job()
        if job is None:
            return False
        log_ctx = {"job_id": job["id"], "fingerprint": job["fingerprint"], "attempts": job["attempts"]}

        if not self.breaker.allow_request():
            # Another worker holds the half-open trial; put the job back untouched.
            metrics.llm_requests.labels(outcome="breaker_open").inc()
            retry_at = self.clock() + timedelta(seconds=max(1.0, self.breaker.seconds_until_retry()))
            self.release_job(job, retry_at, count_attempt=False, error=None)
            return True

        failure = FailureInput(job["error_type"], job["error_message"], job["stack_trace"])
        if looks_like_injection(failure):
            metrics.llm_prompt_injection_suspected.inc()
            logger.warning(
                "Failure text contains instruction-like content; classifying as data",
                extra={**log_ctx, "event": "classifier_prompt_injection_suspected"},
            )

        try:
            result = await asyncio.wait_for(
                self.client.classify(failure),
                timeout=self.settings.llm_timeout_seconds + 5,
            )
        except (LLMUnavailable, asyncio.TimeoutError) as err:
            self.breaker.record_failure()
            kind = getattr(err, "kind", "timeout")
            metrics.llm_requests.labels(outcome=kind).inc()
            reason = str(err) or "LLM request timed out"
            if job["attempts"] >= self.settings.llm_max_attempts:
                count = self.fail_job(job["id"], f"LLM unavailable after {job['attempts']} attempts: {reason}")
                logger.warning(
                    "LLM job failed; %d classification(s) degraded",
                    count,
                    extra={**log_ctx, "event": "classifier_llm_job_failed", "reason": reason},
                )
            else:
                delay = min(
                    self.settings.llm_retry_cap_seconds,
                    self.settings.llm_retry_base_seconds * 2 ** (job["attempts"] - 1),
                )
                self.release_job(job, self.clock() + timedelta(seconds=delay), count_attempt=True, error=reason)
                logger.warning(
                    "LLM call failed; retrying in %.1fs",
                    delay,
                    extra={**log_ctx, "event": "classifier_llm_retry", "reason": reason},
                )
            return True
        except LLMInvalidOutput as err:
            self.breaker.record_success()
            metrics.llm_requests.labels(outcome="invalid_output").inc()
            count = self.complete_job(job["id"], "unknown", None, "Model output failed validation")
            logger.warning(
                "LLM output rejected (%s); %d classification(s) set to unknown",
                err,
                count,
                extra={**log_ctx, "event": "classifier_llm_invalid_output"},
            )
            return True

        self.breaker.record_success()
        metrics.llm_requests.labels(outcome="success").inc()
        metrics.llm_latency.observe(result.latency_seconds)
        metrics.llm_tokens.labels(kind="prompt").inc(result.prompt_tokens)
        metrics.llm_tokens.labels(kind="completion").inc(result.completion_tokens)
        count = self.complete_job(job["id"], result.category, result.confidence, result.reason)
        logger.info(
            "LLM classified fingerprint as %s for %d failure(s)",
            result.category,
            count,
            extra={**log_ctx, "event": "classifier_llm_classified", "category": result.category,
                   "duration_ms": round(result.latency_seconds * 1000, 1)},
        )
        return True

    # ---- loops ---------------------------------------------------------

    async def run_worker(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                handled = await self.process_one()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("LLM worker iteration failed", extra={"event": "classifier_llm_worker_error"})
                handled = False
            if not handled:
                await self.sleep(self.settings.llm_poll_interval_seconds)

    async def run_maintenance(self, stop_event: asyncio.Event, interval_seconds: float = 5.0) -> None:
        while not stop_event.is_set():
            try:
                self.degrade_stale_jobs()
                self.reconcile_orphans()
                self.refresh_pending_gauge()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("LLM maintenance failed", extra={"event": "classifier_llm_maintenance_error"})
            await self.sleep(interval_seconds)

    def start(self, stop_event: asyncio.Event) -> list[asyncio.Task[None]]:
        tasks = [asyncio.create_task(self.run_worker(stop_event)) for _ in range(max(1, self.settings.llm_workers))]
        tasks.append(asyncio.create_task(self.run_maintenance(stop_event)))
        return tasks
