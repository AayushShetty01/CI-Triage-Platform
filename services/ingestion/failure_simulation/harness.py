from __future__ import annotations

import asyncio
import hashlib
import json
import re
import subprocess
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import aio_pika
import httpx
from sqlalchemy import create_engine, func, select
from sqlalchemy.engine import Engine

from app.database import outbox, runs

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
EVIDENCE_ROOT = Path(__file__).resolve().parent / "evidence"
SERVICES = ("postgres", "rabbitmq", "ingestion")


class ComposeHarness:
    def __init__(self) -> None:
        self.cwd = REPOSITORY_ROOT

    def _run(
        self,
        *arguments: str,
        check: bool = True,
        timeout: float = 180,
    ) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            ["docker", "compose", *arguments],
            cwd=self.cwd,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if check and result.returncode:
            raise RuntimeError(
                f"docker compose {' '.join(arguments)} failed "
                f"({result.returncode}): {result.stderr.strip()}"
            )
        return result

    def up(self) -> None:
        self._run(
            "up",
            "--detach",
            "--build",
            "--wait",
            "--wait-timeout",
            "180",
            timeout=300,
        )

    def stop(self, service: str) -> None:
        self._run("stop", service)

    def start(self, service: str) -> None:
        self._run("start", service)

    def kill(self, service: str) -> None:
        self._run("kill", service)

    def restart(self, service: str) -> None:
        self._run("restart", service)

    def stop_rabbitmq_app(self) -> None:
        self._run("exec", "--no-TTY", "rabbitmq", "rabbitmqctl", "stop_app")

    def start_rabbitmq_app(self) -> None:
        self._run("exec", "--no-TTY", "rabbitmq", "rabbitmqctl", "start_app")

    def wait_healthy(self, service: str, timeout: float = 120) -> None:
        deadline = time.monotonic() + timeout
        last_status = "not checked"
        while time.monotonic() < deadline:
            if service == "ingestion":
                try:
                    response = httpx.get("http://127.0.0.1:8001/health", timeout=2)
                    last_status = f"HTTP {response.status_code}"
                    if response.status_code == 200:
                        return
                except httpx.HTTPError as exc:
                    last_status = str(exc)
            else:
                container_id = self._run("ps", "--quiet", service, check=False).stdout.strip()
                if container_id:
                    inspection = subprocess.run(
                        [
                            "docker",
                            "inspect",
                            "--format",
                            "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
                            container_id,
                        ],
                        cwd=self.cwd,
                        check=False,
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    last_status = inspection.stdout.strip() or inspection.stderr.strip()
                    if last_status == "healthy":
                        return
            time.sleep(1)
        raise AssertionError(
            f"Compose service {service!r} did not become healthy within {timeout}s; "
            f"last status: {last_status}"
        )

    def container_environment(self, service: str, variable: str) -> str:
        value = self._run("exec", "--no-TTY", service, "printenv", variable).stdout.strip()
        if not value:
            raise RuntimeError(f"{variable} was empty in Compose service {service!r}")
        return value

    def database_url(self) -> str:
        return self.container_environment("ingestion", "DATABASE_URL").replace(
            "@postgres:", "@127.0.0.1:"
        )

    def rabbitmq_url(self) -> str:
        return self.container_environment("ingestion", "RABBITMQ_URL").replace(
            "@rabbitmq:", "@127.0.0.1:"
        )

    def logs(self, service: str, since: str | None = None) -> str:
        arguments = ["logs", "--no-color", "--timestamps"]
        if since is not None:
            timestamp = datetime.fromisoformat(since).astimezone(timezone.utc)
            arguments.extend(
                ["--since", timestamp.isoformat(timespec="seconds").replace("+00:00", "Z")]
            )
        return self._run(
            *arguments,
            service,
            check=False,
            timeout=60,
        ).stdout


class IngestionClient:
    def __init__(self, base_url: str = "http://127.0.0.1:8001") -> None:
        self.base_url = base_url.rstrip("/")
        self.headers: dict[str, str] = {}
        self.client = httpx.Client(base_url=self.base_url, timeout=15)

    def health_status(self) -> int:
        return self.client.get("/health").status_code

    def metrics(self) -> str:
        response = self.client.get("/metrics")
        response.raise_for_status()
        return response.text

    def post_report(
        self,
        report: bytes,
        commit_sha: str,
        branch: str = "failure-test",
    ) -> httpx.Response:
        return self.client.post(
            "/runs",
            content=report,
            headers={
                **self.headers,
                "Content-Type": "application/xml",
                "X-Commit-Sha": commit_sha,
                "X-Branch": branch,
            },
        )

    def get_raw_report(self, run_id: str) -> bytes:
        response = self.client.get(f"/internal/runs/{run_id}/raw")
        response.raise_for_status()
        return response.content

    async def post_report_async(
        self,
        client: httpx.AsyncClient,
        report: bytes,
        commit_sha: str,
        branch: str = "failure-test",
    ) -> httpx.Response:
        return await client.post(
            "/runs",
            content=report,
            headers={
                **self.headers,
                "Content-Type": "application/xml",
                "X-Commit-Sha": commit_sha,
                "X-Branch": branch,
            },
        )

    def close(self) -> None:
        self.client.close()


class RabbitMQHelper:
    def __init__(self, rabbitmq_url: str) -> None:
        self.rabbitmq_url = rabbitmq_url
        self.queue_name = f"ingestion-failure-{uuid.uuid4().hex}"

    async def _queue(self, *, setup: bool = False):
        connection = await aio_pika.connect_robust(self.rabbitmq_url, timeout=10)
        channel = await connection.channel()
        exchange = await channel.declare_exchange(
            "triage.events",
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )
        queue = await channel.declare_queue(self.queue_name, durable=True)
        await queue.bind(exchange, routing_key="run.received")
        if setup:
            await queue.purge()
        return connection, channel, queue

    async def _is_available(self) -> bool:
        connection = await aio_pika.connect(self.rabbitmq_url, timeout=1)
        await connection.close()
        return True

    async def _count_messages(self) -> int:
        connection, channel, queue = await self._queue()
        try:
            result = await queue.declare()
            return int(result.message_count)
        finally:
            await channel.close()
            await connection.close()

    async def _try_count_messages(self) -> int | None:
        connection = None
        channel = None
        try:
            connection = await aio_pika.connect(self.rabbitmq_url, timeout=0.5)
            channel = await connection.channel()
            queue = await channel.declare_queue(self.queue_name, durable=True)
            result = await queue.declare()
            return int(result.message_count)
        except (
            aio_pika.exceptions.AMQPException,
            OSError,
            asyncio.TimeoutError,
        ):
            return None
        finally:
            if channel is not None and not channel.is_closed:
                await channel.close()
            if connection is not None and not connection.is_closed:
                await connection.close()

    async def _drain_messages(self) -> list[dict[str, Any]]:
        connection, channel, queue = await self._queue()
        messages = []
        try:
            while (message := await queue.get(fail=False)) is not None:
                async with message.process():
                    messages.append(json.loads(message.body))
            return messages
        finally:
            await channel.close()
            await connection.close()

    def prepare(self) -> None:
        async def setup() -> None:
            connection, channel, _ = await self._queue(setup=True)
            await channel.close()
            await connection.close()

        asyncio.run(setup())

    def ensure_queue(self) -> None:
        async def ensure() -> bool:
            connection, channel, _ = await self._queue()
            await channel.close()
            await connection.close()
            return True

        wait_until(
            lambda: asyncio.run(ensure()),
            timeout=30,
            interval=0.25,
            description="durable RabbitMQ test queue to be declared and bound",
        )

    def wait_available(self, timeout: float = 30, interval: float = 0.1) -> None:
        wait_until(
            lambda: asyncio.run(self._is_available()),
            timeout=timeout,
            interval=interval,
            description="RabbitMQ AMQP endpoint to become available",
        )

    def wait_unavailable(self, timeout: float = 15, interval: float = 0.1) -> None:
        def is_unavailable() -> bool:
            try:
                return not asyncio.run(self._is_available())
            except (
                aio_pika.exceptions.AMQPException,
                OSError,
                asyncio.TimeoutError,
            ):
                return True

        wait_until(
            is_unavailable,
            timeout=timeout,
            interval=interval,
            description="RabbitMQ AMQP endpoint to become unavailable",
        )

    def count_messages(self) -> int:
        return asyncio.run(self._count_messages())

    def try_count_messages(self) -> int | None:
        return asyncio.run(self._try_count_messages())

    def drain_messages(self) -> list[dict[str, Any]]:
        return asyncio.run(self._drain_messages())


class DatabaseHelper:
    def __init__(self, database_url: str) -> None:
        self.engine: Engine = create_engine(database_url, pool_pre_ping=True)

    def counts(self) -> dict[str, int]:
        with self.engine.connect() as connection:
            runs_count = connection.execute(select(func.count()).select_from(runs)).scalar_one()
            outbox_count = connection.execute(
                select(func.count()).select_from(outbox)
            ).scalar_one()
            pending_count = connection.execute(
                select(func.count())
                .select_from(outbox)
                .where(outbox.c.published_at.is_(None))
            ).scalar_one()
        return {
            "runs": int(runs_count),
            "outbox": int(outbox_count),
            "pending": int(pending_count),
        }

    def counts_for_run(self, run_id: str) -> dict[str, int]:
        with self.engine.connect() as connection:
            runs_count = connection.execute(
                select(func.count()).select_from(runs).where(runs.c.run_id == run_id)
            ).scalar_one()
            outbox_count = connection.execute(
                select(func.count())
                .select_from(outbox)
                .where(outbox.c.payload["run_id"].as_string() == run_id)
            ).scalar_one()
        return {"runs": int(runs_count), "outbox": int(outbox_count)}

    def stored_run_ids(self, run_ids: set[str]) -> set[str]:
        if not run_ids:
            return set()
        with self.engine.connect() as connection:
            stored_ids = connection.execute(
                select(runs.c.run_id).where(runs.c.run_id.in_(run_ids))
            ).scalars()
            return set(stored_ids)

    def pending_count_for_run_ids(self, run_ids: set[str]) -> int:
        if not run_ids:
            return 0
        with self.engine.connect() as connection:
            count = connection.execute(
                select(func.count())
                .select_from(outbox)
                .where(
                    outbox.c.payload["run_id"].as_string().in_(run_ids),
                    outbox.c.published_at.is_(None),
                )
            ).scalar_one()
        return int(count)

    def can_query(self) -> bool:
        with self.engine.connect() as connection:
            connection.execute(select(1)).scalar_one()
        return True

    def close(self) -> None:
        self.engine.dispose()


def wait_until(
    predicate: Callable[[], Any],
    timeout: float,
    interval: float,
    description: str,
    condition: Callable[[Any], bool] = bool,
) -> Any:
    deadline = time.monotonic() + timeout
    last_observed: Any = "not observed"
    while time.monotonic() < deadline:
        try:
            last_observed = predicate()
        except Exception as exc:
            last_observed = f"{type(exc).__name__}: {exc}"
        else:
            if condition(last_observed):
                return last_observed
        time.sleep(interval)
    raise AssertionError(
        f"Timed out waiting for {description} after {timeout}s; "
        f"last observed value: {last_observed!r}"
    )


def synthetic_junit_report(index: int) -> bytes:
    suite_name = f"failure-scenario-{index:03d}"
    testcase_name = f"sample_case_{index:03d}"
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<testsuite name="{suite_name}" tests="1" failures="0" errors="0" skipped="0">'
        f'<testcase classname="ingestion.failure_simulation" name="{testcase_name}"/>'
        "</testsuite>"
    ).encode("utf-8")


def run_id_for_submission(report: bytes, commit_sha: str) -> str:
    return hashlib.sha256(commit_sha.encode("utf-8") + report).hexdigest()


def metric_value(metrics: str, name: str) -> float:
    pattern = re.compile(rf"^{re.escape(name)}(?:\{{[^}}]*\}})?\s+([-+0-9.eE]+)$", re.MULTILINE)
    match = pattern.search(metrics)
    if match is None:
        raise AssertionError(f"Metric {name!r} was absent from /metrics")
    return float(match.group(1))


@dataclass
class ScenarioEvidence:
    scenario: str
    queue_name: str
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    assertions: list[dict[str, Any]] = field(default_factory=list)
    measurements: dict[str, Any] = field(default_factory=dict)
    drained_messages: list[dict[str, Any]] = field(default_factory=list)

    def check(self, name: str, condition: bool, expected: Any, actual: Any) -> None:
        self.assertions.append(
            {
                "assertion": name,
                "passed": bool(condition),
                "expected": _json_safe(expected),
                "actual": _json_safe(actual),
            }
        )
        assert condition, f"{name}: expected {expected!r}, observed {actual!r}"

    def write(
        self,
        *,
        passed: bool,
        compose: ComposeHarness,
    ) -> Path:
        directory = EVIDENCE_ROOT / self.scenario
        directory.mkdir(parents=True, exist_ok=True)
        for service in SERVICES:
            (directory / f"{service}.log").write_text(
                compose.logs(service, since=self.started_at),
                encoding="utf-8",
            )
        (directory / "drained-messages.json").write_text(
            json.dumps(self.drained_messages, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        summary = {
            "scenario": self.scenario,
            "started_at": self.started_at,
            "ended_at": datetime.now(timezone.utc).isoformat(),
            "passed": passed,
            "queue_name": self.queue_name,
            "measured_counts": self.measurements,
            "assertions": self.assertions,
        }
        (directory / "summary.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return directory


def _json_safe(value: Any) -> Any:
    if isinstance(value, bytes):
        return {
            "sha256": hashlib.sha256(value).hexdigest(),
            "length_bytes": len(value),
        }
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, set):
        return sorted(_json_safe(item) for item in value)
    return value
