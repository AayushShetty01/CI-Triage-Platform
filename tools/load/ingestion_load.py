#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import re
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any

import httpx

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
INGESTION_DIR = REPOSITORY_ROOT / "services" / "ingestion"
sys.path.insert(0, str(INGESTION_DIR))

from failure_simulation.harness import (  # noqa: E402
    ComposeHarness,
    IngestionClient,
    synthetic_junit_report,
    wait_until,
)

SMALL_TESTCASES = 10
LARGE_TESTCASES = 5000


def percentile(values: list[float], percentile_value: float) -> float | None:
    if not values:
        return None
    if not 0 <= percentile_value <= 100:
        raise ValueError("percentile_value must be between 0 and 100")
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile_value / 100
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def summarize_requests(
    results: list[dict[str, Any]],
    elapsed_seconds: float,
) -> dict[str, Any]:
    request_count = len(results)
    accepted_latencies = [
        float(result["latency_ms"])
        for result in results
        if result.get("status") == 202 and result.get("latency_ms") is not None
    ]
    status_counts: dict[str, int] = {}
    for result in results:
        status = str(result.get("status", "connection_error"))
        status_counts[status] = status_counts.get(status, 0) + 1
    success_count = status_counts.get("202", 0)
    return {
        "requests": request_count,
        "success_202": success_count,
        "success_rate": success_count / request_count if request_count else 0.0,
        "non_202_counts": {
            status: count for status, count in status_counts.items() if status != "202"
        },
        "throughput_requests_per_second": (
            request_count / elapsed_seconds if elapsed_seconds > 0 else 0.0
        ),
        "latency_202_ms": {
            "p50": percentile(accepted_latencies, 50),
            "p95": percentile(accepted_latencies, 95),
            "p99": percentile(accepted_latencies, 99),
            "max": max(accepted_latencies) if accepted_latencies else None,
            "mean": mean(accepted_latencies) if accepted_latencies else None,
        },
        "elapsed_seconds": elapsed_seconds,
    }


def _run(arguments: list[str], timeout: float = 30) -> str:
    result = subprocess.run(
        arguments,
        cwd=REPOSITORY_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode:
        raise RuntimeError(
            f"{' '.join(arguments)} failed with {result.returncode}: {result.stderr.strip()}"
        )
    return result.stdout.strip()


def _machine_facts() -> dict[str, Any]:
    cpu_model = platform.processor() or "unknown"
    memory_bytes: int | None = None
    if sys.platform == "darwin":
        cpu_model = _run(["sysctl", "-n", "machdep.cpu.brand_string"])
        memory_bytes = int(_run(["sysctl", "-n", "hw.memsize"]))
    elif Path("/proc/cpuinfo").exists():
        cpuinfo = Path("/proc/cpuinfo").read_text(encoding="utf-8", errors="replace")
        model_match = re.search(r"^model name\s*:\s*(.+)$", cpuinfo, re.MULTILINE)
        if model_match:
            cpu_model = model_match.group(1).strip()
        mem_match = re.search(r"^MemTotal:\s+(\d+)\s+kB$", Path("/proc/meminfo").read_text(), re.MULTILINE)
        if mem_match:
            memory_bytes = int(mem_match.group(1)) * 1024
    settings = {}
    setting_names = (
        "MAX_REPORT_SIZE_BYTES",
        "RELAY_POLL_INTERVAL_SECONDS",
        "OUTBOX_POLL_INTERVAL_SECONDS",
        "RELAY_BACKOFF_CAP_SECONDS",
        "OUTBOX_BATCH_SIZE",
        "INGESTION_RETRY_AFTER_SECONDS",
    )
    for name in setting_names:
        result = subprocess.run(
            ["docker", "compose", "exec", "--no-TTY", "ingestion", "printenv", name],
            cwd=REPOSITORY_ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            settings[name] = result.stdout.strip()
    return {
        "os": platform.platform(),
        "cpu_model": cpu_model,
        "cpu_cores_logical": os.cpu_count(),
        "ram_bytes": memory_bytes,
        "docker_version": _run(["docker", "version", "--format", "{{.Client.Version}}"]),
        "git_commit": _run(["git", "rev-parse", "HEAD"]),
        "ingestion_settings": settings,
    }


def _docker_stats() -> dict[str, Any]:
    container_ids = [
        _run(["docker", "compose", "ps", "--quiet", service])
        for service in ("ingestion", "postgres")
    ]
    output = _run(
        [
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{json .}}",
            *container_ids,
        ]
    )
    samples = []
    for line in output.splitlines():
        if line.strip():
            samples.append(json.loads(line))
    return {"captured_at": datetime.now(timezone.utc).isoformat(), "containers": samples}


def _metrics_snapshot(client: IngestionClient) -> dict[str, float]:
    metrics: dict[str, float] = {}
    for line in client.metrics().splitlines():
        if line.startswith("outbox_pending ") or line.startswith("outbox_oldest_pending_seconds "):
            name, value = line.split(maxsplit=1)
            metrics[name] = float(value)
    required = {"outbox_pending", "outbox_oldest_pending_seconds"}
    if not required.issubset(metrics):
        raise RuntimeError(f"Required outbox metrics missing: {sorted(required - metrics.keys())}")
    return metrics


async def _stats_sampler(stop: asyncio.Event, samples: list[dict[str, Any]]) -> None:
    while not stop.is_set():
        try:
            samples.append(await asyncio.to_thread(_docker_stats))
        except Exception as exc:
            samples.append(
                {
                    "captured_at": datetime.now(timezone.utc).isoformat(),
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
        try:
            await asyncio.wait_for(stop.wait(), timeout=1)
        except asyncio.TimeoutError:
            pass


def _report(index: int, report_size: str) -> bytes:
    testcase_count = SMALL_TESTCASES if report_size == "small" else LARGE_TESTCASES
    return synthetic_junit_report(index, testcase_count=testcase_count)


async def _post_requests(
    ingestion: IngestionClient,
    requests: int,
    concurrency: int,
    report_size: str,
) -> tuple[list[dict[str, Any]], float, list[dict[str, Any]]]:
    semaphore = asyncio.Semaphore(concurrency)
    stats_samples: list[dict[str, Any]] = []
    stop_sampling = asyncio.Event()
    sampler = asyncio.create_task(_stats_sampler(stop_sampling, stats_samples))
    results: list[dict[str, Any]] = []
    async with httpx.AsyncClient(
        base_url=ingestion.base_url,
        timeout=120,
        limits=httpx.Limits(
            max_connections=concurrency,
            max_keepalive_connections=concurrency,
        ),
    ) as client:
        async def submit(index: int) -> dict[str, Any]:
            async with semaphore:
                body = _report(index, report_size)
                commit_sha = f"ingestion-load-{uuid.uuid4().hex}"
                started = time.perf_counter()
                try:
                    response = await ingestion.post_report_async(client, body, commit_sha)
                    latency_ms = (time.perf_counter() - started) * 1000
                    try:
                        run_id = response.json().get("run_id")
                    except (ValueError, AttributeError):
                        run_id = None
                    return {
                        "index": index,
                        "status": response.status_code,
                        "latency_ms": latency_ms,
                        "run_id": run_id,
                    }
                except httpx.HTTPError as exc:
                    latency_ms = (time.perf_counter() - started) * 1000
                    return {
                        "index": index,
                        "status": "connection_error",
                        "latency_ms": latency_ms,
                        "run_id": None,
                        "error": f"{type(exc).__name__}: {exc}",
                    }

        measured_started = time.perf_counter()
        results = await asyncio.gather(*(submit(index) for index in range(requests)))
        elapsed = time.perf_counter() - measured_started
    stop_sampling.set()
    await sampler
    if not stats_samples:
        stats_samples.append(await asyncio.to_thread(_docker_stats))
    return results, elapsed, stats_samples


def _markdown_report(label: str, report_size: str, concurrency: int, summary: dict[str, Any], metrics: dict[str, Any], recovery_seconds: float | None) -> str:
    latency = summary["latency_202_ms"]
    non_202 = summary["non_202_counts"] or {"none": 0}
    non_202_text = ", ".join(f"{key}: {value}" for key, value in non_202.items())
    rows = [
        f"# Ingestion load run: {label}",
        "",
        f"- Report size: {report_size}",
        f"- Concurrency: {concurrency}",
        f"- Requests: {summary['requests']}",
        f"- Success rate (202): {summary['success_rate']:.4%}",
        f"- Non-202 counts: {non_202_text}",
        f"- Throughput: {summary['throughput_requests_per_second']:.3f} requests/s",
        "",
        "| 202 latency | ms |",
        "|---|---:|",
        f"| p50 | {latency['p50']} |",
        f"| p95 | {latency['p95']} |",
        f"| p99 | {latency['p99']} |",
        f"| max | {latency['max']} |",
        "",
        f"- End `outbox_pending`: {metrics['outbox_pending']}",
        f"- End `outbox_oldest_pending_seconds`: {metrics['outbox_oldest_pending_seconds']}",
    ]
    if recovery_seconds is not None:
        rows.append(f"- Broker recovery to pending zero: {recovery_seconds:.6f} seconds")
    rows.append("")
    return "\n".join(rows)


async def run_load(args: argparse.Namespace) -> Path:
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    compose = ComposeHarness()
    ingestion = IngestionClient(
        api_key=compose.container_environment("ingestion", "INGESTION_API_KEY"),
        internal_api_key=compose.container_environment("ingestion", "INTERNAL_API_KEY"),
    )
    rabbitmq_stopped = False
    recovery_seconds = None
    try:
        warmup_report = _report(0, args.report_size)
        warmup_response = ingestion.post_report(
            warmup_report,
            f"ingestion-load-warmup-{uuid.uuid4().hex}",
        )
        warmup = {
            "status": warmup_response.status_code,
            "run_id": warmup_response.json().get("run_id"),
        }
        if warmup_response.status_code != 202:
            raise RuntimeError(f"Warm-up failed with HTTP {warmup_response.status_code}")

        if args.broker_down:
            compose.stop("rabbitmq")
            rabbitmq_stopped = True
        started_at = datetime.now(timezone.utc).isoformat()
        results, elapsed_seconds, stats_samples = await _post_requests(
            ingestion,
            args.requests,
            args.concurrency,
            args.report_size,
        )
        measured_ended_at = datetime.now(timezone.utc).isoformat()
        summary = summarize_requests(results, elapsed_seconds)
        if args.broker_down:
            compose.start("rabbitmq")
            rabbitmq_stopped = False
            compose.wait_healthy("rabbitmq")
            recovery_started = time.perf_counter()
            wait_until(
                lambda: _metrics_snapshot(ingestion)["outbox_pending"],
                timeout=300,
                interval=1,
                description="outbox to drain after RabbitMQ recovery",
                condition=lambda pending: pending == 0,
            )
            recovery_seconds = time.perf_counter() - recovery_started
        metrics = _metrics_snapshot(ingestion)
        facts = _machine_facts()
        run_data = {
            "label": args.label,
            "started_at": started_at,
            "measured_ended_at": measured_ended_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "configuration": {
                "requests": args.requests,
                "concurrency": args.concurrency,
                "report_size": args.report_size,
                "broker_down": args.broker_down,
            },
            "warmup": warmup,
            "summary": summary,
            "requests": results,
            "metrics_at_end": metrics,
            "broker_recovery_to_pending_zero_seconds": recovery_seconds,
            "docker_stats_during_measurement": stats_samples,
            "machine_facts": facts,
        }
        stem = re.sub(r"[^A-Za-z0-9._-]+", "-", args.label).strip("-")
        json_path = output_dir / f"{stem}.json"
        markdown_path = output_dir / f"{stem}.md"
        json_path.write_text(json.dumps(run_data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        markdown_path.write_text(
            _markdown_report(
                args.label,
                args.report_size,
                args.concurrency,
                summary,
                metrics,
                recovery_seconds,
            ),
            encoding="utf-8",
        )
        print(markdown_path)
        return json_path
    finally:
        if rabbitmq_stopped:
            compose.start("rabbitmq")
            compose.wait_healthy("rabbitmq")
        ingestion.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load test the Ingestion upload API.")
    parser.add_argument("--requests", type=int, required=True)
    parser.add_argument("--concurrency", type=int, required=True)
    parser.add_argument("--report-size", choices=("small", "large"), required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--output-dir", default="docs/evidence/ingestion-load")
    parser.add_argument("--broker-down", action="store_true")
    args = parser.parse_args()
    if args.requests < 1 or args.concurrency < 1:
        parser.error("--requests and --concurrency must be positive")
    return args


if __name__ == "__main__":
    asyncio.run(run_load(parse_args()))
