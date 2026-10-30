import hashlib
import logging
import re
import traceback
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from urllib.parse import urlsplit

import pytest
from fastapi.testclient import TestClient

from app.logging_config import IngestionJsonFormatter

ARTIFACTS_DIR = Path(__file__).resolve().parents[3] / "artifacts"
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("ingestion.tests")
logger.setLevel(logging.INFO)
logger.propagate = False
file_handler = logging.FileHandler(ARTIFACTS_DIR / "ingestion-test.log", mode="w", encoding="utf-8")
file_handler.setFormatter(IngestionJsonFormatter())
logger.addHandler(file_handler)

started_at = datetime.now(timezone.utc)
started_at_clock = perf_counter()
test_results: dict[str, str] = {}
failed_details: dict[str, str] = {}
SENSITIVE_VALUE = re.compile(
    r"""(?i)(["']?(?:authorization|proxy-authorization|api[_-]?(?:token|key)|"""
    r"""access[_-]?token|password|secret)["']?\s*[:=]\s*["']?)"""
    r"""(?:bearer\s+)?[^"'\s,;}]+"""
)
URL_PASSWORD = re.compile(r"(?i)(://[^:/@\s]+:)[^@\s]+@")

def _test_id(node_id: str) -> str:
    return f"ING-{hashlib.sha256(node_id.encode('utf-8')).hexdigest()[:8].upper()}"


def _redact(value: str) -> str:
    value = SENSITIVE_VALUE.sub(r"\1[REDACTED]", value)
    return URL_PASSWORD.sub(r"\1[REDACTED]@", value)


def pytest_sessionstart(session: pytest.Session) -> None:
    logger.info(
        "Execution started",
        extra={"event": "test_session_started", "started_at": started_at.isoformat()},
    )


def pytest_runtest_logstart(nodeid: str, location) -> None:
    logger.info(
        "Test started",
        extra={"event": "test_started", "test_id": _test_id(nodeid), "node_id": nodeid},
    )


@pytest.fixture(autouse=True)
def log_api_requests(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    original_request = TestClient.request
    node_id = request.node.nodeid

    def logged_request(client, method, url, *args, **kwargs):
        path = urlsplit(str(url)).path
        request_started = perf_counter()
        test_id = _test_id(node_id)
        logger.info(
            "HTTP request started",
            extra={
                "event": "test_http_request_started",
                "test_id": test_id,
                "method": method.upper(),
                "endpoint": path,
            },
        )
        try:
            response = original_request(client, method, url, *args, **kwargs)
        except Exception:
            logger.error(
                "HTTP request failed",
                extra={
                    "event": "test_http_request_failed",
                    "method": method.upper(),
                    "endpoint": path,
                    "duration_ms": round((perf_counter() - request_started) * 1000, 2),
                },
            )
            logger.error(
                "HTTP request traceback",
                extra={
                    "event": "test_http_request_traceback",
                    "traceback": _redact(traceback.format_exc()),
                },
            )
            raise

        logger.info(
            "HTTP response received",
            extra={
                "event": "test_http_response_received",
                "test_id": test_id,
                "method": method.upper(),
                "endpoint": path,
                "status_code": response.status_code,
                "duration_ms": round((perf_counter() - request_started) * 1000, 2),
            },
        )
        return response

    monkeypatch.setattr(TestClient, "request", logged_request)


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    if report.when == "call":
        test_results[report.nodeid] = report.outcome
    elif report.failed:
        test_results[report.nodeid] = "failed"

    if report.failed:
        test_id = _test_id(report.nodeid)
        logger.error(
            "Test failed",
            extra={"event": "test_failed", "test_id": test_id, "node_id": report.nodeid},
        )
        logger.error(
            "Test failure traceback",
            extra={
                "event": "test_failure_traceback",
                "traceback": _redact(report.longreprtext),
            },
        )
        failed_details[report.nodeid] = _redact(report.longreprtext)
    elif report.skipped:
        logger.warning(
            "Test skipped",
            extra={
                "event": "test_skipped",
                "node_id": report.nodeid,
                "reason": _redact(str(report.longrepr)),
            },
        )
        test_results[report.nodeid] = "skipped"
    elif report.when == "call" and report.passed:
        logger.info(
            "Test passed",
            extra={
                "event": "test_passed",
                "test_id": _test_id(report.nodeid),
                "node_id": report.nodeid,
            },
        )


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    finished_at = datetime.now(timezone.utc)
    duration = perf_counter() - started_at_clock
    passed = sum(outcome == "passed" for outcome in test_results.values())
    failed = sum(outcome == "failed" for outcome in test_results.values())
    skipped = sum(outcome == "skipped" for outcome in test_results.values())
    executed = passed + failed
    pass_percentage = round(100 * passed / executed, 2) if executed else 0.0
    fail_percentage = round(100 * failed / executed, 2) if executed else 0.0
    if exitstatus != 0 or failed:
        status = "FAILED"
    elif passed:
        status = "PASSED"
    else:
        status = "NO TESTS EXECUTED"

    lines = [
        "Ingestion API test run",
        f"Started at: {started_at.isoformat()}",
        f"Finished at: {finished_at.isoformat()}",
        f"Duration: {duration:.2f} seconds",
        f"Total test cases: {session.testscollected}",
        f"Passed: {passed}",
        f"Failed: {failed}",
        f"Skipped: {skipped}",
        f"Pass percentage (of executed tests): {pass_percentage:.2f}%",
        f"Fail percentage (of executed tests): {fail_percentage:.2f}%",
        f"Overall execution status: {status}",
    ]
    if failed_details:
        lines.extend(["", "Failed test IDs and messages:"])
        for node_id, detail in failed_details.items():
            lines.extend([f"- {_test_id(node_id)} {node_id}", detail])

    (ARTIFACTS_DIR / "ingestion-summary.txt").write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )
    logger.info(
        "Execution finished: %s; total=%d passed=%d failed=%d skipped=%d duration=%.2fs",
        status,
        session.testscollected,
        passed,
        failed,
        skipped,
        duration,
    )
    file_handler.flush()
