import hashlib
import json
import logging
import re
import traceback
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from urllib.parse import urlsplit

import pytest
from fastapi.testclient import TestClient

ARTIFACTS_DIR = Path(__file__).resolve().parents[3] / "artifacts"
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("ingestion.tests")
logger.setLevel(logging.INFO)
logger.propagate = False
file_handler = logging.FileHandler(ARTIFACTS_DIR / "ingestion-test.log", mode="w", encoding="utf-8")
file_handler.setFormatter(
    logging.Formatter(
        "%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
)
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
XML_SECRET = re.compile(
    r"(?is)(<(?:[^>]*:)?(?:password|secret|token|api[_-]?key|authorization)\b[^>]*>)"
    r".*?(</(?:[^>]*:)?(?:password|secret|token|api[_-]?key|authorization)\s*>)"
)


def _test_id(node_id: str) -> str:
    return f"ING-{hashlib.sha256(node_id.encode('utf-8')).hexdigest()[:8].upper()}"


def _redact(value: str) -> str:
    value = SENSITIVE_VALUE.sub(r"\1[REDACTED]", value)
    value = URL_PASSWORD.sub(r"\1[REDACTED]@", value)
    return XML_SECRET.sub(r"\1[REDACTED]\2", value)


def _sanitize_value(value):
    if isinstance(value, dict):
        return {
            key: "[REDACTED]" if re.search(
                r"(?i)(authorization|token|password|secret|api[_-]?key)",
                str(key),
            ) else _sanitize_value(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_sanitize_value(item) for item in value]
    if isinstance(value, bytes):
        return _redact(value.decode("utf-8", errors="replace"))
    if isinstance(value, str):
        return _redact(value)
    return value


def _request_payload(kwargs: dict[str, object]) -> str:
    if kwargs.get("json") is not None:
        payload = _sanitize_value(kwargs["json"])
    elif kwargs.get("data") is not None and kwargs.get("files") is None:
        payload = _sanitize_value(kwargs["data"])
    elif kwargs.get("content") is not None:
        content = kwargs["content"]
        if isinstance(content, (str, bytes)):
            payload = _redact(
                content.decode("utf-8", errors="replace")
                if isinstance(content, bytes)
                else content
            )
        else:
            payload = f"<stream {type(content).__name__}>"
    elif kwargs.get("files") is not None:
        files = kwargs["files"]
        file_details = []
        if isinstance(files, dict):
            for field, file in files.items():
                filename = None
                size = None
                if isinstance(file, tuple):
                    filename = file[0] if file else None
                    content = file[1] if len(file) > 1 else None
                    size = len(content) if isinstance(content, (str, bytes)) else None
                file_details.append(
                    {
                        "field": str(field),
                        "filename": _redact(str(filename)) if filename is not None else None,
                        "size_bytes": size,
                    }
                )
        payload = {
            "fields": _sanitize_value(kwargs.get("data") or {}),
            "files": file_details or "<multipart file payload omitted>",
        }
    else:
        payload = "<none>"
    if not isinstance(payload, str):
        payload = json.dumps(payload, sort_keys=True, default=str)
    return payload[:600] + ("...<truncated>" if len(payload) > 600 else "")


def pytest_sessionstart(session: pytest.Session) -> None:
    logger.info("Execution started: %s", started_at.isoformat())


def pytest_runtest_logstart(nodeid: str, location) -> None:
    logger.info("START %s %s", _test_id(nodeid), nodeid)


@pytest.fixture(autouse=True)
def log_api_requests(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    original_request = TestClient.request
    node_id = request.node.nodeid

    def logged_request(client, method, url, *args, **kwargs):
        path = urlsplit(str(url)).path
        request_started = perf_counter()
        test_id = _test_id(node_id)
        logger.info(
            "REQUEST %s %s %s payload=%s",
            test_id,
            method.upper(),
            path,
            _request_payload(kwargs),
        )
        try:
            response = original_request(client, method, url, *args, **kwargs)
        except Exception:
            logger.error(
                "HTTP %s %s failed after %.2f ms",
                method,
                path,
                (perf_counter() - request_started) * 1000,
            )
            logger.error("%s", _redact(traceback.format_exc()))
            raise

        response_body = ""
        if response.status_code >= 400:
            response_body = _redact(response.text[:1000])
        logger.info(
            "RESPONSE %s %s %s status=%s duration_ms=%.2f%s",
            test_id,
            method,
            path,
            response.status_code,
            (perf_counter() - request_started) * 1000,
            f" body={response_body}" if response_body else "",
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
        logger.error("FAIL %s %s", test_id, report.nodeid)
        logger.error("%s", _redact(report.longreprtext))
        failed_details[report.nodeid] = _redact(report.longreprtext)
    elif report.skipped:
        logger.warning("SKIP %s: %s", report.nodeid, _redact(str(report.longrepr)))
        test_results[report.nodeid] = "skipped"
    elif report.when == "call" and report.passed:
        logger.info("PASS %s %s", _test_id(report.nodeid), report.nodeid)


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
