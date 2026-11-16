#!/usr/bin/env python3
"""Exercise a report through the Compose pipeline and its rejection paths."""

import json
import os
import time
import uuid
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

INGESTION_URL = os.getenv("INGESTION_BASE_URL", "http://localhost:8001")
PARSER_URL = os.getenv("PARSER_BASE_URL", "http://localhost:8002")
CLASSIFIER_URL = os.getenv("CLASSIFIER_BASE_URL", "http://localhost:8003")
QUERY_URL = os.getenv("QUERY_URL", "http://localhost:8005")
API_KEY = os.getenv("INGESTION_API_KEY", "dev-ingestion-key-change-me")
MAX_REPORT_SIZE_BYTES = int(os.getenv("MAX_REPORT_SIZE_BYTES", "10485760"))
POLL_TIMEOUT_SECONDS = int(os.getenv("E2E_POLL_TIMEOUT_SECONDS", "90"))

REPORT = b"""<?xml version="1.0"?>
<testsuite name="e2e" tests="2" failures="1">
  <testcase classname="e2e.SampleTests" name="assertion_failure">
    <failure type="AssertionError" message="expected 2, got 1">assertion failed</failure>
  </testcase>
  <testcase classname="e2e.SampleTests" name="passes" />
</testsuite>
"""


def request(
    method: str,
    url: str,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
    allow_early_close: bool = False,
):
    req = Request(url, data=body, headers=headers or {}, method=method)
    try:
        with urlopen(req, timeout=10) as response:
            return response.status, response.read()
    except HTTPError as exc:
        return exc.code, exc.read()
    except URLError as exc:
        if allow_early_close and isinstance(exc.reason, (BrokenPipeError, ConnectionResetError)):
            return None, b""
        raise


def get_json(url: str):
    try:
        status, body = request("GET", url)
    except URLError:
        return 0, None
    if status != 200:
        return status, None
    return status, json.loads(body)


def upload(body: bytes, commit_sha: str, allow_early_close: bool = False):
    return request(
        "POST",
        f"{INGESTION_URL}/runs",
        body,
        {
            "Content-Type": "application/xml",
            "X-API-Key": API_KEY,
            "X-Commit-Sha": commit_sha,
            "X-Branch": "e2e",
        },
        allow_early_close=allow_early_close,
    )


def wait_for_projection(run_id: str):
    deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        parser_status, parsed_run = get_json(f"{PARSER_URL}/runs/{run_id}")
        classifier_status, classified_run = get_json(
            f"{CLASSIFIER_URL}/runs/{run_id}/classifications"
        )
        query_status, projected_run = get_json(f"{QUERY_URL}/api/runs/{run_id}")
        if (
            parser_status == 200
            and classifier_status == 200
            and query_status == 200
            and parsed_run.get("total_tests") == 2
            and len(classified_run.get("classifications", [])) == 1
            and projected_run.get("state") == "complete"
        ):
            return parsed_run, classified_run, projected_run
        time.sleep(1)
    raise AssertionError(
        "Timed out waiting for Parser, Classifier, and Query to project the run "
        f"{run_id}; last statuses were "
        f"{parser_status}, {classifier_status}, {query_status}"
    )


def main() -> None:
    commit_sha = f"e2e-{uuid.uuid4().hex}"
    status, body = upload(REPORT, commit_sha)
    assert status == 202, f"Ingestion returned {status}: {body.decode(errors='replace')}"
    first = json.loads(body)
    run_id = first["run_id"]

    duplicate_status, duplicate_body = upload(REPORT, commit_sha)
    assert duplicate_status == 202, (
        f"Duplicate upload returned {duplicate_status}: "
        f"{duplicate_body.decode(errors='replace')}"
    )
    duplicate = json.loads(duplicate_body)
    assert duplicate["run_id"] == run_id, "Repeated upload created a different run"

    parsed, classified, projected = wait_for_projection(run_id)
    assert parsed["failed_tests"] == 1
    results_status, results_body = get_json(f"{PARSER_URL}/runs/{run_id}/results")
    assert results_status == 200 and len(results_body) == 2, "Parser did not store both tests"
    assert classified["status_counts"].get("classified") == 1
    assert len(projected["failures"]) == 1
    assert projected["classification_counts"].get("classified") == 1

    status, body = upload(REPORT, commit_sha)
    assert status == 202 and json.loads(body)["run_id"] == run_id
    results_status, results_body = get_json(f"{PARSER_URL}/runs/{run_id}/results")
    _, classified_after = get_json(f"{CLASSIFIER_URL}/runs/{run_id}/classifications")
    _, projected_after = get_json(f"{QUERY_URL}/api/runs/{run_id}")
    assert results_status == 200 and len(results_body) == 2, "Duplicate created Parser rows"
    assert len(classified_after["classifications"]) == 1, "Duplicate created classifications"
    assert len(projected_after["failures"]) == 1, "Duplicate created Query failures"

    malformed_status, _ = upload(b"<testsuite name='broken'>", f"bad-{uuid.uuid4().hex}")
    assert malformed_status == 400, f"Malformed XML was not rejected: {malformed_status}"

    oversized_status, _ = upload(
        b"x" * (MAX_REPORT_SIZE_BYTES + 1),
        f"large-{uuid.uuid4().hex}",
        allow_early_close=True,
    )
    if oversized_status is None:
        metrics_status, metrics_body = request("GET", f"{INGESTION_URL}/metrics")
        assert (
            metrics_status == 200 and b'path="/runs",status="413"' in metrics_body
        ), "Oversize upload connection closed without an observed 413 response"
    else:
        assert oversized_status == 413, f"Oversized report was not rejected: {oversized_status}"

    entity_payload = (
        b'<!DOCTYPE testsuite [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>'
        b'<testsuite name="injection"><testcase name="case">'
        b'<failure>&xxe;</failure></testcase></testsuite>'
    )
    injection_status, _ = upload(entity_payload, f"xml-entity-{uuid.uuid4().hex}")
    assert injection_status == 400, f"XML entity payload was not rejected: {injection_status}"
    print(f"E2E smoke passed for run {run_id}")


if __name__ == "__main__":
    main()
