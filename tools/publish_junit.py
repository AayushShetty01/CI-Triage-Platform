#!/usr/bin/env python3
"""Publish CI JUnit reports to the local triage stack."""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from xml.etree import ElementTree


INGESTION_URL = os.getenv("INGESTION_URL", "http://localhost:8001")
PARSER_URL = os.getenv("PARSER_URL", "http://localhost:8002")
CLASSIFIER_URL = os.getenv("CLASSIFIER_URL", "http://localhost:8003")
QUERY_URL = os.getenv("QUERY_URL", "http://localhost:8005")
REPORTS_DIR = Path(os.getenv("REPORTS_DIR", "reports"))
POLL_TIMEOUT_SECONDS = int(os.getenv("POLL_TIMEOUT_SECONDS", "180"))


def _request(url: str, data: bytes | None = None, headers: dict[str, str] | None = None):
    request = Request(url, data=data, headers=headers or {})
    with urlopen(request, timeout=15) as response:
        return response.status, response.read()


def _json_or_pending(url: str) -> dict | None:
    try:
        status, body = _request(url)
    except HTTPError as exc:
        if exc.code in {404, 503}:
            return None
        raise
    if status != 200:
        return None
    return json.loads(body)


def _add_stable_properties(
    report: bytes,
    run_id: str,
    service: str,
    repository: str,
    workflow: str,
    commit_sha: str,
) -> bytes:
    root = ElementTree.fromstring(report)
    properties = ElementTree.Element("properties")
    for name, value in (
        ("ci.run_id", run_id),
        ("ci.service", service),
        ("ci.repository", repository),
        ("ci.workflow", workflow),
        ("ci.commit_sha", commit_sha),
    ):
        ElementTree.SubElement(properties, "property", name=name, value=value)
    root.insert(0, properties)
    return ElementTree.tostring(root, encoding="utf-8", xml_declaration=True)


def _await_projection(run_id: str, expected_tests: int) -> dict:
    deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
    last_status = "not yet projected"
    while time.monotonic() < deadline:
        parsed = _json_or_pending(f"{PARSER_URL}/runs/{run_id}")
        classified = _json_or_pending(f"{CLASSIFIER_URL}/runs/{run_id}/classifications")
        projected = _json_or_pending(f"{QUERY_URL}/api/runs/{run_id}")
        if parsed and projected:
            last_status = (
                f"Parser tests={parsed.get('total_tests')}, "
                f"Classifier status={classified.get('status_counts') if classified else 'no failures'}, "
                f"Query state={projected.get('state')}"
            )
            failed_tests = parsed.get("failed_tests")
            classifier_ready = (
                classified is not None
                and classified.get("status_counts", {}).get("pending", 0) == 0
            ) or (failed_tests == 0 and classified is None)
            if (
                parsed.get("total_tests") == expected_tests
                and projected.get("state") in {"complete", "degraded"}
                and classifier_ready
            ):
                return projected
        time.sleep(2)
    raise TimeoutError(f"Run {run_id} was not fully projected: {last_status}")


def _publish_report(path: Path, api_key: str) -> dict:
    report = path.read_bytes()
    root = ElementTree.fromstring(report)
    expected_tests = len(root.findall(".//testcase"))
    service = path.stem
    repository = os.environ["GITHUB_REPOSITORY"]
    workflow = os.environ["GITHUB_WORKFLOW"]
    github_run_id = os.environ["GITHUB_RUN_ID"]
    attempt = os.environ["GITHUB_RUN_ATTEMPT"]
    external_run_id = f"{github_run_id}-{attempt}-{service}"
    commit_sha = os.environ["GITHUB_SHA"]
    branch = os.environ["GITHUB_REF_NAME"]
    run_url = (
        f"{os.environ['GITHUB_SERVER_URL']}/{repository}/actions/runs/{github_run_id}"
    )
    timestamp = datetime.now(timezone.utc).isoformat()
    body = _add_stable_properties(
        report, external_run_id, service, repository, workflow, commit_sha
    )
    headers = {
        "Content-Type": "application/xml",
        "X-API-Key": api_key,
        "X-Commit-Sha": commit_sha,
        "X-Branch": branch,
        "X-CI-Repository": repository,
        "X-CI-Workflow": workflow,
        "X-CI-Run-Id": external_run_id,
        "X-CI-Run-Attempt": attempt,
        "X-CI-Service": service,
        "X-CI-Actor": os.environ["GITHUB_ACTOR"],
        "X-CI-Run-Url": run_url,
        "X-CI-Timestamp": timestamp,
    }
    status, response_body = _request(f"{INGESTION_URL}/runs", body, headers)
    if status != 202:
        raise RuntimeError(f"Ingestion rejected {path.name} with HTTP {status}")
    accepted = json.loads(response_body)
    projected = _await_projection(accepted["run_id"], expected_tests)
    return {
        "report": path.name,
        "service": service,
        "run_id": accepted["run_id"],
        "ci_run_id": external_run_id,
        "run_url": run_url,
        "projected": projected,
    }


def main() -> None:
    api_key = os.getenv("TRIAGE_INGESTION_API_KEY", "")
    reports = sorted(REPORTS_DIR.glob("*.xml"))
    if not api_key:
        print("TRIAGE_INGESTION_API_KEY is unset; skipping non-blocking triage publish.")
        return
    if not reports:
        print(f"No JUnit XML reports found in {REPORTS_DIR}; nothing to publish.")
        return

    published = [_publish_report(path, api_key) for path in reports]
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    (REPORTS_DIR / "published-runs.json").write_text(
        json.dumps(published, indent=2), encoding="utf-8"
    )
    print(f"Published and projected {len(published)} JUnit report(s).")


if __name__ == "__main__":
    main()
