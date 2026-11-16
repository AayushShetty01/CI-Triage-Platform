from datetime import datetime, timezone
from uuid import uuid4

from fastapi.testclient import TestClient

from app.main import create_app
from app.projection import apply_event


def test_dashboard_and_api_show_pending_then_complete(query_test_database):
    app = create_app(query_test_database, enable_consumer=False)
    run_id = "a" * 64
    with TestClient(app) as client:
        assert client.get("/").status_code == 200
        assert client.get("/app.js").status_code == 200
        assert client.get("/api/runs/unknown").status_code == 404
        parsed = {"event_id": str(uuid4()), "event_type": "FailuresExtracted",
                  "schema_version": "1.0",
                  "occurred_at": datetime.now(timezone.utc).isoformat(), "run_id": run_id,
                  "payload": {"commit_sha": "abc", "branch": "main", "total_tests": 1,
                              "failed_tests": 1, "passed_tests": 0, "skipped_tests": 0,
                              "failures": [{"test_id": "test.one", "test_name": "one", "class_name": "Test"}]}}
        apply_event(app.state.engine, parsed)
        assert client.get(f"/api/runs/{run_id}").json()["state"] == "pending"
        assert client.get("/api/insights").json()["pending"] == 1
        assert client.get("/api/insights").json()["test_counts"] == {
            "total": 1, "passed": 0, "failed": 1, "skipped": 0,
        }
        assert client.get("/api/services").json()["services"] == [
            {
                "service": "unknown",
                "runs": 1,
                "tests": 1,
                "passed": 0,
                "failed": 1,
                "skipped": 0,
                "failures": 1,
                "categories": {},
            }
        ]
        assert len(client.get("/api/runs?branch=main").json()) == 1
        assert client.get("/api/runs?branch=other").json() == []
        assert client.get("/metrics").status_code == 200
        assert client.get("/health").status_code == 200
