from datetime import datetime, timezone
from unittest.mock import patch

from sqlalchemy import insert

from app.database import parsed_runs, test_results


def test_health_check_success(parser_client):
    client, _ = parser_client
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["service"] == "parser"


def test_health_check_db_failure(parser_client):
    client, engine = parser_client
    with patch.object(engine, "connect", side_effect=Exception("DB down")):
        response = client.get("/health")
        assert response.status_code == 503


def test_metrics_endpoint(parser_client):
    client, _ = parser_client
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "parser_health_checks_total" in response.text
    assert "parser_outbox_pending" in response.text


def test_get_run_and_results(parser_client):
    client, engine = parser_client
    now = datetime.now(timezone.utc)
    run_id = "testrun0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

    with engine.begin() as conn:
        conn.execute(
            insert(parsed_runs).values(
                run_id=run_id,
                commit_sha="abcdef123",
                branch="feature-x",
                total_tests=2,
                passed_tests=1,
                failed_tests=1,
                skipped_tests=0,
                error_tests=0,
                status="completed",
                parsed_at=now,
            )
        )
        conn.execute(
            insert(test_results).values(
                [
                    {
                        "run_id": run_id,
                        "test_id": "test_pass",
                        "test_name": "test_pass",
                        "class_name": "Tests",
                        "file_path": None,
                        "duration_seconds": 0.1,
                        "status": "passed",
                        "error_type": None,
                        "error_message": None,
                        "stack_trace": None,
                        "created_at": now,
                    },
                    {
                        "run_id": run_id,
                        "test_id": "test_fail",
                        "test_name": "test_fail",
                        "class_name": "Tests",
                        "file_path": "tests/test.py",
                        "duration_seconds": 0.2,
                        "status": "failed",
                        "error_type": "AssertionError",
                        "error_message": "failed check",
                        "stack_trace": "line 10",
                        "created_at": now,
                    },
                ]
            )
        )

    # 1. Test GET /runs/{run_id}
    res = client.get(f"/runs/{run_id}")
    assert res.status_code == 200
    data = res.json()
    assert data["run_id"] == run_id
    assert data["total_tests"] == 2
    assert data["failed_tests"] == 1

    # 2. Test GET /runs/{run_id}/results
    res = client.get(f"/runs/{run_id}/results")
    assert res.status_code == 200
    results = res.json()
    assert len(results) == 2

    # 3. Test GET /runs/{run_id}/failures
    res = client.get(f"/runs/{run_id}/failures")
    assert res.status_code == 200
    failures = res.json()
    assert len(failures) == 1
    assert failures[0]["test_id"] == "test_fail"
    assert failures[0]["error_type"] == "AssertionError"

    # 4. Non-existent run
    res = client.get("/runs/nonexistent")
    assert res.status_code == 404
