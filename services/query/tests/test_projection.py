from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.pool import StaticPool

from app.database import failures, metadata, processed_events, tests
from app.projection import apply_event
from app.queries import flaky_candidates, get_run, insights


@pytest.fixture
def engine():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False},
                           poolclass=StaticPool).execution_options(schema_translate_map={"query": None})
    metadata.create_all(engine)
    yield engine
    engine.dispose()


def event(kind, run_id, payload):
    return {"event_id": str(uuid4()), "event_type": kind,
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": "1.0", "run_id": run_id, "payload": payload}


def extracted(run_id, failed=True):
    return event("FailuresExtracted", run_id, {
        "commit_sha": run_id[:12], "branch": "main", "total_tests": 1,
        "failed_tests": int(failed), "passed_tests": int(not failed), "skipped_tests": 0,
        "failures": [{"test_id": "pkg.Test::case", "test_name": "case", "class_name": "pkg.Test",
                      "error_type": "AssertionError", "error_message": "expected 2"}] if failed else [],
    })


def classified(run_id, status="classified"):
    return event("FailureClassified", run_id, {
        "test_id": "pkg.Test::case", "test_name": "case", "class_name": "pkg.Test",
        "commit_sha": run_id[:12], "branch": "main", "fingerprint": "a" * 32,
        "category": "assertion" if status == "classified" else "unknown",
        "status": status, "source": "rule" if status == "classified" else "fallback",
        "versions": {"rules_version": "1", "model": None, "prompt_version": None,
                     "prompt_hash": None},
    })


def test_pending_then_classified_and_duplicate_delivery(engine):
    run_id = "a" * 64
    parsed = extracted(run_id)
    assert apply_event(engine, parsed, [{"test_id": "pkg.Test::case", "test_name": "case",
                                         "class_name": "pkg.Test", "status": "failed"}])
    assert get_run(engine, run_id)["state"] == "pending"
    assert not apply_event(engine, parsed)
    assert apply_event(engine, classified(run_id))
    run = get_run(engine, run_id)
    assert run["state"] == "complete"
    assert run["failures"][0]["rules_version"] == "1"
    with engine.connect() as conn:
        assert conn.scalar(select(func.count()).select_from(processed_events)) == 2
        assert conn.scalar(select(func.count()).select_from(failures)) == 1


def test_republished_run_with_new_event_id_does_not_duplicate_counts(engine):
    run_id = "0" * 64
    first = extracted(run_id)
    second = extracted(run_id)
    apply_event(engine, first)
    apply_event(engine, second)
    apply_event(engine, classified(run_id))
    assert insights(engine)["categories"] == {"assertion": 1}
    with engine.connect() as conn:
        assert conn.scalar(select(func.count()).select_from(failures)) == 1


def test_out_of_order_classification_is_not_overwritten(engine):
    run_id = "b" * 64
    apply_event(engine, classified(run_id))
    apply_event(engine, extracted(run_id))
    assert get_run(engine, run_id)["state"] == "complete"
    failure = get_run(engine, run_id)["failures"][0]
    assert failure["category"] == "assertion"
    assert failure["error_message"] == "expected 2"


def test_degraded_is_terminal_and_reported(engine):
    run_id = "c" * 64
    apply_event(engine, extracted(run_id))
    apply_event(engine, classified(run_id, "degraded"))
    assert get_run(engine, run_id)["state"] == "degraded"
    assert insights(engine)["degraded"] == 1


def test_flaky_requires_pass_and_failure_in_distinct_runs(engine):
    failed_run, passed_run = "d" * 64, "e" * 64
    apply_event(engine, extracted(failed_run), [{"test_id": "pkg.Test::case", "test_name": "case",
                                               "class_name": "pkg.Test", "status": "failed"}])
    assert flaky_candidates(engine) == []
    apply_event(engine, extracted(passed_run, False), [{"test_id": "pkg.Test::case", "test_name": "case",
                                                     "class_name": "pkg.Test", "status": "passed"}])
    candidates = flaky_candidates(engine)
    assert len(candidates) == 1
    assert candidates[0]["passed_runs"] == candidates[0]["failed_runs"] == 1
    with engine.connect() as conn:
        assert conn.scalar(select(func.count()).select_from(tests)) == 2


def test_invalid_classification_is_rejected(engine):
    item = classified("f" * 64)
    item["payload"]["category"] = "run_shell_command"
    with pytest.raises(ValueError):
        apply_event(engine, item)


def test_late_older_classification_does_not_replace_newer_result(engine):
    run_id = "1" * 64
    newer = classified(run_id)
    older = classified(run_id, "degraded")
    older["occurred_at"] = "2020-01-01T00:00:00+00:00"
    apply_event(engine, newer)
    apply_event(engine, older)
    assert get_run(engine, run_id)["failures"][0]["status"] == "classified"


def test_insights_flag_mixed_categories_and_versions(engine):
    first, second = "2" * 64, "3" * 64
    for run_id in (first, second):
        apply_event(engine, extracted(run_id))
    apply_event(engine, classified(first))
    changed = classified(second)
    changed["payload"]["category"] = "timeout"
    changed["payload"]["versions"]["rules_version"] = "2"
    apply_event(engine, changed)
    result = insights(engine)
    assert result["top_failures"][0]["category"] == "mixed"
    assert len(result["classifier_versions"]) == 2


def test_junit_without_classname_is_accepted(engine):
    run_id = "4" * 64
    parsed = extracted(run_id)
    parsed["payload"]["failures"][0]["class_name"] = ""
    result = classified(run_id)
    result["payload"]["class_name"] = ""
    apply_event(engine, parsed)
    apply_event(engine, result)
    assert get_run(engine, run_id)["state"] == "complete"
