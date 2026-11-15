from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.engine import Engine

from app.database import failures, runs, tests


def _iso(value: Any) -> Any:
    return value.isoformat() if isinstance(value, datetime) else value


def _row(row: Any) -> dict[str, Any]:
    return {key: _iso(value) for key, value in row.items()}


def list_runs(engine: Engine, limit: int = 50) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(select(runs).order_by(
            func.coalesce(runs.c.received_at, runs.c.parsed_at).desc()
        ).limit(limit)).mappings().all()
        return [_run_summary(conn, row) for row in rows]


def _run_summary(conn, row) -> dict[str, Any]:
    counts = dict(conn.execute(select(failures.c.status, func.count()).where(
        failures.c.run_id == row["run_id"]
    ).group_by(failures.c.status)).all())
    expected = row["failed_tests"]
    pending = counts.get("pending", 0)
    if expected is not None:
        pending += max(0, expected - sum(counts.values()))
    if row["parsed_at"] is None:
        state = "accepted"
    elif pending:
        state = "pending"
    elif counts.get("degraded", 0):
        state = "degraded"
    else:
        state = "complete"
    return {**_row(row), "state": state, "classification_counts": {
        "pending": pending,
        "classified": counts.get("classified", 0),
        "degraded": counts.get("degraded", 0),
    }}


def get_run(engine: Engine, run_id: str) -> dict[str, Any] | None:
    with engine.connect() as conn:
        row = conn.execute(select(runs).where(runs.c.run_id == run_id)).mappings().one_or_none()
        if row is None:
            return None
        failure_rows = conn.execute(select(failures).where(
            failures.c.run_id == run_id
        ).order_by(failures.c.test_id)).mappings().all()
        return {**_run_summary(conn, row), "failures": [_row(f) for f in failure_rows]}


def insights(engine: Engine, days: int = 7, branch: str | None = None) -> dict[str, Any]:
    since = datetime.now(timezone.utc) - timedelta(days=days)
    with engine.connect() as conn:
        run_rows = conn.execute(select(runs).where(
            func.coalesce(runs.c.received_at, runs.c.parsed_at) >= since,
            *([runs.c.branch == branch] if branch else []),
        )).mappings().all()
        ids = [row["run_id"] for row in run_rows]
        if not ids:
            return {"days": days, "branch": branch, "runs": 0, "categories": {},
                    "pending": 0, "degraded": 0, "top_failures": [], "classifier_versions": []}
        failure_rows = conn.execute(select(failures).where(failures.c.run_id.in_(ids))).mappings().all()
        categories: dict[str, int] = {}
        groups: dict[str, dict[str, Any]] = {}
        versions: set[tuple[str | None, str | None, str | None]] = set()
        pending = degraded = 0
        for row in failure_rows:
            if row["status"] == "pending":
                pending += 1
                continue
            if row["status"] == "degraded":
                degraded += 1
            category = row["category"] or "unknown"
            categories[category] = categories.get(category, 0) + 1
            if row["rules_version"]:
                versions.add((row["rules_version"], row["model"], row["prompt_version"]))
            key = row["fingerprint"] or f"{row['run_id']}:{row['test_id']}"
            group = groups.setdefault(key, {"fingerprint": row["fingerprint"],
                                            "example": row["error_message"] or row["test_name"],
                                            "category": category, "count": 0})
            group["count"] += 1
        pending += sum(max(0, (run["failed_tests"] or 0) - sum(
            1 for item in failure_rows if item["run_id"] == run["run_id"]
        )) for run in run_rows)
        return {"days": days, "branch": branch, "runs": len(run_rows),
                "categories": categories, "pending": pending, "degraded": degraded,
                "top_failures": sorted(groups.values(), key=lambda x: x["count"], reverse=True)[:10],
                "classifier_versions": [{"rules_version": r, "model": m, "prompt_version": p}
                                        for r, m, p in sorted(versions, key=str)]}


def flaky_candidates(engine: Engine, days: int = 14, branch: str | None = None) -> list[dict[str, Any]]:
    since = datetime.now(timezone.utc) - timedelta(days=days)
    with engine.connect() as conn:
        rows = conn.execute(select(tests.c.test_id, tests.c.test_name, tests.c.class_name,
                                   tests.c.status, tests.c.run_id).join(
            runs, tests.c.run_id == runs.c.run_id
        ).where(func.coalesce(runs.c.received_at, runs.c.parsed_at) >= since,
                *([runs.c.branch == branch] if branch else []))).all()
    history: dict[str, dict[str, Any]] = {}
    for test_id, name, class_name, status, run_id in rows:
        entry = history.setdefault(test_id, {"test_id": test_id, "test_name": name,
                                             "class_name": class_name, "passed_runs": set(),
                                             "failed_runs": set()})
        if status == "passed":
            entry["passed_runs"].add(run_id)
        elif status in {"failed", "error"}:
            entry["failed_runs"].add(run_id)
    return sorted(({"test_id": item["test_id"], "test_name": item["test_name"],
                    "class_name": item["class_name"], "passed_runs": len(item["passed_runs"]),
                    "failed_runs": len(item["failed_runs"]), "assessment": "potentially_flaky"}
                   for item in history.values() if item["passed_runs"] and item["failed_runs"]),
                  key=lambda item: item["failed_runs"], reverse=True)
