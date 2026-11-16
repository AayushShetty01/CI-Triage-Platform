from sqlalchemy import JSON, Column, DateTime, Float, ForeignKeyConstraint, Index, Integer, MetaData, String, Table, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB

metadata = MetaData(schema="query")

runs = Table(
    "runs", metadata,
    Column("run_id", String(64), primary_key=True),
    Column("commit_sha", String(128)),
    Column("branch", String(255)),
    Column("received_at", DateTime(timezone=True)),
    Column("parsed_at", DateTime(timezone=True)),
    Column("total_tests", Integer),
    Column("failed_tests", Integer),
    Column("passed_tests", Integer),
    Column("skipped_tests", Integer),
    Column("ci_metadata", JSON().with_variant(JSONB, "postgresql")),
)

tests = Table(
    "tests", metadata,
    Column("run_id", String(64), nullable=False),
    Column("test_id", String(255), nullable=False),
    Column("test_name", String(255), nullable=False),
    Column("class_name", String(255), nullable=False),
    Column("status", String(20), nullable=False),
    Column("duration_seconds", String(32)),
    ForeignKeyConstraint(["run_id"], ["query.runs.run_id"]),
    UniqueConstraint("run_id", "test_id", name="uq_query_test_run"),
)
Index("ix_query_tests_history", tests.c.test_id, tests.c.status)

failures = Table(
    "failures", metadata,
    Column("run_id", String(64), nullable=False),
    Column("test_id", String(255), nullable=False),
    Column("test_name", String(255), nullable=False),
    Column("class_name", String(255), nullable=False),
    Column("error_type", String(255)),
    Column("error_message", Text),
    Column("fingerprint", String(32)),
    Column("category", String(32)),
    Column("confidence", Float),
    Column("status", String(20), nullable=False),
    Column("source", String(20)),
    Column("rules_version", String(64)),
    Column("model", String(128)),
    Column("prompt_version", String(64)),
    Column("prompt_hash", String(128)),
    Column("classified_at", DateTime(timezone=True)),
    ForeignKeyConstraint(["run_id"], ["query.runs.run_id"]),
    UniqueConstraint("run_id", "test_id", name="uq_query_failure_run"),
)
Index("ix_query_failures_category", failures.c.category)
Index("ix_query_failures_fingerprint", failures.c.fingerprint)

processed_events = Table(
    "processed_events", metadata,
    Column("event_id", String(36), primary_key=True),
    Column("event_type", String(40), nullable=False),
    Column("processed_at", DateTime(timezone=True), nullable=False),
)
