from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    JSON,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB

metadata = MetaData(schema="parser")

parsed_runs = Table(
    "parsed_runs",
    metadata,
    Column("run_id", String(64), primary_key=True),
    Column("commit_sha", String(128), nullable=False),
    Column("branch", String(255), nullable=False),
    Column("total_tests", Integer, nullable=False, default=0),
    Column("passed_tests", Integer, nullable=False, default=0),
    Column("failed_tests", Integer, nullable=False, default=0),
    Column("skipped_tests", Integer, nullable=False, default=0),
    Column("error_tests", Integer, nullable=False, default=0),
    Column("status", String(32), nullable=False, default="completed"),
    Column("parsed_at", DateTime(timezone=True), nullable=False),
)

test_results = Table(
    "test_results",
    metadata,
    Column("id", BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True),
    Column("run_id", String(64), nullable=False, index=True),
    Column("test_id", String(255), nullable=False, index=True),
    Column("test_name", String(255), nullable=False),
    Column("class_name", String(255), nullable=False),
    Column("file_path", String(512), nullable=True),
    Column("duration_seconds", Float, nullable=False, default=0.0),
    Column("status", String(32), nullable=False),
    Column("error_type", String(255), nullable=True),
    Column("error_message", Text, nullable=True),
    Column("stack_trace", Text, nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("run_id", "test_id", name="uq_test_results_run_test"),
)

outbox = Table(
    "outbox",
    metadata,
    Column("id", BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True),
    Column("event_type", String(64), nullable=False),
    Column("payload", JSON().with_variant(JSONB, "postgresql"), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("published_at", DateTime(timezone=True), nullable=True),
)

Index(
    "ix_parser_outbox_pending",
    outbox.c.id,
    postgresql_where=outbox.c.published_at.is_(None),
)

Index(
    "ix_test_results_test_history",
    test_results.c.test_id,
    test_results.c.created_at,
)
