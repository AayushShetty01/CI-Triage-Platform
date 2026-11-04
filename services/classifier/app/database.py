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

metadata = MetaData(schema="classifier")

_ID = BigInteger().with_variant(Integer, "sqlite")

# One row per consumed FailuresExtracted event: the idempotency guard.
processed_events = Table(
    "processed_events",
    metadata,
    Column("event_id", String(64), primary_key=True),
    Column("run_id", String(64), nullable=False),
    Column("processed_at", DateTime(timezone=True), nullable=False),
)

# Similar failures (same fingerprint) across all runs.
failure_groups = Table(
    "failure_groups",
    metadata,
    Column("fingerprint", String(32), primary_key=True),
    Column("error_type", String(255), nullable=True),
    Column("normalized_message", Text, nullable=False),
    Column("top_frame", String(512), nullable=True),
    Column("occurrence_count", Integer, nullable=False, default=0),
    Column("first_seen_at", DateTime(timezone=True), nullable=False),
    Column("last_seen_at", DateTime(timezone=True), nullable=False),
)

# One row per failed test case per run.
# status: pending (waiting for the LLM) | classified | degraded (LLM unavailable)
# source: rule | llm | llm_cache | fallback
classifications = Table(
    "classifications",
    metadata,
    Column("id", _ID, primary_key=True, autoincrement=True),
    Column("run_id", String(64), nullable=False),
    Column("test_id", String(255), nullable=False),
    Column("test_name", String(255), nullable=False),
    Column("class_name", String(255), nullable=False),
    Column("commit_sha", String(128), nullable=False),
    Column("branch", String(255), nullable=False),
    Column("fingerprint", String(32), nullable=False),
    Column("status", String(16), nullable=False),
    Column("category", String(32), nullable=True),
    Column("source", String(16), nullable=True),
    Column("confidence", Float, nullable=True),
    Column("reason", String(300), nullable=True),
    Column("rule_id", String(64), nullable=True),
    Column("rules_version", String(16), nullable=False),
    Column("model", String(128), nullable=True),
    Column("prompt_version", String(16), nullable=True),
    Column("prompt_hash", String(32), nullable=True),
    Column("llm_job_id", _ID, nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("classified_at", DateTime(timezone=True), nullable=True),
    UniqueConstraint("run_id", "test_id", name="uq_classifications_run_test"),
)

Index("ix_classifications_run_id", classifications.c.run_id)
Index("ix_classifications_fingerprint", classifications.c.fingerprint)
Index("ix_classifications_llm_job_pending", classifications.c.llm_job_id, classifications.c.status)

# Work queue for the LLM worker pool. One job per (fingerprint, model, prompt_version):
# similar failures share one LLM call, and a finished job is the cache.
# status: pending | in_progress | done | failed
llm_jobs = Table(
    "llm_jobs",
    metadata,
    Column("id", _ID, primary_key=True, autoincrement=True),
    Column("fingerprint", String(32), nullable=False),
    Column("model", String(128), nullable=False),
    Column("prompt_version", String(16), nullable=False),
    Column("status", String(16), nullable=False),
    Column("attempts", Integer, nullable=False, default=0),
    Column("next_attempt_at", DateTime(timezone=True), nullable=False),
    Column("locked_until", DateTime(timezone=True), nullable=True),
    Column("queued_at", DateTime(timezone=True), nullable=False),
    Column("last_error", String(300), nullable=True),
    Column("error_type", String(255), nullable=True),
    Column("error_message", Text, nullable=True),
    Column("stack_trace", Text, nullable=True),
    Column("result_category", String(32), nullable=True),
    Column("result_confidence", Float, nullable=True),
    Column("result_reason", String(300), nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("completed_at", DateTime(timezone=True), nullable=True),
    UniqueConstraint("fingerprint", "model", "prompt_version", name="uq_llm_jobs_fp_model_prompt"),
)

Index("ix_llm_jobs_claim", llm_jobs.c.status, llm_jobs.c.next_attempt_at)

outbox = Table(
    "outbox",
    metadata,
    Column("id", _ID, primary_key=True, autoincrement=True),
    Column("event_type", String(64), nullable=False),
    Column("payload", JSON().with_variant(JSONB, "postgresql"), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("published_at", DateTime(timezone=True), nullable=True),
)

Index(
    "ix_classifier_outbox_pending",
    outbox.c.id,
    postgresql_where=outbox.c.published_at.is_(None),
)
