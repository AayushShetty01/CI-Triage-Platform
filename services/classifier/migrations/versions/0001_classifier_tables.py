"""Create classifier tables."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001_classifier_tables"
down_revision = None
branch_labels = None
depends_on = None

S = "classifier"


def upgrade() -> None:
    op.create_table(
        "processed_events",
        sa.Column("event_id", sa.String(64), primary_key=True),
        sa.Column("run_id", sa.String(64), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False),
        schema=S,
    )
    op.create_table(
        "failure_groups",
        sa.Column("fingerprint", sa.String(32), primary_key=True),
        sa.Column("error_type", sa.String(255), nullable=True),
        sa.Column("normalized_message", sa.Text(), nullable=False),
        sa.Column("top_frame", sa.String(512), nullable=True),
        sa.Column("occurrence_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        schema=S,
    )
    op.create_table(
        "classifications",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.String(64), nullable=False),
        sa.Column("test_id", sa.String(255), nullable=False),
        sa.Column("test_name", sa.String(255), nullable=False),
        sa.Column("class_name", sa.String(255), nullable=False),
        sa.Column("commit_sha", sa.String(128), nullable=False),
        sa.Column("branch", sa.String(255), nullable=False),
        sa.Column("fingerprint", sa.String(32), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("category", sa.String(32), nullable=True),
        sa.Column("source", sa.String(16), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("reason", sa.String(300), nullable=True),
        sa.Column("rule_id", sa.String(64), nullable=True),
        sa.Column("rules_version", sa.String(16), nullable=False),
        sa.Column("model", sa.String(128), nullable=True),
        sa.Column("prompt_version", sa.String(16), nullable=True),
        sa.Column("prompt_hash", sa.String(32), nullable=True),
        sa.Column("llm_job_id", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("classified_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("run_id", "test_id", name="uq_classifications_run_test"),
        schema=S,
    )
    op.create_index("ix_classifications_run_id", "classifications", ["run_id"], schema=S)
    op.create_index("ix_classifications_fingerprint", "classifications", ["fingerprint"], schema=S)
    op.create_index(
        "ix_classifications_llm_job_pending", "classifications", ["llm_job_id", "status"], schema=S
    )
    op.create_table(
        "llm_jobs",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("fingerprint", sa.String(32), nullable=False),
        sa.Column("model", sa.String(128), nullable=False),
        sa.Column("prompt_version", sa.String(16), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("queued_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_error", sa.String(300), nullable=True),
        sa.Column("error_type", sa.String(255), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("stack_trace", sa.Text(), nullable=True),
        sa.Column("result_category", sa.String(32), nullable=True),
        sa.Column("result_confidence", sa.Float(), nullable=True),
        sa.Column("result_reason", sa.String(300), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("fingerprint", "model", "prompt_version", name="uq_llm_jobs_fp_model_prompt"),
        schema=S,
    )
    op.create_index("ix_llm_jobs_claim", "llm_jobs", ["status", "next_attempt_at"], schema=S)
    op.create_table(
        "outbox",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("payload", sa.JSON().with_variant(postgresql.JSONB(), "postgresql"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        schema=S,
    )
    op.create_index(
        "ix_classifier_outbox_pending",
        "outbox",
        ["id"],
        schema=S,
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_classifier_outbox_pending", table_name="outbox", schema=S)
    op.drop_table("outbox", schema=S)
    op.drop_index("ix_llm_jobs_claim", table_name="llm_jobs", schema=S)
    op.drop_table("llm_jobs", schema=S)
    op.drop_index("ix_classifications_llm_job_pending", table_name="classifications", schema=S)
    op.drop_index("ix_classifications_fingerprint", table_name="classifications", schema=S)
    op.drop_index("ix_classifications_run_id", table_name="classifications", schema=S)
    op.drop_table("classifications", schema=S)
    op.drop_table("failure_groups", schema=S)
    op.drop_table("processed_events", schema=S)
