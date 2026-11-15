from alembic import op
import sqlalchemy as sa

revision = "0001_query_projection"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table("runs",
        sa.Column("run_id", sa.String(64), primary_key=True),
        sa.Column("commit_sha", sa.String(128)),
        sa.Column("branch", sa.String(255)),
        sa.Column("received_at", sa.DateTime(timezone=True)),
        sa.Column("parsed_at", sa.DateTime(timezone=True)),
        sa.Column("total_tests", sa.Integer()),
        sa.Column("failed_tests", sa.Integer()),
        sa.Column("passed_tests", sa.Integer()),
        sa.Column("skipped_tests", sa.Integer()),
        schema="query")
    op.create_table("tests",
        sa.Column("run_id", sa.String(64), nullable=False),
        sa.Column("test_id", sa.String(255), nullable=False),
        sa.Column("test_name", sa.String(255), nullable=False),
        sa.Column("class_name", sa.String(255), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("duration_seconds", sa.String(32)),
        sa.ForeignKeyConstraint(["run_id"], ["query.runs.run_id"]),
        sa.UniqueConstraint("run_id", "test_id", name="uq_query_test_run"),
        schema="query")
    op.create_index("ix_query_tests_history", "tests", ["test_id", "status"], schema="query")
    op.create_table("failures",
        sa.Column("run_id", sa.String(64), nullable=False),
        sa.Column("test_id", sa.String(255), nullable=False),
        sa.Column("test_name", sa.String(255), nullable=False),
        sa.Column("class_name", sa.String(255), nullable=False),
        sa.Column("error_type", sa.String(255)),
        sa.Column("error_message", sa.Text()),
        sa.Column("fingerprint", sa.String(32)),
        sa.Column("category", sa.String(32)),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("source", sa.String(20)),
        sa.Column("rules_version", sa.String(64)),
        sa.Column("model", sa.String(128)),
        sa.Column("prompt_version", sa.String(64)),
        sa.Column("prompt_hash", sa.String(128)),
        sa.Column("classified_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(["run_id"], ["query.runs.run_id"]),
        sa.UniqueConstraint("run_id", "test_id", name="uq_query_failure_run"),
        schema="query")
    op.create_index("ix_query_failures_category", "failures", ["category"], schema="query")
    op.create_index("ix_query_failures_fingerprint", "failures", ["fingerprint"], schema="query")
    op.create_table("processed_events",
        sa.Column("event_id", sa.String(36), primary_key=True),
        sa.Column("event_type", sa.String(40), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False),
        schema="query")


def downgrade() -> None:
    op.drop_table("processed_events", schema="query")
    op.drop_index("ix_query_failures_fingerprint", table_name="failures", schema="query")
    op.drop_index("ix_query_failures_category", table_name="failures", schema="query")
    op.drop_table("failures", schema="query")
    op.drop_index("ix_query_tests_history", table_name="tests", schema="query")
    op.drop_table("tests", schema="query")
    op.drop_table("runs", schema="query")
