"""Create parser tables: parsed_runs, test_results, and outbox."""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001_parser_tables"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "parsed_runs",
        sa.Column("run_id", sa.String(length=64), primary_key=True),
        sa.Column("commit_sha", sa.String(length=128), nullable=False),
        sa.Column("branch", sa.String(length=255), nullable=False),
        sa.Column("total_tests", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("passed_tests", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("failed_tests", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("skipped_tests", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_tests", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="completed"),
        sa.Column("parsed_at", sa.DateTime(timezone=True), nullable=False),
        schema="parser",
    )

    op.create_table(
        "test_results",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("test_id", sa.String(length=255), nullable=False),
        sa.Column("test_name", sa.String(length=255), nullable=False),
        sa.Column("class_name", sa.String(length=255), nullable=False),
        sa.Column("file_path", sa.String(length=512), nullable=True),
        sa.Column("duration_seconds", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("error_type", sa.String(length=255), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("stack_trace", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("run_id", "test_id", name="uq_test_results_run_test"),
        schema="parser",
    )
    op.create_index(
        "ix_test_results_run_id",
        "test_results",
        ["run_id"],
        schema="parser",
    )
    op.create_index(
        "ix_test_results_test_history",
        "test_results",
        ["test_id", "created_at"],
        schema="parser",
    )

    op.create_table(
        "outbox",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column(
            "payload",
            sa.JSON().with_variant(postgresql.JSONB(), "postgresql"),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        schema="parser",
    )
    op.create_index(
        "ix_parser_outbox_pending",
        "outbox",
        ["id"],
        schema="parser",
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_parser_outbox_pending", table_name="outbox", schema="parser")
    op.drop_table("outbox", schema="parser")
    op.drop_index("ix_test_results_test_history", table_name="test_results", schema="parser")
    op.drop_index("ix_test_results_run_id", table_name="test_results", schema="parser")
    op.drop_table("test_results", schema="parser")
    op.drop_table("parsed_runs", schema="parser")
