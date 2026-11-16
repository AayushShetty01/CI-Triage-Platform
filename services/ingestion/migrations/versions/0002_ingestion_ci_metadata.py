"""Store optional CI run metadata for Query's projection."""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0002_ingestion_ci_metadata"
down_revision = "0001_ingestion_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "runs",
        sa.Column(
            "ci_metadata",
            sa.JSON().with_variant(postgresql.JSONB(), "postgresql"),
            nullable=True,
        ),
        schema="ingestion",
    )


def downgrade() -> None:
    op.drop_column("runs", "ci_metadata", schema="ingestion")
