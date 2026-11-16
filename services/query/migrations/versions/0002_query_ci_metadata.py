"""Project CI provenance and classifier confidence."""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0002_query_ci_metadata"
down_revision = "0001_query_projection"
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
        schema="query",
    )
    op.add_column("failures", sa.Column("confidence", sa.Float()), schema="query")


def downgrade() -> None:
    op.drop_column("failures", "confidence", schema="query")
    op.drop_column("runs", "ci_metadata", schema="query")
