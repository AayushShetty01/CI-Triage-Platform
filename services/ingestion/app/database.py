from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Integer,
    Index,
    JSON,
    LargeBinary,
    MetaData,
    String,
    Table,
)
from sqlalchemy.dialects.postgresql import JSONB

metadata = MetaData(schema="ingestion")

runs = Table(
    "runs",
    metadata,
    Column("run_id", String(64), primary_key=True),
    Column("commit_sha", String(128), nullable=False),
    Column("branch", String(255), nullable=False),
    Column("received_at", DateTime(timezone=True), nullable=False),
    Column("raw_xml", LargeBinary, nullable=False),
    Column("status", String(32), nullable=False),
)

outbox = Table(
    "outbox",
    metadata,
    Column("id", BigInteger().with_variant(Integer, "sqlite"), primary_key=True),
    Column("event_type", String(64), nullable=False),
    Column("payload", JSON().with_variant(JSONB, "postgresql"), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("published_at", DateTime(timezone=True)),
)

Index(
    "ix_outbox_pending",
    outbox.c.id,
    postgresql_where=outbox.c.published_at.is_(None),
)
