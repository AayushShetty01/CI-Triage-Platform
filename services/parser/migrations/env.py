import os

from alembic import context
from sqlalchemy import engine_from_config, pool

from app.database import metadata
from app.logging_config import configure_json_logging

configure_json_logging()

config = context.config
database_url = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://parser:parser_dev@localhost:5432/triage",
)
config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))

target_metadata = metadata


def run_migrations_offline() -> None:
    context.configure(
        url=database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
        version_table_schema="parser",
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_schemas=True,
            version_table_schema="parser",
        )
        with context.begin_transaction():
            context.run_migrations()
    connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
