import os

from alembic import context
from sqlalchemy import engine_from_config, pool

from app.database import metadata

config = context.config
url = os.getenv("DATABASE_URL", "postgresql+psycopg://query:query_dev@localhost:5432/triage")
config.set_main_option("sqlalchemy.url", url.replace("%", "%%"))


def run_online() -> None:
    engine = engine_from_config(config.get_section(config.config_ini_section, {}),
                                prefix="sqlalchemy.", poolclass=pool.NullPool)
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=metadata,
                          include_schemas=True, version_table_schema="query")
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


def run_offline() -> None:
    context.configure(url=url, target_metadata=metadata, literal_binds=True,
                      include_schemas=True, version_table_schema="query")
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    run_offline()
else:
    run_online()
