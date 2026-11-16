import os

import pytest
from sqlalchemy import create_engine, text

from app.database import metadata


def create_test_engine():
    engine = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)
    with engine.begin() as connection:
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS query"))
        metadata.drop_all(connection)
        metadata.create_all(connection)
    return engine


@pytest.fixture
def query_test_database():
    engine = create_test_engine()
    try:
        yield os.environ["DATABASE_URL"]
    finally:
        engine.dispose()
