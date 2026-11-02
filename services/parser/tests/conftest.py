import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine

from app.database import metadata
from app.main import create_app

SAMPLE_PASS_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="sample_suite" tests="2" failures="0" errors="0" skipped="0">
    <testcase classname="tests.test_auth" name="test_login_success" time="0.12"/>
    <testcase classname="tests.test_auth" name="test_logout_success" time="0.08"/>
</testsuite>
"""

SAMPLE_FAILURES_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<testsuites name="all_tests" tests="4" failures="1" errors="1" skipped="1">
    <testsuite name="payments" tests="2" failures="1">
        <testcase classname="payments.CheckoutTests" name="rejects_expired_card" time="0.45" file="tests/test_checkout.py">
            <failure message="expected HTTP 402, got 200" type="AssertionError">
Traceback (most recent call last):
  File "tests/test_checkout.py", line 42, in test_rejects_expired_card
    assert response.status_code == 402
AssertionError: expected HTTP 402, got 200
            </failure>
        </testcase>
        <testcase classname="payments.CheckoutTests" name="accepts_valid_card" time="0.32"/>
    </testsuite>
    <testsuite name="database" tests="2" errors="1" skipped="1">
        <testcase classname="db.ConnectionTests" name="connects_to_cluster" time="30.01">
            <error message="DB connection timed out after 30s" type="TimeoutError">
Traceback (most recent call last):
  File "db/connect.py", line 12, in connects_to_cluster
    raise TimeoutError("DB connection timed out after 30s")
TimeoutError: DB connection timed out after 30s
            </error>
        </testcase>
        <testcase classname="db.ConnectionTests" name="replicates_wal" time="0.0">
            <skipped message="feature flag replication disabled"/>
        </testcase>
    </testsuite>
</testsuites>
"""

SAMPLE_EMPTY_SUITE_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="empty" tests="0">
</testsuite>
"""

MALFORMED_XML = b"""<testsuite name="broken"
<testcase name="invalid"
"""


@pytest.fixture
def parser_test_db(tmp_path):
    db_file = tmp_path / "parser-test.db"
    database_url = f"sqlite:///{db_file}"
    engine = create_engine(
        database_url,
        connect_args={"check_same_thread": False},
        execution_options={"schema_translate_map": {"parser": None}},
    )
    metadata.create_all(engine)
    yield engine, database_url
    engine.dispose()


@pytest.fixture
def parser_client(parser_test_db):
    _, database_url = parser_test_db
    app = create_app(
        database_url=database_url,
        enable_consumer=False,
        enable_relay=False,
    )
    with TestClient(app) as client:
        yield client, app.state.engine
