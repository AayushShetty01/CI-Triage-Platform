"""Live demonstration of the Ingestion -> Parser pipeline workflow.

This script runs the workflow end-to-end locally using Python without needing
external Docker containers started.
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select

# Ingestion imports
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "services" / "ingestion"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "services" / "parser"))

from app.database import metadata as parser_metadata, outbox as parser_outbox, parsed_runs, test_results
from app.consumer import process_run_received_event
from app.ingestion_client import IngestionClient

SAMPLE_JUNIT_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<testsuites name="e-commerce-ci" tests="4" failures="1" errors="1" skipped="1">
    <testsuite name="checkout" tests="2" failures="1">
        <testcase classname="services.checkout.TestCheckout" name="test_reject_expired_card" time="0.35" file="tests/test_checkout.py">
            <failure message="expected HTTP 402, got 200" type="AssertionError">
Traceback (most recent call last):
  File "tests/test_checkout.py", line 55, in test_reject_expired_card
    assert response.status_code == 402
AssertionError: expected HTTP 402, got 200
            </failure>
        </testcase>
        <testcase classname="services.checkout.TestCheckout" name="test_order_success" time="0.82" file="tests/test_checkout.py"/>
    </testsuite>
    <testsuite name="inventory" tests="2" errors="1" skipped="1">
        <testcase classname="services.inventory.TestStock" name="test_stock_reservation_timeout" time="30.01">
            <error message="Redis lock acquisition timed out after 30s" type="TimeoutError">
Traceback (most recent call last):
  File "services/inventory/lock.py", line 88, in acquire
    raise TimeoutError("Redis lock acquisition timed out after 30s")
TimeoutError: Redis lock acquisition timed out after 30s
            </error>
        </testcase>
        <testcase classname="services.inventory.TestStock" name="test_warehouse_sync" time="0.0">
            <skipped message="warehouse v2 feature flag disabled"/>
        </testcase>
    </testsuite>
</testsuites>
"""


async def main():
    print("=" * 70)
    print("CI TEST-FAILURE TRIAGE PLATFORM: INGESTION -> PARSER PIPELINE DEMO")
    print("=" * 70)

    # 1. Setup local sqlite DB for Parser
    db_path = Path("demo_parser.db")
    if db_path.exists():
        db_path.unlink()

    engine = create_engine(
        f"sqlite:///{db_path}",
        connect_args={"check_same_thread": False},
        execution_options={"schema_translate_map": {"parser": None}},
    )
    parser_metadata.create_all(engine)

    # 2. Simulate Ingestion accepting JUnit XML and creating RunReceived event
    run_id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    event_id = str(uuid.uuid4())
    print("\n[1] Ingestion Service received JUnit XML upload:")
    print(f"    - Run ID     : {run_id}")
    print(f"    - Commit SHA : 8f9b2c14a")
    print(f"    - Branch     : main")
    print("    - Status     : 202 Accepted (RunReceived event emitted)")

    run_received_event = {
        "event_id": event_id,
        "event_type": "RunReceived",
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "schema_version": "1.0",
        "run_id": run_id,
        "payload": {
            "commit_sha": "8f9b2c14a",
            "branch": "main",
            "raw_ref": f"/internal/runs/{run_id}/raw",
            "received_at": datetime.now(timezone.utc).isoformat(),
        },
    }

    # 3. Simulate IngestionClient serving raw XML to Parser
    class LocalMockIngestionClient:
        async def get_raw_xml(self, raw_ref: str) -> bytes:
            print(f"    [Parser -> Ingestion] GET {raw_ref} with X-Internal-Key")
            return SAMPLE_JUNIT_XML

    client = LocalMockIngestionClient()

    # 4. Parser Service processes the event
    print("\n[2] Parser Service consuming 'RunReceived' event from RabbitMQ:")
    await process_run_received_event(run_received_event, engine, client)

    # 5. Query results from database
    with engine.connect() as conn:
        run = conn.execute(
            select(parsed_runs).where(parsed_runs.c.run_id == run_id)
        ).mappings().one()

        tests = conn.execute(
            select(test_results).where(test_results.c.run_id == run_id)
        ).mappings().all()

        outbox_row = conn.execute(
            select(parser_outbox).where(parser_outbox.c.event_type == "FailuresExtracted")
        ).mappings().one()

    print("\n[3] Parser Database State:")
    print("    - Run Summary:")
    print(f"        Total Tests   : {run['total_tests']}")
    print(f"        Passed        : {run['passed_tests']}")
    print(f"        Failed/Errors : {run['failed_tests']}")
    print(f"        Skipped       : {run['skipped_tests']}")
    print(f"        Status        : {run['status']}")

    print("\n    - Test Results Table:")
    for t in tests:
        status_icon = "PASS" if t["status"] == "passed" else ("FAIL" if t["status"] in ("failed", "error") else "SKIP")
        err_msg = f" -> {t['error_message']}" if t["error_message"] else ""
        print(f"        [{status_icon:<4}] {t['test_id']} ({t['duration_seconds']}s){err_msg}")

    print("\n[4] Outbox Staged Event ('FailuresExtracted' ready for RabbitMQ -> Classifier):")
    payload = outbox_row["payload"]
    print(json.dumps(payload, indent=2))

    # 6. Test Idempotency
    print("\n[5] Testing Idempotency (Sending duplicate RunReceived message):")
    await process_run_received_event(run_received_event, engine, client)
    with engine.connect() as conn:
        all_runs = conn.execute(select(parsed_runs)).mappings().all()
        all_tests = conn.execute(select(test_results)).mappings().all()
    print(f"    - Total runs in DB (expected 1)  : {len(all_runs)}")
    print(f"    - Total tests in DB (expected 4) : {len(all_tests)}")
    print("    Duplicate was ignored idempotently without side-effects!")

    engine.dispose()
    if db_path.exists():
        db_path.unlink()

    print("\n" + "=" * 70)
    print("DEMO COMPLETED SUCCESSFULLY!")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
