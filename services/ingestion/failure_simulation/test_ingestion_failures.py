from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from xml.etree import ElementTree

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine

from app.database import metadata
from app.main import create_app

ARTIFACTS_DIR = Path(__file__).resolve().parents[3] / "artifacts"
SIMULATION_REPORT = ARTIFACTS_DIR / "failure-simulation-junit.xml"
SIMULATION_LOG = ARTIFACTS_DIR / "failure-simulation-execution.log"
SIMULATION_SUMMARY = ARTIFACTS_DIR / "failure-simulation-summary.txt"


@dataclass(frozen=True)
class MockResponse:
    status_code: int
    body: dict[str, object]
    elapsed_seconds: float = 0.0


@dataclass(frozen=True)
class SimulationCase:
    suite: str
    classname: str
    name: str
    check: Callable[[], None]


@dataclass(frozen=True)
class CaseResult:
    case: SimulationCase
    failure_message: str | None = None
    failure_type: str | None = None


class MockPaymentApplication:
    def __init__(self) -> None:
        self.responses = {
            "valid-card": MockResponse(200, {"status": "authorized"}),
            "expired-card": MockResponse(200, {"status": "authorized"}),
            "duplicate-checkout": MockResponse(201, {"order_id": "order-2"}),
            "declined-card": MockResponse(402, {"status": "declined"}),
            "order-details": MockResponse(200, {"order_id": "order-1"}),
            "payment-state": MockResponse(200, {"status": "pending"}),
            "duplicate-payment": MockResponse(409, {"status": "duplicate"}),
            "receipt": MockResponse(200, {"transaction_id": "txn-100"}),
            "tax": MockResponse(200, {"tax_amount": 0.0}),
            "timely-receipt": MockResponse(
                200,
                {"transaction_id": "txn-100"},
                elapsed_seconds=0.25,
            ),
            "timed-out-receipt": None,
            "health": MockResponse(200, {"status": "ok"}),
        }

    def request(self, operation: str) -> MockResponse | None:
        return self.responses[operation]


class MockPaymentRepository:
    def __init__(self, failed_writes: frozenset[str]) -> None:
        self.failed_writes = failed_writes
        self.records: dict[str, dict[str, str]] = {}

    def persist(self, transaction_id: str) -> None:
        if transaction_id not in self.failed_writes:
            self.records[transaction_id] = {"transaction_id": transaction_id}


def _assert_status(response: MockResponse | None, expected: int) -> MockResponse:
    if response is None:
        raise AssertionError(
            f"Expected HTTP {expected}, but the request timed out and returned no response"
        )
    if response.status_code != expected:
        raise AssertionError(
            f"Expected HTTP {expected}, received HTTP {response.status_code}"
        )
    return response


def _simulation_cases() -> list[SimulationCase]:
    application = MockPaymentApplication()
    repository = MockPaymentRepository(failed_writes=frozenset({"txn-missing"}))
    cases: list[SimulationCase] = []

    def add_status_case(
        suite: str,
        classname: str,
        name: str,
        operation: str,
        expected_status: int,
    ) -> None:
        cases.append(
            SimulationCase(
                suite,
                classname,
                name,
                lambda: _assert_status(application.request(operation), expected_status),
            )
        )

    checkout_suite = "checkout-api"
    checkout_class = "simulation.checkout.CheckoutTests"
    add_status_case(
        checkout_suite, checkout_class, "valid_card_is_authorized", "valid-card", 200
    )
    add_status_case(
        checkout_suite,
        checkout_class,
        "expired_card_requires_payment",
        "expired-card",
        402,
    )
    add_status_case(
        checkout_suite,
        checkout_class,
        "duplicate_checkout_returns_conflict",
        "duplicate-checkout",
        409,
    )
    add_status_case(
        checkout_suite,
        checkout_class,
        "declined_card_returns_payment_required",
        "declined-card",
        402,
    )

    def order_id_is_returned() -> None:
        response = _assert_status(application.request("order-details"), 200)
        actual = response.body.get("order_id")
        if actual != "order-1":
            raise AssertionError(f"Expected order_id 'order-1', received {actual!r}")

    def payment_is_authorized() -> None:
        response = _assert_status(application.request("payment-state"), 200)
        actual = response.body.get("status")
        if actual != "authorized":
            raise AssertionError(
                f"Expected response status 'authorized', received {actual!r}"
            )

    cases.extend(
        [
            SimulationCase(
                checkout_suite, checkout_class, "response_includes_order_id", order_id_is_returned
            ),
            SimulationCase(
                checkout_suite,
                checkout_class,
                "payment_response_is_authorized",
                payment_is_authorized,
            ),
        ]
    )

    payment_suite = "payment-api"
    payment_class = "simulation.payment.PaymentTests"
    add_status_case(
        payment_suite,
        payment_class,
        "duplicate_payment_returns_conflict",
        "duplicate-payment",
        409,
    )

    def receipt_includes_transaction_id() -> None:
        response = _assert_status(application.request("receipt"), 200)
        actual = response.body.get("transaction_id")
        if actual != "txn-100":
            raise AssertionError(
                f"Expected transaction_id 'txn-100', received {actual!r}"
            )

    def tax_amount_is_correct() -> None:
        response = _assert_status(application.request("tax"), 200)
        actual = response.body.get("tax_amount")
        if actual != 0.8:
            raise AssertionError(f"Expected tax_amount 0.8, received {actual!r}")

    def response_arrives_within_deadline() -> None:
        response = _assert_status(application.request("timely-receipt"), 200)
        if response.elapsed_seconds > 1.0:
            raise AssertionError(
                f"Expected response within 1.0 seconds, received after "
                f"{response.elapsed_seconds:.2f} seconds"
            )

    add_status_case(
        payment_suite,
        payment_class,
        "timed_out_request_returns_response",
        "timed-out-receipt",
        200,
    )
    add_status_case(payment_suite, payment_class, "health_check_succeeds", "health", 200)
    cases.extend(
        [
            SimulationCase(
                payment_suite,
                payment_class,
                "receipt_includes_transaction_id",
                receipt_includes_transaction_id,
            ),
            SimulationCase(
                payment_suite,
                payment_class,
                "tax_amount_matches_invoice",
                tax_amount_is_correct,
            ),
            SimulationCase(
                payment_suite,
                payment_class,
                "response_arrives_within_deadline",
                response_arrives_within_deadline,
            ),
        ]
    )

    persistence_suite = "payment-persistence"
    persistence_class = "simulation.payment.PersistenceTests"

    def successful_payment_is_persisted() -> None:
        repository.persist("txn-persisted")
        if repository.records.get("txn-persisted") is None:
            raise AssertionError("Expected successful payment txn-persisted to be stored")

    def declined_payment_is_not_persisted() -> None:
        if repository.records.get("txn-declined") is not None:
            raise AssertionError("Expected declined payment txn-declined not to be stored")

    def authorized_payment_is_persisted() -> None:
        repository.persist("txn-missing")
        if repository.records.get("txn-missing") is None:
            raise AssertionError(
                "Expected authorized payment txn-missing to be stored, "
                "but no record was found"
            )

    cases.extend(
        [
            SimulationCase(
                persistence_suite,
                persistence_class,
                "successful_payment_is_persisted",
                successful_payment_is_persisted,
            ),
            SimulationCase(
                persistence_suite,
                persistence_class,
                "declined_payment_is_not_persisted",
                declined_payment_is_not_persisted,
            ),
            SimulationCase(
                persistence_suite,
                persistence_class,
                "authorized_payment_is_persisted",
                authorized_payment_is_persisted,
            ),
        ]
    )
    return cases


def _execute_simulation(cases: list[SimulationCase]) -> list[CaseResult]:
    results = []
    for case in cases:
        try:
            case.check()
        except (AssertionError, TimeoutError) as exc:
            results.append(
                CaseResult(
                    case=case,
                    failure_message=str(exc),
                    failure_type=type(exc).__name__,
                )
            )
        else:
            results.append(CaseResult(case=case))
    return results


def _junit_report(results: list[CaseResult]) -> bytes:
    suites: dict[str, list[CaseResult]] = defaultdict(list)
    for result in results:
        suites[result.case.suite].append(result)

    total = len(results)
    failed = sum(result.failure_message is not None for result in results)
    root = ElementTree.Element(
        "testsuites",
        {
            "name": "controlled-ci-failure-simulation",
            "tests": str(total),
            "failures": str(failed),
            "errors": "0",
            "skipped": "0",
        },
    )
    for suite_name, suite_results in suites.items():
        suite_failures = sum(
            result.failure_message is not None for result in suite_results
        )
        suite = ElementTree.SubElement(
            root,
            "testsuite",
            {
                "name": suite_name,
                "tests": str(len(suite_results)),
                "failures": str(suite_failures),
                "errors": "0",
                "skipped": "0",
            },
        )
        for result in suite_results:
            testcase = ElementTree.SubElement(
                suite,
                "testcase",
                {
                    "classname": result.case.classname,
                    "name": result.case.name,
                },
            )
            if result.failure_message is not None:
                failure = ElementTree.SubElement(
                    testcase,
                    "failure",
                    {
                        "message": result.failure_message,
                        "type": result.failure_type or "AssertionError",
                    },
                )
                failure.text = result.failure_message

    return ElementTree.tostring(root, encoding="utf-8", xml_declaration=True) + b"\n"


def _write_artifacts(
    results: list[CaseResult],
    report: bytes,
    api_verification: str,
) -> None:
    passed = sum(result.failure_message is None for result in results)
    failed = len(results) - passed
    log_lines = [
        "Controlled CI failure simulation execution",
        "The simulated failures below are not failures of the ingestion API.",
    ]
    summary_lines = [
        "Controlled CI failure simulation report",
        "Failed cases are simulated payment-application outcomes, not ingestion API failures.",
        f"Total simulated cases: {len(results)}",
        f"Passed: {passed}",
        f"Failed: {failed}",
        "Skipped: 0",
        f"Ingestion API verification: {api_verification}",
        "",
        "Simulated case results:",
    ]
    for result in results:
        qualified_name = f"{result.case.suite}.{result.case.name}"
        if result.failure_message is None:
            log_lines.append(f"PASS {qualified_name}")
            summary_lines.append(f"PASS {qualified_name}")
        else:
            log_lines.append(f"FAIL {qualified_name}: {result.failure_message}")
            summary_lines.append(
                f"FAIL {qualified_name}: {result.failure_message}"
            )
    log_lines.append(
        f"Simulation results: total={len(results)} passed={passed} failed={failed} skipped=0"
    )
    log_lines.append(f"Ingestion API verification: {api_verification}")

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    SIMULATION_REPORT.write_bytes(report)
    SIMULATION_LOG.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
    SIMULATION_SUMMARY.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")


@pytest.fixture
def ingestion_client(tmp_path: Path):
    database_url = f"sqlite:///{tmp_path / 'failure-simulation.db'}"
    engine = create_engine(
        database_url,
        connect_args={"check_same_thread": False},
        execution_options={"schema_translate_map": {"ingestion": None}},
    )
    metadata.create_all(engine)
    app = create_app(
        database_url=database_url,
        enable_relay=False,
        max_report_size_bytes=1024 * 1024,
    )
    with TestClient(app) as client:
        yield client
    engine.dispose()
    app.state.engine.dispose()


def test_simulated_failures_are_accepted_and_preserved(
    ingestion_client: TestClient,
) -> None:
    results = _execute_simulation(_simulation_cases())
    report = _junit_report(results)
    _write_artifacts(results, report, "pending")

    passed = sum(result.failure_message is None for result in results)
    failed = len(results) - passed
    assert len(results) == 15
    assert passed == 9
    assert failed == 6

    response = ingestion_client.post(
        "/runs",
        content=report,
        headers={
            "Content-Type": "application/xml",
            "X-Commit-Sha": "failure-simulation-commit",
            "X-Branch": "ci/failure-simulation",
        },
    )
    assert response.status_code == 202
    assert response.json()["status"] == "accepted"
    run_id = response.json()["run_id"]

    raw_response = ingestion_client.get(f"/internal/runs/{run_id}/raw")
    assert raw_response.status_code == 200
    assert raw_response.content == report

    stored_root = ElementTree.fromstring(raw_response.content)
    stored_suites = stored_root.findall("testsuite")
    stored_cases = stored_root.findall(".//testcase")
    stored_failures = {
        case.get("name"): failure.get("message")
        for case in stored_cases
        if (failure := case.find("failure")) is not None
    }
    stored_case_locations = {
        (suite.get("name"), case.get("name"))
        for suite in stored_suites
        for case in suite.findall("testcase")
    }
    expected_failures = {
        result.case.name: result.failure_message
        for result in results
        if result.failure_message is not None
    }
    expected_names = {result.case.name for result in results}
    expected_case_locations = {
        (result.case.suite, result.case.name) for result in results
    }
    stored_passed = sum(case.find("failure") is None for case in stored_cases)

    assert int(stored_root.get("tests", "0")) == len(results)
    assert int(stored_root.get("failures", "0")) == failed
    assert stored_passed == passed
    assert len(stored_cases) == len(results)
    assert len(stored_failures) == failed
    assert {case.get("name") for case in stored_cases} == expected_names
    assert stored_failures == expected_failures
    assert stored_case_locations == expected_case_locations
    assert {suite.get("name") for suite in stored_suites} == {
        result.case.suite for result in results
    }

    _write_artifacts(
        results,
        report,
        "accepted (HTTP 202); raw report retrieved byte-for-byte",
    )
    summary = SIMULATION_SUMMARY.read_text(encoding="utf-8")
    log = SIMULATION_LOG.read_text(encoding="utf-8")
    assert "Total simulated cases: 15" in summary
    assert "Passed: 9" in summary
    assert "Failed: 6" in summary
    assert "not ingestion API failures" in summary
    assert "byte-for-byte" in summary
    assert "Simulation results: total=15 passed=9 failed=6 skipped=0" in log
