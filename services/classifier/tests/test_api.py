import asyncio

from fastapi.testclient import TestClient

from app.consumer import process_failures_extracted_event
from app.main import create_app
from tests.conftest import RUN_A, failure, failures_extracted


def test_api_endpoints(settings, fake_ollama):
    app = create_app(settings, llm_transport=fake_ollama.transport)
    with TestClient(app) as client:
        engine = app.state.engine
        event = failures_extracted(
            RUN_A,
            [
                failure("p.T::a", "AssertionError", "expected 1 got 2"),
                failure("o.T::b", "KeyError", "'customer_id'"),
            ],
        )
        asyncio.run(process_failures_extracted_event(event, engine, app.state.settings))

        assert client.get("/health").json()["status"] == "healthy"

        body = client.get(f"/runs/{RUN_A}/classifications").json()
        assert body["status_counts"] == {"classified": 1, "pending": 1}
        assert client.get(f"/runs/{'f' * 64}/classifications").status_code == 404

        status = client.get("/llm/status").json()
        assert status["jobs"] == {"pending": 1}
        assert status["model"] == "test-model"

        asyncio.run(app.state.worker_pool.process_one())
        body = client.get(f"/runs/{RUN_A}/classifications").json()
        assert body["status_counts"] == {"classified": 2}

        groups = client.get("/groups").json()
        assert len(groups) == 2
        assert {g["category"] for g in groups} == {"assertion", "flaky"}

        text = client.get("/metrics").text
        assert "classifier_llm_requests_total" in text
        assert "classifier_outbox_pending" in text
