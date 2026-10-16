from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_health_returns_ok() -> None:
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_metrics_endpoint_exposes_health_check_counter() -> None:
    client.get("/health")
    response = client.get("/metrics")

    assert response.status_code == 200
    assert "ingestion_health_checks_total" in response.text
