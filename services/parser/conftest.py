import os

_TEST_ENV = {
    "DATABASE_URL": "postgresql+psycopg://triage_admin:triage_admin_dev@localhost:5432/triage_test",
    "RABBITMQ_URL": "amqp://guest:guest@localhost:5672/",
    "INGESTION_BASE_URL": "http://testserver:8001",
    "INTERNAL_API_KEY": "test-internal-api-key",
    "OUTBOX_POLL_INTERVAL_SECONDS": "1",
    "RELAY_POLL_INTERVAL_SECONDS": "1",
    "RELAY_BACKOFF_CAP_SECONDS": "30",
    "OUTBOX_BATCH_SIZE": "50",
}

for _name, _value in _TEST_ENV.items():
    os.environ.setdefault(_name, _value)
