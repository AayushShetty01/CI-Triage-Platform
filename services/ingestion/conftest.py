import os

_TEST_ENV = {
    "DATABASE_URL": "postgresql+psycopg://triage_admin:triage_admin_dev@localhost:5432/triage_test",
    "INGESTION_TEST_DATABASE_URL": "",
    "INGESTION_TEST_RABBITMQ_URL": "",
    "RABBITMQ_URL": "amqp://guest:guest@localhost:5672/",
    "MAX_REPORT_SIZE_BYTES": "10485760",
    "OUTBOX_POLL_INTERVAL_SECONDS": "1",
    "RELAY_POLL_INTERVAL_SECONDS": "1",
    "RELAY_BACKOFF_CAP_SECONDS": "30",
    "OUTBOX_BATCH_SIZE": "50",
    "INGESTION_RETRY_AFTER_SECONDS": "5",
    "INGESTION_API_KEY": "test-ingestion-api-key",
    "INTERNAL_API_KEY": "test-internal-api-key",
}

for _name, _value in _TEST_ENV.items():
    os.environ.setdefault(_name, _value)
