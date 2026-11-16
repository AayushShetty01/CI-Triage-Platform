import os

_TEST_ENV = {
    "DATABASE_URL": "postgresql+psycopg://triage_admin:triage_admin_dev@localhost:5432/triage_test",
    "RABBITMQ_URL": "amqp://guest:guest@localhost:5672/",
    "ENABLE_CONSUMER": "false",
    "ENABLE_RELAY": "false",
    "ENABLE_LLM_WORKER": "false",
    "OUTBOX_POLL_INTERVAL_SECONDS": "1",
    "RELAY_POLL_INTERVAL_SECONDS": "1",
    "RELAY_BACKOFF_CAP_SECONDS": "30",
    "OUTBOX_BATCH_SIZE": "50",
    "LLM_ENABLED": "false",
    "OLLAMA_BASE_URL": "http://ollama:11434",
    "OLLAMA_MODEL": "test-model",
    "LLM_WORKERS": "1",
    "LLM_TIMEOUT_SECONDS": "1",
    "LLM_MAX_ATTEMPTS": "1",
    "LLM_RETRY_BASE_SECONDS": "0",
    "LLM_RETRY_CAP_SECONDS": "1",
    "LLM_DEGRADE_AFTER_SECONDS": "1",
    "LLM_MAX_INPUT_CHARS": "4000",
    "LLM_MAX_OUTPUT_TOKENS": "128",
    "LLM_POLL_INTERVAL_SECONDS": "0.1",
    "BREAKER_FAILURE_THRESHOLD": "2",
    "BREAKER_RESET_SECONDS": "1",
}

for _name, _value in _TEST_ENV.items():
    os.environ.setdefault(_name, _value)
