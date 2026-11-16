import os

_TEST_ENV = {
    "DATABASE_URL": "postgresql+psycopg://triage_admin:triage_admin_dev@localhost:5432/triage_test",
    "RABBITMQ_URL": "amqp://guest:guest@localhost:5672/",
    "PARSER_BASE_URL": "http://parser:8002",
}

for _name, _value in _TEST_ENV.items():
    os.environ.setdefault(_name, _value)
