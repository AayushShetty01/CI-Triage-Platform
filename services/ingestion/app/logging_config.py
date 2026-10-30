import json
import logging
import os
import re
import sys
import traceback as traceback_module
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any

request_id_context: ContextVar[str | None] = ContextVar("request_id", default=None)
run_id_context: ContextVar[str | None] = ContextVar("run_id", default=None)
_configured = False
URL_CREDENTIALS = re.compile(r"(?i)(://[^:/@\s]+:)[^@\s]+(@)")
KEY_HEADER_VALUE = re.compile(
    r"""(?i)(["']?(?:authorization|x-api-key|x-internal-key)["']?\s*[:=]\s*["']?)"""
    r"""(?:bearer\s+)?[^"'\s,;}]+"""
)


def _safe_text(value: str) -> str:
    value = URL_CREDENTIALS.sub(r"\1[REDACTED]\2", value)
    value = KEY_HEADER_VALUE.sub(r"\1[REDACTED]", value)
    for variable in (
        "INGESTION_API_KEY",
        "INTERNAL_API_KEY",
        "RABBITMQ_PASSWORD",
        "INGESTION_DB_PASSWORD",
        "POSTGRES_ADMIN_PASSWORD",
    ):
        secret = os.getenv(variable)
        if secret and len(secret) >= 8:
            value = value.replace(secret, "[REDACTED]")
    return value


class IngestionJsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname.lower(),
            "service": "ingestion",
            "message": _safe_text(record.getMessage()),
            "event": _safe_text(getattr(record, "event", record.getMessage())),
        }
        fields = (
            "run_id",
            "event_id",
            "request_id",
            "endpoint",
            "status_code",
            "duration_ms",
            "consecutive_failures",
            "backoff_seconds",
            "test_id",
            "node_id",
            "method",
            "reason",
            "started_at",
            "traceback",
            "exception_type",
        )
        for field in fields:
            value = getattr(record, field, None)
            if value is not None:
                entry[field] = _safe_text(value) if isinstance(value, str) else value
        if "request_id" not in entry and (request_id := request_id_context.get()):
            entry["request_id"] = request_id
        if "run_id" not in entry and (run_id := run_id_context.get()):
            entry["run_id"] = run_id
        if record.exc_info and record.exc_info[0] is not None:
            entry["exception_type"] = record.exc_info[0].__name__
            if record.exc_info[2] is not None:
                entry["traceback"] = _safe_text(
                    "".join(traceback_module.format_tb(record.exc_info[2]))
                )
        return json.dumps(entry, separators=(",", ":"), default=str)


def configure_json_logging() -> None:
    global _configured
    if _configured:
        return
    root = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(IngestionJsonFormatter())
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.INFO)

    for name in ("uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.addHandler(handler)
        logger.propagate = False
    logging.getLogger("uvicorn.access").disabled = True
    _configured = True
