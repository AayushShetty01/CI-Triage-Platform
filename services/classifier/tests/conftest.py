import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest
from sqlalchemy import create_engine

from app.database import metadata
from app.settings import Settings

RUN_A = "a" * 64
RUN_B = "b" * 64

REPO_DIR = Path(__file__).resolve().parents[3]
CONTRACTS_DIR = REPO_DIR / "contracts" / "events"


def failure(test_id: str, error_type: str | None, message: str | None, stack: str | None = None) -> dict:
    cls, _, name = test_id.partition("::")
    return {
        "test_id": test_id,
        "test_name": name or test_id,
        "class_name": cls,
        "file_path": None,
        "duration_seconds": 0.1,
        "error_type": error_type,
        "error_message": message,
        "stack_trace": stack,
    }


def failures_extracted(run_id: str, failures: list[dict], event_id: str | None = None) -> dict:
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "event_type": "FailuresExtracted",
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "schema_version": "1.0",
        "run_id": run_id,
        "payload": {
            "commit_sha": "0123456789abcdef",
            "branch": "main",
            "total_tests": len(failures) + 1,
            "failed_tests": len(failures),
            "passed_tests": 1,
            "skipped_tests": 0,
            "failures": failures,
        },
    }


@pytest.fixture
def engine(tmp_path):
    eng = create_engine(
        f"sqlite:///{tmp_path / 'classifier.db'}",
        connect_args={"check_same_thread": False},
        execution_options={"schema_translate_map": {"classifier": None}},
    )
    metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(
        database_url=f"sqlite:///{tmp_path / 'classifier.db'}",
        enable_consumer=False,
        enable_relay=False,
        enable_llm_worker=False,
        ollama_model="test-model",
        llm_max_attempts=3,
        llm_retry_base_seconds=0.0,
        llm_degrade_after_seconds=120,
        breaker_failure_threshold=2,
        breaker_reset_seconds=30,
    )


class FakeOllama:
    """httpx transport standing in for Ollama's /api/chat."""

    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.reply: dict | str = {"category": "flaky", "confidence": 0.8, "reason": "Ordering dependent"}
        self.fail_with: Exception | None = None
        self.status_code = 200

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(json.loads(request.content))
        if self.fail_with is not None:
            raise self.fail_with
        content = self.reply if isinstance(self.reply, str) else json.dumps(self.reply)
        return httpx.Response(
            self.status_code,
            json={"message": {"role": "assistant", "content": content}, "prompt_eval_count": 120, "eval_count": 20},
        )

    @property
    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handler)


@pytest.fixture
def fake_ollama() -> FakeOllama:
    return FakeOllama()
