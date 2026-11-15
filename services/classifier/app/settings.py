import os
from dataclasses import dataclass


def _bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _float(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value else default


def _int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value else default


@dataclass
class Settings:
    database_url: str = "postgresql+psycopg://classifier:classifier_dev@localhost:5432/triage"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"

    enable_consumer: bool = True
    enable_relay: bool = True
    enable_llm_worker: bool = True

    outbox_poll_interval_seconds: float = 1.0
    relay_backoff_cap_seconds: float = 30.0
    outbox_batch_size: int = 50

    llm_enabled: bool = True
    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "qwen2.5:1.5b"
    llm_workers: int = 2
    llm_timeout_seconds: float = 30.0
    llm_max_attempts: int = 3
    llm_retry_base_seconds: float = 2.0
    llm_retry_cap_seconds: float = 60.0
    llm_degrade_after_seconds: float = 120.0
    llm_max_input_chars: int = 4000
    llm_max_output_tokens: int = 128
    llm_poll_interval_seconds: float = 0.5
    breaker_failure_threshold: int = 5
    breaker_reset_seconds: float = 30.0

    @classmethod
    def from_env(cls) -> "Settings":
        d = cls()
        return cls(
            database_url=os.getenv("DATABASE_URL", d.database_url),
            rabbitmq_url=os.getenv("RABBITMQ_URL", d.rabbitmq_url),
            enable_consumer=_bool("ENABLE_CONSUMER", d.enable_consumer),
            enable_relay=_bool("ENABLE_RELAY", d.enable_relay),
            enable_llm_worker=_bool("ENABLE_LLM_WORKER", d.enable_llm_worker),
            outbox_poll_interval_seconds=_float(
                "RELAY_POLL_INTERVAL_SECONDS",
                _float("OUTBOX_POLL_INTERVAL_SECONDS", d.outbox_poll_interval_seconds),
            ),
            relay_backoff_cap_seconds=_float("RELAY_BACKOFF_CAP_SECONDS", d.relay_backoff_cap_seconds),
            outbox_batch_size=_int("OUTBOX_BATCH_SIZE", d.outbox_batch_size),
            llm_enabled=_bool("LLM_ENABLED", d.llm_enabled),
            ollama_base_url=os.getenv("OLLAMA_BASE_URL", d.ollama_base_url),
            ollama_model=os.getenv("OLLAMA_MODEL", d.ollama_model),
            llm_workers=_int("LLM_WORKERS", d.llm_workers),
            llm_timeout_seconds=_float("LLM_TIMEOUT_SECONDS", d.llm_timeout_seconds),
            llm_max_attempts=_int("LLM_MAX_ATTEMPTS", d.llm_max_attempts),
            llm_retry_base_seconds=_float("LLM_RETRY_BASE_SECONDS", d.llm_retry_base_seconds),
            llm_retry_cap_seconds=_float("LLM_RETRY_CAP_SECONDS", d.llm_retry_cap_seconds),
            llm_degrade_after_seconds=_float("LLM_DEGRADE_AFTER_SECONDS", d.llm_degrade_after_seconds),
            llm_max_input_chars=_int("LLM_MAX_INPUT_CHARS", d.llm_max_input_chars),
            llm_max_output_tokens=_int("LLM_MAX_OUTPUT_TOKENS", d.llm_max_output_tokens),
            llm_poll_interval_seconds=_float("LLM_POLL_INTERVAL_SECONDS", d.llm_poll_interval_seconds),
            breaker_failure_threshold=_int("BREAKER_FAILURE_THRESHOLD", d.breaker_failure_threshold),
            breaker_reset_seconds=_float("BREAKER_RESET_SECONDS", d.breaker_reset_seconds),
        )
