from prometheus_client import Counter, Gauge, Histogram

health_checks = Counter("classifier_health_checks_total", "Successful classifier health checks.")
events_consumed = Counter(
    "classifier_events_consumed_total", "FailuresExtracted messages received from RabbitMQ."
)
events_processed = Counter(
    "classifier_events_processed_total",
    "FailuresExtracted messages handled, by outcome.",
    ("status",),
)
poison_messages = Counter(
    "classifier_poison_messages_total", "Messages rejected to the dead-letter queue."
)
classifications_total = Counter(
    "classifier_classifications_total",
    "Failure classifications written, by source, category and status.",
    ("source", "category", "status"),
)
llm_requests = Counter(
    "classifier_llm_requests_total",
    "LLM calls by outcome (success, timeout, error, invalid_output, breaker_open).",
    ("outcome",),
)
llm_latency = Histogram(
    "classifier_llm_latency_seconds",
    "LLM call latency for calls that returned.",
    buckets=(0.25, 0.5, 1, 2, 4, 8, 15, 30, 60),
)
llm_tokens = Counter(
    "classifier_llm_tokens_total",
    "Tokens reported by Ollama, by kind (prompt, completion).",
    ("kind",),
)
llm_prompt_injection_suspected = Counter(
    "classifier_llm_prompt_injection_suspected_total",
    "LLM inputs that contained instruction-like text (classified anyway, output validated).",
)
llm_jobs_pending = Gauge(
    "classifier_llm_jobs_pending", "LLM jobs waiting or in progress."
)
llm_circuit_state = Gauge(
    "classifier_llm_circuit_state", "LLM circuit breaker state: 0 closed, 1 half-open, 2 open."
)
outbox_pending = Gauge("classifier_outbox_pending", "Unpublished classifier outbox events.")
outbox_oldest_pending_seconds = Gauge(
    "classifier_outbox_oldest_pending_seconds",
    "Age in seconds of the oldest unpublished classifier outbox event.",
)
relay_publish_errors = Counter(
    "classifier_relay_publish_errors_total", "Failed classifier outbox relay iterations."
)
relay_consecutive_failures = Gauge(
    "classifier_relay_consecutive_failures", "Consecutive failed classifier outbox relay iterations."
)
