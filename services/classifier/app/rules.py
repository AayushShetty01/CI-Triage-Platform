"""Deterministic rule-based classification, applied before the LLM.

Rules are ordered: the first match wins. Bump RULES_VERSION whenever a rule is
added, removed, reordered, or changed, so stored classifications record which
rule set produced them.
"""

import re
from dataclasses import dataclass

RULES_VERSION = "1"

CATEGORIES = ("assertion", "timeout", "infrastructure", "flaky", "unknown")


@dataclass(frozen=True)
class Rule:
    rule_id: str
    category: str
    error_type: re.Pattern[str] | None = None
    text: re.Pattern[str] | None = None

    def matches(self, error_type: str, text: str) -> bool:
        if self.error_type is not None and self.error_type.search(error_type):
            return True
        if self.text is not None and self.text.search(text):
            return True
        return False


@dataclass(frozen=True)
class RuleMatch:
    rule_id: str
    category: str


def _rx(pattern: str) -> re.Pattern[str]:
    return re.compile(pattern, re.IGNORECASE)


RULES: tuple[Rule, ...] = (
    # Known nondeterminism signatures first: these are flaky even when they surface
    # as an assertion or timeout.
    Rule(
        "flaky.known_signature",
        "flaky",
        error_type=_rx(r"StaleElementReference|ConcurrentModification"),
        text=_rx(r"\bflaky\b|\bintermittent(ly)?\b|race condition|address already in use"),
    ),
    Rule(
        "timeout.error_type",
        "timeout",
        error_type=_rx(r"timeout|timedout|deadlineexceeded"),
    ),
    Rule(
        "timeout.message",
        "timeout",
        text=_rx(r"timed out|time-?out|deadline exceeded|took longer than"),
    ),
    Rule(
        "infrastructure.error_type",
        "infrastructure",
        error_type=_rx(
            r"ConnectionError|ConnectionRefused|ConnectionReset|ConnectException|"
            r"UnknownHost|gaierror|OSError|IOError|OutOfMemory|MemoryError|"
            r"ServiceUnavailable"
        ),
    ),
    Rule(
        "infrastructure.message",
        "infrastructure",
        text=_rx(
            r"connection (refused|reset)|econnrefused|econnreset|no route to host|"
            r"name or service not known|temporary failure in name resolution|"
            r"503 service unavailable|502 bad gateway|no space left on device|"
            r"out of memory|oomkilled|cannot connect to the docker daemon|"
            r"too many open files|broken pipe"
        ),
    ),
    Rule(
        "assertion.error_type",
        "assertion",
        error_type=_rx(r"assert|ComparisonFailure|ExpectationFailed|AssertionFailedError"),
    ),
    Rule(
        "assertion.message",
        "assertion",
        text=_rx(r"\bexpected\b.{0,200}\b(got|but was|but got|actual|to (equal|be))\b"),
    ),
)


def classify_by_rules(
    error_type: str | None,
    error_message: str | None,
    stack_trace: str | None,
) -> RuleMatch | None:
    """Return the first matching rule, or None if the LLM should decide."""
    etype = error_type or ""
    # Only the message and the last few stack lines are matched: deep frames
    # mention things like "socket" or "timeout" parameters that are not causes.
    stack_tail = "\n".join((stack_trace or "").strip().splitlines()[-3:])
    text = f"{error_message or ''}\n{stack_tail}"
    for rule in RULES:
        if rule.matches(etype, text):
            return RuleMatch(rule_id=rule.rule_id, category=rule.category)
    return None


def has_no_signal(error_type: str | None, error_message: str | None, stack_trace: str | None) -> bool:
    """True when there is nothing for the LLM to read, so calling it would be wasted cost."""
    return not any(
        value and value.strip() for value in (error_type, error_message, stack_trace)
    )
