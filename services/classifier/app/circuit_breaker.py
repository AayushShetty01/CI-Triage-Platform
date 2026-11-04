"""A small circuit breaker for the LLM dependency.

closed     -> calls pass through; consecutive failures are counted.
open       -> calls are refused until reset_timeout_seconds has elapsed.
half_open  -> exactly one trial call is allowed; success closes, failure re-opens.
"""

import time
from collections.abc import Callable

CLOSED = "closed"
OPEN = "open"
HALF_OPEN = "half_open"

STATE_VALUES = {CLOSED: 0, HALF_OPEN: 1, OPEN: 2}


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        reset_timeout_seconds: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
        on_state_change: Callable[[str], None] | None = None,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be at least 1")
        self.failure_threshold = failure_threshold
        self.reset_timeout_seconds = reset_timeout_seconds
        self._clock = clock
        self._on_state_change = on_state_change
        self._state = CLOSED
        self._consecutive_failures = 0
        self._opened_at = 0.0
        self._trial_in_flight = False

    @property
    def state(self) -> str:
        # Report half_open as soon as the cool-down has elapsed, even before a call.
        if self._state == OPEN and self._clock() - self._opened_at >= self.reset_timeout_seconds:
            return HALF_OPEN
        return self._state

    def seconds_until_retry(self) -> float:
        if self._state != OPEN:
            return 0.0
        return max(0.0, self.reset_timeout_seconds - (self._clock() - self._opened_at))

    def allow_request(self) -> bool:
        if self._state == CLOSED:
            return True
        if self._state == OPEN:
            if self._clock() - self._opened_at < self.reset_timeout_seconds:
                return False
            self._set_state(HALF_OPEN)
        # half_open: allow a single trial call at a time.
        if self._trial_in_flight:
            return False
        self._trial_in_flight = True
        return True

    def record_success(self) -> None:
        self._consecutive_failures = 0
        self._trial_in_flight = False
        if self._state != CLOSED:
            self._set_state(CLOSED)

    def record_failure(self) -> None:
        self._trial_in_flight = False
        if self._state == HALF_OPEN:
            self._open()
            return
        self._consecutive_failures += 1
        if self._consecutive_failures >= self.failure_threshold:
            self._open()

    def _open(self) -> None:
        self._opened_at = self._clock()
        self._consecutive_failures = 0
        self._set_state(OPEN)

    def _set_state(self, state: str) -> None:
        self._state = state
        if self._on_state_change is not None:
            self._on_state_change(state)
