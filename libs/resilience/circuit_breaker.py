"""Circuit breaker pattern to prevent cascading failures."""

import functools
import time
from collections.abc import Callable
from enum import StrEnum


class CircuitOpen(Exception):
    """Raised when the circuit breaker is open."""

    pass


class BreakerState(StrEnum):
    """Circuit breaker states."""

    CLOSED = "CLOSED"  # Normal operation
    OPEN = "OPEN"  # Failing, requests blocked
    HALF_OPEN = "HALF_OPEN"  # Testing recovery


class CircuitBreaker:
    """Prevents repeated calls to failing services.

    Args:
        failure_threshold: The number of failures before the circuit opens.
        timeout_secs: The time in seconds to wait before closing the circuit.
        tracked_exceptions: The exceptions to track.
    """

    def __init__(
        self,
        failure_threshold: int = 3,
        timeout_secs: float = 60,
        tracked_exceptions: tuple[type[Exception], ...] = (Exception,),
    ):
        self.failure_limit = failure_threshold
        self.timeout = timeout_secs
        self.tracked_exceptions = tracked_exceptions

        self.state = BreakerState.CLOSED
        self.failures = 0
        self.last_failure: float | None = None

    def __call__(self, func: Callable) -> Callable:
        """Decorator for circuit-protected functions."""

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            self.check_before_call()

            try:
                result = func(*args, **kwargs)
                self.succeed()
                return result
            except self.tracked_exceptions:
                self.fail()
                raise
            except Exception:
                raise

        return wrapper

    def check_before_call(self) -> None:
        """Raise exception if circuit is open."""
        if self.state != BreakerState.OPEN:
            return

        elapsed = time.time() - (self.last_failure or 0)
        if elapsed >= self.timeout:
            self.state = BreakerState.HALF_OPEN
        else:
            remaining = int(self.timeout - elapsed)
            raise CircuitOpen(f"Circuit open, retry in {remaining}s")

    def succeed(self) -> None:
        """Reset circuit on success."""
        self.state = BreakerState.CLOSED
        self.failures = 0

    def fail(self) -> None:
        """Record failure and possibly open circuit."""
        self.failures += 1

        if self.state == BreakerState.HALF_OPEN or self.failures >= self.failure_limit:
            self.state = BreakerState.OPEN
            self.last_failure = time.time()
