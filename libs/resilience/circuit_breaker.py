import time
import functools
from typing import Callable, Any

from enum import StrEnum

class CircuitBreakerTripped(Exception):
    """Raised when the circuit breaker is open."""
    pass



class CircuitBreakerState(StrEnum):
    CLOSED = "CLOSED"     # Healthy
    OPEN = "OPEN"         # Error: Stop execution
    HALF_OPEN = "HALF_OPEN" # Testing: Allow one trial
    

class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 3,
        recovery_timeout: int = 60,
        expected_exceptions: tuple[type[Exception], ...] = (Exception,),
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exceptions = expected_exceptions
        
        # State tracking
        self.state = CircuitBreakerState.CLOSED
        self.failures = 0
        self.last_failure_time: float | None = None

    def __call__(self, func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            self._before_call()
            
            try:
                result = func(*args, **kwargs)
                self._on_success()
                return result
            except Exception as e:
                if isinstance(e, self.expected_exceptions):
                    self._on_failure(exception=e)
                raise
        return wrapper

    def _before_call(self) -> None:
        """Logic to determine if the call should proceed."""
        if self.state == CircuitBreakerState.OPEN:
            elapsed = time.time() - (self.last_failure_time or 0)
            
            if elapsed >= self.recovery_timeout:
                self.state = CircuitBreakerState.HALF_OPEN
            else:
                remaining = int(self.recovery_timeout - elapsed)
                raise CircuitBreakerTripped(f"Breaker OPEN. Retry in {remaining}s")

    def _on_success(self) -> None:
        """Reset everything on a successful call."""
        self.state = CircuitBreakerState.CLOSED
        self.failures = 0

    def _on_failure(self, exception: Exception) -> None:
        """Handle increments and state transitions on error."""
        self.failures += 1
        
        # In HALF_OPEN, a single failure trips it immediately
        if self.state == CircuitBreakerState.HALF_OPEN or self.failures >= self.failure_threshold:
            self.state = CircuitBreakerState.OPEN
            self.last_failure_time = time.time()
            # print(f"Circuit Breaker TRIPPED due to: {exception}")
            
    @property
    def current_state(self) -> CircuitBreakerState:
        return self.state