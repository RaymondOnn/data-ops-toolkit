import time
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
        threshold: int, 
        recovery_timeout: int = 300,
        exceptions: list[Exception] | None = None
    ) -> None:
        self.threshold = threshold
        self._state = CircuitBreakerState.CLOSED
        self.recovery_timeout = recovery_timeout
        self.exceptions = exceptions or []
        
    @property
    def state(self) -> CircuitBreakerState:
        """
        The current state of the circuit breaker.

        :return: The current state of the circuit breaker as a CircuitBreakerState enum.
        :rtype: CircuitBreakerState
        """
        return self._state
    
    def can_attempt(self) -> bool:
        return self.state != "OPEN"
    
    def tripped(self) -> bool:
        return self.state == "OPEN"
        
    def success(self) -> None:
        self._state = CircuitBreakerState.CLOSED
        
    def trip(self) -> None:
        self._state = CircuitBreakerState.OPEN
        
    
    def reset(self) -> None:
        """
        Reset the circuit breaker back to its original state ("CLOSED").
        """
        self._state = CircuitBreakerState.CLOSED
        
    def __repr__(self) -> str:
        return f"CircuitBreaker(threshold={self.threshold}, state={self.state}"
        

    def evaluate_state(self, current_status: str, last_failure_time: float) -> CircuitBreakerState:
        """Determines logic-based state based on Registry data."""
        if current_status != "OPEN":
            return CircuitBreakerState.CLOSED
        
        # Check if cooling-off period has passed
        if (time.time() - last_failure_time) > self.recovery_timeout:
            return CircuitBreakerState.HALF_OPEN
            
        return CircuitBreakerState.OPEN

    def should_trip(self, fails: int) -> bool:
        return fails >= self.threshold
    
    def get_current_state(
        self, 
        status: str, 
        last_failure_time: float, 
        retry_attempts: int = 0
    ) -> CircuitBreakerState:
        """
        Logic engine to determine state.
        Uses exponential backoff: 300s -> 600s -> 1200s... capped at max_timeout.
        """
        if status != CircuitBreakerState.OPEN:
            return CircuitBreakerState.CLOSED

        # Calculate backoff: base_timeout * 2^(retries)
        # We use max(0, retry_attempts - 1) so the first recovery attempt starts at base_timeout
        # Exponential Backoff: 300s, 600s, 1200s, 2400s...
        current_timeout = self.recovery_timeout * (2 ** max(0, retry_attempts - 1))
        
        elapsed = time.time() - last_failure_time
        
        if elapsed > current_timeout:
            return CircuitBreakerState.HALF_OPEN
        
        return CircuitBreakerState.OPEN
    
    

def circuit_breaker(
    failure_threshold: int = 3,
    recovery_timeout: int = 60,
    failure_exceptions: tuple[type[Exception], ...] = (Exception,),
    recover: bool = True
):
    def decorator(func: Callable):
        state = _global_circuit_breaker_state.setdefault(func, {
            "failures": 0,
            "last_failure_time": 0.0,
            "open": False
        })

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if state["open"]:
                if not recover:
                    raise RuntimeError(f"Global circuit breaker permanently OPEN for {func.__name__}")

                elapsed = time.time() - state["last_failure_time"]
                if elapsed < recovery_timeout:
                    raise RuntimeError(f"Global circuit breaker OPEN for {func.__name__}, retry in {int(recovery_timeout - elapsed)}s")
                else:
                    state["open"] = False
                    state["failures"] = 0

            try:
                result = func(*args, **kwargs)
                state["failures"] = 0
                return result
            except failure_exceptions as exc:
                state["failures"] += 1
                if state["failures"] >= failure_threshold:
                    state["open"] = True
                    state["last_failure_time"] = time.time()
                    LOG.error(f"Global circuit breaker tripped on {func.__name__}")
                raise
        return wrapper
    return decorator