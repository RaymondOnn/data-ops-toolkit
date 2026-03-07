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
    def __init__(self, threshold: int, recovery_timeout: int = 300) -> None:
        self.threshold = threshold
        self._state = CircuitBreakerState.CLOSED
        self.recovery_timeout = recovery_timeout
        
    @property
    def state(self) -> CircuitBreakerState:
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