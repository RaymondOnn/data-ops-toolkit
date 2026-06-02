import time

import pytest
from libs.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerState,
    CircuitBreakerTripped,
)


class TestCircuitBreaker:
    """Unit tests for the CircuitBreaker implementation."""

    def test_closed_on_success(self):
        """
        GIVEN a CircuitBreaker wrapping a healthy function
        THEN the state should remain CLOSED
        WHEN the function is called and succeeds
        """
        breaker = CircuitBreaker()

        @breaker
        def healthy_call():
            return "success"

        assert healthy_call() == "success"
        assert breaker.current_state == CircuitBreakerState.CLOSED
        assert breaker.failures == 0

    def test_trips_after_threshold(self):
        """
        GIVEN a failure threshold of 2
        THEN the breaker should trip to OPEN
        WHEN the wrapped function fails twice
        """
        breaker = CircuitBreaker(failure_threshold=2)

        @breaker
        def failing_call():
            raise ValueError("Boom")

        # Failure 1
        with pytest.raises(ValueError):
            failing_call()
        assert breaker.current_state == CircuitBreakerState.CLOSED
        assert breaker.failures == 1

        # Failure 2 - Trip
        with pytest.raises(ValueError):
            failing_call()
        assert breaker.current_state == CircuitBreakerState.OPEN
        assert breaker.failures == 2

    def test_raises_tripped_exception_when_open(self):
        """
        GIVEN a CircuitBreaker in OPEN state
        THEN it should raise CircuitBreakerTripped without calling the function
        WHEN the function is invoked
        """
        breaker = CircuitBreaker(failure_threshold=1)

        @breaker
        def some_action():
            return "should not be called"

        # Trip the breaker
        with pytest.raises(ValueError):
            some_action()

        assert breaker.current_state == CircuitBreakerState.OPEN

        with pytest.raises(CircuitBreakerTripped) as exc:
            some_action()
        assert "Breaker OPEN" in str(exc.value)

    def test_recovery_to_half_open(self):
        """
        GIVEN an OPEN breaker and a short recovery timeout
        THEN it should transition through HALF_OPEN to CLOSED
        WHEN the timeout expires and a call succeeds
        """
        breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=0.1)

        @breaker
        def recovering_call():
            return "recovered"

        # Trip
        with pytest.raises(ValueError):
            recovering_call()

        time.sleep(0.15)

        # Next call should succeed and close the circuit
        assert recovering_call() == "recovered"
        assert breaker.current_state == CircuitBreakerState.CLOSED

    def test_half_open_failure_trips_immediately(self):
        """
        GIVEN a breaker that has transitioned to HALF_OPEN
        THEN it should return to OPEN immediately on the next failure
        WHEN a failure occurs during the trial period
        """
        breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=0.1)
        breaker._on_failure(ValueError("Initial trip"))
        assert breaker.current_state == CircuitBreakerState.OPEN

        time.sleep(0.15)
        # Breaker is now logically HALF_OPEN but won't change state until _before_call

        @breaker
        def trial_failure():
            raise RuntimeError("Trial failed")

        with pytest.raises(RuntimeError):
            trial_failure()

        assert breaker.current_state == CircuitBreakerState.OPEN
        # In HALF_OPEN, failures increments from previous count
        assert breaker.failures == 2

    def test_ignores_non_expected_exceptions(self):
        """
        GIVEN a breaker configured for specific exceptions
        THEN it should ignore other exception types
        WHEN an unconfigured exception is raised
        """
        breaker = CircuitBreaker(expected_exceptions=(RuntimeError,))

        @breaker
        def unexpected_fail():
            raise ValueError("Wrong error")

        with pytest.raises(ValueError):
            unexpected_fail()

        assert breaker.failures == 0
        assert breaker.current_state == CircuitBreakerState.CLOSED
