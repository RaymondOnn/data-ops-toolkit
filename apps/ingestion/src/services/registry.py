import functools
import time
from typing import Any, Callable

import diskcache
import structlog


from src.utils.constants import ENGINE_CACHE_PATH
from libs.resilence.circuit_breaker import (
    CircuitBreaker, 
    CircuitBreakerTripped,
    CircuitBreakerState
)




LOG = structlog.getLogger(__name__)

class ServiceRegistry:
    _cache = diskcache.Cache(f"{ENGINE_CACHE_PATH}/services")

    @classmethod
    def get_status(cls, name: str) -> str:
        return cls._cache.get(f"status:{name}", "CLOSED")

    @classmethod
    def update_status(cls, name: str, status: str):
        cls._cache.set(f"status:{name}", status, expire=3600)

    @classmethod
    def get_last_failure_time(cls, name: str) -> float:
        # Default to 0 so new services aren't penalized
        return cls._cache.get(f"last_fail:{name}", 0.0)

    @classmethod
    def set_last_failure_time(cls, name: str, timestamp: float):
        cls._cache.set(f"last_fail:{name}", timestamp)

    @classmethod
    def increment_failure(cls, name: str) -> int:
        with cls._cache.transact():
            val = cls._cache.get(f"fails:{name}", 0) + 1
            cls._cache.set(f"fails:{name}", val, expire=600)
            return val

    @classmethod
    def reset(cls, name: str):
        cls._cache.delete(f"status:{name}")
        cls._cache.delete(f"fails:{name}")
        cls._cache.delete(f"last_fail:{name}")

        
def protect_service(threshold: int = 3, timeout: int = 300) -> Callable:
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self, *args: Any, **kwargs: Any) -> Any:
            # 1. Initialize logic engine with provided config
            breaker = CircuitBreaker(threshold=threshold, recovery_timeout=timeout)
            
            # 2. Pull raw data from the shared ServiceRegistry (Diskcache)
            status = ServiceRegistry.get_status(self.name)
            last_fail_at = ServiceRegistry.get_last_failure_time(self.name)
            
            # 3. Determine logical state
            state = breaker.get_current_state(status, last_fail_at)

            if state == CircuitBreakerState.OPEN:
                LOG.error("circuit_breaker_open", service=self.name)
                raise CircuitBreakerTripped(f"Circuit for {self.name} is OPEN. Stand-down active.")

            try:
                # 4. Attempt execution (for CLOSED or HALF_OPEN states)
                result = func(self, *args, **kwargs)
                
                # 5. Success: If we were HALF_OPEN, this "re-closes" the circuit
                if state == CircuitBreakerState.HALF_OPEN:
                    LOG.info("circuit_breaker_recovered", service=self.name)
                
                ServiceRegistry.reset(self.name)
                return result

            except Exception as e:
                # 6. Failure: Update Registry and check if we need to trip
                fail_count = ServiceRegistry.increment_failure(self.name)
                now = time.time()
                ServiceRegistry.set_last_failure_time(self.name, now)
                
                if breaker.should_trip(fail_count) or state == CircuitBreakerState.HALF_OPEN:
                    ServiceRegistry.update_status(self.name, "OPEN")
                    LOG.error("circuit_breaker_tripped", service=self.name, fails=fail_count)
                
                raise e
        return wrapper
    return decorator
