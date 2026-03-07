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
    _cache: diskcache.Cache = diskcache.Cache(f"{ENGINE_CACHE_PATH}/services")

    @classmethod
    def get_status(cls, name: str) -> str:
        return str(cls._cache.get(f"status:{name}", "CLOSED"))

    @classmethod
    def update_status(cls, name: str, status: str) -> None:
        cls._cache.set(f"status:{name}", status, expire=3600)

    @classmethod
    def get_last_failure_time(cls, name: str) -> float:
        return float(cls._cache.get(f"last_fail:{name}", 0.0))

    @classmethod
    def set_last_failure_time(cls, name: str, timestamp: float) -> None:
        cls._cache.set(f"last_fail:{name}", timestamp)

    @classmethod
    def get_retry_attempts(cls, name: str) -> int:
        """Tracks consecutive recovery failures for exponential backoff."""
        return int(cls._cache.get(f"retries:{name}", 0))

    @classmethod
    def increment_retry_attempt(cls, name: str) -> int:
        with cls._cache.transact():
            val = cls._cache.get(f"retries:{name}", 0) + 1
            cls._cache.set(f"retries:{name}", val, expire=86400) # 24h TTL
            return int(val)

    @classmethod
    def increment_failure(cls, name: str) -> int:
        with cls._cache.transact():
            val: int = cls._cache.get(f"fails:{name}", 0) + 1
            cls._cache.set(f"fails:{name}", val, expire=600)
            return val

    @classmethod
    def reset(cls, name: str) -> None:
        """Clear all health data upon successful recovery."""
        cls._cache.delete(f"status:{name}")
        cls._cache.delete(f"fails:{name}")
        cls._cache.delete(f"last_fail:{name}")
        cls._cache.delete(f"retries:{name}")

        
def protect_service(threshold: int = 3, timeout: int = 300)  -> Callable[..., Any]:
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            # 1. Initialize logic engine with provided config
            breaker = CircuitBreaker(threshold=threshold, recovery_timeout=timeout)
            
            # 1. Pull current persistent state
            status = ServiceRegistry.get_status(self.name)
            last_fail = ServiceRegistry.get_last_failure_time(self.name)
            retries = ServiceRegistry.get_retry_attempts(self.name)
            
            # 2. Evaluate logical state
            state = breaker.get_current_state(status, last_fail, retries)

            if state == CircuitBreakerState.OPEN:
                raise CircuitBreakerTripped(f"Circuit for {self.name} is OPEN. Backoff in effect.")

            try:
                # 3. Attempt execution
                result = func(self, *args, **kwargs)
                
                # 4. If we were testing (HALF_OPEN) and succeeded, clear the registry
                if state == CircuitBreakerState.HALF_OPEN:
                    LOG.info("service_recovered", service=self.name)
                
                ServiceRegistry.reset(self.name)
                return result

            except Exception as e:
                # 5. Handle Failure
                fail_count = ServiceRegistry.increment_failure(self.name)
                ServiceRegistry.set_last_failure_time(self.name, time.time())
                
                # If we fail during a recovery attempt (HALF_OPEN), increment retry count
                if state == CircuitBreakerState.HALF_OPEN:
                    ServiceRegistry.increment_retry_attempt(self.name)
                
                if breaker.should_trip(fail_count) or state == CircuitBreakerState.HALF_OPEN:
                    ServiceRegistry.update_status(self.name, "OPEN")
                    LOG.error("circuit_tripped", service=self.name, fail_count=fail_count)
                
                raise e
        return wrapper
    return decorator 
