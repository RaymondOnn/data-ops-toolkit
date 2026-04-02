import logging
import random
import threading
import time
from typing import Any, Optional

from libs.auth.provider import SecretProvider

LOG = logging.getLogger(__name__)


class Secret:
    """A wrapper that hides the actual secret until needed."""

    def __init__(self, secret_id: str, provider: SecretProvider | None = None):
        self.secret_id = secret_id
        self.provider = provider
        self._value = None

    def resolve(self, sanitize: bool = False, force_refresh: bool = False) -> str:
        """Fetch the actual string. Used by DB Clients right before connection."""
        if not self._value or force_refresh:
            if not self.provider:
                raise ValueError(f"Provider not set for secret: {self.secret_id}")
            self._value = self.provider.get_secret(self.secret_id)

        if sanitize:
            import urllib.parse

            self._value = urllib.parse.quote_plus(self._value)

        return str(self._value)

    def __repr__(self) -> str:
        # This shows up in debugger and logs
        return f"<Secret id='{self.secret_id}' (HIDDEN)>"

    def __str__(self) -> str:
        n_chars = 0 if not self._value else len(self._value)

        # This shows up in print()
        return f"******** (length: {n_chars})"


class RotatingSecret(Secret):
    """
    A thread-safe wrapper for cloud secrets that supports automatic rotation via TTL.
    Ensures that credentials stay fresh without requiring application restarts.
    """
    def __init__(
        self, 
        secret_id: str, 
        provider: Optional[Any] = None,
        ttl_seconds: int = 3600
    ):
        super().__init__(secret_id, provider)
        self.ttl = ttl_seconds
        self._last_fetch_time: float = 0
        self._lock = threading.Lock()

    def resolve(self, sanitize: bool = False, force_refresh: bool = False) -> str:
        """
        Overrides resolve to inject TTL-based caching logic.
        Retrieves the latest secret value. If the cache is expired,
        it refreshes the value from the provider.
        """
        now = time.time()

        # 1. Fast path: Use memory cache if fresh
        if self._value and (now - self._last_fetch_time) < self.ttl and not force_refresh:
            return super().resolve(sanitize=sanitize)

        # 2. Slow path: Acquire lock and refresh
        with self._lock:
            if not self._value or (now - self._last_fetch_time) >= self.ttl:
                self._value = self.provider.get_secret(self.secret_id)
                
                # Apply Jitter to prevent synchronized API hits
                jitter = self.ttl * 0.1
                self._last_fetch_time = time.time() + random.uniform(-jitter, jitter)
                LOG.debug("Secret cache refreshed", extra={"secret_id": self.secret_id})

        return super().resolve(sanitize=sanitize)

    def update(self, new_value: str | dict[str, Any]) -> None:
        """Update the value in the provider and invalidate cache."""
        self.provider.update_secret(self.secret_id, new_value)
        self._value = None # Invalidate
        self._last_fetch_time = 0
