"""Secret wrapper with lazy resolution and rotation support."""

import logging
import random
import threading
import time
from typing import Any

from .log import mask_in_logs
from .provider import SecretProvider

LOG = logging.getLogger(__name__)


class Secret:
    """Lazy secret wrapper that resolves value only when needed."""

    def __init__(self, secret_id: str, provider: SecretProvider | None = None):
        self.secret_id = secret_id
        self._provider = provider
        self._value: str | None = None

    def resolve(self, url_encode: bool = False, force: bool = False) -> str:
        """Get the actual secret value."""
        if not self._value or force:
            if not self._provider:
                raise ValueError(f"No provider for secret: {self.secret_id}")
            self._value = self._provider.get(self.secret_id)
            if self._value:
                mask_in_logs(self._value)

        if url_encode:
            import urllib.parse

            self._value = urllib.parse.quote_plus(self._value)

        return self._value

    def __repr__(self) -> str:
        return f"<Secret '{self.secret_id}'>"

    def __str__(self) -> str:
        return "********"


class RotatingSecret(Secret):
    """Secret that auto-refreshes after TTL."""

    def __init__(
        self,
        secret_id: str,
        provider: SecretProvider | None = None,
        ttl_seconds: int = 3600,
    ):
        super().__init__(secret_id, provider)
        self.ttl = ttl_seconds
        self._last_fetch: float = 0
        self._lock = threading.Lock()

    def resolve(self, url_encode: bool = False, force: bool = False) -> str:
        now = time.time()

        # Fast path: use cached value
        if self._value and (now - self._last_fetch) < self.ttl and not force:
            return super().resolve(url_encode=url_encode)

        # Slow path: refresh with lock
        with self._lock:
            if not self._value or (now - self._last_fetch) >= self.ttl:
                if not self._provider:
                    raise ValueError(f"No provider for secret: {self.secret_id}")
                self._value = self._provider.get(self.secret_id)
                # Add jitter to prevent thundering herd
                jitter = self.ttl * 0.1 * random.uniform(-1, 1)
                self._last_fetch = time.time() + jitter
                LOG.debug(f"Refreshed secret: {self.secret_id}")

        return super().resolve(url_encode=url_encode)

    def update(self, new_value: str | dict[str, Any]) -> None:
        """Update secret in provider and invalidate cache."""
        if not self._provider:
            raise ValueError(f"No provider for secret: {self.secret_id}")
        self._provider.set(self.secret_id, new_value)
        self._value = None
        self._last_fetch = 0
