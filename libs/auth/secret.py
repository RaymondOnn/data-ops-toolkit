import logging
import random
import threading
import time
from typing import Any

from libs.utils.log import register_log_masking

from .provider import SecretProvider

LOG = logging.getLogger(__name__)


class Secret:
    """A wrapper that hides the actual secret until needed."""

    def __init__(self, secret_id: str, provider: SecretProvider | None = None):
        """Initializes the Secret wrapper.

        Args:
            secret_id: The unique identifier for the secret in the provider.
            provider: The SecretProvider used to resolve the actual value.
        """
        self.secret_id = secret_id
        self.provider = provider
        self._value: str | None = None

    def resolve(self, sanitize: bool = True, force_refresh: bool = False) -> str:
        """Fetches the actual plaintext string.

        Used by DB Clients right before connection. Automatically registers
        the value for log masking.

        Args:
            sanitize: If True, URL-encodes the secret value (useful for DSNs).
            force_refresh: If True, ignores cached value and fetches from provider.

        Returns:
            str: The plaintext secret value.

        Raises:
            ValueError: If no provider is associated with this secret.
        """
        if not self._value or force_refresh:
            provider = self.provider
            if not provider:
                raise ValueError(f"Provider not set for secret: {self.secret_id}")
            self._value = provider.get_secret(self.secret_id)

            # Automatically register the plaintext for global log masking
            if self._value:
                register_log_masking(self._value)

        if sanitize:
            import urllib.parse

            self._value = urllib.parse.quote_plus(self._value)

        return str(self._value)

    @property
    def is_redacted(self) -> bool:
        """
        Checks if this secret is currently protected by log masking.

        Returns:
            bool: True if the value is registered in the global mask list.
        """
        from libs.utils.log import is_masked

        return self._value is not None and is_masked(self._value)

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
        provider: SecretProvider | None = None,
        ttl_seconds: int = 3600,
    ):
        """Initializes a RotatingSecret with Time-To-Live logic.

        Args:
            secret_id: The identifier for the secret.
            provider: The provider used for fetching/updating.
            ttl_seconds: Cache duration in seconds before forcing a refresh.
        """
        super().__init__(secret_id, provider)
        self.ttl = ttl_seconds
        self._last_fetch_time: float = 0
        self._lock = threading.Lock()

    def resolve(self, sanitize: bool = False, force_refresh: bool = False) -> str:
        """
        Retrieves the latest secret value, refreshing if the TTL expired.

        Overrides base resolve to inject TTL-based caching.

        Args:
            sanitize: If True, URL-encodes the value.
            force_refresh: If True, bypasses TTL and fetches from source.

        Returns:
            str: The current plaintext value.

        Raises:
            ValueError: If no provider is set and the cache is empty or expired.
        """
        now = time.time()

        # 1. Fast path: Use memory cache if fresh
        if (
            self._value
            and (now - self._last_fetch_time) < self.ttl
            and not force_refresh
        ):
            return super().resolve(sanitize=sanitize)

        # 2. Slow path: Acquire lock and refresh
        with self._lock:
            if not self._value or (now - self._last_fetch_time) >= self.ttl:
                if not self.provider:
                    raise ValueError(
                        f"Provider not set for rotating secret: {self.secret_id}"
                    )
                self._value = self.provider.get_secret(self.secret_id)

                # Apply Jitter to prevent synchronized API hits
                jitter = self.ttl * 0.1
                self._last_fetch_time = time.time() + random.uniform(-jitter, jitter)
                LOG.debug("Secret cache refreshed", extra={"secret_id": self.secret_id})

        return super().resolve(sanitize=sanitize)

    def update(self, new_value: str | dict[str, Any]) -> None:
        """Updates the secret in the provider and invalidates the local cache.

        Args:
            new_value: The new content to persist.
        """
        if not self.provider:
            raise ValueError(f"Provider not set for secret update: {self.secret_id}")
        self.provider.update_secret(self.secret_id, new_value)
        self._value = None  # Invalidate
        self._last_fetch_time = 0
