import time
import urllib.parse
from unittest.mock import MagicMock, patch

import pytest
from libs.auth.secret import RotatingSecret, Secret


class TestSecret:
    """Unit tests for the base Secret wrapper."""

    def test_resolve_fetch_from_provider(self):
        """
        GIVEN a Secret with a provider
        THEN it should fetch and return the value from the provider
        WHEN resolve is called for the first time
        """
        mock_provider = MagicMock()
        mock_provider.get_secret.return_value = "raw_secret"
        secret = Secret("db_pass", provider=mock_provider)

        val = secret.resolve()

        assert val == "raw_secret"
        mock_provider.get_secret.assert_called_once_with("db_pass")

    def test_resolve_caching_behavior(self):
        """
        GIVEN a Secret that has already been resolved
        THEN it should return the cached value without calling the provider
        WHEN resolve is called again
        """
        mock_provider = MagicMock()
        mock_provider.get_secret.return_value = "cached_val"
        secret = Secret("api_key", provider=mock_provider)

        secret.resolve()  # First call
        val = secret.resolve()  # Second call

        assert val == "cached_val"
        assert mock_provider.get_secret.call_count == 1

    @patch("libs.auth.secret.register_log_masking")
    def test_log_masking_registration(self, mock_register):
        """
        GIVEN a new Secret resolution
        THEN the plaintext should be registered for log masking
        WHEN resolve is called
        """
        mock_provider = MagicMock()
        mock_provider.get_secret.return_value = "sensitive_data"
        secret = Secret("token", provider=mock_provider)

        secret.resolve()

        mock_register.assert_called_with("sensitive_data")

    def test_resolve_sanitization(self):
        """
        GIVEN a secret with special URL characters
        THEN it should return a URL-encoded string
        WHEN resolve is called with sanitize=True
        """
        mock_provider = MagicMock()
        mock_provider.get_secret.return_value = "pass#word"
        secret = Secret("dsn", provider=mock_provider)

        val = secret.resolve(sanitize=True)

        assert val == urllib.parse.quote_plus("pass#word")
        assert "#" not in val


class TestRotatingSecret:
    """Unit tests for the RotatingSecret TTL implementation."""

    def test_ttl_expiration_forces_refresh(self):
        """
        GIVEN a RotatingSecret with a short TTL
        THEN it should call the provider again after the TTL expires
        WHEN resolve is called
        """
        mock_provider = MagicMock()
        mock_provider.get_secret.side_effect = ["val1", "val2"]
        # Start with a 1-second TTL
        secret = RotatingSecret("rotate", provider=mock_provider, ttl_seconds=1)

        val1 = secret.resolve()
        assert val1 == "val1"

        # Mock time passing beyond the TTL
        with patch("time.time", return_value=time.time() + 10):
            val2 = secret.resolve()
            assert val2 == "val2"
            assert mock_provider.get_secret.call_count == 2

    def test_update_invalidates_cache(self):
        """
        GIVEN a RotatingSecret with a cached value
        THEN it should push the update to the provider and clear local cache
        WHEN update is called
        """
        mock_provider = MagicMock()
        secret = RotatingSecret("key", provider=mock_provider)
        secret._value = "old"
        secret._last_fetch_time = time.time()

        secret.update("new_value")

        mock_provider.update_secret.assert_called_with("key", "new_value")
        assert secret._value is None
        assert secret._last_fetch_time == 0

    def test_rotating_resolve_missing_provider(self):
        """
        GIVEN a RotatingSecret without a provider
        THEN it should raise a ValueError on TTL expiration
        WHEN resolve is called
        """
        secret = RotatingSecret("fail")
        with pytest.raises(ValueError, match="Provider not set"):
            secret.resolve()
