from unittest.mock import MagicMock, patch

import pytest
from libs.auth.factory import AuthFactory


class TestAuthFactory:
    """Unit tests for the AuthFactory singleton and dispatch logic."""

    @pytest.fixture(autouse=True)
    def reset_factory(self):
        """
        GIVEN a test environment
        THEN reset the factory singleton before and after each test
        WHEN the fixture setup/teardown runs
        """
        AuthFactory._provider = None
        yield
        AuthFactory._provider = None

    @patch("libs.auth.factory.LocalSecretProvider")
    def test_get_provider_singleton(self, mock_local_cls):
        """
        GIVEN a request for a provider
        THEN the same instance should be returned for subsequent calls
        WHEN get_provider is invoked multiple times
        """
        p1 = AuthFactory.get_provider("dev", type="local_file")
        p2 = AuthFactory.get_provider("dev", type="local_file")

        assert p1 is p2
        mock_local_cls.assert_called_once()

    @patch("libs.auth.factory.LocalSecretProvider")
    def test_get_provider_default_dev(self, mock_local_cls):
        """
        GIVEN a dev environment and no specific type
        THEN return a LocalSecretProvider pointing to the default path
        WHEN get_provider is called
        """
        provider = AuthFactory.get_provider("dev")

        assert isinstance(provider, MagicMock)
        mock_local_cls.assert_called_once()
        # Verify default path was set
        _, kwargs = mock_local_cls.call_args
        assert kwargs["path"] == "./.secrets.json"

    @patch("libs.auth.factory.AWSSecretProvider")
    def test_get_provider_prod_override(self, mock_aws_cls):
        """
        GIVEN a 'prod' environment string
        THEN force the use of AWS Secret Manager regardless of config type
        WHEN get_provider is invoked
        """
        _ = AuthFactory.get_provider("prod", type="local_file")

        mock_aws_cls.assert_called_once()
        assert AuthFactory._provider == mock_aws_cls.return_value

    @patch("libs.auth.factory.LocalEncryptedProvider")
    @patch("os.getenv")
    def test_get_provider_encrypted_validation(self, mock_getenv, mock_enc_cls):
        """
        GIVEN a 'secure_file' type
        THEN ensure the master_key is retrieved and passed to the provider
        WHEN get_provider is called with valid configuration
        """
        mock_getenv.return_value = "my-secret-master-key"

        AuthFactory.get_provider("dev", type="secure_file")

        mock_enc_cls.assert_called_once()
        _, kwargs = mock_enc_cls.call_args
        assert kwargs["master_key"] == "my-secret-master-key"

    def test_get_provider_encrypted_missing_key(self):
        """
        GIVEN a 'secure_file' type without a master key in config or env
        THEN raise a ValueError
        WHEN get_provider is called
        """
        with (
            patch("os.getenv", return_value=None),
            pytest.raises(ValueError, match="requires a master_key"),
        ):
            AuthFactory.get_provider("dev", type="secure_file")

    def test_get_provider_unsupported_type(self):
        """
        GIVEN an unknown provider type identifier
        THEN raise a ValueError listing available strategies
        WHEN get_provider is called
        """
        with pytest.raises(ValueError, match="Unsupported provider type"):
            AuthFactory.get_provider("dev", type="invalid_type")

    @patch("libs.auth.factory.AWSSecretProvider")
    def test_get_provider_with_custom_config(self, mock_aws_cls):
        """
        GIVEN a custom configuration dictionary
        THEN pass those parameters directly to the provider constructor
        WHEN get_provider is called
        """
        custom_cfg = {
            "type": "aws_sm",
            "client": {"region": "us-west-2"},
            "service": {"endpoint_url": "http://localstack"},
        }

        AuthFactory.get_provider("dev", **custom_cfg)

        mock_aws_cls.assert_called_once()
        _, kwargs = mock_aws_cls.call_args
        assert kwargs["client"]["region"] == "us-west-2"
