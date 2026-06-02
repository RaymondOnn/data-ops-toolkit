from unittest.mock import MagicMock, patch

import pytest
from libs.cloud.aws import AWSClient, AWSClientConfig


class TestAWSClientConfig:
    """Unit tests for AWSClientConfig validation logic."""

    def test_validate_success_with_keys(self):
        """
        GIVEN both access and secret keys
        THEN validate should pass without error
        WHEN validate is called
        """
        config = AWSClientConfig(
            region="us-east-1",
            aws_access_key_id="key",
            aws_secret_access_key="secret",
        )
        config.validate()

    def test_validate_failure_missing_secret(self):
        """
        GIVEN only an access key without a corresponding secret
        THEN validate should raise a ValueError
        WHEN validate is called
        """
        config = AWSClientConfig(region="us-east-1", aws_access_key_id="key")
        with pytest.raises(
            ValueError, match="provided without 'aws_secret_access_key'"
        ):
            config.validate()

    def test_validate_success_default_chain(self):
        """
        GIVEN no explicit identity parameters
        THEN validate should pass (falling back to standard AWS providers)
        WHEN validate is called
        """
        config = AWSClientConfig(region="us-east-1")
        config.validate()


class TestAWSClient:
    """Unit tests for AWSClient singleton and session management."""

    @pytest.fixture(autouse=True)
    def reset_singleton(self):
        """Ensure each test starts with a fresh singleton state."""
        AWSClient._instance = None
        yield
        AWSClient._instance = None

    @patch("boto3.Session")
    def test_singleton_behavior(self, mock_session):
        """
        GIVEN multiple instantiation attempts
        THEN the same object reference should be returned
        WHEN AWSClient is initialized
        """
        config = AWSClientConfig(region="us-east-1")
        c1 = AWSClient(config=config)
        c2 = AWSClient(config=config)
        assert c1 is c2

    @patch("boto3.Session")
    def test_get_session_standard(self, mock_session_class):
        """
        GIVEN a configuration without a Role ARN
        THEN get_session should return the base session without assuming roles
        WHEN get_session is called
        """
        config = AWSClientConfig(region="us-east-1")
        client = AWSClient(config=config)

        session = client.get_session()
        assert session == mock_session_class.return_value

    @patch("boto3.Session")
    def test_get_client_caching(self, mock_session_class):
        """
        GIVEN an initialized AWSClient
        THEN get_client should return a client from the session with correct parameters
        WHEN get_client is called for 's3'
        """
        mock_session = mock_session_class.return_value
        mock_s3 = MagicMock()
        mock_session.client.return_value = mock_s3

        config = AWSClientConfig(region="us-east-1")
        client = AWSClient(config=config)

        client_instance = client.get_client("s3")
        assert client_instance == mock_s3
        mock_session.client.assert_called_with(
            "s3", region_name="us-east-1", endpoint_url=None
        )
