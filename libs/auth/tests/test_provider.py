import json
import os
from unittest.mock import MagicMock, patch

import pytest
from cryptography.fernet import Fernet
from libs.auth.provider import (
    AWSSecretProvider,
    LocalEncryptedProvider,
    LocalSecretProvider,
    encrypt_local_secret,
)


class TestLocalSecretProvider:
    """Unit tests for the LocalSecretProvider."""

    def test_get_secret_priority_file(self, tmp_path):
        """
        GIVEN a key exists in both the JSON file and environment
        THEN the value from the JSON file should be returned (Priority 1)
        WHEN get_secret is called
        """
        secrets_file = tmp_path / "secrets.json"
        secrets_file.write_text(json.dumps({"api_key": "file_value"}))

        with patch.dict(os.environ, {"api_key": "env_value"}):
            provider = LocalSecretProvider(path=str(secrets_file))
            assert provider.get_secret("api_key") == "file_value"

    def test_get_secret_priority_env(self, tmp_path):
        """
        GIVEN a key is missing from the file but exists in the environment
        THEN the environment variable should be returned (Priority 2)
        WHEN get_secret is called
        """
        secrets_file = tmp_path / "empty.json"
        secrets_file.write_text("{}")

        with patch.dict(os.environ, {"DB_PASS": "secret_pass"}):
            provider = LocalSecretProvider(path=str(secrets_file))
            assert provider.get_secret("DB_PASS") == "secret_pass"

    def test_update_secret_persists(self, tmp_path):
        """
        GIVEN a new secret value
        THEN it should be stored in memory and written to the file
        WHEN update_secret is called
        """
        secrets_file = tmp_path / "writable.json"
        secrets_file.write_text("{}")
        provider = LocalSecretProvider(path=str(secrets_file))

        provider.update_secret("new_key", "new_val")

        # Check memory
        assert provider.get_secret("new_key") == "new_val"
        # Check file
        updated_data = json.loads(secrets_file.read_text())
        assert updated_data["new_key"] == "new_val"


class TestAWSSecretProvider:
    """Unit tests for the AWSSecretProvider."""

    @patch("libs.cloud.aws.AWSClient")
    def test_get_secret_boto_call(self, mock_aws_cls):
        """
        GIVEN a secret ID
        THEN it should call get_secret_value on the underlying boto3 client
        WHEN get_secret is invoked
        """
        mock_aws = mock_aws_cls.return_value
        mock_sm_client = MagicMock()
        mock_aws.get_client.return_value = mock_sm_client
        mock_sm_client.get_secret_value.return_value = {"SecretString": "p-123"}

        config = {
            "client": {"region": "us-east-1"},
            "service": {"endpoint_url": None},
        }
        provider = AWSSecretProvider(**config)

        val = provider.get_secret("prod/api/token")

        assert val == "p-123"
        mock_sm_client.get_secret_value.assert_called_with(SecretId="prod/api/token")

    @patch("libs.cloud.aws.AWSClient")
    def test_update_secret_dict(self, mock_aws_cls):
        """
        GIVEN a dictionary value for an AWS secret
        THEN it should be serialized to a JSON string before calling AWS
        WHEN update_secret is called
        """
        mock_sm_client = MagicMock()
        mock_aws_cls.return_value.get_client.return_value = mock_sm_client

        provider = AWSSecretProvider(client={}, service={"endpoint_url": None})
        secret_data = {"username": "admin", "password": "abc"}

        provider.update_secret("my-db-creds", secret_data)

        mock_sm_client.put_secret_value.assert_called_with(
            SecretId="my-db-creds", SecretString=json.dumps(secret_data)
        )


class TestLocalEncryptedProvider:
    """Unit tests for the LocalEncryptedProvider."""

    @pytest.fixture
    def encrypted_setup(self, tmp_path):
        """Generates a key and an encrypted secrets file for testing."""
        key = Fernet.generate_key()
        f = Fernet(key)
        data = {"token": f.encrypt(b"decrypted_val").decode()}

        file_path = tmp_path / "encrypted.json"
        file_path.write_text(json.dumps(data))

        return str(file_path), key.decode()

    def test_get_secret_success(self, encrypted_setup):
        """
        GIVEN a valid encrypted file and the correct master key
        THEN return the decrypted plaintext value
        WHEN get_secret is called
        """
        path, key = encrypted_setup
        provider = LocalEncryptedProvider(encrypted_file_path=path, master_key=key)

        assert provider.get_secret("token") == "decrypted_val"

    def test_update_secret_raises_not_implemented(self, encrypted_setup):
        """
        GIVEN a LocalEncryptedProvider
        THEN it should raise NotImplementedError as it is read-only
        WHEN update_secret is called
        """
        path, key = encrypted_setup
        provider = LocalEncryptedProvider(encrypted_file_path=path, master_key=key)

        with pytest.raises(NotImplementedError):
            provider.update_secret("any", "val")


def test_encrypt_local_secret_logic():
    """
    GIVEN a plaintext string and a key
    THEN return a string that can be successfully decrypted by Fernet
    WHEN encrypt_local_secret is invoked
    """
    key = Fernet.generate_key()
    plaintext = "super-secret"

    encrypted = encrypt_local_secret(plaintext, key)

    f = Fernet(key)
    assert f.decrypt(encrypted.encode()).decode() == plaintext
