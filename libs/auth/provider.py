import json
import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet

LOG = logging.getLogger(__name__)


class SecretProvider(ABC):
    """Abstract base class for secret management providers."""

    @abstractmethod
    def get_secret(self, secret_id: str) -> str:
        """
        Retrieves a secret value by its identifier.

        Args:
            secret_id: The unique identifier for the secret.

        Returns:
            str: The plaintext secret value.
        """
        pass

    @abstractmethod
    def update_secret(self, secret_id: str, value: Any) -> None:
        """
        Updates a secret value in the provider.

        Args:
            secret_id: The unique identifier for the secret.
            value: The new value to store. Can be a string or dictionary.
        """
        pass


class LocalSecretProvider(SecretProvider):
    """
    Secret provider for development and testing environments.

    Supports resolution from a local JSON file with fallback to
    system environment variables.
    """

    def __init__(self, **config: Any) -> None:
        """
        Initializes the LocalSecretProvider.

        Args:
            **config: Configuration containing the 'path' to the JSON file.
        """
        LOG.debug("Initializing LocalSecretProvider", extra={"config": config})
        file_path = str(config.get("path"))
        self.path = Path(file_path) if file_path else None
        self._data: dict[str, str] = {}
        self._load_secrets()

    def _load_secrets(self) -> None:
        """Internal helper to load secrets from the configured JSON path."""
        if self.path and self.path.exists():
            LOG.info("Loading secrets from local file", extra={"path": str(self.path)})
            with self.path.open() as file:
                self._data = json.load(file)
        else:
            LOG.warning(
                "Local secrets file not found or path not provided",
                extra={"path": self.path},
            )

    def get_secret(self, secret_id: str) -> str:
        """
        Retrieves a secret based on priority: File > Env > Default.

        Args:
            secret_id: The key to look up.

        Returns:
            str: The resolved secret value.
        """
        val = self._data.get(secret_id)
        if val:
            LOG.debug("Secret resolved from JSON file", extra={"secret_id": secret_id})
            return val

        val = os.getenv(secret_id)
        if val:
            LOG.debug(
                "Secret resolved from environment variable",
                extra={"secret_id": secret_id},
            )
            return val

        LOG.warning(
            "Secret not found in file or env, using fallback",
            extra={"secret_id": secret_id},
        )
        return "dev_fallback_value"

    def update_secret(self, secret_id: str, value: Any) -> None:
        """
        Updates a secret in memory and persists it to the JSON file.

        Args:
            secret_id: The unique identifier for the secret.
            value: The value to store.
        """
        self._data[secret_id] = str(value)
        if self.path:
            with self.path.open("w") as f:
                json.dump(self._data, f, indent=4)


class AWSSecretProvider(SecretProvider):
    """For Production: Fetches from AWS Secrets Manager."""

    def __init__(self, **config) -> None:
        """
        Initializes the AWS Secrets Manager provider.

        Args:
            **config: Configuration for AWSClient and service-specific overrides.
        """

        from libs.cloud.aws import AWSClient, AWSClientConfig

        # Standard library unpacking - no msgspec for shared libs
        client_cfg = config.get("client", {})
        aws_config = AWSClientConfig(
            region=client_cfg.get("region", "ap-southeast-1"),
            sts_endpoint_url=client_cfg.get("sts_endpoint_url"),
            role_arn=client_cfg.get("role_arn"),
            profile_name=client_cfg.get("profile_name"),
            aws_access_key_id=client_cfg.get("aws_access_key_id"),
            aws_secret_access_key=client_cfg.get("aws_secret_access_key"),
        )

        self.aws_client = AWSClient(config=aws_config)

        svc_cfg = config["service"]
        self.client = self.aws_client.get_client(
            "secretsmanager", endpoint_url=svc_cfg["endpoint_url"]
        )

    def get_secret(self, secret_id: str) -> str:
        """
        Fetches a secret string from AWS Secrets Manager.

        Args:
            secret_id: The AWS SecretId (Name or ARN).

        Returns:
            str: The SecretString from the AWS response.
        """
        LOG.debug(
            "Fetching secret from AWS Secrets Manager", extra={"secret_id": secret_id}
        )
        # Implementation of boto3 get_secret_value
        response = self.client.get_secret_value(SecretId=secret_id)
        return str(response["SecretString"])

    def update_secret(self, secret_id: str, value: Any) -> None:
        """
        Updates an existing secret value in AWS Secrets Manager.

        Args:
            secret_id: The AWS SecretId.
            value: The new content to store (dict or string).
        """
        str_val = json.dumps(value) if isinstance(value, dict) else str(value)
        self.client.put_secret_value(SecretId=secret_id, SecretString=str_val)


def encrypt_local_secret(plaintext, key) -> str:
    """
    Helper to encrypt a plaintext string using a Fernet key.

    Args:
        plaintext: The raw string to encrypt.
        key: A valid Fernet-compatible encryption key.

    Returns:
        str: The encrypted token as a string.
    """
    f = Fernet(key)
    return str(f.encrypt(plaintext.encode()).decode())


class LocalEncryptedProvider(SecretProvider):
    """Read-only provider for encrypted local JSON secret files."""

    def __init__(self, **config: Any) -> None:
        """
        Initializes the LocalEncryptedProvider.

        Args:
            **config: Config requiring 'encrypted_file_path' and 'master_key'.
        """
        encrypted_file_path = str(config.get("encrypted_file_path"))
        master_key = str(config.get("master_key"))

        self.path = Path(encrypted_file_path)
        self.f = Fernet(master_key)
        LOG.debug("Initializing LocalEncryptedProvider", extra={"path": str(self.path)})
        self._data: dict[str, str] = {}
        self._load_secrets()

    def _load_secrets(self) -> None:
        """Load and cache the encrypted secrets map."""
        if not self.path.exists():
            LOG.error("Encrypted secrets file missing", extra={"path": str(self.path)})
            raise FileNotFoundError(f"Secrets file not found: {self.path}")

        with self.path.open() as file:
            self._data = json.load(file)
        LOG.info(
            "Encrypted secrets loaded into memory", extra={"count": len(self._data)}
        )

    def get_secret(self, secret_id: str) -> str:
        """
        Decrypts and retrieves a secret from the local storage.

        Args:
            secret_id: The key to look up.

        Returns:
            str: The decrypted plaintext value.
        """
        encrypted_val = self._data.get(secret_id)
        if not encrypted_val:
            LOG.error(
                "Secret key not found in encrypted file", extra={"secret_id": secret_id}
            )
            raise ValueError(f"Secret {secret_id} not found locally.")

        LOG.debug("Decrypting secret", extra={"secret_id": secret_id})
        return str(self.f.decrypt(encrypted_val.encode()).decode())

    def update_secret(self, secret_id: str, value: Any) -> None:
        """
        Updates are not supported for this provider.

        Args:
            secret_id: Ignored.
            value: Ignored.
        """
        raise NotImplementedError(
            "LocalEncryptedProvider does not support secret updates."
        )
