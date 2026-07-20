"""Secret provider implementations for local and cloud storage."""

import json
import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet

LOG = logging.getLogger(__name__)


class SecretProvider(ABC):
    """Base class for secret management providers."""

    @abstractmethod
    def get(self, secret_id: str) -> str:
        """Retrieve a secret value."""
        pass

    @abstractmethod
    def set(self, secret_id: str, value: Any) -> None:
        """Store a secret value."""
        pass


class LocalSecretProvider(SecretProvider):
    """Local JSON file provider (development only)."""

    def __init__(self, secrets_json: str):
        self.path = Path(secrets_json)
        self._secrets: dict[str, str] = self._load(self.path)

    def _load(self, path: Path) -> dict[str, str]:
        if not path.exists():
            raise FileNotFoundError(f"File not found at path: {path}")

        LOG.info(f"Loading secrets from {path}")

        with path.open() as f:
            return json.load(f) or {}

    def get(self, secret_id: str) -> str:
        if secret_id and (value := self._secrets.get(secret_id)):
            return value
        return os.getenv(secret_id, "dev_fallback")

    def set(self, secret_id: str, value: Any) -> None:
        self._secrets[secret_id] = str(value)
        if self.path:
            with self.path.open("w") as f:
                json.dump(self._secrets, f, indent=4)


class LocalEncryptedProvider(SecretProvider):
    """Encrypted local JSON provider."""

    def __init__(self, secrets_json: str, master_key: str):
        self.path = Path(secrets_json)
        self._fernet = Fernet(master_key)
        self._secrets = self._load(self.path)

    def _load(self, path: Path) -> dict[str, str]:
        if not self.path.exists():
            raise FileNotFoundError(f"Secrets file not found: {self.path}")

        with path.open() as f:
            return json.load(f)

    def get(self, secret_id: str) -> str:
        encrypted = self._secrets.get(secret_id)
        if not encrypted:
            raise ValueError(f"Secret {secret_id} not found")
        return self._fernet.decrypt(encrypted.encode()).decode()

    def set(self, secret_id: str, value: Any) -> None:
        raise NotImplementedError("Encrypted provider is read-only")


class AWSSecretProvider(SecretProvider):
    """AWS Secrets Manager provider (production)."""

    def __init__(
        self,
        region: str,
        sm_endpoint_url: str,
        sts_endpoint_url: str,
        role_arn: str,
        profile: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
    ):
        from libs.cloud.aws import AWSClient, AWSConfig

        aws_config = AWSConfig(
            region=region,
            sts_endpoint=sts_endpoint_url,
            role_arn=role_arn,
            profile=profile,
            access_key=aws_access_key_id,
            secret_key=aws_secret_access_key,
        )

        aws = AWSClient(config=aws_config)
        self._client = aws.get_client("secretsmanager", endpoint=sm_endpoint_url)

    def get(self, secret_id: str) -> str:
        LOG.debug(f"Fetching secret from AWS: {secret_id}")
        response = self._client.get_secret_value(SecretId=secret_id)
        return response["SecretString"]

    def set(self, secret_id: str, value: Any) -> None:
        content = json.dumps(value) if isinstance(value, dict) else str(value)
        self._client.put_secret_value(SecretId=secret_id, SecretString=content)
