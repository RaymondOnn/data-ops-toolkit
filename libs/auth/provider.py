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

    def __init__(self, **config):
        self.path = Path(config.get("path", "./.secrets.json"))
        self._secrets = self._load() if self.path.exists() else {}

    def _load(self) -> dict[str, str]:
        LOG.info(f"Loading secrets from {self.path}")
        with self.path.open() as f:
            return json.load(f)

    def get(self, secret_id: str) -> str:
        return self._secrets.get(secret_id) or os.getenv(secret_id, "dev_fallback")

    def set(self, secret_id: str, value: Any) -> None:
        self._secrets[secret_id] = str(value)
        if self.path:
            with self.path.open("w") as f:
                json.dump(self._secrets, f, indent=4)


class LocalEncryptedProvider(SecretProvider):
    """Encrypted local JSON provider."""

    def __init__(self, **config):
        self.path = Path(config["encrypted_file_path"])
        self._fernet = Fernet(config["master_key"])
        self._secrets = self._load()

    def _load(self) -> dict[str, str]:
        if not self.path.exists():
            raise FileNotFoundError(f"Secrets file not found: {self.path}")
        with self.path.open() as f:
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

    def __init__(self, **config):
        from libs.cloud.aws import AWSClient, AWSClientConfig

        client_cfg = config.get("client", {})
        aws_config = AWSClientConfig(
            region=client_cfg.get("region", "ap-southeast-1"),
            sts_endpoint_url=client_cfg.get("sts_endpoint_url"),
            role_arn=client_cfg.get("role_arn"),
            profile_name=client_cfg.get("profile_name"),
            aws_access_key_id=client_cfg.get("aws_access_key_id"),
            aws_secret_access_key=client_cfg.get("aws_secret_access_key"),
        )

        aws = AWSClient(config=aws_config)
        svc_cfg = config["service"]
        self._client = aws.get_client(
            "secretsmanager", endpoint_url=svc_cfg["endpoint_url"]
        )

    def get(self, secret_id: str) -> str:
        LOG.debug(f"Fetching secret from AWS: {secret_id}")
        response = self._client.get_secret_value(SecretId=secret_id)
        return response["SecretString"]

    def set(self, secret_id: str, value: Any) -> None:
        content = json.dumps(value) if isinstance(value, dict) else str(value)
        self._client.put_secret_value(SecretId=secret_id, SecretString=content)
