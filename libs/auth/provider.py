import json
import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet

LOG = logging.getLogger(__name__)


class SecretProvider(ABC):
    @abstractmethod
    def get_secret(self, secret_id: str) -> str:
        pass


class LocalSecretProvider(SecretProvider):
    """For Dev/Test: Reads from a local JSON file with environment fallback."""

    def __init__(self, **config: dict[str, Any]) -> None:
        LOG.debug("Initializing LocalSecretProvider", extra={"config": config})
        file_path = str(config.get("path"))
        self.path = Path(file_path) if file_path else None
        self._data: dict[str, str] = {}
        self._load_secrets()

    def _load_secrets(self) -> None:
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
        # Priority: 1. JSON File | 2. Environment Variable | 3. Fallback String
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


class AWSSecretProvider(SecretProvider):
    """For Production: Fetches from AWS Secrets Manager."""

    def __init__(self, **config) -> None:
        import boto3

        self.config = config
        LOG.info(
            "Initializing AWSSecretProvider", extra={"region": config.get("region")}
        )
        self.client = boto3.client("secretsmanager", region=config["region"])

    def get_secret(self, secret_id: str) -> str:
        LOG.debug(
            "Fetching secret from AWS Secrets Manager", extra={"secret_id": secret_id}
        )
        # Implementation of boto3 get_secret_value
        response = self.client.get_secret_value(SecretId=secret_id)
        return str(response["SecretString"])


def encrypt_local_secret(plaintext, key) -> str:
    f = Fernet(key)
    return str(f.encrypt(plaintext.encode()).decode())


class LocalEncryptedProvider(SecretProvider):
    def __init__(self, **config: dict[str, Any]) -> None:
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
        encrypted_val = self._data.get(secret_id)
        if not encrypted_val:
            LOG.error(
                "Secret key not found in encrypted file", extra={"secret_id": secret_id}
            )
            raise ValueError(f"Secret {secret_id} not found locally.")

        LOG.debug("Decrypting secret", extra={"secret_id": secret_id})
        return str(self.f.decrypt(encrypted_val.encode()).decode())

        # Decrypt to a file
        # with open("output.txt", "wb") as f:
        #     f.write(self.f.decrypt(encrypted_val.encode()))
