import json
import os
from abc import ABC, abstractmethod
from pathlib import Path

from cryptography.fernet import Fernet


class SecretProvider(ABC):
    @abstractmethod
    def get_secret(self, secret_id: str) -> str:
        pass


class LocalSecretProvider(SecretProvider):
    """For Dev/Test: Reads from environment variables or a local file."""

    def get_secret(self, secret_id: str) -> str:
        return os.getenv(secret_id, "dev_fallback_value")


class AWSSecretProvider(SecretProvider):
    """For Production: Fetches from AWS Secrets Manager."""

    def __init__(self, **config) -> None:
        import boto3
        self.config = config
        self.client = boto3.client("secretsmanager", region=config["region"])

    def get_secret(self, secret_id: str) -> str:
        # Implementation of boto3 get_secret_value
        response = self.client.get_secret_value(SecretId=secret_id)
        return str(response["SecretString"])


def encrypt_local_secret(plaintext, key) -> str:
    f = Fernet(key)
    return str(f.encrypt(plaintext.encode()).decode())


class LocalEncryptedProvider(SecretProvider):
    def __init__(self, encrypted_file_path: str, master_key: str) -> None:
        self.path = Path(encrypted_file_path)
        self.f = Fernet(master_key)
        self._data: dict[str, str] = {}
        self._load_secrets()

    def _load_secrets(self) -> None:
        """Load and cache the encrypted secrets map."""
        if not os.path.exists(self.path):
            raise FileNotFoundError(f"Secrets file not found: {self.path}")
            
        with open(self.path, "r") as file:
            self._data = json.load(file)

    def get_secret(self, secret_id: str) -> str:
        encrypted_val = self._data.get(secret_id)
        if not encrypted_val:
            raise ValueError(f"Secret {secret_id} not found locally.")

        # Decrypt in memory only
        return str(self.f.decrypt(encrypted_val.encode()).decode())

        # Decrypt to a file
        # with open("output.txt", "wb") as f:
        #     f.write(self.f.decrypt(encrypted_val.encode()))
