"""Secret provider factory with environment-aware selection."""

import logging
import os

from .provider import (
    AWSSecretProvider,
    LocalEncryptedProvider,
    LocalSecretProvider,
    SecretProvider,
)

LOG = logging.getLogger(__name__)

# Provider registry
_PROVIDERS: dict[str, type[SecretProvider]] = {
    "local_file": LocalSecretProvider,
    "secure_file": LocalEncryptedProvider,
    "aws_sm": AWSSecretProvider,
}

DEFAULT_SECRETS_PATH = ".secrets.json"
DEFAULT_ENCRYPTED_PATH = ".secrets.enc"
DEFAULT_MASTER_KEY_ENV = "MASTER_KEY"


class AuthFactory:
    """Factory for creating secret providers."""

    _instance: SecretProvider | None = None

    @classmethod
    def get_provider(cls, env: str, **config) -> SecretProvider:
        """Get or create a secret provider singleton."""
        if cls._instance:
            return cls._instance

        env = env or os.getenv("APP_ENV", "dev").lower()
        provider_type = config.get("type", "local_file").strip().lower()

        # Force AWS in production
        if env == "prod":
            LOG.info("Production environment - forcing AWS Secrets Manager")
            provider_type = "aws_sm"

        LOG.info(f"Creating secret provider: {provider_type}")

        # Prepare config
        if provider_type == "secure_file":
            config["master_key"] = config.get(
                DEFAULT_MASTER_KEY_ENV.casefold()
            ) or os.getenv(DEFAULT_MASTER_KEY_ENV)
            if not config["master_key"]:
                raise ValueError("secure_file provider requires master_key")
            config.setdefault(
                "encrypted_file_path", config.get("path", DEFAULT_ENCRYPTED_PATH)
            )

        elif provider_type == "local_file":
            config.setdefault("path", DEFAULT_SECRETS_PATH)

        # Instantiate
        provider_class = _PROVIDERS.get(provider_type)
        if not provider_class:
            raise ValueError(f"Unknown provider type: {provider_type}")

        cls._instance = provider_class(**config)
        return cls._instance
