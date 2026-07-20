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
    def get_provider(cls, **config) -> SecretProvider:
        """Get or create a secret provider singleton."""
        if cls._instance:
            return cls._instance

        provider_type = config["key"].strip().casefold()
        LOG.info(f"Creating secret provider: {provider_type}")

        # Prepare config
        match provider_type:
            case "secure_file":
                master_key = config.get(DEFAULT_MASTER_KEY_ENV.casefold()) or os.getenv(
                    DEFAULT_MASTER_KEY_ENV
                )
                if master_key:
                    raise ValueError("secure_file provider requires master_key")

                config = {
                    "secrets_json": config["secrets_json"],
                    "master_key": master_key,
                }

            case "local_file":
                config = {
                    "secrets_json": config["secrets_json"],
                }
            case "aws_sm":
                config = {
                    "region": config["region"],
                    "sm_endpoint_url": config["sm_endpoint_url"],
                    "sts_endpoint_url": config["sts_endpoint_url"],
                    "role_arn": config["role_arn"],
                    "profile": config["profile"],
                    "aws_access_key_id": config["aws_access_key_id"],
                    "aws_secret_access_key": config["aws_secret_access_key"],
                }
            case _:
                raise ValueError(f"Unknown provider type: {provider_type}")

        # Instantiate
        provider_class = _PROVIDERS.get(provider_type)
        if not provider_class:
            raise ValueError("Unable to find matching provider type.")

        return provider_class(**config)
