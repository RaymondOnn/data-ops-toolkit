import logging
import os
from typing import ClassVar

from .provider import (
    AWSSecretProvider,  # Ensure AWSSecretProvider is imported for direct instantiation
)
from .provider import LocalEncryptedProvider, LocalSecretProvider, SecretProvider

LOG = logging.getLogger(__name__)
MASTER_KEY_ENV_VAR = "MASTER_KEY"


class AuthFactory:
    _provider: SecretProvider | None = None

    _STRATEGIES: ClassVar[dict[str, type]] = {
        "env_file": LocalSecretProvider,
        "file_encrypted": LocalEncryptedProvider,
        "aws_manager": AWSSecretProvider,
    }

    @classmethod
    def get_provider(cls, env: str, **config) -> SecretProvider:
        """
        Singleton provider based on environment.
        """
        if cls._provider:
            return cls._provider

        env = env or os.getenv("APP_ENV", "dev").lower()

        # 1. Determine Provider Type
        provider_type = config.get("type", "env_file").strip().casefold()

        # Override for production
        if env == "prod":
            LOG.info("Production environment detected. Forcing 'aws_manager' provider.")
            provider_type = "aws_manager"

        LOG.info("Configuring auth provider", extra={"type": provider_type, "env": env})

        # 2. Pre-flight Validation (Logic happens before Init)
        if provider_type == "file_encrypted":
            config["master_key"] = os.getenv(
                MASTER_KEY_ENV_VAR, config.get("master_key")
            )
            if not config.get("master_key"):
                raise ValueError(
                    f"Provider '{provider_type}' requires a master_key (config or {MASTER_KEY_ENV_VAR} env var)"
                )
            # Ensure we have a path for the encrypted file
            config.setdefault(
                "encrypted_file_path", config.get("path", "./.secrets.json")
            )

        elif provider_type == "aws_manager":
            # Ensure AWSSecretProvider is imported for direct instantiation
            from libs.cloud.aws import AWSSessionManager

            if "region" not in config:
                # Fallback or error
                config["region"] = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
                LOG.debug(
                    "AWS Region not in config, using default",
                    extra={"region": config["region"]},
                )

            session = None
            if role_arn := config.get("role_arn"):
                LOG.info(
                    "Role ARN provided, initializing AWSSessionManager for STS",
                    extra={"role_arn": role_arn},
                )
                session_manager = AWSSessionManager(region=config["region"])
                session = session_manager.get_assumed_role_session(role_arn)

            cls._provider = AWSSecretProvider(session=session, **config)
            return cls._provider

        elif provider_type == "env_file":
            # Ensure we have a fallback path if none provided
            config.setdefault("path", "./.secrets.json")

        # 3. Dictionary Dispatch
        provider_class = cls._STRATEGIES.get(provider_type)
        if not provider_class:
            raise ValueError(
                f"Unsupported provider type: '{provider_type}'. "
                f"Available: {list(cls._STRATEGIES.keys())}"
            )

        # 4. Instantiate Singleton
        LOG.debug(
            "Instantiating provider class", extra={"cls": provider_class.__name__}
        )
        cls._provider = provider_class(
            **config
        )  # This will now be skipped if AWSSecretProvider was already instantiated above

        if not cls._provider:
            raise ValueError(
                f"Failed to instantiate provider class: {provider_class.__name__}"
            )

        return cls._provider
