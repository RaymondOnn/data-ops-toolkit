import logging
import os

from .provider import (
    AWSSecretProvider,
    LocalEncryptedProvider,
    LocalSecretProvider,
    SecretProvider,
)

LOG = logging.getLogger(__name__)
MASTER_KEY_ENV_VAR = "MASTER_KEY"

class AuthFactory:
    _provider: SecretProvider | None = None

    @classmethod
    def get_provider(cls, env: str, **config) -> SecretProvider:
        """
        Singleton provider based on environment.
        """
        if cls._provider is None:
            # Check for a 'STAGE' or 'ENV' variable
            env = env or os.getenv("APP_ENV", "dev").lower()
            provider_type = config.get("type", "env")

            if env == "prod":
                # AWSSecretProvider from your secret.py
                cls._provider = AWSSecretProvider(**config)

            if provider_type == "local_encrypted":
                # Resolves master key from env if the value provided is an env var name
                master_key = os.getenv(MASTER_KEY_ENV_VAR, config.get("master_key"))
                if not master_key:
                    LOG.info("Master key required for LocalEncryptedProvider.")
                    # LocalSecretProvider from your secret.py
                cls._provider = LocalEncryptedProvider(
                    encrypted_file_path=config.get("path", "./.secrets.json"),
                    master_key=str(master_key)
                )

            if provider_type == "local":
                # LocalSecretProvider from your secret.py
                cls._provider = LocalSecretProvider()
                
            # If no provider was set by this point, inform the user
            
            if cls._provider is None:
                raise ValueError(
                    f"No valid provider configuration found: {env}, {config}"
                )

        return cls._provider
