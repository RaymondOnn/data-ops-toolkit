import os
from typing import Optional

from .provider import (
    SecretProvider,
    LocalSecretProvider,
    AWSSecretProvider,
)


class AuthFactory:
    _provider: Optional[SecretProvider] = None

    @classmethod
    def get_provider(cls) -> SecretProvider:
        """
        Singleton provider based on environment.
        """
        if cls._provider is None:
            # Check for a 'STAGE' or 'ENV' variable
            env = os.getenv("APP_ENV", "dev").lower()

            if env == "prod":
                # AWSSecretProvider from your secret.py
                cls._provider = AWSSecretProvider(region="ap-southeast-1")
            else:
                # LocalSecretProvider from your secret.py
                cls._provider = LocalSecretProvider()

        return cls._provider
