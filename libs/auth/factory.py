# """Secret provider factory with environment-aware selection."""

# import logging
# import os

# from .provider import ProviderRegistry, SecretProvider

# LOG = logging.getLogger(__name__)

# DEFAULT_SECRETS_PATH = ".secrets.json"
# DEFAULT_ENCRYPTED_PATH = ".secrets.enc"
# DEFAULT_MASTER_KEY_ENV = "MASTER_KEY"


# class AuthFactory:
#     """Factory for creating secret providers."""

#     _instance: SecretProvider | None = None

#     @classmethod
#     def get_provider(cls, **config) -> SecretProvider:
#         """Get or create a secret provider singleton."""
#         if cls._instance:
#             return cls._instance

#         provider_type = config["key"].strip().casefold()
#         LOG.info(f"Creating secret provider: {provider_type}")

#         if provider_type not in ProviderRegistry:
#             raise ValueError(
#                 f"Unknown provider type: '{provider_type}'. "
#                 f"Available providers: {ProviderRegistry.keys()}"
#             )

#         provider_class = SecretProvider.get_provider_cls(provider_type)

#         # Prepare kwarg payload based on type
#         match provider_type:
#             case "secure_file":
#                 master_key = config.get(DEFAULT_MASTER_KEY_ENV.casefold()) or os.getenv(
#                     DEFAULT_MASTER_KEY_ENV
#                 )
#                 if not master_key:
#                     raise ValueError("secure_file provider requires master_key")

#                 provider_kwargs = {
#                     "secrets_json": config["secrets_json"],
#                     "master_key": master_key,
#                 }

#             case "local_file":
#                 provider_kwargs = {
#                     "secrets_json": config["secrets_json"],
#                 }
#             case "aws_sm":
#                 provider_kwargs = {
#                     "region": config["region"],
#                     "sm_endpoint_url": config["sm_endpoint_url"],
#                     "sts_endpoint_url": config["sts_endpoint_url"],
#                     "role_arn": config["role_arn"],
#                     "profile": config.get("profile"),
#                     "aws_access_key_id": config.get("aws_access_key_id"),
#                     "aws_secret_access_key": config.get("aws_secret_access_key"),
#                 }
#             case _:
#                 provider_kwargs = config

#         cls._instance = provider_class(**provider_kwargs)
#         return cls._instance
