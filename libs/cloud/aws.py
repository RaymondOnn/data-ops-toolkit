import logging
import os
import threading
from dataclasses import dataclass
from typing import Any

import boto3
from aiobotocore.session import get_session as get_aio_session
from botocore.credentials import RefreshableCredentials
from botocore.session import get_session

LOG = logging.getLogger(__name__)


@dataclass
class AWSConfig:
    """
    Configuration for AWS Infrastructure and identity.

    Ensures loud failures if mandatory connection parameters are missing or
    incorrectly paired.

    Attributes:
        region: AWS Region name (e.g., 'us-east-1').
        sts_endpoint_url: Optional custom endpoint for STS.
        role_arn: IAM Role ARN to assume via STS.
        profile_name: Named profile from local AWS credentials.
        aws_access_key_id: Explicit AWS Access Key.
        aws_secret_access_key: Explicit AWS Secret Key.
    """

    region: str
    sts_endpoint: str | None = None
    role_arn: str | None = None
    profile: str | None = None
    access_key: str | None = None
    secret_key: str | None = None

    def validate(self) -> None:
        """Validate credential configuration."""

        def normalize(v):
            if v is None:
                return None
            if isinstance(v, str):
                s = v.strip().lower()
                if s in ("null", "none", ""):
                    return None
            return v

        self.profile = normalize(self.profile)
        self.access_key = normalize(self.access_key)
        self.secret_key = normalize(self.secret_key)

        if self.access_key and not self.secret_key:
            raise ValueError("aws_access_key_id provided without aws_secret_access_key")
        if self.secret_key and not self.access_key:
            raise ValueError("aws_secret_access_key provided without aws_access_key_id")


class AWSClient:
    """Singleton AWS client with credential management."""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, config: AWSConfig | None = None):
        if hasattr(self, "_initialized"):
            return

        if not config:
            raise ValueError("AWSClient requires config")

        # Aggressively sanitize environment variables to prevent "null" string pollution.
        for var in [
            "AWS_PROFILE",
            "AWS_DEFAULT_PROFILE",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_DEFAULT_REGION",
        ]:
            val = os.environ.get(var)
            if val is not None and str(val).strip().lower() in ("null", "none", ""):
                LOG.debug(f"Unsetting poisoned env var: {var}='{val}'")
                del os.environ[var]

        config.validate()
        self.config = config

        # boto3.Session follows a prioritized fallback chain:
        # 1. Explicit parameters (aws_access_key_id, etc.)
        # 2. Environment variables (AWS_ACCESS_KEY_ID, etc.)
        # 3. Environment variable AWS_PROFILE (checks this even if profile_name=None)
        # 4. Shared credentials file (~/.aws/credentials)
        # 5. Shared config file (~/.aws/config)
        # 6. ECS/IAM Instance Role metadata
        # We pass explicit values only when they exist to allow natural fallback
        # to IAM Roles in cloud environments.
        self._base_session = boto3.Session(
            region_name=config.region,
            profile_name=config.profile,
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key,
        )
        self._session = None
        self._initialized = True

    def get_session(self, role_session_name: str = "IngestionEngine") -> boto3.Session:
        """Get AWS session (with STS assume-role if configured)."""
        if self._session:
            return self._session

        if not self.config.role_arn:
            self._session = self._base_session
            return self._session

        self._session = self._create_sts_session(
            self.config.role_arn, role_session_name
        )
        return self._session

    def get_async_session(self):
        """Get async session for aiobotocore."""
        session = get_aio_session()
        creds = self.get_session()._session.get_credentials()

        # Add missing method expected by s3fs
        if not hasattr(creds, "get_account_id"):
            creds.get_account_id = lambda: None

        session._credentials = creds
        session.set_config_variable("region", self.config.region)
        return session

    def get_client(self, service: str, endpoint: str | None = None) -> Any:
        """Get boto3 client for service."""
        session = self.get_session()
        return session.client(
            service, region_name=self.config.region, endpoint_url=endpoint
        )

    def _create_sts_session(self, role_arn: str, session_name: str) -> boto3.Session:
        """Create session with auto-refreshing STS credentials."""

        def refresh():
            LOG.info(f"Refreshing STS credentials for role {role_arn}")

            # Build STS client kwargs
            sts_kwargs = {
                "service_name": "sts",
                "region_name": self.config.region,
            }
            if self.config.sts_endpoint:
                sts_kwargs["endpoint_url"] = self.config.sts_endpoint

            sts = self._base_session.client(**sts_kwargs)
            resp = sts.assume_role(RoleArn=role_arn, RoleSessionName=session_name)
            creds = resp["Credentials"]

            return {
                "access_key": creds["AccessKeyId"],
                "secret_key": creds["SecretAccessKey"],
                "token": creds["SessionToken"],
                "expiry_time": creds["Expiration"].isoformat(),
            }

        refreshable = RefreshableCredentials.create_from_metadata(
            metadata=refresh(),
            refresh_using=refresh,
            method="sts-assume-role",
        )

        # Add missing method for compatibility
        if not hasattr(refreshable, "get_account_id"):
            refreshable.get_account_id = lambda: None

        bc_session = get_session()
        bc_session.set_config_variable("profile", None)
        bc_session._credentials = refreshable
        bc_session.set_config_variable("region", self.config.region)

        return boto3.Session(botocore_session=bc_session)

    def get_credentials(self) -> Any:
        """Get current credentials."""
        creds = self.get_session()._session.get_credentials()
        if not hasattr(creds, "get_account_id") or creds.get_account_id() is None:
            creds.get_account_id = lambda: "000000000000"
        return creds
