import logging
import threading
from dataclasses import dataclass
from typing import Any

import boto3
from botocore.credentials import RefreshableCredentials
from botocore.session import get_session

LOG = logging.getLogger(__name__)


@dataclass
class AWSClientConfig:
    """
    Strict configuration for AWS Infrastructure.
    Ensures loud failures if mandatory connection parameters are missing.
    """

    region: str
    sts_endpoint_url: str | None = None
    role_arn: str | None = None
    profile_name: str | None = None
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None

    def validate(self) -> None:
        """
        Validates that a valid identity is provided.
        Requirement: Either profile_name OR (access_key AND secret_key) must be present.
        """
        # Handle potential 'null' or empty strings from YAML expansion
        p_name = None if self.profile_name in (None, "null", "") else self.profile_name
        a_key = (
            None
            if self.aws_access_key_id in (None, "null", "")
            else self.aws_access_key_id
        )
        s_key = (
            None
            if self.aws_secret_access_key in (None, "null", "")
            else self.aws_secret_access_key
        )

        # Logic: If they attempt to provide keys, they must provide BOTH.
        # If both are missing/None, boto3 will naturally fall back to Environment Variables or IAM Roles.
        if a_key and not s_key:
            raise ValueError(
                "AWS Configuration Error: 'aws_access_key_id' provided without 'aws_secret_access_key'."
            )
        if s_key and not a_key:
            raise ValueError(
                "AWS Configuration Error: 'aws_secret_access_key' provided without 'aws_access_key_id'."
            )

        if p_name:
            LOG.debug(f"AWS Identity initialized via CLI Profile: {p_name}")
        elif a_key:
            LOG.debug("AWS Identity initialized via Explicit Access Keys")
        else:
            LOG.info("AWS Identity initialized via Default Credential Chain (Env/IAM)")


class AWSClient:
    """
    Singleton AWS Client that manages a unified boto3 session.
    Handles STS AssumeRole with self-refreshing credentials.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        config: AWSClientConfig | None = None,
    ):
        # Ensure init only runs once for the singleton
        if hasattr(self, "_initialized"):
            return

        if config is None:
            raise ValueError("AWSClient must be initialized with an AWSClientConfig.")

        # Enforce identity validation before creating the session
        config.validate()

        self.config = config

        # Sanitize inputs: Convert empty/null strings to None to allow
        # boto3's internal credential provider chain to function.
        profile = config.profile_name
        if profile in (None, "null", ""):
            profile = None

        access_key = config.aws_access_key_id
        if access_key in (None, "null", ""):
            access_key = None

        secret_key = config.aws_secret_access_key
        if secret_key in (None, "null", ""):
            secret_key = None

        self._base_session = boto3.Session(
            region_name=config.region,
            profile_name=profile,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        self._session = None
        self._initialized = True

    def get_session(self, session_name: str = "IngestionEngine") -> boto3.Session:
        """Returns the active session, initializing STS refresh if a role is provided."""
        if self._session:
            return self._session

        if not self.config.role_arn:
            self._session = self._base_session
            return self._session

        self._session = self._create_refreshable_session(
            self.config.role_arn, session_name
        )
        return self._session

    def _create_refreshable_session(
        self, role_arn: str, session_name: str
    ) -> boto3.Session:
        """
        Internal: Creates a botocore session that automatically refreshes its credentials
        via STS AssumeRole before they expire.
        """

        def refresh_credentials():
            """Internal method to trigger the STS assume_role call."""
            LOG.info("Refreshing temporary STS credentials", extra={"role": role_arn})
            sts = self._base_session.client(
                service_name="sts",
                region_name=self.config.region,
                endpoint_url=self.config.sts_endpoint_url,
            )

            response = sts.assume_role(RoleArn=role_arn, RoleSessionName=session_name)

            credentials = response["Credentials"]
            return {
                "access_key": credentials["AccessKeyId"],
                "secret_key": credentials["SecretAccessKey"],
                "token": credentials["SessionToken"],
                "expiry_time": credentials["Expiration"].isoformat(),
            }

        session_credentials = RefreshableCredentials.create_from_metadata(
            metadata=refresh_credentials(),
            refresh_using=refresh_credentials,
            method="sts-assume-role",
        )

        bc_session = get_session()
        bc_session._credentials = session_credentials
        bc_session.set_config_variable("region", self.config.region)

        return boto3.Session(botocore_session=bc_session)

    def get_client(self, service_name: str, endpoint_url: str | None = None) -> Any:
        """Returns a service-specific client (S3, SecretsManager, etc.) from the singleton session."""
        session = self.get_session()
        return session.client(
            service_name,
            region_name=self.config.region,
            endpoint_url=endpoint_url,
        )
