import logging
import threading
from dataclasses import dataclass
from typing import Any

import boto3
from aiobotocore.session import get_session as get_aio_session
from botocore.credentials import RefreshableCredentials
from botocore.session import get_session

LOG = logging.getLogger(__name__)


@dataclass
class AWSClientConfig:
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
    sts_endpoint_url: str | None = None
    role_arn: str | None = None
    profile_name: str | None = None
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None

    def validate(self) -> None:
        """
        Validates that a valid identity combination is provided.

        Requirement: Either profile_name OR (access_key AND secret_key)
        must be present.

        Raises:
            ValueError: If an incomplete set of access keys is provided.
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
        # If both are missing/None, boto3 will naturally fall back to
        # Environment Variables or IAM Roles.
        if a_key and not s_key:
            raise ValueError(
                "AWS Configuration Error: "
                "'aws_access_key_id' provided without 'aws_secret_access_key'."
            )
        if s_key and not a_key:
            raise ValueError(
                "AWS Configuration Error: "
                "'aws_secret_access_key' provided without 'aws_access_key_id'."
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
        """Implements the Singleton pattern using a thread-safe lock."""
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        config: AWSClientConfig | None = None,
    ):
        """
        Initializes the AWSClient singleton.

        Args:
            config: Strictly typed configuration object.

        Raises:
            ValueError: If the config object is not provided.
        """
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
        """
        Returns the active session, initializing STS refresh if a role is set.

        Args:
            session_name: Logical identifier for the STS session.

        Returns:
            boto3.Session: An initialized and potentially refreshable session.
        """
        if self._session:
            return self._session

        if not self.config.role_arn:
            self._session = self._base_session
            return self._session

        self._session = self._create_refreshable_session(
            self.config.role_arn, session_name
        )
        return self._session

    def get_async_session(self):
        """
        Returns an aiobotocore session for async libraries like s3fs.

        Returns:
            AioSession: An asynchronous AWS session.
        """
        aio_session = get_aio_session()

        # Get the credentials object from your existing sync session
        sync_session = self.get_session()
        creds = sync_session._session.get_credentials()

        # Patch the missing method s3fs expects (as we discussed)
        if not hasattr(creds, "get_account_id"):
            creds.get_account_id = lambda: None

        # Inject the credentials into the async session
        aio_session._credentials = creds
        aio_session.set_config_variable("region", self.config.region)

        return aio_session

    def _create_refreshable_session(
        self, role_arn: str, session_name: str
    ) -> boto3.Session:
        """
        Creates a botocore session that automatically refreshes credentials.

        Uses STS AssumeRole to periodically rotate tokens in the background.

        Args:
            role_arn: The full IAM Role ARN to assume.
            session_name: The name used for the assumed role session.

        Returns:
            boto3.Session: A session object containing refresh logic.
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

        # s3fs/aiobotocore expects this method to exist on the credentials object
        if not hasattr(session_credentials, "get_account_id"):
            session_credentials.get_account_id = lambda: None

        bc_session = get_session()
        bc_session.set_config_variable("profile", None)
        bc_session._credentials = session_credentials
        bc_session.set_config_variable("region", self.config.region)

        return boto3.Session(botocore_session=bc_session)

    def get_client(self, service_name: str, endpoint_url: str | None = None) -> Any:
        """
        Returns a service-specific client from the singleton session.

        Args:
            service_name: Name of the AWS service (e.g., 's3', 'sts').
            endpoint_url: Optional override for the service endpoint.

        Returns:
            Any: A configured boto3 client instance.
        """
        session = self.get_session()
        return session.client(
            service_name,
            region_name=self.config.region,
            endpoint_url=endpoint_url,
        )

    def get_current_credentials(self) -> Any:
        """
        Returns the raw credentials object from the active session.

        Returns:
            Any: The botocore credentials instance.
        """
        creds = self.get_session()._session.get_credentials()

        # LocalStack fix: Force the account ID to 0s
        # S3FS/Botocore uses this to resolve the bucket owner
        if not hasattr(creds, "get_account_id") or creds.get_account_id() is None:
            creds.get_account_id = lambda: "000000000000"

        return creds
