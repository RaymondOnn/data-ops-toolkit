import logging

import boto3
from botocore.credentials import RefreshableCredentials
from botocore.session import get_session

LOG = logging.getLogger(__name__)


class AWSSessionManager:
    """
    Handles enterprise AWS connectivity, specifically assuming roles via STS
    to obtain temporary, auto-refreshing credentials.
    """

    def __init__(self, region: str = "us-east-1"):
        self.region = region
        self._base_session = boto3.Session(region_name=region)

    def get_assumed_role_session(
        self, role_arn: str, session_name: str = "TaskManager"
    ) -> boto3.Session:
        """
        Returns a boto3.Session that automatically refreshes its credentials
        via STS AssumeRole before they expire.
        """

        def refresh_credentials():
            """Internal method to trigger the STS assume_role call."""
            LOG.info("Refreshing temporary STS credentials", extra={"role": role_arn})
            sts = self._base_session.client("sts")

            response = sts.assume_role(RoleArn=role_arn, RoleSessionName=session_name)

            credentials = response["Credentials"]
            return {
                "access_key": credentials["AccessKeyId"],
                "secret_key": credentials["SecretAccessKey"],
                "token": credentials["SessionToken"],
                "expiry_time": credentials["Expiration"].isoformat(),
            }

        # 1. Create a refreshable credential object
        session_credentials = RefreshableCredentials.create_from_metadata(
            metadata=refresh_credentials(),
            refresh_using=refresh_credentials,
            method="sts-assume-role",
        )

        # 2. Attach these credentials to a botocore session
        bc_session = get_session()
        bc_session._credentials = session_credentials
        bc_session.set_config_variable("region", self.region)

        # 3. Build a standard boto3.Session from the botocore session
        # This session can now be used indefinitely as it refreshes itself.
        return boto3.Session(botocore_session=bc_session)
