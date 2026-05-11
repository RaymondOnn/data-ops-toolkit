import logging
from typing import Any

import fsspec
from libs.file.base import FileSystemClient
from s3fs import S3FileSystem

LOG = logging.getLogger(__name__)


class S3Client(FileSystemClient):
    """
    S3 Driver for AWS, MinIO, or LocalStack.

    Storage Options:
        - key (str): AWS Access Key ID
        - password (str): AWS Secret Access Key
        - token (str): Temporary session token
        - client_kwargs (dict): e.g., {'region_name': 'us-east-1', 'endpoint_url': '...'}
        - config_kwargs (dict): e.g., {'retries': {'max_attempts': 10}}
        - default_fill_cache (bool): Set to False to prevent 2GB RAM spikes.
    """

    def __init__(self, url: str, storage_options: dict[str, Any] | None = None) -> None:
        super().__init__(url, storage_options)

    @property
    def fs(self) -> S3FileSystem:
        """Establishes the S3 connection using mapped credentials."""
        if not hasattr(self, "_fs") or self._fs is None:
            # Singleton access: Retrieves the master session configured at app startup
            from libs.cloud.aws import AWSClient, AWSClientConfig

            client_cfg = self.opts["client"]
            aws = AWSClient(
                config=AWSClientConfig(
                    region=client_cfg["region"],
                    sts_endpoint_url=client_cfg["sts_endpoint_url"],
                    role_arn=client_cfg["role_arn"],
                    profile_name=client_cfg.get("profile_name"),
                    aws_access_key_id=client_cfg.get("aws_access_key_id"),
                    # Allow 'password' as an alias for secret key
                    aws_secret_access_key=client_cfg.get("password"),  
                )
            )

            # Inject AWS identity from singleton if not explicitly provided.
            # This ensures S3 uses the shared session (and STS refresh logic)
            # without requiring secrets to be passed for IAM-based access.
            self.opts.setdefault("session", aws.get_session())
            self.opts.setdefault("region_name", aws.config.region)

            self._fs: S3FileSystem = fsspec.filesystem("s3", **self.opts)

        return self._fs
