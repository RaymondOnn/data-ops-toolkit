import logging
from typing import TYPE_CHECKING, Any

import fsspec
from libs.cloud.aws import AWSClient, AWSClientConfig
from libs.file.base import FileSystemClient
from s3fs import S3FileSystem

# if TYPE_CHECKING:

LOG = logging.getLogger(__name__)


class S3Client(FileSystemClient):
    """
    S3 Driver for AWS, MinIO, or LocalStack.

    Storage Options:
        - key (str): AWS Access Key ID
        - password (str): AWS Secret Access Key
        - token (str): Temporary session token
        - client_kwargs (dict): e.g., {'region_name': 'ap-southeast-1', 'endpoint_url': '...'}
        - config_kwargs (dict): e.g., {'retries': {'max_attempts': 10}}
        - default_fill_cache (bool): Set to False to prevent 2GB RAM spikes.
    """

    def __init__(self, url: str, storage_options: dict[str, Any] | None = None) -> None:
        super().__init__(url, storage_options)
        print(f"Initialized S3Client with URL: {url}")

    @property
    def fs(self) -> "S3FileSystem":
        """Establishes the S3 connection using mapped credentials."""

        if not hasattr(self, "_fs") or self._fs is None:
            client_cfg = self.opts.pop("client")
            
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
            )  # your existing config

            # 2. Get the actual credentials from your refreshable session
            creds = aws.get_current_credentials()

            # Configure s3fs options
            s3_endpoint_url = self.opts.pop("s3_endpoint_url")
            s3_opts = self.opts.copy()
            s3_opts.update(
                {
                    "key": creds.access_key,
                    "secret": creds.secret_key,
                    "token": creds.token,
                    "asynchronous": False,  # Crucial: Stops s3fs from seeking __aenter__
                    "anon": self.opts.get("anon", False),
                    "use_listings_cache": False,
                    "client_kwargs": {
                        "region_name": aws.config.region,
                        "endpoint_url": s3_endpoint_url,
                        "use_ssl": self.opts.get("use_ssl", False),
                        "verify": False
                    },
                    "config_kwargs": {
                        "s3": {
                            "signature_version": "s3v4",
                            "addressing_style": "path"
                        },  # Essential for LocalStack compatibility
                    },
                }
            )
            print(f"S3 Client Config: {s3_opts}")
            # self._fs = S3FileSystem(
            #     key=creds.access_key,
            #     secret=creds.secret_key,
            #     token=creds.token,
            #     use_ssl=False,
            #     asynchronous=False,
            #     use_listings_cache=False,  # CRITICAL: Don't cache the 404
            #     client_kwargs={
            #         "endpoint_url": s3_endpoint_url,
            #         "region_name": aws.config.region,
            #     },
            #     config_kwargs={
            #         "s3": {
            #             "addressing_style": "path", # Force http://host/bucket
            #             "signature_version": "s3v4"
            #         }
            #     }
            # )
            # 4. Create the filesystem using the 's3' protocol
            # Type cast for IDE support if necessary
            LOG.debug(f"S3Client: Routing data to {s3_endpoint_url} using region {aws.config.region}")
            self._fs = fsspec.filesystem("s3", **s3_opts)

        return self._fs
