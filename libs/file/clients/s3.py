import logging
import time
from typing import Any

import fsspec
from s3fs import S3FileSystem

from libs.file.base import FileSystemClient

LOG = logging.getLogger(__name__)


class S3Client(FileSystemClient):
    """
    S3 Driver for AWS, MinIO, or LocalStack.

    Storage Options:
        - key (str): AWS Access Key ID
        - secret (str): AWS Secret Access Key
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
            # Map generic 'password' to S3-specific 'secret'
            if "password" in self.opts:
                self.opts["secret"] = self.opts.pop("password")

            # Abstracting Mocking logic:
            # If an endpoint_url is provided (Moto server or internal mock),
            # ensure the filesystem is configured for it.
            if "endpoint_url" in self.opts:
                self.opts.setdefault("use_ssl", False)
                self.opts.setdefault("anon", False)

            self._fs: S3FileSystem = fsspec.filesystem("s3", **self.opts)

            # If we are mocking, ensure the bucket exists (Moto starts empty)
            if self.opts.get("use_mock"):
                bucket = self.url.split("://")[-1].split("/")[0]
                if not self._fs.exists(bucket):
                    LOG.debug("Moto/Mock detected: Pre-creating bucket", bucket=bucket)
                    self._fs.mkdir(bucket)

        return self._fs

    def reconnect(self, max_retries: int = 3) -> None:
        """Exponential backoff for long 50M row streams."""

        for i in range(max_retries):
            try:
                self._fs = None  # Force clear
                return
            except Exception:
                if i == max_retries - 1:
                    raise
                time.sleep(2**i)
