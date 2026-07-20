"""S3 filesystem client with AWS credential integration."""

import logging
from pathlib import Path
from typing import Any

from libs.cloud.aws import AWSClient, AWSConfig
from libs.file.base import FileSystemClient
from s3fs import S3FileSystem

LOG = logging.getLogger(__name__)


class S3Client(FileSystemClient):
    """
    S3 client for AWS, MinIO, or LocalStack.

    Storage Options:
        client (dict): AWS client configuration (region, role_arn, etc.)
        s3_endpoint_url (str): Custom endpoint URL for S3-compatible storage
        anon (bool): Whether to use anonymous access
        use_ssl (bool): Whether to use SSL (default: False for LocalStack)
    """

    def __init__(
        self,
        url: str,
        region: str,
        sts_endpoint_url: str,
        role_arn: str | None = None,
        profile: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        use_ssl: bool = False,
        anon: bool = False,
        s3_endpoint_url: str | None = None,
        **options: Any,
    ) -> None:
        """Initialize S3 client with storage options."""
        super().__init__(url, **options)
        self.region = region
        self.sts_endpoint_url = sts_endpoint_url
        self.role_arn = role_arn
        self.profile = profile
        self.access_key = access_key
        self.secret_key = secret_key
        self.anon = anon
        self.use_ssl = use_ssl
        self.s3_endpoint_url = s3_endpoint_url

    @property
    def fs(self) -> S3FileSystem:
        """Get the underlying S3FileSystem instance."""
        if self._fs is not None:
            return self._fs

        # Configure AWS client using explicit args or client config dict fallback
        aws = AWSClient(
            config=AWSConfig(
                region=self.region,
                sts_endpoint=self.sts_endpoint_url,
                role_arn=self.role_arn,
                profile=self.profile,
                access_key=self.access_key,
                secret_key=self.secret_key,
            )
        )

        # Get credentials from refreshable session
        creds = aws.get_credentials()

        # Create S3 filesystem
        self._fs = S3FileSystem(
            key=creds.access_key,
            secret=creds.secret_key,
            token=creds.token,
            anon=self.anon,
            asynchronous=False,
            use_ssl=self.use_ssl,
            use_listings_cache=False,  # Don't cache 404s
            client_kwargs={
                "endpoint_url": self.s3_endpoint_url,
                "region_name": aws.config.region,
            },
            config_kwargs={
                "s3": {
                    "addressing_style": "path",  # Required for LocalStack
                    "signature_version": "s3v4",
                }
            },
        )

        LOG.debug(
            f"S3Client initialized: endpoint={self.s3_endpoint_url}, region={aws.config.region}"
        )
        return self._fs

    def exists(self, path: str | Path) -> bool:
        """Check if bucket or object exists."""
        resolved = self.resolve(path)

        # Bucket-level check (s3://bucket-name)
        if resolved.count("/") == 2:
            bucket_name = resolved.replace("s3://", "").rstrip("/")
            try:
                resp = self.fs.call_s3("list_buckets")
                buckets = [b["Name"] for b in resp.get("Buckets", [])]
                return bucket_name in buckets
            except Exception:
                LOG.exception("Failed to list buckets")
                return False

        # Object-level check
        try:
            return self.fs.exists(resolved)
        except Exception as e:
            error_msg = str(e)
            if "NoSuchBucket" in error_msg or "NoSuchKey" in error_msg:
                LOG.debug(f"Path does not exist: {resolved}")
                return False
            LOG.exception("Unexpected error in exists()")
            raise

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _ensure_bucket(self, path: str) -> None:
        """Ensure bucket exists, create if missing (useful for LocalStack)."""
        bucket = path.replace("s3://", "").lstrip("/").split("/")[0]
        if not self.exists(f"s3://{bucket}"):
            LOG.info(f"Creating missing bucket: {bucket}")
            self.fs.mkdir(bucket)

    # =========================================================================
    # Commented Methods (Keep for reference in case base class doesn't work out)
    # =========================================================================

    # def find(self, path: str, pattern: str = "*") -> Generator[str, None, None]:
    #     """
    #     Searches for files under a path matching a glob-like pattern.
    #
    #     Args:
    #         path: The directory or prefix to search.
    #         pattern: Glob pattern to filter results (default '*').
    #
    #     Yields:
    #         str: The full S3 path of matching objects.
    #     """
    #     resolved = self.resolve(path)
    #     for p in self.fs.find(resolved):
    #         full_path = self.fs.unstrip_protocol(p)
    #         if pattern == "*" or Path(full_path).match(pattern):
    #             yield full_path

    # def cp(
    #     self, source: str, destination: str, recursive: bool = True, **kwargs
    # ) -> None:
    #     """
    #     Copies files between local/S3 and S3/S3.
    #
    #     Args:
    #         source: Source path (local or S3).
    #         destination: Destination S3 path.
    #         recursive: Whether to copy directories recursively.
    #         **kwargs: Additional options passed to s3fs.
    #     """
    #     src = self.resolve(source)
    #     dst = self.resolve(destination)
    #
    #     # 1. Identify the Source Nature
    #     is_local_source = not src.startswith("s3://")
    #
    #     # 2. Local-to-S3 Interaction (Upload)
    #     if is_local_source:
    #         LOG.info(f"S3Client: Upload -> {src} to {dst}")
    #         return self.fs.put(src, dst, recursive=recursive, **kwargs)
    #
    #     # 3. S3-to-S3 Interaction (Copy)
    #     LOG.info(f"S3Client: Copy -> {src} to {dst}")
    #     self._ensure_bucket(dst)
    #     return self.fs.cp(src, dst, recursive=recursive, **kwargs)

    # def _transfer_via_local(self, src: str, dst: str, recursive: bool) -> None:
    #     """
    #     Transfer remote to remote via local temporary storage.
    #     (Fallback when cross-provider copy is not supported)
    #     """
    #     import tempfile
    #
    #     with tempfile.TemporaryDirectory() as tmpdir:
    #         self.fs.get(src, tmpdir, recursive=recursive)
    #         self.fs.put(tmpdir, dst, recursive=recursive)
