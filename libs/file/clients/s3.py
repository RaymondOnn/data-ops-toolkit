import logging
from collections.abc import Generator
from pathlib import Path
from typing import Any

from libs.cloud.aws import AWSClient, AWSClientConfig
from libs.file.base import FileSystemClient
from s3fs import S3FileSystem

LOG = logging.getLogger(__name__)


class S3Client(FileSystemClient):
    """
    S3 Driver for AWS, MinIO, or LocalStack.

    Storage Options:
        key (str): AWS Access Key ID.
        password (str): AWS Secret Access Key.
        token (str): Temporary session token.
        client_kwargs (dict): e.g., {'region_name': 'ap-southeast-1', ...}.
        config_kwargs (dict): e.g., {'retries': {'max_attempts': 10}}.
        default_fill_cache (bool): Set to False to prevent 2GB RAM spikes.
    """

    def __init__(self, url: str, storage_options: dict[str, Any] | None = None) -> None:
        """
        Initializes the S3Client.

        Args:
            url: The base S3 URL (e.g., 's3://my-bucket').
            storage_options: Driver-specific configuration dictionary.
        """
        super().__init__(url, storage_options)

    @property
    def fs(self) -> S3FileSystem:
        """
        Establishes the S3 connection using mapped credentials.

        Returns:
            S3FileSystem: The underlying s3fs instance.
        """
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
                        "verify": False,
                    },
                    "config_kwargs": {
                        "s3": {
                            "signature_version": "s3v4",
                            "addressing_style": "path",
                        },  # Essential for LocalStack compatibility
                    },
                }
            )
            self._fs = S3FileSystem(
                key=creds.access_key,
                secret=creds.secret_key,
                token=creds.token,
                use_ssl=False,
                asynchronous=False,
                use_listings_cache=False,  # CRITICAL: Don't cache the 404
                client_kwargs={
                    "endpoint_url": s3_endpoint_url,
                    "region_name": aws.config.region,
                },
                config_kwargs={
                    "s3": {
                        "addressing_style": "path",  # Force http://host/bucket
                        "signature_version": "s3v4",
                    }
                },
            )
            # 4. Create the filesystem using the 's3' protocol
            # Type cast for IDE support if necessary
            LOG.debug(
                f"S3Client: Routing data to {s3_endpoint_url} using "
                f"region {aws.config.region}"
            )
            # self._fs = fsspec.filesystem("s3", **s3_opts)

        return self._fs

    def exists(self, path: str | Path) -> bool:
        """
        Checks if a path or bucket exists on S3.

        Includes a workaround for LocalStack NoSuchBucket errors for root-level
        bucket checks by using list_buckets.

        Args:
            path: The S3 path to check.

        Returns:
            bool: True if the path exists, False otherwise.
        """
        resolved = self.resolve_path(str(path))

        # Check if it's just the bucket root (s3://bucket-name)
        if resolved.count("/") == 2:  # "s3://bucket-name" has 2 slashes
            bucket_name = resolved.replace("s3://", "").rstrip("/")
            try:
                resp = self.fs.call_s3("list_buckets")
                bucket_names = [b["Name"] for b in resp.get("Buckets", [])]
                LOG.debug(f"Bucket check: {bucket_name} in {bucket_names}")
                return bucket_name in bucket_names
            except Exception as e:
                LOG.error(f"Failed to list buckets: {e}")
                return False

        # For files, use driver but catch the phantom 404
        try:
            return self.fs.exists(resolved)
        except Exception as e:
            error_msg = str(e)
            if "NoSuchBucket" in error_msg or "NoSuchKey" in error_msg:
                LOG.debug(f"Path doesn't exist (caught expected error): {resolved}")
                return False
            LOG.error(f"Unexpected error in exists(): {e}")
            raise

    def find(self, path: str, pattern: str = "*") -> Generator[str, None, None]:
        """
        Searches for files under a path matching a glob-like pattern.

        Args:
            path: The directory or prefix to search.
            pattern: Glob pattern to filter results (default '*').

        Yields:
            str: The full S3 path of matching objects.
        """
        resolved = self.resolve_path(path)
        # s3fs.find is optimized to use S3 Prefixes rather than recursive LS
        for p in self.fs.find(resolved):
            # Ensure protocol is attached for downstream processing
            full_path = self.fs.unstrip_protocol(p)
            if pattern == "*" or Path(full_path).match(pattern):
                yield full_path

    def cp(
        self, source: str, destination: str, recursive: bool = True, **kwargs
    ) -> None:
        """
        Copies files between local/S3 and S3/S3.

        Args:
            source: Source path (local or S3).
            destination: Destination S3 path.
            recursive: Whether to copy directories recursively.
            **kwargs: Additional options passed to s3fs.
        """
        src = self.resolve_path(source)
        dst = self.resolve_path(destination)

        # 1. Identify the Source Nature
        # If the source doesn't have s3://, it's a local file/folder.
        is_local_source = not src.startswith("s3://")

        # 2. Local-to-S3 Interaction (Upload)
        if is_local_source:
            LOG.info(f"S3Client: Inter-filesystem Upload -> {src} to {dst}")
            # s3fs.put() is optimized for local-to-s3 transfers
            return self.fs.put(src, dst, recursive=recursive, **kwargs)

        # 3. S3-to-S3 Interaction (Copy)
        LOG.info(f"S3Client: Intra-filesystem Copy -> {src} to {dst}")
        # Validate target bucket exists before expansion (The LocalStack fix)
        self._ensure_bucket_exists(dst)

        return self.fs.cp(src, dst, recursive=recursive, **kwargs)

    def ls(self, path: str = "", detail: bool = False) -> list[Any]:
        """
        Lists objects in an S3 directory.

        Args:
            path: The S3 path to list.
            detail: If True, returns full metadata for each object.

        Returns:
            list[Any]: A list of object names or dictionaries if detail=True.
        """
        return self.fs.ls(path, detail=detail)

    def open(self, path: str, mode: str = "rb") -> Any:
        """
        Opens an S3 object for reading or writing.

        Args:
            path: The S3 path to open.
            mode: Standard file opening mode (e.g., 'rb', 'wb').

        Returns:
            Any: An S3File object provided by s3fs.
        """
        return self.fs.open(path, mode=mode)

    def _ensure_bucket_exists(self, path: str) -> None:
        """
        Validates that the target bucket exists, creating it if missing.

        Useful for LocalStack scenarios where buckets aren't pre-provisioned.

        Args:
            path: An S3 path from which the bucket name is extracted.
        """
        bucket = path.replace("s3://", "").lstrip("/").split("/")[0]
        if not self.exists(f"s3://{bucket}"):
            LOG.info(f"S3Client: Creating missing bucket {bucket}")
            self.fs.mkdir(bucket)

    def rm(self, path: str, recursive: bool = False) -> None:
        """
        Deletes objects from S3.

        Args:
            path: The S3 path to delete.
            recursive: If True, deletes all objects under the prefix.
        """
        self.fs.rm(path, recursive=recursive)
