import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import structlog
from libs.clients.base import ClientCantConnect
from libs.file import FileSystemClient, FileSystemSkills, FormatFactory
from libs.resilience.circuit_breaker import CircuitBreaker

from .base import ArchiveMixin, Service, SinkMixin, SourceMixin
from .factory import ServiceFactory
from .registry import protect_service

if TYPE_CHECKING:
    from libs.auth.models import Secret
    from libs.file.formats.base import FormatHandler

LOG = structlog.get_logger(__name__)

breaker = CircuitBreaker(
    failure_threshold=3,
    recovery_timeout=300,
    expected_exceptions=(ClientCantConnect, ConnectionError, TimeoutError),
)


class BaseStorageService(Service):
    """
    Registry-aware Service that manages any Filesystem.
    """

    def __init__(
        self,
        name: str,
        url: str,
        capabilities: list[FileSystemSkills],
        storage_options: dict[str, Any],
        **config: Any,
    ) -> None:

        super().__init__(name, **config)
        self.url = url
        # The 'url' parameter represents the base folder/directory (or bucket root).
        # For local files, this should be a directory path like 'file:///tmp/data'
        # or an absolute path '/tmp/data'.
        self.capabilities = capabilities
        self.opts = storage_options
        self.client = self._init_client(**config)

    def _init_client(self, **config: Any) -> FileSystemClient:
        from libs.file.base import create_fs_client

        # 1. Resolve Secret (Password/Keys)
        # Assuming 'password' is the Secret object containing S3/Azure keys
        if config.get("password") is not None:
            secret: Secret = config["password"]
            config["password"] = secret.resolve(sanitize=True) if secret else {}

        # 2. Merge Credentials with Storage Options
        # Patch: Create a new dict instead of using .update() which returns None
        merged_opts = {**self.opts, **config}

        return create_fs_client(
            url=self.url, capabilities=self.capabilities, storage_options=merged_opts
        )


class StorageSource(BaseStorageService, SourceMixin):
    @protect_service(breaker)
    def get_work_units(self, target: str, num_partitions: int) -> list[dict[str, Any]]:
        """
        Uses the internal client to split 50M rows.
        Works across S3, Azure, GCS, or Local.
        """
        if not getattr(self.client, "fs", None):
            raise RuntimeError("Filesystem client is not initialized")

        # Use the discovery utility to handle file vs folder automatically
        files = list(self.client.walk_paths(target))

        LOG.debug(
            "Generating work units",
            target=target,
            files_found=len(files),
            partitions=num_partitions,
        )
        return [{"files": files[i::num_partitions]} for i in range(num_partitions)]

    @protect_service(breaker)
    def fetch_data(self, unit: list[str] | str) -> pl.DataFrame:
        """
        Reads a list of files (the work unit) into a single Polars DataFrame.
        Supports Parquet, CSV, and JSON formats.
        """
        if not unit:
            return pl.DataFrame()

        # Handle both single path strings and lists of paths
        paths = [unit] if isinstance(unit, str) else unit

        # Resolve paths via the client (handles file:// vs s3:// etc)
        resolved_paths = [self.client.resolve_path(p) for p in paths]

        # 1. Determine format from the first file to select the handler
        ext = Path(resolved_paths[0]).suffix.lstrip(".").lower()
        handler: FormatHandler = FormatFactory.get_handler(
            ext, self.client.fs, self.opts
        )

        # 2. Iterate and fetch individually (supports per-file repairs/cleaning)
        dfs = [handler.to_df(p) for p in resolved_paths]

        if not dfs:
            return pl.DataFrame()

        return pl.concat(dfs)


class StorageSink(BaseStorageService, SinkMixin):
    @protect_service(breaker)
    def stage_data(self, source_dir: Path, target_table: str) -> tuple[str, int]:
        """
        Phase 1: Organize Parquet files into a staging directory.
        Returns a tuple of (path to the staged folder, number of items loaded).
        """
        # Create a unique staging path: e.g., tmp/staging/orders_1710123456/
        staging_path = f"tmp/staging/{target_table}_{int(time.time())}"

        # We use the client's 'copy_dir' which should be a server-side
        # operation (S3-to-S3) to avoid pulling 50M rows into our 2GB RAM.
        self.client.copy_dir(source_dir, staging_path)

        staging_full_path = self.client.resolve_path(staging_path)
        files_staged = len(self.client.fs.find(staging_full_path))

        LOG.info(
            "Staged files",
            source=str(source_dir),
            target=staging_path,
            count=files_staged,
        )
        return staging_path, files_staged

    @protect_service(breaker)
    def promote_data(
        self,
        staging_table: str,
        target_table: str,
        partition_col: str,
        partition_val: str,
    ) -> None:
        """
        Phase 2: Idempotent swap for File Systems.
        Equivalent to 'REPLACE PARTITION'.
        """
        # Target path: e.g., data/orders/dt=2026-03-10/
        final_path = f"{target_table}/{partition_col}={partition_val}"

        # 1. Idempotency: Remove existing data for this partition
        if self.client.exists(final_path):
            self.client.delete_dir(final_path)

        # 2. Atomic Move: Move the staged folder to the production path
        # On S3, this is a metadata-only rename or a fast copy/delete
        self.client.move_dir(staging_table, final_path)
        LOG.info("Promoted data", staging=staging_table, final=final_path)

    @protect_service(breaker)
    def is_equal(
        self,
        reference: Path,
        other: Path,
        exclude_columns: list[str] | None = None,
    ) -> None:
        # Check if the number of files is the same

        def get_meta(path):
            # Ensure trailing slash for accurate relative path slicing
            root = path.rstrip("/") + "/"
            # find() handles recursion automatically
            data = self.client.fs.find(root, detail=True)
            # Store {relative_path: (size, ETag)}
            return {k[len(root) :]: (v["size"], v.get("ETag")) for k, v in data.items()}

        meta_reference = get_meta(reference)
        meta_other = get_meta(other)

        # 1. Check for missing files
        only_in_reference = set(meta_reference) - set(meta_other)
        only_in_other = set(meta_other) - set(meta_reference)

        # 2. Check for content differences in common files
        common_keys = set(meta_reference) & set(meta_other)
        mismatched = [k for k in common_keys if meta_reference[k] != meta_other[k]]

        # Reporting
        if not only_in_reference and not only_in_other and not mismatched:
            LOG.info("Folders are identical", match=True)
            return

        if only_in_reference:
            LOG.error("Validation mismatch", missing_in_target=list(only_in_reference))
        if only_in_other:
            LOG.error("Validation mismatch", extra_in_target=list(only_in_other))
        if mismatched:
            LOG.error("Content mismatch", files=mismatched)

    @protect_service(breaker)
    def clone(self, reference: str, other: str) -> None:
        self.client.copy_dir(reference, other)


class StorageArchive(BaseStorageService, ArchiveMixin):
    @protect_service(breaker)
    def archive_data(self, source_dir: Path, archive_path: str) -> None:
        """
        Archive data to the destination path.
        """
        self.client.copy_dir(source_dir, archive_path)


# --- Role 1: Reading Flat Files (Landing Zone) ---
@ServiceFactory.register("flat_file")
class FlatFileService(StorageSource):
    """Specifically for reading source data from landing zones."""

    def __init__(self, name: str, **config: Any) -> None:
        url = config.pop("url", "")
        storage_options = config.pop("storage_options", {})

        super().__init__(
            name=name,
            url=url,
            capabilities=[FileSystemSkills.FILE],
            storage_options=storage_options,
            **config,
        )


# --- Role 2: Standard Archival (Moving artifacts) ---
@ServiceFactory.register("standard_archive")
class StandardArchiveService(StorageArchive):
    """Standard archival for job artifacts and logs."""

    def __init__(self, name: str, **config: Any) -> None:
        url = config.pop("url", "s3://archive-bucket")
        storage_options = config.pop("storage_options", {})

        super().__init__(
            name=name,
            url=url,
            capabilities=[FileSystemSkills.ARCHIVE],
            storage_options=storage_options,
            **config,
        )


# --- Role 3: CAS Archival (Immutable/Hashed storage) ---
@ServiceFactory.register("cas_archive")
class CASArchive(StorageArchive):
    """Content Addressable Storage for immutable records."""

    def __init__(self, name: str, **config: Any) -> None:
        url = config.pop("url", "s3://cas-vault")
        storage_options = config.pop("storage_options", {"s3_storage_class": "GLACIER"})

        # Vaults often use specific storage classes (e.g., Glacier or WORM)
        super().__init__(
            name=name,
            url=url,
            capabilities=[FileSystemSkills.CAS],
            storage_options=storage_options,
            **config,
        )


# --- Role 4: Data Lake (Source and Sink) ---
@ServiceFactory.register("data_lake")
class DataLakeService(StorageSource, StorageSink):
    """General purpose S3/Azure Blob for reading and writing."""

    def __init__(self, name: str, **config: Any) -> None:
        url = config.pop("url", "s3://data-lake")
        storage_options = config.pop("storage_options", {})

        super().__init__(
            name=name,
            url=url,
            capabilities=[FileSystemSkills.FILE],
            storage_options=storage_options,
            **config,
        )
