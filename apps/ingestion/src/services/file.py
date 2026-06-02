"""
Filesystem-based Service Layer.

This module provides the orchestration logic for interacting with various
storage backends (S3, local, etc.) by bridging the application's Service
Registry with the lower-level FileSystemClient and its capability Mixins.
"""

import time
from functools import cached_property
from pathlib import Path
from typing import Any, cast

import polars as pl
from libs.auth.secret import Secret
from libs.clients.base import ClientCantConnect  # Unused import
from libs.file import FileSystemSkills
from libs.file.base import FileSystemClient
from libs.resilience.circuit_breaker import CircuitBreaker
from libs.utils.dict import find_keys_by_pattern, update_nested_key
from loguru import logger
from upath import UPath

from .base import Archive, Service, Sink, Source
from .factory import ServiceFactory
from .registry import protect_service

LOG = logger
MIN_BLOCK_SIZE_BYTES = (
    50 * 1024 * 1024
)  # minimum size for a single Ray task to justify parallelism

breaker = CircuitBreaker(
    failure_threshold=3,
    recovery_timeout=300,
    expected_exceptions=(ClientCantConnect, ConnectionError, TimeoutError),
)


class BaseStorageService(Service):
    """Registry-aware Service that manages any Filesystem backend."""

    def __init__(
        self,
        name: str,
        url: str,
        capabilities: set[FileSystemSkills],
        storage_options: dict[str, Any],
        **config: Any,
    ) -> None:
        """
        Initializes the storage service.

        Args:
            name: The unique identifier for the service instance.
            url: The base folder or bucket root.
                e.g., 's3://bucket' or '/tmp/data'.
            capabilities: A set of Skills (Mixins) to inject into the
                underlying client.
            storage_options: Driver-specific connection parameters.
            **config: Additional configuration including credentials
                (resolved via Secret objects).

        Decision: Dynamic Capability Injection.
        By passing a set of 'FileSystemSkills' (Mixins) to the underlying
        client factory, we allow the same service logic to gain new powers
        (like CAS or Archive virtualization) purely through configuration
        without altering the class hierarchy.
        """
        super().__init__(name, **config)
        self.url = url
        # The 'url' parameter represents the base folder/directory (or bucket root).
        # For local files, this should be a directory path like 'file:///tmp/data'
        # or an absolute path '/tmp/data'.
        self.capabilities = capabilities
        self.opts = storage_options
        self._config = config

    @cached_property
    def client(self) -> FileSystemClient:
        """
        Lazily initializes the protocol-aware FileSystemClient.

        Returns:
            FileSystemClient: An instance of a managed filesystem driver.

        Decision: Late Binding.
        We initialize the client lazily to ensure that when this service
        object is serialized and sent to Ray workers, the connection
        handles are recreated fresh within the worker's process space.
        """
        from libs.file.base import create_fs_client

        config = self._config.copy()

        # 1. Resolve any Secret objects in the config
        for path, value in find_keys_by_pattern(
            config, pattern="secret", ignore_case=True
        ):
            if isinstance(value, Secret):
                update_nested_key(
                    data=config,
                    path=path,
                    new_key="password",
                    new_value=value.resolve(sanitize=True),
                )

        # 2. Merge Credentials with Storage Options
        merged_opts = {**self.opts, **config}

        return create_fs_client(
            url=self.url, capabilities=self.capabilities, storage_options=merged_opts
        )

    def reset_client(self) -> None:
        """Invalidates the cached FileSystemClient.

        Decision: State Recovery.
        Manual invalidation allows the Circuit Breaker to force a full
        re-initialization of the driver if a network pipe is permanently
        broken, rather than retrying on a stale handle.
        """
        if "client" in self.__dict__:
            LOG.warning(f"Resetting filesystem client for service: {self.name}")
            self.__dict__.pop("client", None)


class StorageSource(BaseStorageService, Source):
    """Implementation of data extraction logic for file-based sources."""

    def get_total_count(self, target, filter_condition=None, **kwargs) -> int:
        """Discovers files and aggregates row counts to assist with scaling.

        Args:
            target: The resource identifier (directory or path).
            filter_condition: Optional glob pattern.
            **kwargs: Includes 'archive_path' for virtualized counts.

        Returns:
            int: The total row count (exact or estimated).

        Decision: Metadata-Only Discovery (ADR 009).
        For formats like Parquet, we perform a metadata scan using Polars.
        This provides exact row counts for 50M+ row files in milliseconds
        without pulling data into memory.

        Decision: Heuristic Estimation.
        For non-splittable formats (JSON/XML), we estimate counts based on
        file size (1 row per 1KB) to prevent the driver from performing a
        full scan, which would breach the 2GB RAM limit.
        """
        archive_path = kwargs.get("archive_path")

        try:
            resolve_format_handler_func = getattr(
                self.client, "resolve_format_handler", None
            )
            if not resolve_format_handler_func:
                raise RuntimeError("Format handler resolution is not available")
            handler = resolve_format_handler_func(target, archive_path=archive_path)
            files = list(handler.discover(target, filter_condition))
            if not files:
                return 0

            # 1. Precise Count for Splittable Formats
            if handler.is_splittable:
                total = 0
                for f in files:
                    # pl.len() on a LazyFrame (Scan) is metadata-only for Parquet
                    count_df = handler.to_df(f).select(pl.len()).collect()
                    total += cast("pl.DataFrame", count_df).item()
                return total

            # 2. Heuristic Estimate for Non-Splittable (JSON/XML)
            # We avoid scanning these rows on the driver to prevent OOM.
            # Estimate: 1 row per 1KB of raw data.
            total_bytes = sum(self.client.fs.size(f) for f in files)
            return max(len(files), int(total_bytes // 1024))
        except Exception:
            return 0

    def resolve_identity(
        self, target: str, discovered_items: list[str] | None = None
    ) -> str:
        """Resolves a human-readable identifier for auditing purposes.

        Decision: Domain-Specific Identity.
        By having the service resolve its own identity, we ensure that
        audit trails remain consistent even if the physical storage path
        changes between environments.
        """
        items = discovered_items or []
        if len(items) == 1:
            return Path(items[0]).name

        # Decision: Clean Identity.
        # We no longer parse '::' as paths are structured. We rstrip to
        # ensure directory targets return the folder name for the audit trail.
        return Path(target.rstrip("/")).name or target

    @protect_service(breaker)
    def parallelize(
        self,
        target: str,
        num_workers: int | None = None,
        filter_condition: str | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """
        Calculates how to split the dataset into parallel chunks.

        Implements file-level parallelism, intra-file slicing, and small-file
        coalescing based on dataset volume and format.

        Args:
            target: The directory or file path to split.
            num_workers: The requested number of parallel chunks.
            filter_condition: Optional filter for discovery.
            archive_path: Optional physical archive container.
        Returns:
            list[dict[str, Any]]: A list of work unit definitions.

        Decision: Hybrid Parallelism Strategy.
        We use 'File-level' parallelism for standard datasets, but switch
        to 'Intra-file' slicing for exceptionally large files to maximize
        Ray cluster utilization and keep workers under 2GB RAM.
        """
        if not getattr(self.client, "fs", None):
            raise RuntimeError("Filesystem client is not initialized")

        archive_path = kwargs.get("archive_path")

        try:
            resolve_format_handler_func = getattr(
                self.client, "resolve_format_handler", None
            )
            if not resolve_format_handler_func:
                raise RuntimeError("Format handler resolution is not available")
            handler = resolve_format_handler_func(target, archive_path=archive_path)
        except FileNotFoundError:
            return []

        # 2. Use Handler-specific discovery (e.g. CSVHandler
        # knows to find .csv and .txt)
        files = list(handler.discover(target, filter_condition))
        if not files:
            return []

        # Decision: File-based Scaling.
        # For files, we default to one worker per file, capped by num_workers
        # if provided by the user.
        if not num_workers:
            num_workers = len(files)

        # 1. Coalescing Strategy
        if coalesced := self._try_combine_small_files(files):
            return coalesced

        # 2. Parallel Strategy (Sharding vs Slicing)
        return self._distribute_work(files, num_workers, archive_path, handler)

    def _try_combine_small_files(self, files: list[str]) -> list[dict[str, Any]] | None:
        """Determines if the dataset volume justifies distributed processing.

        Args:
            files: List of discovered file paths.

        Returns:
            list[dict[str, Any]] | None: A single work unit if coalescing is
                justified by small total volume, otherwise None.

        Decision: Small File Coalescing.
        Parallelizing tiny files (e.g., total volume < 50MB) creates more Ray
        overhead than benefit due to task serialization and pod scheduling.
        We coalesce files below the MIN_BLOCK_SIZE_BYTES threshold into a
        single work unit to minimize scheduling latency.
        """
        total_bytes = sum(self.client.fs.size(f) for f in files)
        if total_bytes < MIN_BLOCK_SIZE_BYTES:
            LOG.info(
                "Small dataset detected. Coalescing into a single work unit.",
                total_kb=round(total_bytes / 1024, 2),
                file_count=len(files),
            )
            return [{"files": files}]
        return None

    def _distribute_work(
        self,
        files: list[str],
        num_workers: int,
        archive_path: str | None,
        handler: Any,
    ) -> list[dict[str, Any]]:
        """Calculates work units using either file-level sharding or intra-file slicing.

        Args:
            files: List of discovered file paths.
            num_workers: The target number of parallel workers.
            archive_path: Optional path to a physical archive container.
            handler: The format handler instance for the files.

        Returns:
            list[dict[str, Any]]: A list of work unit definitions.

        Decision: Hybrid Parallelism Strategy.
        We prioritize 'File-level' sharding (Sharding) for high file counts
        as it has the lowest metadata overhead. For scenarios with few
        massive files, we switch to 'Intra-file' slicing (Slicing) using
        pl.len() metadata scans to maximize cluster utilization while
        maintaining the 2GB memory ceiling.
        """
        # STRATEGY 1: File-level Parallelism (Sharding)
        # We use this if we have enough files, OR if the format is not splittable.
        if len(files) >= num_workers or num_workers == 1 or not handler.is_splittable:
            active_workers = min(num_workers, len(files))
            LOG.debug(
                "Slicing: File-level parallelism (Sharding)", workers=active_workers
            )
            return [
                {"files": files[i::active_workers], "archive_path": archive_path}
                for i in range(active_workers)
                if files[i::active_workers]
            ]

        # STRATEGY 2: Intra-file Parallelism (Slicing)
        # Use this when you have few large files and many workers.
        LOG.info(
            "Calculating intra-file slices (Slicing) for optimized parallelism",
            files=len(files),
            target_workers=num_workers,
        )
        file_metadata, total_rows = [], 0
        for f in files:
            # scan_*.select(pl.len()) is metadata-only and extremely fast
            res = handler.to_df(f).select(pl.len()).collect()
            count = cast("pl.DataFrame", res).item()
            total_rows += count
            file_metadata.append({"path": f, "rows": count})

        rows_per_unit = total_rows // num_workers
        work_units = []

        for meta in file_metadata:
            f_path, f_rows = meta["path"], meta["rows"]
            # Calculate how many units this specific file should be split into
            units_for_file = max(1, round(f_rows / rows_per_unit))
            actual_slice_size = f_rows // units_for_file

            for i in range(units_for_file):
                offset = i * actual_slice_size
                # Decision: Remainder Capture.
                # We ensure the last slice of a file captures any remaining rows
                # resulting from integer division to prevent data loss.
                length = (
                    actual_slice_size if i < units_for_file - 1 else f_rows - offset
                )

                work_units.append(
                    {
                        "files": [f_path],
                        "slice": {"offset": offset, "length": length},
                        "archive_path": archive_path,
                    }
                )

        return work_units

    @protect_service(breaker)
    def fetch_data(
        self, unit: dict[str, Any] | list[str] | str
    ) -> pl.DataFrame | pl.LazyFrame:
        """Fetches data for a given work unit into a Polars object.

        Args:
            unit: The work unit definition (file list or slice config).

        Returns:
            pl.DataFrame | pl.LazyFrame: The extracted data.

        Decision: Zero-Copy Format.
        We return LazyFrames where possible, allowing Polars to delay
        materialization until the final sink, maintaining a low memory
        footprint even with millions of rows.
        """
        # Normalize the unit format
        if isinstance(unit, dict):
            paths = unit["files"]
            slice_conf = unit.get("slice")
            archive_path = unit.get("archive_path")
        else:
            paths = [unit] if isinstance(unit, str) else unit
            slice_conf = None
            archive_path = None

        # Resolve paths via the client, but only if they don't already have a protocol.
        resolved_paths = [
            p if "://" in str(p) else self.client.resolve_path(p) for p in paths
        ]

        # 1. Determine format from the first file
        resolve_format_handler_func = getattr(
            self.client, "resolve_format_handler", None
        )
        if not resolve_format_handler_func:
            raise RuntimeError("Format handler resolution is not available")
        handler = resolve_format_handler_func(
            resolved_paths[0], archive_path=archive_path
        )

        # 2. Convert to LazyFrames
        lfs = []
        for p in resolved_paths:
            lf = handler.to_df(p)
            if slice_conf:
                lf = lf.slice(slice_conf["offset"], slice_conf["length"])
            lfs.append(lf)

        if not lfs:
            return pl.DataFrame()

        return pl.concat(lfs)


class StorageSink(BaseStorageService, Sink):
    """Implementation of data promotion and validation for file-based sinks."""

    @protect_service(breaker)
    def stage_data(
        self,
        source_dir: Path,
        target_table: str,
        file_ext: str = "parquet",
    ) -> tuple[str, int]:
        """
        Phase 1: Staging. Copies data into a temporary storage directory.

        Args:
            source_dir: The directory containing local artifacts.
            target_table: The final destination identifier.
            file_ext: The expected extension for staged files.

        Returns:
            tuple[str, int]: The temporary staging path and the number of
                files staged.

        Decision: Non-Destructive Staging.
        We copy data to a timestamped temporary directory first. This
        allows us to verify the total volume and schema before overwriting
        the production partition.
        """
        # Create a unique staging path: e.g., tmp/staging/orders_1710123456/
        staging_path = f"tmp/staging/{target_table}_{int(time.time())}"
        self.client.cp(str(source_dir), staging_path)

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
        Phase 2: Promotion. Performs an idempotent move from staging to prod.

        Args:
            staging_table: The temporary path containing staged data.
            target_table: The root path of the destination dataset.
            partition_col: The name of the partition column.
            partition_val: The specific partition value to promoted.

        Decision: Atomic Promotion.
        By using an idempotent 'Delete-then-Move' pattern, we ensure that
        partial failures do not result in duplicate or corrupted data
        visible to downstream users.
        """
        # Target path: e.g., data/orders/dt=2026-03-10/
        final_path = f"{target_table}/{partition_col}={partition_val}"

        # 1. Idempotency: Remove existing data for this partition
        if self.client.exists(final_path):
            self.client.rm(final_path)

        # 2. Atomic Move: Move the staged folder to the production path
        # On S3, this is a metadata-only rename or a fast copy/delete
        self.client.mv(staging_table, final_path)
        LOG.info("Promoted data", staging=staging_table, final=final_path)

    @protect_service(breaker)
    def is_equal(
        self,
        reference: Path,
        other: Path,
        exclude_columns: set[str] | None = None,
    ) -> None:
        """
        Compares two storage paths for file-level identicality.

        Uses file sizes and ETags (if available) to verify that the
        reference and candidate paths match.

        Args:
            reference: The baseline path.
            other: The candidate path.
            exclude_columns: Ignored for filesystem-level comparison.

        Decision: Content-Aware Checksumming.
        We compare both file sizes and ETags (if supported by the driver)
        to verify identicality. This is significantly faster than a
        byte-for-byte comparison for cloud storage.
        """
        # Check if the number of files is the same

        def get_meta(path_str: str | Path):
            resolved_root = UPath(self.client.resolve_path(path_str), **self.opts)
            # find() returns paths that are usually bucket-relative or protocol-stripped
            data = self.client.fs.find(str(resolved_root), detail=True)

            meta = {}
            for k, v in data.items():
                # Re-attach protocol if missing to perform UPath comparison
                full_item_path = UPath(self.client.fs.unstrip_protocol(k), **self.opts)
                # Extract relative path from the root for comparison
                rel_path = str(full_item_path.relative_to(resolved_root))
                meta[rel_path] = (v["size"], v.get("ETag"))
            return meta

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
        """
        Creates a copy of a directory or file.

        Args:
            reference: Source path.
            other: Destination path.

        Decision: Protocol-Native Cloning.
        We use the client's 'cp' method which, on cloud providers like S3,
        performs a server-side copy. This avoids transferring data through
        the orchestrator's memory.
        """
        self.client.cp(reference, other)


class StorageArchive(BaseStorageService, Archive):
    """Specialized Service for long-term data archival."""

    @protect_service(breaker)
    def archive_data(self, source_dir: Path, archive_path: str) -> None:
        """
        Moves or copies data to a persistent archival destination.

        Args:
            source_dir: The directory containing items to archive.
            archive_path: The destination root or specific identifier.
        """
        self.client.cp(str(source_dir), archive_path)


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
            capabilities={FileSystemSkills.FILE},
            storage_options=storage_options,
            **config,
        )


# --- Role 2: Standard Archival (Moving artifacts) ---
@ServiceFactory.register("standard_archive")
class StandardArchiveService(StorageArchive):
    """Standard archival for job artifacts and logs."""

    def __init__(self, name: str, **config: Any) -> None:
        storage_options = config.pop("storage_options", {})

        super().__init__(
            name=name,
            url=config.pop("url"),
            capabilities={FileSystemSkills.ARCHIVE},
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
            capabilities={FileSystemSkills.CAS},
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
            capabilities={FileSystemSkills.FILE},
            storage_options=storage_options,
            **config,
        )
