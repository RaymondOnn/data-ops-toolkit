import time
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import polars as pl
from libs.auth.models import Secret
from libs.clients.base import ClientCantConnect
from libs.file import FileSystemClient, FileSystemSkills, FormatFactory
from libs.resilience.circuit_breaker import CircuitBreaker
from libs.utils.dict import find_keys_by_pattern, update_nested_key
from loguru import logger
from upath import UPath

from .base import Archive, Service, Sink, Source
from .factory import ServiceFactory
from .registry import protect_service

if TYPE_CHECKING:
    from libs.file.formats.base import FormatHandler

LOG = logger

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
        capabilities: set[FileSystemSkills],
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
        self._config = config

    @cached_property
    def client(self) -> FileSystemClient:
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
        """Invalidates the cached FileSystemClient."""
        if "client" in self.__dict__:
            LOG.warning(f"Resetting filesystem client for service: {self.name}")
            self.__dict__.pop("client", None)

    def _get_handler(self, target_path: str) -> "FormatHandler":
        """Resolves the appropriate FormatHandler by peeking at the filesystem."""
        LOG.debug(f"🔍 Determining format handler for path: {target_path}")

        # 1. Construct a protocol-aware UPath
        # We build the path directly to avoid resolve_path()'s local filesystem sniffing
        if "://" in target_path:
            path_obj = UPath(target_path, **self.opts)
        else:
            # Ensure target_path is relative to the service root,
            # bypassing local absolute checks
            path_obj = UPath(self.url, **self.opts) / target_path.lstrip("/")

        # 2. Fast-path: Extract extension if the target is a specific file
        ext = path_obj.suffix.lstrip(".").lower()

        # 3. Fallback: Peek at the filesystem if no extension is present (directory discovery)
        if not ext:
            LOG.debug(f"No extension in path '{target_path}'. Peeking filesystem...")
            peek = next(self.client.walk_paths(str(path_obj)), None)
            if not peek:
                raise FileNotFoundError(f"No files found at {target_path}")
            ext = UPath(peek).suffix.lstrip(".").lower()
            LOG.debug(f"✅ Peeked file: {peek} | Extension: '{ext}'")

        LOG.info(f"🚀 Final resolved extension for handler: '{ext}'")

        return FormatFactory.get_handler(ext, self.client.fs, self.opts)


class StorageSource(BaseStorageService, Source):
    def get_total_count(self, target: str, filter_condition: str | None = None) -> int:
        """
        Discovers files and aggregates row counts to assist with worker scaling.
        For splittable formats (Parquet/CSV), uses metadata/newline scans.
        For non-splittable (JSON/XML), estimates rows based on file size.
        """
        try:
            handler = self._get_handler(target)
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

    @protect_service(breaker)
    def get_work_units(
        self, target: str, num_workers: int, filter_condition: str | None = None
    ) -> list[dict[str, Any]]:
        """
        Uses the internal client to split 50M rows.
        Works across S3, Azure, GCS, or Local.
        """
        if not getattr(self.client, "fs", None):
            raise RuntimeError("Filesystem client is not initialized")

        try:
            handler = self._get_handler(target)
        except FileNotFoundError:
            return []

        # 2. Use Handler-specific discovery (e.g. CSVHandler
        # knows to find .csv and .txt)
        files = list(handler.discover(target, filter_condition))
        if not files:
            return []

        # --- SMALL FILE COALESCING LOGIC ---
        # Heuristic: If files are tiny, don't waste Ray overhead on parallelism.
        # However, we also check if the number of files is small.
        # 50MB is a safe 'minimum' for a single Ray task in a 2GB environment.
        MIN_BLOCK_SIZE_BYTES = 50 * 1024 * 1024

        # Fast metadata check for total size
        total_bytes = sum(self.client.fs.size(f) for f in files)

        # If total volume is small (e.g. 5 files totaling 5MB),
        # one worker is significantly more efficient than spinning up 5-10 Ray tasks.
        if total_bytes < MIN_BLOCK_SIZE_BYTES:
            LOG.info(
                "Small dataset detected. Coalescing into a single work unit.",
                total_kb=round(total_bytes / 1024, 2),
                file_count=len(files),
            )
            return [{"files": files}]

        # STRATEGY 1: File-level Parallelism (Standard)
        # We use this if we have enough files, OR if the format is not splittable (JSON/XML)
        if len(files) >= num_workers or num_workers == 1 or not handler.is_splittable:
            active_workers = min(num_workers, len(files))
            LOG.debug(
                "Using file-level parallelism",
                files=len(files),
                workers=active_workers,
                splittable=handler.is_splittable,
            )
            return [
                {"files": files[i::active_workers]}
                for i in range(active_workers)
                if files[i::active_workers]
            ]

        # STRATEGY 2: Intra-file Parallelism (Overslicing)
        # Use this when you have few large files and many workers.
        LOG.info(
            "Calculating intra-file slices for optimized parallelism",
            files=len(files),
            target_workers=num_workers,
        )

        # We need row counts to calculate slice boundaries
        # scan_*.select(pl.len()) is metadata-only and extremely fast
        total_rows = 0
        file_metadata = []
        for f in files:
            res = handler.to_df(f).select(pl.len()).collect()
            count = cast("pl.DataFrame", res).item()
            total_rows += count
            file_metadata.append({"path": f, "rows": count})

        rows_per_unit = total_rows // num_workers
        work_units = []

        for meta in file_metadata:
            f_path = meta["path"]
            f_rows = meta["rows"]

            # Calculate how many units this specific file should be split into
            units_for_file = max(1, round(f_rows / rows_per_unit))
            actual_slice_size = f_rows // units_for_file

            for i in range(units_for_file):
                offset = i * actual_slice_size
                # Ensure the last slice captures remaining rows due to rounding
                length = (
                    actual_slice_size if i < units_for_file - 1 else f_rows - offset
                )

                work_units.append(
                    {"files": [f_path], "slice": {"offset": offset, "length": length}}
                )

        return work_units

    @protect_service(breaker)
    def fetch_data(
        self, unit: dict[str, Any] | list[str] | str
    ) -> pl.DataFrame | pl.LazyFrame:
        """
        Reads a list of files (the work unit) into a single Polars DataFrame.
        Supports Parquet, CSV, and JSON formats.
        """
        # Normalize the unit format
        if isinstance(unit, dict):
            paths = unit["files"]
            slice_conf = unit.get("slice")
        else:
            paths = [unit] if isinstance(unit, str) else unit
            slice_conf = None

        # Resolve paths via the client, but only if they don't already have a protocol.
        resolved_paths = [
            p if "://" in str(p) else self.client.resolve_path(p) for p in paths
        ]

        # 1. Determine format from the first file
        handler = self._get_handler(resolved_paths[0])

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
    @protect_service(breaker)
    def stage_data(
        self,
        source_dir: Path,
        target_table: str,
        file_ext: str = "parquet",
    ) -> tuple[str, int]:
        """
        Phase 1: Organize Parquet files into a staging directory.
        Returns a tuple of (path to the staged folder, number of items loaded).
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
        Phase 2: Idempotent swap for File Systems.
        Equivalent to 'REPLACE PARTITION'.
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
        self.client.cp(reference, other)


class StorageArchive(BaseStorageService, Archive):
    @protect_service(breaker)
    def archive_data(self, source_dir: Path, archive_path: str) -> None:
        """
        Archive data to the destination path.
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
