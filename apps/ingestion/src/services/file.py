"""Filesystem-based service layer for storage backends."""

import time
from contextlib import suppress
from functools import cached_property
from pathlib import Path
from typing import Any

import polars as pl
from apps.ingestion.src.core.monitor import monitor
from apps.ingestion.src.utils.exceptions import TryAgainLater
from libs.auth.secret import Secret
from libs.clients.base import ClientCantConnect
from libs.file import FileSystemClient, FileSystemSkills, FormatFactory, filter_files
from libs.resilience.circuit_breaker import CircuitBreaker
from libs.utils.dict import find_keys_by_pattern, set_nested_key
from loguru import logger

from .base import Archive, Service, Sink, Source
from .factory import ServiceFactory

LOG = logger
MIN_BLOCK = 50 * 1024 * 1024  # 50MB
MIN_BYTES_PER_WORKER = 100 * 1024 * 1024  # 100MB floor

breaker = CircuitBreaker(
    failure_threshold=3,
    timeout_secs=300,
    tracked_exceptions=(ClientCantConnect, ConnectionError, TimeoutError),
)


class BaseStorage(Service):
    """Base storage service with filesystem client."""

    def __init__(
        self,
        name: str,
        url: str,
        capabilities: set[FileSystemSkills],
        storage_options: dict[str, Any],
        **config,
    ):
        # Remove storage_options from config if present to avoid duplication
        config.pop("storage_options", None)

        super().__init__(
            name,
            url=url,
            capabilities=capabilities,
            storage_options=storage_options,
            **config,
        )
        self.url = url
        self.capabilities = capabilities
        self.options = storage_options
        self._config = config

    @cached_property
    def client(self) -> FileSystemClient:
        """Lazy-initialized filesystem client."""
        from libs.file.base import create_fs_client

        resolved_config = {}
        for path, value in find_keys_by_pattern(
            self._config, pattern="secret|password", ignore_case=True
        ):
            if isinstance(value, Secret):
                resolved_config = set_nested_key(
                    self._config, path, "password", value.resolve(url_encode=True)
                )

        return create_fs_client(
            url=self.url,
            capabilities=self.capabilities,
            options={**self.options, **resolved_config},
        )

    @property
    def fs(self):
        return self.client.fs

    def reset(self) -> None:
        """Reset cached client."""
        if "client" in self.__dict__:
            LOG.warning(f"Resetting client for {self.name}")
            self.close()
            del self.__dict__["client"]

    def close(self) -> None:
        """
        Close the underlying filesystem client.

        Ensures that any open file handles or connection pools in the
        FileSystemClient are released properly.
        """
        if "client" in self.__dict__:
            with suppress(Exception):
                self.client.close()

    def exists(self, target: str) -> bool:
        return self.client.exists(target)

    @monitor(breaker)
    def count_units(self, target: str, filter_condition: str | None = None) -> int:
        """Count the number of units (files/objects) at the target location.

        Decision: Metadata Discovery.
        We perform a shallow scan of the filesystem to determine the workload.
        This is a prerequisite for the 'Worker Density' heuristic used in
        parallelization to avoid over-parallelizing small datasets.

        Args:
            target: The base path to scan.
            filter_condition: An optional glob pattern to filter files.

        Returns:
            int: Total file count.
        """
        try:
            # Simple file count without format detection
            full_path = self.client.resolve(target)
            if self.client.fs.isfile(full_path):
                return 1
            all_files = self.client.fs.find(full_path)
            return len(filter_files(all_files, filter_condition, target))
        except (FileNotFoundError, RuntimeError):
            return 0


class StorageSource(BaseStorage, Source):
    """File-based data source."""

    def resolve_identity(
        self, target: str, items: list[str] | None = None, **kwargs
    ) -> str:
        items = items or []
        file_pattern = kwargs.get("file_pattern")

        if file_pattern:
            # Archive with internal pattern
            return f"📦 {Path(target).name} → {file_pattern}"
        if len(items) == 1:
            # Single file
            return f"📄 {Path(items[0]).name}"
        if len(items) > 1:
            # Multiple files
            return f"📁 {Path(target).name} ({len(items)} files)"
        # Fallback
        return f"📂 {Path(target).name or target}"

    @monitor(breaker)
    def parallelize(
        self,
        target: str,
        num_workers: int | None = None,
        filter_condition: str | None = None,
        **kwargs,
    ) -> list[dict]:
        """Calculates optimal file distribution across Ray workers.

        Decision: Workload Balancing (ADR 007).
        Instead of simple modulo distribution, we use size-aware bin-packing
        to balance the byte-load across workers. We also enforce a 'Density
        Floor' to ensure that the cost of spawning a Ray worker is justified
        by the volume of data it processes.

        Args:
            target: Base path or archive to scan.
            num_workers: Targeted concurrency level.
            filter_condition: Glob pattern for file selection.
            **kwargs: Includes 'temp_folder' and 'cleanup' flags.

        Returns:
            list[dict]: A list of work unit definitions.
        """
        temp_folder = kwargs.get("temp_folder")
        cleanup = kwargs.get("cleanup", True)
        client = self.client
        temp_dir = None

        try:
            if client.is_archive(target):
                temp_dir = client.extract_archive(
                    archive_path=target, target_dir=temp_folder
                )
                LOG.info(f"Extracted archive {target} to {temp_dir}")
                target = temp_dir

            # Now process as regular filesystem
            full_path = self.client.resolve(target)

            if self.client.fs.isfile(full_path):
                # Single file
                files = [full_path]
                LOG.info(f"Single file mode: {full_path}")
            else:
                # Directory
                all_files = self.client.fs.find(full_path)
                files = filter_files(all_files, filter_condition, target)
                LOG.info(f"Directory mode: found {len(files)} files in {target}")

            if not files:
                raise TryAgainLater(
                    reason=f"Resource not found: {target} (pattern: {filter_condition})",
                    service_name=self.name,
                    wait_seconds=self._config.get("missing_file_retry_sec", 600),
                )

            # Detect format from first file (for splittable check)
            ext = Path(files[0]).suffix.lstrip(".").lower()
            handler = FormatFactory.get(ext, self.client.fs, self.client.options)

            # Metadata-driven scaling
            total_bytes = sum(self.fs.size(f) for f in files)

            # Heuristic Constants

            # Case 1: Coalesce small files (Setup cost > Processing cost)
            if total_bytes < MIN_BYTES_PER_WORKER:
                LOG.info(f"Coalescing small files ({total_bytes/1024:.1f}KB)")
                return [{"files": files}]

            # Case 2: If num_workers not set,
            # Determine actual worker count based on density floor
            if not num_workers:
                num_workers = int(total_bytes // MIN_BYTES_PER_WORKER)

            num_workers = max(1, num_workers)

            # Case 3: A few large files vs many workers
            if len(files) < num_workers and handler.splittable:
                LOG.debug(f"Intra-file slicing with {num_workers} workers")
                return self._slice_files(files, num_workers, handler)

            # Case 4: Greedy File-level sharding (The Default)
            active = min(num_workers, len(files))
            LOG.debug(f"File-level greedy sharding with {active} workers")
            return self._balance_workload(files, active)

        finally:
            if temp_dir and cleanup:
                import shutil

                shutil.rmtree(temp_dir, ignore_errors=True)

    def _balance_workload(self, files: list[str], num_workers: int) -> list[dict]:
        """Distributes files across workers to balance the total byte-load.

        Uses Longest Processing Time (LPT) scheduling:
        Sort files by size descending, then assign each to the least
        loaded worker. This minimizes tail latency compared to simple
        round-robin distribution.

        Args:
            files: List of resolved file paths.
            num_workers: Number of buckets to create.

        Returns:
            list[dict]: Balanced worker units.
        """
        from dataclasses import dataclass, field

        @dataclass
        class WorkerBucket:
            """Worker bucket for greedy file distribution."""

            files: list[str] = field(default_factory=list)
            load: int = 0

        if not files or num_workers <= 0:
            return []

        # Sort files by size descending (largest first)
        sized = [(f, self.fs.size(f)) for f in files]
        sized.sort(key=lambda x: x[1], reverse=True)

        # Initialize buckets
        buckets = [WorkerBucket() for _ in range(num_workers)]

        # Assign each file to the least loaded worker
        for path, size in sized:
            lightest = min(buckets, key=lambda b: b.load)
            lightest.files.append(path)
            lightest.load += size

        # Return only non-empty buckets
        return [{"files": b.files} for b in buckets if b.files]

    def _slice_files(self, files: list[str], workers: int, handler) -> list[dict]:
        """Slices large splittable files into row-based work units.

        Decision: Row-Budget Slicing.
        We treat all files as a single contiguous pool of rows and divide
        them equally across workers, ensuring even distribution for
        splittable formats like Parquet.
        """
        metadata = []
        total = 0
        for f in files:
            rows = handler.count_rows(f) if handler.splittable else 0
            total += rows
            metadata.append({"path": f, "rows": rows})

        if total == 0:
            # Fallback: one worker per file
            return [{"files": [f]} for f in files]

        per_worker = max(1, total // workers)
        units = []

        for meta in metadata:
            path, rows = meta["path"], meta["rows"]
            slices = max(1, round(rows / per_worker))
            slice_size = rows // slices

            for i in range(slices):
                offset = i * slice_size
                length = slice_size if i < slices - 1 else rows - offset
                units.append(
                    {
                        "files": [path],
                        "slice": {"offset": offset, "length": length},
                    }
                )

        return units

    @monitor(breaker)
    def pull(self, unit: dict | list | str) -> pl.LazyFrame:
        """Fetch data for a work unit."""
        if isinstance(unit, dict):
            paths = unit["files"]
            slice_conf = unit.get("slice")
        else:
            paths = [unit] if isinstance(unit, str) else unit
            slice_conf = None

        # Resolve paths
        resolved = [p if "://" in str(p) else self.client.resolve(p) for p in paths]

        # Get handler from first file's extension (no get_handler needed)
        ext = Path(resolved[0]).suffix.lstrip(".").lower()
        handler = FormatFactory.get(ext, self.client.fs, self.client.options)

        frames = []
        for p in resolved:
            df = handler.to_df(p)
            if slice_conf:
                df = df.slice(slice_conf["offset"], slice_conf["length"])
            frames.append(df)

        return pl.concat(frames) if frames else pl.LazyFrame()


class StorageSink(BaseStorage, Sink):
    """File-based data sink."""

    @monitor(breaker)
    def stage(self, source: Path, target: str, ext: str = "parquet") -> tuple[str, int]:
        """Copy data to staging area."""
        staging = f"tmp/staging/{target}_{int(time.time())}"
        self.client.cp(str(source), staging)

        full = self.client.resolve(staging)
        count = len(self.fs.find(full))
        LOG.info(f"Staged {count} files to {staging}")
        return staging, count

    @monitor(breaker)
    def promote(
        self,
        staging: str,
        target: str,
        partition_by: str,
        partition_value: str,
        expected_count: int,
    ) -> None:
        """Move staged data to production."""
        final = f"{target}/{partition_by}={partition_value}"

        if self.client.exists(final):
            self.client.rm(final, recursive=True)

        self.client.mv(staging, final)
        LOG.info(f"Promoted to {final}")

    @monitor(breaker)
    def clone(self, source: str, dest: str) -> None:
        """Copy directory or file."""
        self.client.cp(source, dest)

    @monitor(breaker)
    def delete(self, target: str) -> None:
        """Delete a path."""
        if self.client.exists(target):
            self.client.rm(target, recursive=True)
            LOG.info(f"Dropped: {target}")


class StorageArchive(BaseStorage, Archive):
    """Data archival service."""

    @monitor(breaker)
    def store(self, source: Path, dest: str) -> None:
        """Archive data to destination."""
        bucket = self.client.url
        final_path = dest if "://" in dest else f"{bucket}/{dest}"
        self.client.cp(str(source), final_path, recursive=True)


# Service registrations
@ServiceFactory.register("flat_file")
class FlatFileService(StorageSource):
    def __init__(self, name: str, **config):
        # Extract storage_options from config
        storage_options = config.pop("storage_options", {})
        url = config.pop("url", "")

        super().__init__(
            name=name,
            url=url,
            capabilities={FileSystemSkills.FILE},
            storage_options=storage_options,
            **config,
        )


@ServiceFactory.register("standard_archive")
class StandardArchive(StorageArchive):
    def __init__(self, name: str, **config):
        storage_options = config.pop("storage_options", {})
        url = config.pop("url", "")

        super().__init__(
            name=name,
            url=url,
            capabilities={FileSystemSkills.ARCHIVE},
            storage_options=storage_options,
            **config,
        )


@ServiceFactory.register("cas_archive")
class CASArchive(StorageArchive):
    """Content Addressable Storage for immutable records."""

    def __init__(self, name: str, **config) -> None:
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


@ServiceFactory.register("data_lake")
class DataLake(StorageSource, StorageSink):
    def __init__(self, name: str, **config):
        # Extract storage_options from config
        storage_options = config.pop("storage_options", {})
        url = config.pop("url", "")

        super().__init__(
            name=name,
            url=url,
            capabilities={FileSystemSkills.FILE},
            storage_options=storage_options,
            **config,
        )
