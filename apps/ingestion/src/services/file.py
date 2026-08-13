"""Filesystem-based service layer for storage backends."""

import time
from contextlib import suppress
from pathlib import Path
from typing import Any

import polars as pl
from libs.auth.secret import Secret
from libs.clients.base import ClientCantConnect
from libs.database.sql import SQLContext
from libs.file import (
    FileSystemConnector,
    FileSystemSkills,
    filter_files,
    get_file_ext,
)
from libs.resilience.circuit_breaker import CircuitBreaker
from libs.utils.dict import find_keys_by_pattern, set_nested_key
from loguru import logger
from msgspec import Struct, field

from src.services.base import Archive, Service, Sink, Source
from src.services.health.monitor import monitor
from src.utils.exceptions import TryAgainLater

from .factory import ServiceFactory

LOG = logger
MIN_BLOCK = 50 * 1024 * 1024  # 50MB
MIN_BYTES_PER_WORKER = 100 * 1024 * 1024  # 100MB floor

breaker = CircuitBreaker(
    failure_threshold=3,
    timeout_secs=300,
    tracked_exceptions=(ClientCantConnect, ConnectionError, TimeoutError),
)


@ServiceFactory.register(source_type="file")
class FileService(Service):
    """Base storage service with filesystem client."""

    def __init__(
        self,
        url: str,
        skills: set[FileSystemSkills],
        name: str | None = None,
        **config,
    ):
        # Remove storage_options from config if present to avoid duplication
        config.pop("storage_options", None)

        self.name = name or config.get("type") or self.__class__.__name__.lower()
        self.url = url
        self.skills = skills
        self._config = config

    def probe(self, target: str | None = None) -> bool:
        """Health probe for file storage connector.

        Checks existence of specific resource target if provided,
        or falls back to root or configured URL path.
        """
        try:
            probe_path = target if target else "/"
            # Fallback to the base URL or provided root if probe_path is root
            if probe_path == "/" and self.url:
                probe_path = self.url

            return self.connector.fs.exists(probe_path)
        except Exception as e:
            LOG.warning(f"File service health probe failed for {self.name}: {e}")
            return False

    @property
    def connector(self) -> FileSystemConnector:
        """Lazy-initialized filesystem client."""
        skills = {FileSystemSkills(skill_name) for skill_name in self.skills}
        resolved_config = {}
        for path, value in find_keys_by_pattern(
            self._config, pattern="secret|password", ignore_case=True
        ):
            if isinstance(value, Secret):
                resolved_config = set_nested_key(
                    self._config, path=path, new_value=value.resolve(url_encode=True)
                )

        return FileSystemConnector(url=self.url, skills=skills, **resolved_config)

    @property
    def fs(self):
        return self.connector.fs

    def reset(self) -> None:
        """Reset cached client."""
        if "client" in self.__dict__:
            LOG.warning(f"Resetting client for {self.__class__.__qualname__}")
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
                self.connector.close()

    def exists(self, target: str) -> bool:
        return self.connector.fs.exists(target)

    @monitor(breaker)
    def count_units(self, target: str, filter_condition: str | None = None) -> int:
        """Count the number of units (files/objects) at the target location.

        Args:
            target: The base path to scan.
            filter_condition: An optional glob pattern to filter files.

        Returns:
            int: Total file count.

        Notes:
        - We perform a shallow scan of the filesystem to determine the workload.
        - This is a prerequisite for the 'Worker Density' heuristic used in
          parallelization to avoid over-parallelizing small datasets.
        """
        try:
            # Simple file count without format detection
            full_path = self.connector.fs.resolve(target)
            if self.connector.is_file(full_path):
                return 1
            all_files = self.connector.find(full_path)
            return len(filter_files(list(all_files), filter_condition, target))
        except (FileNotFoundError, RuntimeError):
            return 0


@ServiceFactory.register(source_type="file", role="source")
class FileSource(FileService, Source):
    """File-based data source."""

    def resolve_identity(
        self, target: str, items: list[str] | None = None, **kwargs
    ) -> str:
        items = items or []
        glob = kwargs.get("glob")

        if glob:
            # Archive with internal pattern
            return f"📦 {Path(target).name} → {glob}"
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
        sql_context: SQLContext,
        num_workers: int | None = None,
        filter_condition: str | None = None,
        **kwargs,
    ) -> list[dict]:
        """Calculates optimal file distribution across Ray workers.

        Args:
            target: Base path or archive to scan.
            num_workers: Targeted concurrency level.
            filter_condition: Glob pattern for file selection.
            **kwargs: Includes 'temp_folder' and 'cleanup' flags.

        Returns:
            list[dict]: A list of work unit definitions.
        """
        resolved_target = self.connector.resolve(target)
        # temp_folder = kwargs.get("temp_folder")
        # cleanup = kwargs.get("cleanup", True)
        # temp_dir = None

        # try:
        # 1. Use ArchiveContext classmethod for clean format detection
        if self.connector.is_supported_archive(resolved_target):
            # 2. Inspect contents using ArchiveContext as a context manager
            with self.connector.archive(resolved_target) as archive:
                archive_files = archive.list_contents()

            # Construct virtual path pointers
            files_with_sizes = [
                (f"zip://{resolved_target}!!{row['filename']}", row["size"])
                for row in archive_files
            ]

            if filter_condition:
                files_with_sizes = [
                    (p, s)
                    for p, s in files_with_sizes
                    if Path(p.split("!!")[-1]).match(filter_condition)
                ]

            files = [p for p, _ in files_with_sizes]
            total_bytes = sum(s for _, s in files_with_sizes)
        else:
            if self.fs.fs.isfile(resolved_target):
                # Single file
                files = [resolved_target]
                LOG.info(f"Single file mode: {resolved_target}")
            else:
                # Directory
                all_files = list(self.connector.find(resolved_target))
                files = filter_files(all_files, filter_condition, target)
                LOG.info(f"Directory mode: found {len(files)} files in {target}")

            # Metadata-driven scaling
            total_bytes = sum(self.fs.fs.size(f) for f in files)

        if not files:
            raise TryAgainLater(
                reason=f"Resource not found: {target} (pattern: {filter_condition})",
                service_name=self.name,
                wait_seconds=self._config.get("missing_file_retry_sec", 600),
            )

        # Detect format from first file (for splittable check)
        # ext = Path(files[0]).suffix.lstrip(".").lower()
        # handler = FormatFactory.get(ext, self.fs, self.fs.options)

        # Coalesce small files (Setup cost > Processing cost)
        # if total_bytes < MIN_BYTES_PER_WORKER:
        #     LOG.info(f"Coalescing small files ({total_bytes/1024:.1f}KB)")
        #     units = [{"files": files}]

        num_workers = max(1, num_workers or (total_bytes // MIN_BYTES_PER_WORKER))

        # A few large files vs many workers
        # if len(files) < num_workers and handler.splittable:
        #     LOG.debug(f"Intra-file slicing with {num_workers} workers")
        #     units = self._slice_files(files, num_workers, handler)

        # Default: Greedy File-level sharding
        # LOG.debug(f"File-level greedy sharding with {active} workers")
        return self._balance_workload(files, min(num_workers, len(files)))

        # sql_configs = {
        #     "sql_context": sql_context
        #     # "skip_blank_lines": kwargs.get("skip_blank_lines"),
        #     # "header": kwargs.get("header"),
        # }
        # for unit in units:
        #     unit.update(sql_configs)

        # return units

        # finally:
        #     if temp_dir and cleanup:
        #         import shutil

        #         shutil.rmtree(temp_dir, ignore_errors=True)

    def _balance_workload(self, files: list[str], num_workers: int) -> list[dict]:
        """Distributes files across workers to balance the total byte-load.

        Uses Longest Processing Time (LPT) scheduling:
          - Sort files by size descending, then assign each to the least
            loaded worker.
          - Minimizes tail latency compared to simple round-robin
        distribution.

        Args:
            files: List of resolved file paths.
            num_workers: Number of buckets to create.

        Returns:
            list[dict]: Balanced worker units.
        """

        class WorkerBucket(Struct):
            """Worker bucket for greedy file distribution."""

            files: list[str] = field(default_factory=list)
            load: int = 0

        if not files or num_workers <= 0:
            return []

        # Sort files by size descending (largest first)
        sized = [(f, self.fs.fs.size(f)) for f in files]
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

    @monitor(breaker)
    def pull(self, unit: dict | list | str, **kwargs: Any) -> pl.DataFrame:
        """Fetch data for a work unit."""
        # Normalize incoming unit structures up front
        unit_dict = unit if isinstance(unit, dict) else {}
        paths = (
            unit_dict["files"]
            if isinstance(unit, dict)
            else ([unit] if isinstance(unit, str) else unit)
        )

        sql_context = kwargs.get("sql_context")
        temp_folder = (
            unit_dict.get("temp_folder") if isinstance(unit_dict, dict) else None
        )

        # 2. Delegate path formatting, archive staging, and query evaluation to FileReader
        lazy_frame = self.connector.data.extract(
            source=paths,
            sql_context=sql_context,
            temp_folder=temp_folder,
        )

        # 3. Materialize final collected result into an eager Polars DataFrame
        return lazy_frame.collect()


@ServiceFactory.register(source_type="file", role="sink")
class FileSink(FileService, Sink):
    """File-based data sink."""

    @monitor(breaker)
    def stage(
        self,
        source_dir: Path,
        target: str,
        expected_count: int,
        file_ext: str = "parquet",
        audit_values: dict[str, Any] | None = None,
        **kwargs,
    ) -> tuple[str, int]:
        """Phase 1: Write dataset into a temporary staging location."""
        staging_dir = f"tmp/staging/{target.rstrip('/')}_{int(time.time())}"
        resolved_source = self.connector.resolve(source_dir)
        resolved_staging = self.connector.resolve(staging_dir)

        LOG.info(
            f"Phase 1: Staging data from {resolved_source} -> {resolved_staging} (Target Format: {file_ext})"
        )

        # 1. Discover source files
        source_files = self.connector.find(resolved_source)
        if not source_files:
            raise FileNotFoundError(f"No source files found at {resolved_source}")

        # Detect source format dynamically from the first file
        src_format = get_file_ext(source_files[0])
        tgt_format = file_ext.lower().lstrip(".")

        # 2. Same Format: Perform direct storage copy (Zero-copy pass-through)
        if src_format == tgt_format:
            LOG.info(
                f"Source format matches target format ({src_format}). Performing direct copy."
            )
            self.connector.cp(resolved_source, resolved_staging, recursive=True)

        # 3. Format Mismatch: Convert formats using FileReader skill
        else:
            LOG.info(
                f"Format mismatch detected ({src_format} -> {tgt_format}). Executing DuckDB format conversion."
            )
            file_reader = self.connector.skills[FileSystemSkills.FILE]

            # Destination path inside staging directory
            dst_file = f"{resolved_staging}/data_0.{tgt_format}"

            file_reader._convert_format(
                source_path=resolved_source,
                target_path=dst_file,
                source_format=src_format,
                target_format=tgt_format,
                partition_cols=kwargs.get("partition_cols"),
            )

        # 4. Validate Staged Output File Count
        staged_files = self.connector.find(resolved_staging)
        file_count = len(staged_files)

        if expected_count > 0 and file_count < expected_count:
            raise RuntimeError(
                f"Staging validation failed for {staging_dir}: "
                f"Expected at least {expected_count} file(s), found {file_count}."
            )

        LOG.info(f"Phase 1 Complete: Staged {file_count} file(s) at {staging_dir}")
        return staging_dir, file_count

    @monitor(breaker)
    def promote(
        self,
        staging: str,
        target: str,
        expected_count: int,
        partition_on: str | None = None,
        partition_value: str | None = None,
    ) -> None:
        """Phase 2: Promote staged directory contents into production location atomically."""
        resolved_staging = self.connector.resolve(staging)

        # 1. Resolve Target Destination & Hive Partition Pathing
        if partition_on and partition_value:
            final_target_path = f"{target.rstrip('/')}/{partition_on}={partition_value}"
        else:
            final_target_path = target

        resolved_target = self.connector.resolve(final_target_path)

        LOG.info(
            f"Phase 2: Promoting data from {resolved_staging} -> {resolved_target}"
        )

        # 2. Pre-Promotion Validation
        staged_files = self.connector.find(resolved_staging)
        staged_count = len(staged_files)

        if staged_count == 0:
            raise RuntimeError(
                f"Promotion failed: Staging location '{resolved_staging}' is empty."
            )

        if expected_count > 0 and staged_count < expected_count:
            raise RuntimeError(
                f"Promotion validation failed for {resolved_staging}: "
                f"Expected at least {expected_count} file(s), but found {staged_count}."
            )

        try:
            # 3. Destination Preparation & Target Cleanup (Atomic Overwrite)
            if self.connector.exists(resolved_target):
                LOG.info(
                    f"Target destination exists. Cleaning target location prior to swap: {resolved_target}"
                )
                self.connector.rm(resolved_target, recursive=True)

            # 4. Atomic Move / Copy Step
            # Performs atomic directory rename on local/POSIX or multi-part move/copy on cloud stores
            self.connector.mv(resolved_staging, resolved_target, recursive=True)
            LOG.info(
                f"Phase 2 Complete: Promoted {staged_count} file(s) to {resolved_target}"
            )

        finally:
            # 5. Guaranteed Staging Cleanup
            if self.connector.exists(resolved_staging):
                LOG.debug(f"Cleaning residual staging path: {resolved_staging}")
                self.connector.rm(resolved_staging, recursive=True)

    @monitor(breaker)
    def clone(self, source: str, dest: str) -> None:
        """Copy directory or file."""
        self.connector.cp(source, dest)

    @monitor(breaker)
    def delete(self, target: str) -> None:
        """Delete a path."""
        if self.connector.exists(target):
            self.connector.rm(target, recursive=True)
            LOG.info(f"Dropped: {target}")


@ServiceFactory.register(source_type="file", role="archive")
class FileArchive(FileService, Archive):
    """Data archival service."""

    @monitor(breaker)
    def store(self, source: Path, dest: str) -> None:
        """Archive data to destination."""
        bucket = self.fs.url
        final_path = dest if "://" in dest else f"{bucket}/{dest}"
        self.connector.cp(str(source), final_path, recursive=True)
