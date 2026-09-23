"""Filesystem-based service layer for storage backends."""

import time
from collections.abc import Generator
from contextlib import suppress
from pathlib import Path
from typing import Any

import polars as pl
from libs.auth.secret import Secret
from libs.clients.base import ClientCantConnect
from libs.file import (
    FileSkill,
    FileSystemConnector,
    filter_files,
    get_file_ext,
)
from libs.resilience.circuit_breaker import CircuitBreaker
from libs.utils.dict import find_keys_by_pattern, set_nested_key
from loguru import logger
from msgspec import Struct, field

from src.services.base import Service
from src.services.contracts import Archive, Sink, Source
from src.services.health.monitor import monitor
from src.utils.exceptions import TryAgainLater

LOG = logger
MIN_BLOCK = 50 * 1024 * 1024  # 50MB
MIN_BYTES_PER_WORKER = 100 * 1024 * 1024  # 100MB floor

breaker = CircuitBreaker(
    failure_threshold=3,
    timeout_secs=300,
    tracked_exceptions=(ClientCantConnect, ConnectionError, TimeoutError),
)


@Service.register()
class FileService(Service):
    """Base storage service with filesystem client."""

    def __init__(
        self,
        url: str,
        skills: set[FileSkill] | None = None,
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
        resolved_config = {}
        for path, value in find_keys_by_pattern(
            self._config, pattern="secret|password", ignore_case=True
        ):
            if isinstance(value, Secret):
                resolved_config = set_nested_key(
                    self._config, path=path, new_value=value.resolve(url_encode=True)
                )

        return FileSystemConnector(url=self.url, **resolved_config)

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


@Service.register()
class FileSource(FileService, Source):
    """File-based data source."""

    def resolve_identity(
        self, target: str, items: list[str] | None = None, **kwargs
    ) -> str:
        """Resolve human-readable resource identity label for file sources.

        Rules & Formatting Cases:
        1. Single File:
            Format: "<filepath>"
            Example: "/data/orders.csv"

        2. Archive File:
            - Single file inside: "<archive_path>::<inner_filepath>"
            - Multiple files/pattern: "<archive_path>::<inner_pattern_or_glob>"
            Examples:
            - "/data/exports.zip::orders_jan.csv"
            - "/data/exports.zip::*.parquet"

        3. Data Folder:
            - Single file in folder: "<folder_path>/<single_filename>"
            - Pattern or glob: "<folder_path>/<glob_pattern>"
            - Multiple files fallback: "<folder_path>/*"
            Examples:
            - "/data/inbox/order_123.json"
            - "/data/inbox/*.parquet"

        Args:
            target: Target directory, file path, or archive path.
            items: List of resolved file paths matching the work unit.
            **kwargs: Additional parameters, such as 'glob'.

        Returns:
            str: Formatted string representation of the source resource identity.
        """
        items = items or []
        glob = kwargs.get("glob")

        # Clean protocol prefixes if present
        clean_target = target.replace("zip://", "").replace("archive://", "")

        # --- CASE 1: ARCHIVE FILE ---
        if "!!" in clean_target or self.connector.is_supported_archive(clean_target):
            if "!!" in clean_target:
                archive_path, inner_spec = clean_target.split("!!", 1)
            else:
                archive_path, inner_spec = clean_target, (glob or "*")

            # Single file inside archive
            if len(items) == 1:
                inner_name = Path(items[0].split("!!")[-1]).name
                return f"{archive_path}::{inner_name}"

            return f"{archive_path}::{inner_spec}"

        # --- CASE 2: DIRECT SINGLE FILE ---
        if (
            len(items) == 1
            and not glob
            and self.connector.is_file(self.connector.resolve(items[0]))
        ):
            return f"{items[0]}"

        # --- CASE 3: DATA FOLDER ---
        folder_path = clean_target.rstrip("/")
        if len(items) == 1:
            file_name = Path(items[0]).name
            return f"{folder_path}/{file_name}"

        if glob:
            pattern = glob.lstrip("/")
            return f"{folder_path}/{pattern}"

        return f"{folder_path}/*"

    def _list_files_metadata(
        self, target: str, sql_where: str | None = None
    ) -> pl.DataFrame:
        """Collect file metadata into a Polars DataFrame.

        Possible fields available in fsspec info() dictionary depending on filesystem:
        - name / path (str): Full path or key of the object (e.g., "bucket/path/to/file.parquet").
        - size (int): File size in bytes.
        - type (str): Entry type ("file" or "directory").
        - mtime / created / updated (float|datetime):
            * mtime: Modified time epoch float (Local POSIX, S3, GCS).
            * created: Creation timestamp epoch float.
            * LastModified / updated: ISO/datetime object (Cloud stores like S3/GCS).
        - ETag / md5 / check_name (str): Content hash identifier.
        - storageClass (str): Object storage tier (e.g., "STANDARD", "GLACIER" on S3/GCS).
        - islink (bool): True if target is a symbolic link (POSIX).

        Args:
            target: Target filesystem path or directory.

        Returns:
            pl.DataFrame: DataFrame containing standardized path, name, size, and last_modified columns.
        """
        resolved_target = self.connector.resolve(target)

        # 1. Fetch metadata records
        if self.fs.fs.isfile(resolved_target):
            info = self.fs.fs.info(resolved_target)
            records = [
                {
                    "path": resolved_target,
                    "name": Path(resolved_target).name,
                    "last_modified": info.get("mtime", 0.0),
                    "size": info.get("size", 0),
                }
            ]
        else:
            all_paths = self.connector.find(resolved_target)
            records = []
            for path in all_paths:
                info = self.fs.fs.info(path)
                records.append(
                    {
                        "path": path,
                        "name": Path(path).name,
                        "last_modified": info.get("mtime", 0.0),
                        "size": info.get("size", 0),
                    }
                )

        if not records:
            return pl.DataFrame(
                schema={
                    "path": pl.String,
                    "name": pl.String,
                    "last_modified": pl.Float64,
                    "size": pl.Int64,
                }
            )

        df = pl.DataFrame(records)

        # 2. Apply SQL WHERE filtering directly on the Polars DataFrame
        if sql_where:
            # SQL supports LIKE ('%2024-01-02%') or REGEXP_MATCHES(name, 'pattern')
            ctx = pl.SQLContext(files_meta=df)
            df = ctx.execute(f"SELECT * FROM files_meta WHERE {sql_where}").collect()

        return df

    @monitor(breaker)
    def parallelize(
        self,
        target: str,
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
        sql_where = kwargs.get("sql_where")
        # temp_folder = kwargs.get("temp_folder")
        # cleanup = kwargs.get("cleanup", True)
        # temp_dir = None

        # --- CASE 1: ARCHIVE FILES ---
        if self.connector.is_supported_archive(resolved_target):
            with self.connector.archive.open(resolved_target) as archive:
                archive_files = archive.list_contents()

            # Convert archive entries to Polars DataFrame for metadata filtering
            records = [
                {
                    "path": f"zip://{resolved_target}!!{row['filename']}",
                    "name": Path(row["filename"]).name,
                    "size": row["size"],
                    "last_modified": row.get("mtime", 0.0),
                }
                for row in archive_files
            ]
            df_meta = pl.DataFrame(records)

            # Apply SQL/Glob conditions
            if filter_condition:
                df_meta = df_meta.filter(
                    pl.col("name").str.contains(filter_condition.replace("*", ".*"))
                )

            if sql_where:
                ctx = pl.SQLContext(files_meta=df_meta)
                df_meta = ctx.execute(
                    f"SELECT * FROM files_meta WHERE {sql_where}"
                ).collect()

            files = df_meta["path"].to_list()
            total_bytes = df_meta["size"].sum() if not df_meta.is_empty() else 0

        # --- CASE 2: DIRECT FILESYSTEM PATHS ---
        else:
            df_meta = self._list_files_metadata(target, sql_where=sql_where)

            # Apply traditional glob filter if supplied
            if filter_condition and not df_meta.is_empty():
                regex_glob = filter_condition.replace(".", r"\.").replace("*", ".*")
                df_meta = df_meta.filter(pl.col("path").str.contains(regex_glob))

            files = df_meta["path"].to_list()
            total_bytes = df_meta["size"].sum() if not df_meta.is_empty() else 0

        if not files:
            raise TryAgainLater(
                reason=f"Resource not found or empty: {target}",
                service_name=self.name,
                wait_seconds=self._config.get("missing_file_retry_sec", 600),
            )

        num_workers = int(max(1, num_workers or (total_bytes // MIN_BYTES_PER_WORKER)))
        return self._balance_workload(files, min(num_workers, len(files)))

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
    def pull(
        self, unit: dict | list | str, **kwargs: Any
    ) -> Generator[pl.DataFrame, None, None]:
        """Stream data for a work unit as a single-chunk generator.

        File sources must fully materialise the LazyFrame (no native row-level
        streaming exists for filesystem reads), but yield it as one chunk so the
        caller can treat all Source implementations uniformly as generators.

        Args:
            unit: A file path string, list of paths, or a dict with a ``files`` key.

        Yields:
            pl.DataFrame: The fully collected dataset for this work unit.
        """
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

        # Delegate path formatting, archive staging, and query evaluation to FileReader
        lazy_frame = self.connector.data.extract(
            source=paths,
            sql_context=sql_context,
            temp_folder=temp_folder,
        )

        yield lazy_frame.collect()


@Service.register()
class FileSink(FileService, Sink):
    """File-based data sink."""

    @monitor(breaker)
    def stage(
        self,
        source_dir: Path,
        target: str,
        expected_count: int,
        file_format: str = "parquet",
        **kwargs,
    ) -> tuple[str, int]:
        """Phase 1: Write dataset into a temporary staging location."""
        staging_dir = f"tmp/staging/{target.rstrip('/')}_{int(time.time())}"
        resolved_source = self.connector.resolve(source_dir)
        resolved_staging = self.connector.resolve(staging_dir)

        LOG.info(
            f"Phase 1: Staging data from {resolved_source} -> {resolved_staging} (Target Format: {file_format})"
        )

        # 1. Discover source files
        source_files = self.connector.find(resolved_source)
        if not source_files:
            raise FileNotFoundError(f"No source files found at {resolved_source}")

        # Detect source format dynamically from the first file
        src_format = get_file_ext(source_files[0])
        tgt_format = file_format.lower().lstrip(".")

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

            # Destination path inside staging directory
            dst_file = f"{resolved_staging}/data_0.{tgt_format}"

            self.connector.data._convert_format(
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
        source: str,
        destination: str,
        expected_count: int,
        partition_on: str | None = None,
        partition_value: str | None = None,
    ) -> None:
        """Phase 2: Promote staged directory contents into production location atomically."""
        resolved_staging = self.connector.resolve(source)

        # 1. Resolve Target Destination & Hive Partition Pathing
        if partition_on and partition_value:
            final_target_path = (
                f"{destination.rstrip('/')}/{partition_on}={partition_value}"
            )
        else:
            final_target_path = destination

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
                    f"Target destination exists. Cleaning destination location prior to swap: {resolved_target}"
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
                LOG.debug(f"Cleaning residual source path: {resolved_staging}")
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


@Service.register()
class FileArchive(FileService, Archive):
    """Data archival service."""

    @monitor(breaker)
    def store(self, source: Path, dest: str) -> None:
        """Archive data to destination."""
        bucket = self.fs.url
        final_path = dest if "://" in dest else f"{bucket}/{dest}"
        self.connector.cp(str(source), final_path, recursive=True)
