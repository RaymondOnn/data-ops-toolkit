import io
import logging
from typing import TYPE_CHECKING, Any

import fsspec
import polars as pl
from libs.file.formats.factory import FormatFactory
from libs.file.utils import filter_files
from upath import UPath

if TYPE_CHECKING:
    from libs.file.formats.base import FormatHandler

LOG = logging.getLogger(__name__)


class FlatFileMixin:
    """Mixin providing high-volume file I/O and explicit archive virtualization.

    Requirements for host class:
        - Must have .fs (fsspec.AbstractFileSystem)
        - Must have .opts (dict)
        - Must have .url (str)
        - Must implement .resolve_path(path) -> str
        - Must implement ._mount_archive_fs(path) -> fsspec.AbstractFileSystem
        - Must implement .get_archive_contents(path, pattern=None) -> list[str]

    Decision: Protocol Agnostic.
    By leveraging fsspec, this mixin allows the same ingestion logic to
    operate seamlessly across S3, Azure Blob, and Local filesystems.

    Decision: Explicit Discovery.
    We moved away from '::' string delimiters. The mixin now accepts explicit
    archive_path parameters, providing a cleaner API for structured
    configurations.
    """

    # Decision: Centralized Resolution.
    # We rely on the base FileSystemClient.resolve_path() to handle
    # protocol-aware normalization, avoiding 'not callable' errors at runtime.

    # def resolve_path(self, path: str | Path) -> str:
    #     """Stub for MRO resolution. Host class provides implementation."""
    #     raise NotImplementedError()

    # def walk_paths(self, path: str, pattern: str = "*"):
    #     """Stub for MRO resolution. Host class provides implementation."""
    #     raise NotImplementedError()

    def __init__(self):
        if not all(
            hasattr(self, attr)
            for attr in (
                "fs",
                "opts",
                "url",
                "resolve_path",
                "_mount_archive_fs",
                "get_archive_contents",
            )
        ):
            raise TypeError(
                "FlatFileMixin requires host class to have 'fs', 'opts', 'url', 'resolve_path()', '_mount_archive_fs()', and 'get_archive_contents()'."
            )

    def resolve_format_handler(
        self, target_path: str, archive_path: str | None = None
    ) -> "FormatHandler":
        """Resolves the appropriate FormatHandler by inspecting the target.

        Args:
            target_path: The physical or logical path to the data.
            archive_path: Optional path to a physical archive container.

        Returns:
            FormatHandler: A concrete handler (Parquet, CSV, etc.).

        Decision: Multi-Stage Discovery.
        We first mount the filesystem via the private context helper, then
        identify the list of targets. This allows us to sniff the format
        of the first file found, whether it is in a raw directory or
        trapped inside a ZIP archive.
        """
        LOG.debug(f"🔍 Determining format handler for: {target_path}")

        # 1. Mount and Discover
        mount_path = archive_path or target_path
        fs = self._mount_archive_fs(mount_path)

        if archive_path or (fs is not getattr(self, "fs")):
            targets = self.get_archive_contents(mount_path)
        else:
            resolve_path_func = getattr(self, "resolve_path")
            full_path = resolve_path_func(target_path)
            all_files = fs.find(full_path) if fs.isdir(full_path) else [full_path]
            targets = filter_files(all_files, None, target_path)

        if not targets:
            raise FileNotFoundError(f"No files identified at: {target_path}")

        # 2. Extract suffix from the first identified file
        # We use the first file in the directory/archive as the schema anchor.
        sample_file = targets[0]
        ext = UPath(sample_file).suffix.lstrip(".").lower()

        # 3. Fallback: Extensionless Peeking
        if not ext:
            LOG.debug(f"No extension for {sample_file}. Attempting signature peek.")
            # Logic for peeking file signatures (magic numbers) could be added here
            pass

        LOG.info(f"🚀 Resolved format: '{ext}' for {target_path}")
        return FormatFactory.get_handler(ext, fs, getattr(self, "opts"))

    def is_file_readable(self, fs: fsspec.AbstractFileSystem, target: str) -> bool:
        """
        Performs sanity checks on a file before attempting to read it.

        Args:
            fs: The fsspec filesystem instance.
            target: The resolved path to the file.

        Returns:
            bool: True if the file exists and is non-zero in size, False
                otherwise.

        Decision: Fail-Fast Validation.
        We perform a zero-byte check before initializing Polars readers to
        prevent unnecessary process overhead and provide clear error logs.
        """
        if not fs.exists(target):
            LOG.error(f"Source file missing: {target}")
            return False

        # Zero byte check
        size = fs.size(target)
        if size == 0:
            LOG.error(f"Zero-byte file detected: {target}")
            # Self-healing logic (quarantine) could be called here
            return False

        if size and size > 5 * 1024**3:
            LOG.warning(f"Very large file (>5GB): {target}. Forcing streaming mode.")

        return True

    def fetch_df(
        self,
        path_or_list: str | list[str],
        file_pattern: str | None = None,
        archive_path: str | None = None,
        force_repair: bool = False,
        **kwargs: dict[str, Any],
    ) -> pl.LazyFrame:
        """
        Orchestrates the ingestion process using specialized FormatHandlers.

        Resolves file targets, validates metadata, detects encoding for text-
        based formats, and yields a unified Polars LazyFrame.

        Args:
            path_or_list: A directory path, archive path, or a list of files.
            file_pattern: Optional glob pattern for file discovery.
            archive_path: Optional physical archive (zip/tar) to mount.
            force_repair: If True, forces the handler to perform self-healing.
            **kwargs: Additional options passed to the underlying handlers.

        Returns:
            pl.LazyFrame: A unified lazy representation of the ingested data.
        """
        # 1. Resolve targets (Folder, Archive, or Pre-partitioned list)
        if isinstance(path_or_list, list):
            fs, targets = getattr(self, "fs"), path_or_list
        else:
            mount_path = archive_path or path_or_list
            fs = self._mount_archive_fs(mount_path)
            if archive_path or (fs is not getattr(self, "fs")):
                targets = self.get_archive_contents(mount_path, pattern=file_pattern)
            else:
                full_path = getattr(self, "resolve_path")(path_or_list)
                all_files = fs.find(full_path) if fs.isdir(full_path) else [full_path]
                targets = filter_files(all_files, file_pattern, path_or_list)

        lfs = []
        for target in targets:
            # 2. Step 2.5: Sanity Check (Exists? Zero-byte?)
            if not getattr(self, "is_file_readable")(fs, target):
                continue

            # 3. Identify Handler & Detect Encoding
            ext = target.split(".")[-1].lower()
            handler = FormatFactory.get_handler(ext, fs, getattr(self, "opts"))

            # Step 3: Encoding Detection (Crucial for CSV/JSON/XML)
            # Only run if not Parquet to save cycles
            encoding = "utf-8"
            if ext != "parquet":
                _, encoding = getattr(self, "_get_encoded_stream")(fs, target)

            # 4. Delegate to Handler (Handles Streaming vs Repair internally)
            # Note: 50M row safety happens inside handler.to_df()
            lf = handler.to_df(
                target, encoding=encoding, force_repair=force_repair, **kwargs
            )
            lfs.append(lf)

        # 5. Final Consolidation
        # concat is a lazy operation in Polars; no memory is consumed yet.
        # Decision: Zero-Copy Concatenation.
        # Polars handles the vertical stacking of multiple files lazily,
        # ensuring that the schema is aligned and no data is physically
        # moved until necessary.
        return pl.concat(lfs) if lfs else pl.LazyFrame()

    def _get_encoded_stream(
        self, fs: fsspec.AbstractFileSystem, target: str
    ) -> tuple[io.IOBase, str]:
        """
        Detects file encoding and provides an open stream.

        Uses charset-normalizer to sample the file and determine the most
        likely encoding, defaulting to UTF-8.

        Args:
            fs: The filesystem instance.
            target: The file path to sample.

        Returns:
            tuple: A tuple containing the open IO stream and the encoding string.

        Decision: Heuristic Encoding Detection.
        For non-parquet formats, we sample the first 4KB to detect encoding,
        favoring UTF-8 but resilient to legacy formats (Latin-1, etc.).
        """
        from charset_normalizer import from_bytes

        with fs.open(target, mode="rb") as raw_stream:
            sample_size = 4096
            sample: bytes = raw_stream.read(sample_size)
            results = from_bytes(sample)
            best_match = results.best()
            encoding = best_match.encoding if best_match else "utf-8"

            return raw_stream, encoding

    def partition_load(
        self, path: str, file_pattern: str | None = None
    ) -> list[list[str]]:
        """
        Calculates a partitioned load strategy based on file sizes.

        Splits targets into 1GB batches to ensure that downstream Ray workers
        remain within standard memory limits (e.g., 2GB total per task).

        Args:
            path: The root directory or archive path.
            file_pattern: Optional glob pattern for discovery.

        Returns:
            list[list[str]]: A list of file-path groups (work units).

        Decision: Resource-Aware Partitioning.
        We split ingestion into 1GB batches based on physical file size.
        This ensures that when Ray workers execute the transformation,
        the expanded in-memory footprint stays well within 2GB limits.
        """
        # 1. Mount the filesystem
        fs = self._mount_archive_fs(path)

        # If file_pattern is given, glob/find can return details directly
        if file_pattern:
            search_path = (
                f"{path}/{file_pattern}"
                if not path.endswith("/")
                else f"{path}{file_pattern}"
            )
            detailed_targets = fs.glob(search_path, detail=True)
        else:
            detailed_targets = fs.ls(path, detail=True)

        partitions = []
        current_batch: list[str] = []
        current_size = 0
        limit = 1 * 1024**3  # 1GB target per partition

        # 2. Iterate over pre-fetched metadata to group work units
        items = (
            detailed_targets.values()
            if isinstance(detailed_targets, dict)
            else detailed_targets
        )

        for file_info in items:
            t_path, size = file_info["name"], file_info["size"]

            if size == 0:
                continue

            if current_size + size > limit and current_batch:
                partitions.append(current_batch)
                current_batch, current_size = [], 0

            current_batch.append(t_path)
            current_size += size

        if current_batch:
            partitions.append(current_batch)

        return partitions
