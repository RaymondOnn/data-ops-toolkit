import io
import logging
import shutil
from pathlib import Path

import fsspec
import polars as pl
from libs.file.formats.factory import FormatFactory
from libs.file.utils import filter_files

LOG = logging.getLogger(__name__)


class FileMixin:
    """Mixin providing high-volume file I/O and explicit archive virtualization.

    Decision: Protocol Agnostic.
    By leveraging fsspec, this mixin allows the same ingestion logic to
    operate seamlessly across S3, Azure Blob, and Local filesystems.

    Decision: Explicit Discovery.
    We moved away from '::' string delimiters. The mixin now accepts explicit
    archive_path parameters, providing a cleaner API for structured
    configurations.
    """

    fs: fsspec.AbstractFileSystem
    opts: dict

    def resolve(self, path: str) -> str:
        raise NotImplementedError

    def is_archive(self, path: str) -> bool:
        raise NotImplementedError

    def extract_archive(self, path: str) -> str:
        raise NotImplementedError

    def is_readable(self, fs: fsspec.AbstractFileSystem, path: str) -> bool:
        """Check if file exists and is non-empty."""
        if not fs.exists(path):
            LOG.error(f"Missing file: {path}")
            return False

        size = fs.size(path)
        if size == 0:
            LOG.error(f"Zero-byte file: {path}")
            return False

        if size > 5 * 1024**3:
            LOG.warning(f"Large file (>5GB): {path}")
        return True

    def read_files(
        self,
        source: str | list[str],
        file_pattern: str | None = None,
        archive: str | None = None,
        repair: bool = False,
        **kwargs,
    ) -> pl.LazyFrame:
        """Read files into Polars LazyFrame."""

        temp_dir = None

        try:
            # Resolve targets
            if isinstance(source, list):
                fs, targets = self.fs, source
            # Handle archive files
            elif archive or self.is_archive(source):
                archive = archive or source
                temp_dir = self.extract_archive(archive)
                LOG.debug(f"Extracted archive: {temp_dir}")

                fs = self.fs
                # Find files in extracted directory
                full_path = self.resolve(temp_dir)
                if file_pattern:
                    all_files = fs.find(full_path)
                    targets = filter_files(all_files, file_pattern, temp_dir)
                else:
                    targets = [str(p) for p in Path(temp_dir).rglob("*") if p.is_file()]
            else:
                fs = self.fs
                mount_path = source
                full_path = self.resolve(mount_path)
                all_files = fs.find(full_path) if fs.isdir(full_path) else [full_path]
                targets = filter_files(all_files, file_pattern, source)

            frames = []
            for target in targets:
                if not self.is_readable(fs, target):
                    continue

                ext = Path(target).suffix.lstrip(".").lower()
                handler = FormatFactory.get(ext, fs, self.opts)

                encoding = "utf-8"
                if ext != "parquet":
                    _, encoding = self._detect_encoding(fs, target)

                frames.append(
                    handler.to_df(
                        target, encoding=encoding, force_repair=repair, **kwargs
                    )
                )

            return pl.concat(frames) if frames else pl.LazyFrame()

        finally:
            if temp_dir:
                shutil.rmtree(temp_dir, ignore_errors=True)

    def _detect_encoding(
        self, fs: fsspec.AbstractFileSystem, path: str
    ) -> tuple[io.IOBase, str]:
        """Detect file encoding using charset-normalizer."""
        from charset_normalizer import from_bytes

        with fs.open(path, "rb") as stream:
            sample = stream.read(4096)
            result = from_bytes(sample).best()
            encoding = result.encoding if result else "utf-8"
            return stream, encoding

    def split_by_size(
        self,
        path: str,
        pattern: str | None = None,
        batch_size_bytes: int = 1 * 1024**3,  # 1GB default
    ) -> list[list[str]]:
        """Split files into batches of approximately batch_size_bytes.

        Args:
            path: The directory or archive path to scan
            pattern: Optional glob pattern to filter files
            batch_size_bytes: Target size per batch in bytes (default 1GB)

        Returns:
            list[list[str]]: List of file batches
        """
        temp_dir = None
        try:
            # Extract archive if needed
            if self.is_archive(path):
                temp_dir = self.extract_archive(path)
                path = temp_dir

            # Get files with sizes
            files: list[tuple[str, int]] = [
                (f, self.fs.size(f))
                for f in self.fs.find(path)
                if not pattern or (Path(f).match(pattern) and self.fs.size(f) > 0)
            ]
            files.sort(key=lambda x: x[1], reverse=True)

            # Pack batches
            batches: list[list[str]] = []
            batch: list[str] = []
            size: int = 0
            for f, s in files:
                if s > batch_size_bytes and not batch:
                    batches.append([f])
                elif size + s > batch_size_bytes and batch:
                    batches.append(batch)
                    batch, size = [], 0
                    batch.append(f)
                    size = s
                else:
                    batch.append(f)
                    size += s
            if batch:
                batches.append(batch)

            return batches
        finally:
            if temp_dir:
                shutil.rmtree(temp_dir, ignore_errors=True)
