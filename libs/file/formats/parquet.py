import io
import logging
from pathlib import Path
from typing import IO, Any, cast

import polars as pl

from libs.file.formats.base import FormatHandler

LOG = logging.getLogger(__name__)


class ParquetHandler(FormatHandler):
    """
    Handles reading and writing data in Apache Parquet format.

    Parquet is the preferred format for the toolkit due to its columnar
    storage and support for predicate pushdown via Polars.
    """

    @property
    def is_splittable(self) -> bool:
        """
        Indicates that Parquet is a splittable format.

        Returns:
            bool: Always True for Parquet.
        """
        return True

    def discover(self, input_path: Path | str, pattern: str | None = None) -> set[str]:
        """
        Expands a path into a list of Parquet files.

        Args:
            input_path: The base path or directory to search.
            pattern: Optional glob pattern to filter discovered files.

        Returns:
            set[str]: A set of fully qualified paths to discovered Parquet files.
        """
        # Standardize the path by stripping the protocol if present
        # so fsspec doesn't treat it as relative to CWD.
        path_str = self.fs._strip_protocol(str(input_path))

        if pattern:
            path_str = f"{path_str.rstrip('/')}/{pattern.lstrip('/')}"

        # If the path already contains a wildcard, expand it directly
        if "*" in path_str:
            return {
                str(self.fs.unstrip_protocol(str(p)))
                for p in self.fs.glob(path_str)
                if self.fs.isfile(p)
            }

        if self.fs.isfile(path_str):
            return {str(self.fs.unstrip_protocol(path_str))}

        pattern = f"{path_str.rstrip('/')}/**/*.parquet" if not pattern else path_str
        return {
            str(self.fs.unstrip_protocol(str(p)))
            for p in self.fs.glob(pattern)
            if self.fs.isfile(p)
        }

    def read(self, input_path: Path | str, **kwargs: Any) -> io.BytesIO:
        """
        Reads raw Parquet bytes directly into an in-memory buffer.

        Note: For data processing, use `to_df` as it leverages Polars'
        streaming capabilities instead of loading raw bytes.

        Args:
            input_path: The path to the file(s) to read.
            **kwargs: Additional options, including 'encoding'.

        Returns:
            io.BytesIO: An in-memory buffer containing the combined bytes.
        """
        paths = self.discover(input_path)
        if not paths:
            LOG.warning(f"No files discovered for path: {input_path}")
            return io.BytesIO(b"")

        combined = b""
        for p in paths:
            with self.fs.open(p, "rb") as f:
                data = f.read()
                if isinstance(data, str):
                    data = data.encode(kwargs.get("encoding", "utf-8"))
                combined += data
        return io.BytesIO(combined)

    def to_df(self, input_path: Path | str, **kwargs: Any) -> pl.LazyFrame:
        """
        Reads Parquet files into a unified Polars LazyFrame.

        Utilizes `pl.scan_parquet` to enable predicate pushdown and memory-
        efficient streaming for large datasets.

        Args:
            input_path: The path to the file(s) or directory to read.
            **kwargs: Additional options passed to `pl.scan_parquet`.

        Returns:
            pl.LazyFrame: A Polars LazyFrame representing the combined data.
        """
        paths = self.discover(input_path)
        if not paths:
            LOG.warning(f"No files discovered for path: {input_path}")
            return pl.LazyFrame()

        LOG.debug(f"ParquetHandler scanning paths. Found {len(paths)} files.")
        # Polars scan_parquet handles list of paths natively and efficiently
        return pl.scan_parquet(list(paths), storage_options=self.opts)

    def from_df(self, df: pl.LazyFrame | pl.DataFrame, output_path: Path | str) -> None:
        """
        Writes a Polars DataFrame or LazyFrame to the specified output path.

        Uses `sink_parquet` for LazyFrames to enable streaming and
        `write_parquet` for eager DataFrames.

        Args:
            df: The Polars DataFrame or LazyFrame to write.
            output_path: The destination path for the output file.
        """
        if isinstance(df, pl.LazyFrame):
            df.sink_parquet(
                output_path,
                maintain_order=False,  # Faster performance
                compression="snappy",
                row_group_size=100_000,  # Optimized for 2GB RAM
            )
        else:
            df.write_parquet(output_path, compression="snappy")

    def write(self, data: bytes, output_path: Path | str):
        """
        Writes raw bytes directly to the specified output path.

        Args:
            data: The bytes object to write.
            output_path: The destination path for the file.
        """
        with self.fs.open(output_path, "wb") as f:
            # Cast f to an IO[bytes] so Ty knows .write() accepts bytes
            cast("IO[bytes]", f).write(data)
