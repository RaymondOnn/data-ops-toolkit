"""Parquet format handler."""

import io
import logging
from pathlib import Path
from typing import IO, Any, cast

import polars as pl

from .base import FormatHandler

LOG = logging.getLogger(__name__)


class ParquetHandler(FormatHandler):
    """Handler for Apache Parquet files."""

    @property
    def splittable(self) -> bool:
        """Check if Parquet files are splittable.

        Returns:
            bool: True if Parquet files are splittable, False otherwise.
        """
        return True

    def discover(self, path: Path | str, pattern: str | None = None) -> set[str]:
        """Discover Parquet files.

        Args:
            path: The path to the Parquet file.
            pattern: The pattern to match.

        Returns:
            set[str]: Set of discovered Parquet files.
        """
        return self._glob_files(path, pattern, "**/*.parquet")

    def to_df(self, path: Path | str, **kwargs: Any) -> pl.LazyFrame:
        """Convert Parquet files to Polars LazyFrame.

        Args:
            path: The path to the Parquet file.
            **kwargs: Additional keyword arguments.

        Returns:
            pl.LazyFrame: The LazyFrame containing the Parquet data.
        """
        files = list(self.discover(path))
        if not files:
            LOG.warning(f"No Parquet files found: {path}")
            return pl.LazyFrame()
        return pl.scan_parquet(list(files), storage_options=self.options)

    def from_df(self, df: pl.LazyFrame | pl.DataFrame, path: Path | str) -> None:
        """Write LazyFrame to Parquet files.

        Args:
            df: The LazyFrame to write.
            path: The path to write the files to.
        """
        if isinstance(df, pl.LazyFrame):
            df.sink_parquet(path, compression="snappy", row_group_size=100_000)
        else:
            df.write_parquet(path, compression="snappy")

    def read_raw(self, path: Path | str, **kwargs: Any) -> io.BytesIO:
        """Read raw data from path.

        Args:
            path: The path to the file.
            **kwargs: Additional keyword arguments.

        Returns:
            io.BytesIO: The raw data.
        """
        files = self.discover(path)
        if not files:
            return io.BytesIO(b"")

        combined = b""
        for f in files:
            with self.fs.open(f, "rb") as fp:
                data = fp.read()
                if isinstance(data, str):
                    data = data.encode(kwargs.get("encoding", "utf-8"))
                combined += data
        return io.BytesIO(combined)

    def write_raw(self, data: bytes, path: Path | str) -> None:
        """Write raw data to path.

        Args:
            data: The raw data to write.
            path: The path to write the data to.
        """
        with self.fs.open(path, "wb") as f:
            cast("IO[bytes]", f).write(data)
