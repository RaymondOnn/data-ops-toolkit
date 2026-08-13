"""Parquet format handler."""

import io
import logging
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow.dataset as ds

from .base import FormatHandler

LOG = logging.getLogger(__name__)


class ParquetHandler(FormatHandler):
    """Handler for Apache Parquet files."""

    splittable = True

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

    def write(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        target_path: str,
        partition_cols: list[str] | None = None,
        max_rows_per_file: int = 500_000,
        compression: str = "zstd",
    ) -> None:
        """
        Writes a Polars DataFrame out to disk/cloud storage using PyArrow Dataset
        for advanced Hive-style partitioning and file size rolling.
        """
        resolved_dst = self.fs.resolve(target_path)

        # Ensure evaluation results strictly in a DataFrame
        eager_df: pl.DataFrame
        if isinstance(df, pl.LazyFrame):
            res = df.collect()
            eager_df = res.to_frame() if isinstance(res, pl.Series) else res
        elif isinstance(df, pl.Series):
            eager_df = df.to_frame()
        else:
            eager_df = df

        arrow_table = eager_df.to_arrow()

        # Write partitioned dataset via PyArrow
        ds.write_dataset(
            data=arrow_table,
            base_dir=resolved_dst,
            format="parquet",
            filesystem=self.fs.fs,
            partitioning=partition_cols,
            max_rows_per_file=max_rows_per_file,  # Auto-rolls large files!
            file_options=ds.ParquetFileFormat().make_write_options(
                compression=compression
            ),
            existing_data_behavior="overwrite_or_ignore",
        )

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
