"""Parquet format handler."""

import io
import logging
from typing import Any

import polars as pl
import pyarrow.dataset as ds
from upath import UPath

from .base import FormatHandler

LOG = logging.getLogger(__name__)


@FormatHandler.register("parquet")
class ParquetHandler(FormatHandler):
    """Handler for Apache Parquet files."""

    def discover(self, path: UPath | str, pattern: str | None = None) -> set[UPath]:
        """Discover Parquet files.

        Args:
            path: The path to the Parquet file.
            pattern: The pattern to match.

        Returns:
            set[str]: Set of discovered Parquet files.
        """
        return self._glob_files(path, pattern, "**/*.parquet")

    def to_df(
        self, path: UPath | str, pattern: str | None = None, **kwargs: Any
    ) -> pl.LazyFrame:
        """Convert Parquet files or directories to a Polars LazyFrame.

        Args:
            path: Target directory or file path.
            pattern: Optional glob pattern for file discovery (e.g. "**/*.parquet").
            **kwargs: Overrides forwarded directly to `pl.scan_parquet`
                (e.g., hive_partitioning=True, low_memory=True, row_index_name="__row_id").

        Returns:
            pl.LazyFrame: Polars LazyFrame query plan.
        """
        target = UPath(path)

        # 1. Merge defaults, handler options, and kwargs
        scan_opts: dict[str, Any] = {
            "hive_partitioning": True,  # Default to True
            **self.options,
            **kwargs,
        }

        # 2. Extract storage options safely
        if (
            "storage_options" not in scan_opts
            and hasattr(self, "options")
            and (storage_opts := self.options.get("storage_options"))
        ):
            scan_opts["storage_options"] = storage_opts

        # 3. Strip write-only keys
        for write_only_key in ("partition_cols", "compression", "max_rows_per_file"):
            scan_opts.pop(write_only_key, None)

        # 4. Handle single file vs directory scanning
        if target.is_file():
            # If target is a single file, turn off hive partitioning unless explicitly overridden
            scan_opts.setdefault("hive_partitioning", False)
            return pl.scan_parquet(str(target), **scan_opts)

        glob_pattern = pattern or "**/*.parquet"
        scan_path = str(target / glob_pattern)

        if not self.discover(path, pattern=pattern):
            LOG.warning(
                f"No Parquet files found matching '{glob_pattern}' under: {path}"
            )
            return pl.LazyFrame()

        # Pass all kwargs (including hive_partitioning) directly to scan_parquet
        return pl.scan_parquet(scan_path, **scan_opts)

    def from_df(self, df: pl.LazyFrame | pl.DataFrame, path: UPath | str) -> None:
        """Write LazyFrame to Parquet files.

        Args:
            df: The LazyFrame to write.
            path: The path to write the files to.
        """

        resolved_dst = UPath(path)
        eager_df = df.collect() if isinstance(df, pl.LazyFrame) else df

        partition_cols = self.options.get("partition_cols", [])
        compression = self.options.get("compression", "snappy")

        # Only extract max_rows_per_file if explicitly configured by the user
        max_rows_per_file = self.options.get("max_rows_per_file")

        write_kwargs = {
            "data": eager_df.to_arrow(),
            "base_dir": str(resolved_dst),
            "format": "parquet",
            "filesystem": resolved_dst.fs,
            "partitioning": partition_cols,
            "file_options": ds.ParquetFileFormat().make_write_options(
                compression=compression
            ),
            "existing_data_behavior": "overwrite_or_ignore",
        }

        # Dynamically append file/group constraints only when explicitly requested
        if max_rows_per_file:
            write_kwargs["max_rows_per_file"] = max_rows_per_file
            write_kwargs["max_rows_per_group"] = min(1_048_576, max_rows_per_file)

        ds.write_dataset(**write_kwargs)

    def read_raw(self, path: UPath | str, **kwargs: Any) -> io.BytesIO:
        files = self.discover(path)
        if not files:
            return io.BytesIO(b"")

        combined = b""
        for f in files:
            combined += f.read_bytes()

        return io.BytesIO(combined)

    def write_raw(self, data: bytes, path: UPath | str) -> None:
        """Read raw data from path.

        Args:
            path: The path to the file.
            **kwargs: Additional keyword arguments.

        Returns:
            io.BytesIO: The raw data.
        """
        UPath(path).write_bytes(data)
