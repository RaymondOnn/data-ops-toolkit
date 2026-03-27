import io
import logging
from pathlib import Path
from typing import IO, Any, cast

import polars as pl

from libs.file.formats.base import FormatHandler

LOG = logging.getLogger(__name__)


class ParquetHandler(FormatHandler):
    def discover(self, input_path: Path | str) -> list[str]:
        """Expands a path into a list of Parquet files."""
        path_str = str(input_path)

        # If the path already contains a wildcard, expand it directly
        if "*" in path_str:
            return [
                str(self.fs.unstrip_protocol(p))
                for p in self.fs.glob(path_str)
                if self.fs.isfile(p)
            ]

        if self.fs.isfile(path_str):
            return [path_str]

        pattern = f"{path_str.rstrip('/')}/**/*.parquet"
        return [
            str(self.fs.unstrip_protocol(p))
            for p in self.fs.glob(pattern)
            if self.fs.isfile(p)
        ]

    def read_file(self, input_path: Path | str, **kwargs: Any) -> io.BytesIO:
        """Parquet is binary; read directly into buffer."""
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
        Decision: Always use scan_parquet for 50M row performance.
        Returns a LazyFrame to allow for predicate pushdown and streaming.
        """
        paths = self.discover(input_path)
        if not paths:
            LOG.warning(f"No files discovered for path: {input_path}")
            return pl.LazyFrame()

        LOG.debug(f"ParquetHandler scanning paths. Found {len(paths)} files.")
        # Polars scan_parquet handles list of paths natively and efficiently
        return pl.scan_parquet(paths, storage_options=self.opts)

    def from_df(self, df: pl.LazyFrame | pl.DataFrame, output_file: Path | str) -> None:
        """
        Decision: Execution-Aware Sink.
        1. If LazyFrame: Use .sink_parquet() for memory-efficient streaming.
        2. If DataFrame: Use .write_parquet() for Ray worker chunks.
        """
        if isinstance(df, pl.LazyFrame):
            df.sink_parquet(
                output_file,
                maintain_order=False,  # Faster performance
                compression="snappy",
                row_group_size=100_000,  # Optimized for 2GB RAM
            )
        else:
            df.write_parquet(output_file, compression="snappy")

    def write_file(self, data: bytes, output_file: Path | str):
        with self.fs.open(output_file, "wb") as f:
            # Cast f to an IO[bytes] so Ty knows .write() accepts bytes
            cast("IO[bytes]", f).write(data)
