import io
from pathlib import Path
from typing import Any

import polars as pl

from libs.file.formats.base import FormatHandler


class ParquetHandler(FormatHandler):
    def read_mem(self, input_file: Path, **kwargs: Any) -> io.BytesIO:
        """Parquet is binary; read directly into buffer."""
        with self.fs.open(input_file, "rb") as f:
            return io.BytesIO(f.read())

    def to_df(self, input_file: Path, **kwargs: Any) -> pl.LazyFrame:
        """
        Decision: Always use scan_parquet for 50M row performance.
        Returns a LazyFrame to allow for predicate pushdown and streaming.
        """
        return pl.scan_parquet(input_file, storage_options=self.opts)

    def from_df(self, df: pl.LazyFrame | pl.DataFrame, output_file: Path) -> None:
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

    def write_file(self, data: bytes, output_file: Path):
        with self.fs.open(output_file, "wb") as f:
            f.write(data)
