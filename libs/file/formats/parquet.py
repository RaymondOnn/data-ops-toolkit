import io

import polars as pl

from libs.clients.fs.format.base import FormatHandler




class ParquetHandler(FormatHandler):
    def read_mem(self, target: str, **kwargs) -> io.BytesIO:
        """Parquet is binary; read directly into buffer."""
        with self.fs.open(target, "rb") as f:
            return io.BytesIO(f.read())

    def to_df(self, target: str, **kwargs) -> pl.LazyFrame:
        """
        Decision: Always use scan_parquet for 50M row performance.
        Returns a LazyFrame to allow for predicate pushdown and streaming.
        """
        return pl.scan_parquet(target, storage_options=self.opts)

    def from_df(self, df: pl.LazyFrame | pl.DataFrame, target: str) -> None:
        """
        Decision: Execution-Aware Sink.
        1. If LazyFrame: Use .sink_parquet() for memory-efficient streaming.
        2. If DataFrame: Use .write_parquet() for Ray worker chunks.
        """
        if isinstance(df, pl.LazyFrame):
            df.sink_parquet(
                target, 
                maintain_order=False, # Faster performance
                compression="snappy", 
                row_group_size=100_000 # Optimized for 2GB RAM
            )
        else:
            df.write_parquet(target, compression="snappy")

    def write_file(self, data: bytes, target: str):
        with self.fs.open(target, "wb") as f:
            f.write(data)
            
            