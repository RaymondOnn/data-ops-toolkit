import io

import polars as pl

from libs.clients.fs.format.base import FormatHandler




class ParquetHandler(FormatHandler):
    def read_mem(self, target: str, **kwargs) -> io.BytesIO:
        """Parquet is binary; read directly into buffer."""
        with self.fs.open(target, "rb") as f:
            return io.BytesIO(f.read())

    def to_df(self, target: str, **kwargs) -> pl.LazyFrame:
        # Always use scan_parquet for 50M row performance
        return pl.scan_parquet(target, storage_options=self.opts)

    def from_df(self, lf: pl.LazyFrame, target: str):
        """Highly optimized streaming sink."""
        lf.sink_parquet(target, compression="snappy", row_group_size=100_000)

    def write_file(self, data: bytes, target: str):
        with self.fs.open(target, "wb") as f:
            f.write(data)