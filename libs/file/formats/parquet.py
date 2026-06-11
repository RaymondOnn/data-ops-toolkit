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
        return True

    def discover(self, path: Path | str, pattern: str | None = None) -> set[str]:
        return self._glob_files(path, pattern, "**/*.parquet")

    def to_df(self, path: Path | str, **kwargs: Any) -> pl.LazyFrame:
        files = list(self.discover(path))
        if not files:
            LOG.warning(f"No Parquet files found: {path}")
            return pl.LazyFrame()
        return pl.scan_parquet(list(files), storage_options=self.options)

    def from_df(self, df: pl.LazyFrame | pl.DataFrame, path: Path | str) -> None:
        if isinstance(df, pl.LazyFrame):
            df.sink_parquet(path, compression="snappy", row_group_size=100_000)
        else:
            df.write_parquet(path, compression="snappy")

    def read_raw(self, path: Path | str, **kwargs: Any) -> io.BytesIO:
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
        with self.fs.open(path, "wb") as f:
            cast("IO[bytes]", f).write(data)
