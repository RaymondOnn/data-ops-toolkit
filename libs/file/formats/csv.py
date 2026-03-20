import io
from pathlib import Path
from typing import Any

import polars as pl

from .base import FormatHandler


class CSVHandler(FormatHandler):
    def read_mem(self, input_file: Path, **kwargs: Any) -> io.BytesIO:
        """Strips BOM and handles encoding-safe reading."""
        encoding = kwargs.get("encoding", "utf-8")
        with self.fs.open(input_file, "rb") as f:
            raw_data = f.read()

            # Strip BOM if it exists
            if raw_data.startswith(b"\xef\xbb\xbf"):
                raw_data = raw_data[3:]

            # If encoding isn't UTF-8, we normalize it here to avoid
            # Polars' 'Invalid UTF-8' errors during read_csv
            if encoding.lower() != "utf-8":
                content = raw_data.decode(encoding, errors="ignore").encode("utf-8")
                return io.BytesIO(content)

            return io.BytesIO(raw_data)

    def to_df(self, input_file: Path, **kwargs: Any) -> pl.LazyFrame:
        size = self.fs.size(input_file)
        # Performance: Use scan_csv for files > 2GB to avoid OOM
        if size > 2 * 1024**3 and not kwargs.get("force_repair"):
            return pl.scan_csv(
                input_file,
                storage_options=self.opts,
                encoding=kwargs.get("encoding", "utf-8"),
            )

        buffer = self.read_mem(input_file, **kwargs)
        return pl.read_csv(buffer, encoding=kwargs.get("encoding", "utf-8")).lazy()

    def from_df(self, df: pl.LazyFrame | pl.DataFrame, output_file: Path) -> None:
        """Streaming write for 50M rows."""
        if isinstance(df, pl.LazyFrame):
            df.sink_csv(output_file)
        else:
            df.write_csv(output_file)

    def write_file(self, data: bytes, output_file: Path) -> None:
        with self.fs.open(output_file, "wb") as f:
            f.write(data)
