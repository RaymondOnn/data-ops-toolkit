"""CSV format handler."""

import io
import logging
from typing import Any

import polars as pl
from upath import UPath

from .base import FormatHandler

LOG = logging.getLogger(__name__)


@FormatHandler.register("csv")
class CSVHandler(FormatHandler):
    """Handler for CSV and delimited text files."""

    def discover(self, path: UPath | str, pattern: str | None = None) -> set[UPath]:
        """Discover CSV files.

        Args:
            path: The path to the CSV file.
            pattern: The pattern to match.

        Returns:
            set[UPath]: Set of discovered CSV files.
        """
        return self._glob_files(path, pattern, "**/*.[ct][sx][vt]")

    def to_df(self, path: UPath | str, **kwargs: Any) -> pl.LazyFrame:
        """Convert CSV files to Polars LazyFrame.

        Args:
            path: The path to the CSV file.
            **kwargs: Additional keyword arguments.

        Returns:
            pl.LazyFrame: The LazyFrame containing the CSV data.
        """
        files = [str(f) for f in self.discover(path)]
        if not files:
            return pl.LazyFrame()

        # Polars scan_csv handles UPath/fsspec paths natively
        return pl.scan_csv(
            files,
            has_header=kwargs.get("header", False),
            encoding=kwargs.get("encoding", "utf-8"),
            storage_options=self.options,
        )

    def from_df(self, df: pl.LazyFrame | pl.DataFrame, path: UPath | str) -> None:
        """Write LazyFrame to CSV files.

        Args:
            df: The LazyFrame to write.
            path: The path to write the files to.
        """
        dst = UPath(path)
        if isinstance(df, pl.LazyFrame):
            df.sink_csv(str(dst))
        else:
            df.write_csv(str(dst))

    def read_raw(self, path: UPath | str, **kwargs: Any) -> io.BytesIO:
        """Read raw data from CSV files.

        Args:
            path: The path to the CSV file.
            **kwargs: Additional keyword arguments.

        Returns:
            io.BytesIO: The raw data.
        """
        encoding = kwargs.get("encoding", "utf-8")
        files = self.discover(path)
        if not files:
            return io.BytesIO(b"")

        combined = b""
        for f in files:
            raw = f.read_bytes()

            # Strip BOM
            if raw.startswith(b"\xef\xbb\xbf"):
                LOG.info(f"Stripping BOM from {f}")
                raw = raw[3:]

            # Normalize encoding
            if encoding.lower() != "utf-8":
                raw = raw.decode(encoding, errors="ignore").encode("utf-8")

            combined += raw

        return io.BytesIO(combined)

    def write_raw(self, data: bytes, path: UPath | str) -> None:
        """Write raw data to path.

        Args:
            data: The raw data to write.
            path: The path to write the data to.
        """
        UPath(path).write_bytes(data)


FormatHandler.register_item("tsv", CSVHandler)
FormatHandler.register_item("txt", CSVHandler)
