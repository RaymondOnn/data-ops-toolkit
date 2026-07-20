"""CSV format handler."""

import io
import logging
from pathlib import Path
from typing import IO, Any, cast

import polars as pl

from .base import FormatHandler

LOG = logging.getLogger(__name__)


class CSVHandler(FormatHandler):
    """Handler for CSV and delimited text files."""

    @property
    def splittable(self) -> bool:
        """Check if CSV files are splittable.

        Returns:
            bool: True if CSV files are splittable, False otherwise.
        """
        return True

    def discover(self, path: Path | str, pattern: str | None = None) -> set[str]:
        """Discover CSV files.

        Args:
            path: The path to the CSV file.
            pattern: The pattern to match.

        Returns:
            set[str]: Set of discovered CSV files.
        """
        return self._glob_files(path, pattern, "**/*.[ct][sx][vt]")

    def to_df(self, path: Path | str, **kwargs: Any) -> pl.LazyFrame:
        """Convert CSV files to Polars LazyFrame.

        Args:
            path: The path to the CSV file.
            **kwargs: Additional keyword arguments.

        Returns:
            pl.LazyFrame: The LazyFrame containing the CSV data.
        """
        files = self.discover(path)
        if not files:
            return pl.LazyFrame()

        encoding = kwargs.get("encoding", "utf-8")
        has_header = kwargs.get("header", False)
        skip_blank_lines = kwargs.get("skip_blank_lines", False)

        lfs = []

        for f in files:
            size = self.fs.size(f)
            if size and size > 2 * 1024**3 and not self.options.get("force_repair"):
                # Use scan for large files (>2GB)
                lf = pl.scan_csv(
                    source=f,
                    has_header=has_header,
                    # storage_options=self.options,
                    encoding=encoding,
                )
                if skip_blank_lines:
                    lf = lf.filter(pl.any_horizontal(pl.all().is_not_null()))
                lfs.append(lf)
            else:
                buffer = self.read_raw(f, encoding=encoding)
                df = pl.read_csv(buffer)
                if skip_blank_lines:
                    df = df.filter(pl.any_horizontal(pl.all().is_not_null()))
                lfs.append(df.lazy())

        return pl.concat(lfs) if lfs else pl.LazyFrame()

    def from_df(self, df: pl.LazyFrame | pl.DataFrame, path: Path | str) -> None:
        """Write LazyFrame to CSV files.

        Args:
            df: The LazyFrame to write.
            path: The path to write the files to.
        """
        if isinstance(df, pl.LazyFrame):
            df.sink_csv(path)
        else:
            df.write_csv(path)

    def read_raw(self, path: Path | str, **kwargs: Any) -> io.BytesIO:
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
            with self.fs.open(f, "rb") as fp:
                raw = fp.read()

                # Strip BOM
                if raw.startswith(b"\xef\xbb\xbf"):
                    LOG.info(f"Stripping BOM from {f}")
                    raw = raw[3:]

                # Normalize encoding
                if encoding.lower() != "utf-8":
                    raw = raw.decode(encoding, errors="ignore")  # .encode("utf-8")

                combined += raw

        return io.BytesIO(combined)

    def write_raw(self, data: bytes, path: Path | str) -> None:
        """Write raw data to path.

        Args:
            data: The raw data to write.
            path: The path to write the data to.
        """
        with self.fs.open(path, "wb") as f:
            cast("IO[bytes]", f).write(data)
