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
        return True

    def discover(self, path: Path | str, pattern: str | None = None) -> set[str]:
        return self._glob_files(path, pattern, "**/*.[ct][sx][vt]")

    def to_df(self, path: Path | str, **kwargs: Any) -> pl.LazyFrame:
        files = self.discover(path)
        if not files:
            return pl.LazyFrame()

        encoding = kwargs.get("encoding", "utf-8")
        lfs = []

        for f in files:
            size = self.fs.size(f)
            # Use scan for large files (>2GB)
            if size and size > 2 * 1024**3 and not kwargs.get("force_repair"):
                lfs.append(
                    pl.scan_csv(f, storage_options=self.options, encoding=encoding)
                )
            else:
                buffer = self.read_raw(f, encoding=encoding)
                lfs.append(pl.read_csv(buffer).lazy())

        return pl.concat(lfs) if lfs else pl.LazyFrame()

    def from_df(self, df: pl.LazyFrame | pl.DataFrame, path: Path | str) -> None:
        if isinstance(df, pl.LazyFrame):
            df.sink_csv(path)
        else:
            df.write_csv(path)

    def read_raw(self, path: Path | str, **kwargs: Any) -> io.BytesIO:
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
                    raw = raw.decode(encoding, errors="ignore").encode("utf-8")

                combined += raw

        return io.BytesIO(combined)

    def write_raw(self, data: bytes, path: Path | str) -> None:
        with self.fs.open(path, "wb") as f:
            cast("IO[bytes]", f).write(data)
