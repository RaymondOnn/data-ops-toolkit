"""XML format handler."""

import io
import logging
import re
from pathlib import Path
from typing import IO, Any, cast

import polars as pl
import xmltodict

from .base import FormatHandler

LOG = logging.getLogger(__name__)


class XMLHandler(FormatHandler):
    """Handler for XML files."""

    def discover(self, path: Path | str, pattern: str | None = None) -> set[str]:
        return self._glob_files(path, pattern, "**/*.xml")

    def to_df(self, path: Path | str, **kwargs: Any) -> pl.LazyFrame:
        files = self.discover(path)
        if not files:
            return pl.LazyFrame()

        lfs = []
        for f in files:
            buffer = self.read_raw(f, **kwargs)
            data = xmltodict.parse(buffer.read())
            lfs.append(pl.DataFrame(data).lazy())

        return pl.concat(lfs) if lfs else pl.LazyFrame()

    def from_df(self, df: pl.LazyFrame | pl.DataFrame, path: Path | str) -> None:
        raise NotImplementedError("XML write not supported")

    def read_raw(self, path: Path | str, **kwargs: Any) -> io.BytesIO:
        encoding = kwargs.get("encoding", "utf-8")
        files = self.discover(path)
        if not files:
            return io.BytesIO(b"")

        combined = b""
        for f in files:
            with self.fs.open(f, "rb") as fp:
                raw = fp.read().decode(encoding, errors="ignore")
                # Strip illegal XML characters (0-31 except \t, \n, \r)
                cleaned = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", raw)
                if len(cleaned) != len(raw):
                    LOG.info(f"Sanitized XML: {f}")
                combined += cleaned.encode("utf-8")

        return io.BytesIO(combined)

    def write_raw(self, data: bytes, path: Path | str) -> None:
        with self.fs.open(path, "wb") as f:
            cast("IO[bytes]", f).write(data)
