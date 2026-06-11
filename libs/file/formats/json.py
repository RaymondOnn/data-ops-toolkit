"""JSON/NDJSON format handler."""

import io
import logging
import re
from pathlib import Path
from typing import Any

import polars as pl

from .base import FormatHandler

LOG = logging.getLogger(__name__)


class JSONHandler(FormatHandler):
    """Handler for JSON and NDJSON files."""

    @property
    def splittable(self) -> bool:
        return False

    def discover(self, path: Path | str, pattern: str | None = None) -> set[str]:
        return self._glob_files(path, pattern, "**/*.json*")

    def to_df(self, path: Path | str, **kwargs: Any) -> pl.LazyFrame:
        files = self.discover(path)
        if not files:
            return pl.LazyFrame()

        # Check format (NDJSON vs JSON array)
        first_file = next(iter(files))
        is_ndjson = self._is_ndjson(first_file)

        if is_ndjson:
            return pl.scan_ndjson(
                list(files), storage_options=self.options, ignore_errors=True
            )

        # Standard JSON - read and repair
        lfs = []
        for f in files:
            size = self.fs.size(f)
            if size and size > 1.5 * 1024**3:
                LOG.warning(f"Large JSON file may cause OOM: {f}")

            buffer = self.read_raw(f, **kwargs)
            lfs.append(pl.read_json(buffer).lazy())

        return pl.concat(lfs) if lfs else pl.LazyFrame()

    def from_df(self, df: pl.LazyFrame | pl.DataFrame, path: Path | str) -> None:
        if isinstance(df, pl.LazyFrame):
            df.sink_ndjson(path)
        else:
            df.write_ndjson(path)

    def read_raw(self, path: Path | str, **kwargs: Any) -> io.BytesIO:
        encoding = kwargs.get("encoding", "utf-8")
        files = self.discover(path)
        if not files:
            return io.BytesIO(b"")

        combined = b""
        for f in files:
            with self.fs.open(f, "rb") as fp:
                raw = fp.read()
                if isinstance(raw, str):
                    raw = raw.encode(encoding)

                content = raw.decode(encoding, errors="ignore")
                # Fix trailing commas in JSON
                cleaned = re.sub(r",\s*([\]}])", r"\1", content)
                if len(cleaned) != len(content):
                    LOG.info(f"Fixed trailing commas in {f}")

                combined += cleaned.encode(encoding)

        return io.BytesIO(combined)

    def write_raw(self, data: bytes, path: Path | str) -> None:
        with self.fs.open(path, "wb") as f:
            f.write(data)

    def _is_ndjson(self, path: str) -> bool:
        """Check if file is NDJSON format."""
        if any(path.endswith(ext) for ext in [".ndjson", ".jsonl"]):
            return True

        # Peek at first non-whitespace character
        try:
            with self.fs.open(path, "rb") as f:
                chunk = f.read(1024).strip()
                return chunk and chunk[0:1] == b"{"
        except (OSError, UnicodeError):
            # File access or decoding issues - assume not NDJSON
            return False
