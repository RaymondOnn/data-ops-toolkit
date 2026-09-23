"""JSON/NDJSON format handler."""

import io
import logging
import re
from typing import Any

import polars as pl
from upath import UPath

from .base import FormatHandler

LOG = logging.getLogger(__name__)


@FormatHandler.register("json")
class JSONHandler(FormatHandler):
    """Handler for JSON and NDJSON files."""

    def discover(self, path: UPath | str, pattern: str | None = None) -> set[UPath]:
        """Discover JSON files.

        Args:
            path: The path to the JSON file.
            pattern: The pattern to match.

        Returns:
            set[UPath]: Set of discovered JSON files.
        """
        return self._glob_files(path, pattern, "**/*.json*")

    def to_df(self, path: UPath | str, **kwargs: Any) -> pl.LazyFrame:
        """Convert JSON files to Polars LazyFrame.

        Args:
            path: The path to the JSON file.
            **kwargs: Additional keyword arguments.

        Returns:
            pl.LazyFrame: The LazyFrame containing the JSON data.
        """
        files = sorted(self.discover(path))
        if not files:
            return pl.LazyFrame()

        first_file = files[0]
        if self._is_ndjson(first_file):
            return pl.scan_ndjson([str(f) for f in files], storage_options=self.options)

        # Standard JSON array read
        dfs = [pl.read_json(str(f)) for f in files]
        return pl.concat([df.lazy() for df in dfs]) if dfs else pl.LazyFrame()

    def from_df(self, df: pl.LazyFrame | pl.DataFrame, path: UPath | str) -> None:
        """Write LazyFrame to JSON files.

        Args:
            df: The LazyFrame to write.
            path: The path to write the files to.
        """
        dst = UPath(path)
        eager_df = df.collect() if isinstance(df, pl.LazyFrame) else df
        eager_df.write_json(str(dst))

    def read_raw(self, path: UPath | str, **kwargs: Any) -> io.BytesIO:
        """Read raw data from path.

        Args:
            path: The path to the file.
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
            content = raw.decode(encoding, errors="ignore")
            cleaned = re.sub(r",\s*([\]}])", r"\1", content)
            combined += cleaned.encode(encoding)

        return io.BytesIO(combined)

    def write_raw(self, data: bytes, path: UPath | str) -> None:
        """Write raw data to path.

        Args:
            data: The raw data to write.
            path: The path to write the data to.
        """
        UPath(path).write_bytes(data)

    def _is_ndjson(self, file_path: UPath) -> bool:
        """Check if file is NDJSON format.

        Args:
            path: The path to the file.

        Returns:
            bool: True if the file is NDJSON, False otherwise.
        """
        if file_path.suffix in [".ndjson", ".jsonl"]:
            return True

        try:
            with file_path.open("rb") as f:
                chunk = f.read(2048).strip()
                return chunk.startswith(b"{")
        except (OSError, UnicodeError):
            return False


FormatHandler.register_item("jsonl", JSONHandler)
FormatHandler.register_item("ndjson", JSONHandler)
