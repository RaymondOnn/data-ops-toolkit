"""JSON/NDJSON format handler."""

import io
import logging
import re
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow.dataset as ds

from .base import FormatHandler

LOG = logging.getLogger(__name__)


class JSONHandler(FormatHandler):
    """Handler for JSON and NDJSON files."""

    @property
    def splittable(self) -> bool:
        """Check if JSON files are splittable.

        Returns:
            bool: True if JSON files are splittable, False otherwise.
        """
        return False

    def discover(self, path: Path | str, pattern: str | None = None) -> set[str]:
        """Discover JSON files.

        Args:
            path: The path to the JSON file.
            pattern: The pattern to match.

        Returns:
            set[str]: Set of discovered JSON files.
        """
        return self._glob_files(path, pattern, "**/*.json*")

    def to_df(self, path: Path | str, **kwargs: Any) -> pl.LazyFrame:
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

        # Check format (NDJSON vs JSON array)
        first_file = next(iter(files))
        is_ndjson = self._is_ndjson(first_file)
        skip_blank_lines = kwargs.get("skip_blank_lines", False)
        infer_schema_length = kwargs.get("infer_schema_length", 1000)
        ignore_errors = kwargs.get("ignore_errors", True)

        if is_ndjson:
            lf = pl.scan_ndjson(
                source=files,
                storage_options=self.options,
                infer_schema_length=infer_schema_length,
                ignore_errors=ignore_errors,
            )
            if skip_blank_lines:
                lf = lf.filter(pl.all_horizontal().is_not_null())
            return lf

        # Standard JSON
        try:
            resolved = [self.fs.resolve(f) for f in files]
            dataset = ds.dataset(
                resolved,
                format="json",
                filesystem=self.fs.fs,
            )
            lf = pl.from_arrow(dataset.to_table()).lazy()
            if skip_blank_lines:
                lf = lf.filter(pl.any_horizontal(pl.all().is_not_null()))
            return lf
        except Exception as err:
            LOG.warning(
                f"PyArrow JSON parse failed ({err}). Falling back to native reader."
            )

            # Fallback option: read and repair
            lfs = []
            for f in files:
                size = self.fs.fs.size(f)
                if size and size > 1.5 * 1024**3:
                    LOG.warning(f"Large JSON file may cause OOM: {f}")

                buffer = self.read_raw(f, **kwargs)
                lfs.append(pl.read_json(buffer).lazy())

            return pl.concat(lfs) if lfs else pl.LazyFrame()

    def from_df(self, df: pl.LazyFrame | pl.DataFrame, path: Path | str) -> None:
        """Write LazyFrame to JSON files.

        Args:
            df: The LazyFrame to write.
            path: The path to write the files to.
        """
        if isinstance(df, pl.LazyFrame):
            df.sink_ndjson(path)
        else:
            df.write_ndjson(path)

    def read_raw(self, path: Path | str, **kwargs: Any) -> io.BytesIO:
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

    def write_raw(self, data: bytes, path: str) -> None:
        """Write raw data to path.

        Args:
            data: The raw data to write.
            path: The path to write the data to.
        """
        with self.fs.open(path, "wb") as f:
            f.write(data)

    def _is_ndjson(self, path: str) -> bool:
        """Check if file is NDJSON format.

        Args:
            path: The path to the file.

        Returns:
            bool: True if the file is NDJSON, False otherwise.
        """
        if any(path.endswith(ext) for ext in [".ndjson", ".jsonl"]):
            return True

        # Peek at first non-whitespace character
        try:
            with self.fs.open(path, "rb") as f:
                # Read initial chunk to inspect structural character
                chunk = f.read(2048).strip()
                if not chunk:
                    return False
                # If first character is '{', it's almost certainly NDJSON record-stream
                # Standard JSON arrays start with '['
                return chunk.startswith(b"{")
        except (OSError, UnicodeError):
            return False
