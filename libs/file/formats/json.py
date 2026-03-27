import io
import logging
import re
from pathlib import Path
from typing import Any

import polars as pl
from fsspec import AbstractFileSystem

from .base import FormatHandler

LOG = logging.getLogger(__name__)


class JSONHandler(FormatHandler):
    fs: AbstractFileSystem

    def discover(self, input_path: Path | str) -> list[str]:
        """Expands a path into a list of JSON files."""
        path_str = str(input_path)

        # If the path already contains a wildcard, expand it directly
        if "*" in path_str:
            return [
                str(self.fs.unstrip_protocol(p))
                for p in self.fs.glob(path_str)
                if self.fs.isfile(p)
            ]

        if self.fs.isfile(path_str):
            return [path_str]

        # Matches .json, .jsonl, .ndjson
        pattern = f"{path_str.rstrip('/')}/**/*.json*"
        return [
            str(self.fs.unstrip_protocol(p))
            for p in self.fs.glob(pattern)
            if self.fs.isfile(p)
        ]

    def read_file(self, input_path: Path | str, **kwargs: Any) -> io.BytesIO:
        """Handles 'Trailing Comma' repairs for standard JSON."""
        encoding = kwargs.get("encoding", "utf-8")
        paths = self.discover(input_path)
        if not paths:
            LOG.warning(f"No files discovered for path: {input_path}")
            return io.BytesIO(b"")

        combined = b""
        for p in paths:
            # 1. Use 'rb' to get a raw stream
            with self.fs.open(p, "rb") as f:
                # Read the entire file as bytes
                raw_bytes = f.read()

                # 2. Safety check for 'ty' / Pyright normalization
                if isinstance(raw_bytes, str):
                    raw_bytes = raw_bytes.encode(encoding)

                # 3. Decode only once for the repair
                content = raw_bytes.decode(encoding, errors="ignore")

                # 4. Perform the 'Trailing Comma' repair
                clean_content = re.sub(r",\s*([\]}])", r"\1", content)
                if len(clean_content) != len(content):
                    LOG.info(
                        "Self-healing: Fixed trailing commas in JSON",
                        extra={"path": str(p)},
                    )

                combined += clean_content.encode(encoding)
        return io.BytesIO(combined)

    def to_df(self, input_path: Path | str, **kwargs: Any) -> pl.LazyFrame:
        """
        Unified Reader:
        1. High-Performance: Uses scan_ndjson for .ndjson/.jsonl or
           NDJSON-formatted .json.
        2. Defensive: Uses read_json + repair for standard .json (arrays).
        """
        paths = self.discover(input_path)
        if not paths:
            LOG.warning(f"No files discovered for path: {input_path}")
            return pl.LazyFrame()

        # For simplicity, we determine the mode based on the first file in the batch.
        # Usually, a batch of work units shares the same format.
        first_file = paths[0]
        is_ndjson = any(str(first_file).endswith(ext) for ext in [".ndjson", ".jsonl"])

        if not is_ndjson:
            # Peek to see if it's NDJSON (starts with '{') or Standard
            # JSON (starts with '[')
            try:
                with self.fs.open(first_file, "rb") as f:
                    # Read a small chunk to find the first non-whitespace character
                    chunk = f.read(1024).strip()
                    if chunk and chunk[0:1] == b"{":
                        is_ndjson = True
            except Exception:
                LOG.debug(
                    f"Peeking failed for {first_file}, "
                    "defaulting to standard JSON path."
                )

        if is_ndjson:
            return pl.scan_ndjson(
                paths,
                storage_options=self.opts,
                ignore_errors=kwargs.get("ignore_errors", True),
            )

        # Standard JSON path (loops and repairs per file)
        lfs = []
        for p in paths:
            size = self.fs.size(p)
            if size and size > 1.5 * 1024**3:
                LOG.warning(f"Standard JSON {p} is very large. Risk of OOM.")

            buffer = self.read_file(p, **kwargs)
            lfs.append(pl.read_json(buffer).lazy())

        return pl.concat(lfs) if lfs else pl.LazyFrame()

    def from_df(self, df: pl.LazyFrame | pl.DataFrame, output_file: Path | str) -> None:
        """
        Streaming write: ALWAYS uses NDJSON for better 50M row performance
        and memory safety (2GB RAM limit).
        """
        if isinstance(df, pl.LazyFrame):
            df.sink_ndjson(output_file)
        else:
            df.write_ndjson(output_file)

    def write_file(self, data: bytes, output_file: Path | str) -> None:
        with self.fs.open(output_file, "wb") as f:
            f.write(data)
