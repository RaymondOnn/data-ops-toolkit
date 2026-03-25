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

    def read_mem(self, input_file: Path | str, **kwargs: Any) -> io.BytesIO:
        """Handles 'Trailing Comma' repairs for standard JSON."""
        encoding = kwargs.get("encoding", "utf-8")

        # 1. Use 'rb' to get a raw stream
        with self.fs.open(input_file, "rb") as f:
            # Read the entire file as bytes
            raw_bytes = f.read()

            # 2. Safety check for 'ty' / Pyright
            # Some FS clients return 'str' even with 'rb'. We normalize to bytes.
            if isinstance(raw_bytes, str):
                raw_bytes = raw_bytes.encode(encoding)

            # 3. Decode only once for the repair
            # Use 'ignore' or 'replace' to prevent crashes on corrupted data
            content = raw_bytes.decode(encoding, errors="ignore")

            # 4. Perform the 'Trailing Comma' repair
            # Regex: [1, 2, 3,] -> [1, 2, 3]
            clean_content = re.sub(r",\s*([\]}])", r"\1", content)

            # 5. Return as a BytesIO stream for msgspec/polars consumption
            return io.BytesIO(clean_content.encode(encoding))

    def to_df(self, input_file: Path | str, **kwargs: Any) -> pl.LazyFrame:
        """
        Unified Reader:
        1. High-Performance: Uses scan_ndjson for .ndjson/.jsonl or
           NDJSON-formatted .json.
        2. Defensive: Uses read_json + repair for standard .json (arrays).
        """
        is_ndjson = any(str(input_file).endswith(ext) for ext in [".ndjson", ".jsonl"])

        if not is_ndjson:
            # Peek to see if it's NDJSON (starts with '{') or Standard
            # JSON (starts with '[')
            try:
                with self.fs.open(input_file, "rb") as f:
                    # Read a small chunk to find the first non-whitespace character
                    chunk = f.read(1024).strip()
                    if chunk and chunk[0:1] == b"{":
                        is_ndjson = True
            except Exception:
                LOG.debug(
                    f"Peeking failed for {input_file}, "
                    "defaulting to standard JSON path."
                )

        if is_ndjson:
            return pl.scan_ndjson(
                input_file,
                storage_options=self.opts,
                ignore_errors=kwargs.get("ignore_errors", True),
            )

        # Standard JSON requires full load into memory for repair
        size = self.fs.size(input_file)
        if size and size > 1.5 * 1024**3:  # 1.5GB warning
            LOG.warning(f"Standard JSON {input_file} is very large. Risk of OOM.")

        buffer = self.read_mem(input_file, **kwargs)
        return pl.read_json(buffer).lazy()

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
