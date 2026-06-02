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
    """
    Handles reading and writing data in JSON and NDJSON formats.

    Supports standard JSON (arrays of objects) and Newline Delimited JSON
    (NDJSON/JSONL). Includes self-healing logic to repair trailing commas
    common in human-edited or legacy exported files.
    """

    fs: AbstractFileSystem

    @property
    def is_splittable(self) -> bool:
        """
        Indicates if the format supports splittable reading.

        Returns:
            bool: Always False. While NDJSON is technically splittable,
                standard JSON arrays are not, so we default to False for
                safety in unified pathing.
        """
        return False

    def discover(self, input_path: Path | str, pattern: str | None = None) -> set[str]:
        """
        Expands a path into a list of JSON, JSONL, or NDJSON files.

        Args:
            input_path: The base path or directory to search.
            pattern: Optional glob pattern to filter discovered files.

        Returns:
            set[str]: A set of fully qualified paths to discovered files.
        """
        # Standardize the path by stripping the protocol if present
        # so fsspec doesn't treat it as relative to CWD.
        path_str = self.fs._strip_protocol(str(input_path))

        if pattern:
            path_str = f"{path_str.rstrip('/')}/{pattern.lstrip('/')}"

        # If the path already contains a wildcard, expand it directly
        if "*" in path_str:
            return {
                str(self.fs.unstrip_protocol(str(p)))
                for p in self.fs.glob(path_str)
                if self.fs.isfile(p)
            }

        if self.fs.isfile(path_str):
            return {str(self.fs.unstrip_protocol(path_str))}

        # Matches .json, .jsonl, .ndjson
        pattern = f"{path_str.rstrip('/')}/**/*.json*" if not pattern else path_str
        return {
            str(self.fs.unstrip_protocol(str(p)))
            for p in self.fs.glob(pattern)
            if self.fs.isfile(p)
        }

    def read(self, input_path: Path | str, **kwargs: Any) -> io.BytesIO:
        """
        Reads raw bytes and repairs malformed JSON syntax.

        Performs a regex-based 'Trailing Comma' repair (e.g., [1,2,] -> [1,2])
        to ensure standard JSON parsers don't fail on common syntax errors.

        Args:
            input_path: The path to the file(s) to read.
            **kwargs: Additional options, including 'encoding' (default 'utf-8').

        Returns:
            io.BytesIO: An in-memory buffer containing the repaired JSON bytes.
        """
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
        Reads JSON or NDJSON files into a unified Polars LazyFrame.

        Differentiates between NDJSON (high-performance scanning) and
        standard JSON (defensive reading with repair) by checking file
        extensions or peeking at the first few bytes.

        Args:
            input_path: The path to the file(s) or directory to read.
            **kwargs: Additional options, including 'ignore_errors' and
                'encoding'.

        Returns:
            pl.LazyFrame: A Polars LazyFrame representing the combined data.
        """
        paths = self.discover(input_path)
        if not paths:
            LOG.warning(f"No files discovered for path: {input_path}")
            return pl.LazyFrame()

        # For simplicity, we determine the mode based on the first file in the batch.
        # Usually, a batch of work units shares the same format.
        first_file = next(iter(paths))
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
                list(paths),
                storage_options=self.opts,
                ignore_errors=kwargs.get("ignore_errors", True),
            )

        # Standard JSON path (loops and repairs per file)
        lfs = []
        for p in paths:
            size = self.fs.size(p)
            if size and size > 1.5 * 1024**3:
                LOG.warning(f"Standard JSON {p} is very large. Risk of OOM.")

            buffer = self.read(p, **kwargs)
            lfs.append(pl.read_json(buffer).lazy())

        return pl.concat(lfs) if lfs else pl.LazyFrame()

    def from_df(self, df: pl.LazyFrame | pl.DataFrame, output_path: Path | str) -> None:
        """
        Writes data to the specified output path in NDJSON format.

        Always utilizes NDJSON for writing to ensure better performance on
        large datasets (50M+ rows) and to stay within memory limits.

        Args:
            df: The Polars DataFrame or LazyFrame to write.
            output_path: The destination path for the output file.
        """
        if isinstance(df, pl.LazyFrame):
            df.sink_ndjson(output_path)
        else:
            df.write_ndjson(output_path)

    def write(self, data: bytes, output_path: Path | str) -> None:
        """
        Writes raw bytes directly to the specified output path.

        Args:
            data: The bytes object to write.
            output_path: The destination path for the file.
        """
        with self.fs.open(output_path, "wb") as f:
            f.write(data)
