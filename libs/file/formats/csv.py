import io
import logging
from pathlib import Path
from typing import IO, Any, BinaryIO, cast

import polars as pl

from .base import FormatHandler

LOG = logging.getLogger(__name__)


class CSVHandler(FormatHandler):
    """
    Handles reading and writing data in CSV (Comma Separated Values) format.

    This handler supports various CSV-like files, including TSV and generic
    text files. It provides self-healing capabilities like BOM stripping
    and encoding normalization.
    """

    @property
    def is_splittable(self) -> bool:
        """
        Indicates that CSV is a splittable format.

        Splittable formats allow for efficient parallel processing and
        lazy metadata reading.

        Returns:
            bool: Always True for CSV.
        """
        return True

    def discover(self, input_path: Path | str, pattern: str | None = None) -> set[str]:
        """
        Expands a path into a list of CSV/Text files.

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

        # Matches .csv, .txt, .tsv
        pattern = (
            f"{path_str.rstrip('/')}/**/*.[ct][sx][vt]" if not pattern else path_str
        )
        print(f"Discovering files with pattern: {pattern}")
        return {
            str(self.fs.unstrip_protocol(str(p)))
            for p in self.fs.glob(pattern)
            if self.fs.isfile(p)
        }

    def read(self, input_path: Path | str, **kwargs: Any) -> io.BytesIO:
        """
        Reads raw bytes from the given path into an in-memory buffer.

        This method includes self-healing logic to strip Byte Order Marks (BOM)
        and normalize encoding to UTF-8, ensuring consistent data processing.

        Args:
            input_path: The path to the file(s) to read.
            **kwargs: Additional options, including 'encoding' (default 'utf-8').

        Returns:
            io.BytesIO: An in-memory buffer containing the combined,
                sanitized bytes of the discovered files.
        """
        encoding = kwargs.get("encoding", "utf-8")
        paths = self.discover(input_path)
        if not paths:
            LOG.warning(f"No files discovered for path: {input_path}")
            return io.BytesIO(b"")

        combined = b""
        for p in paths:
            with cast("BinaryIO", self.fs.open(p, "rb")) as f:
                raw_data = f.read()

                # Strip BOM if it exists
                if raw_data.startswith(b"\xef\xbb\xbf"):
                    LOG.info(
                        "Self-healing: Stripping BOM from CSV", extra={"path": str(p)}
                    )
                    raw_data = raw_data[3:]

                # Normalize encoding to UTF-8 per file
                if encoding.lower() != "utf-8":
                    raw_data = raw_data.decode(encoding, errors="ignore").encode(
                        "utf-8"
                    )
                combined += raw_data
        return io.BytesIO(combined)

    def to_df(self, input_path: Path | str, **kwargs: Any) -> pl.LazyFrame:
        """
        Reads one or more CSV files into a unified Polars LazyFrame.

        Optimizes reading for large files by using `pl.scan_csv` for files
        exceeding a certain size threshold, otherwise uses `pl.read_csv`
        with self-healing for smaller files.

        Args:
            input_path: The path to the file(s) or directory to read.
            **kwargs: Additional format-specific reading options,
                including 'encoding' and 'force_repair'.

        Returns:
            pl.LazyFrame: A Polars LazyFrame representing the combined data.
        """
        paths = self.discover(input_path)
        if not paths:
            LOG.warning(f"No files discovered for path: {input_path}")
            return pl.LazyFrame()

        lfs = []
        for p in paths:
            size = self.fs.size(p)
            # Performance: Use scan_csv for files > 2GB to avoid OOM
            if size and size > (2 * 1024**3) and not kwargs.get("force_repair"):
                lfs.append(
                    pl.scan_csv(
                        p,
                        storage_options=self.opts,
                        encoding=kwargs.get("encoding", "utf-8"),
                    )
                )
                continue

            buffer = self.read(p, **kwargs)
            lfs.append(
                pl.read_csv(buffer, encoding=kwargs.get("encoding", "utf-8")).lazy()
            )

        return pl.concat(lfs) if lfs else pl.LazyFrame()

    def from_df(self, df: pl.LazyFrame | pl.DataFrame, output_path: Path | str) -> None:
        """
        Writes a Polars DataFrame or LazyFrame to the specified output path
        in CSV format.

        Utilizes Polars' streaming write capabilities for LazyFrames
        (`sink_csv`) and direct write for DataFrames (`write_csv`).

        Args:
            df: The Polars DataFrame or LazyFrame to write.
            output_path: The destination path for the output CSV file.
        """
        if isinstance(df, pl.LazyFrame):
            df.sink_csv(output_path)
        else:
            df.write_csv(output_path)

    def write(self, data: bytes, output_path: Path | str) -> None:
        """
        Writes raw bytes directly to the specified output path.

        Args:
            data: The bytes object to write.
            output_path: The destination path for the file.
        """
        with self.fs.open(output_path, "wb") as f:
            # Cast f to an IO[bytes] so Ty knows .write() accepts bytes
            cast("IO[bytes]", f).write(data)
