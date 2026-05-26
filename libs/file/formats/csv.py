import io
import logging
from pathlib import Path
from typing import IO, Any, BinaryIO, cast

import polars as pl

from .base import FormatHandler

LOG = logging.getLogger(__name__)


class CSVHandler(FormatHandler):
    @property
    def is_splittable(self) -> bool:
        return True

    def discover(self, input_path: Path | str, pattern: str | None = None) -> set[str]:
        """Expands a path into a list of CSV/Text files."""
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
        """Strips BOM and handles encoding-safe reading."""
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
        """Reads one or more CSV files into a unified LazyFrame."""
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
        """Streaming write for 50M rows."""
        if isinstance(df, pl.LazyFrame):
            df.sink_csv(output_path)
        else:
            df.write_csv(output_path)

    def write(self, data: bytes, output_path: Path | str) -> None:
        with self.fs.open(output_path, "wb") as f:
            # Cast f to an IO[bytes] so Ty knows .write() accepts bytes
            cast("IO[bytes]", f).write(data)
