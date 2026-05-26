import io
import logging
import re
from pathlib import Path
from typing import IO, Any, cast

import polars as pl
import xmltodict

from libs.file.formats.base import FormatHandler

LOG = logging.getLogger(__name__)


class XMLHandler(FormatHandler):
    def discover(self, input_path: Path | str, pattern: str | None = None) -> set[str]:
        """Expands a path into a list of XML files."""
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

        pattern = f"{path_str.rstrip('/')}/**/*.xml" if not pattern else path_str
        return {
            str(self.fs.unstrip_protocol(str(p)))
            for p in self.fs.glob(pattern)
            if self.fs.isfile(p)
        }

    def _sanitize(self, path: str, **kwargs: Any):
        encoding = kwargs.get("encoding", "utf-8")
        with self.fs.open(path, "rb") as f:
            raw = f.read().decode(encoding, errors="ignore")
            # Regex Repair: Strip chars 0-31 except \t, \n, \r
            clean = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", raw)
            if len(clean) != len(raw):
                LOG.info(
                    f"Sanitized XML string: {path!s}",
                )
            return clean.encode("utf-8")

    def read(self, input_path: Path | str, **kwargs: Any) -> io.BytesIO:
        """Removes illegal ASCII control characters."""
        paths = self.discover(input_path)
        if not paths:
            LOG.warning(f"No files discovered for path: {input_path}")
            return io.BytesIO(b"")

        combined = b""
        for p in paths:
            combined += self._sanitize(p, **kwargs)
        return io.BytesIO(combined)

    def to_df(self, input_path: Path | str, **kwargs: Any) -> pl.LazyFrame:
        """Reads XML files into a unified LazyFrame."""
        paths = self.discover(input_path)
        if not paths:
            LOG.warning(f"No files discovered for path: {input_path}")
            return pl.LazyFrame()

        lfs = []
        for p in paths:
            LOG.debug(f"Reading XML file: {p!s}")
            try:
                buffer = self.read(p, **kwargs)
                # XML to Polars bridge
                data = xmltodict.parse(buffer.read())
                # Note: This creates an in-memory DataFrame per file
                lfs.append(pl.DataFrame(data).lazy())
            except Exception:
                LOG.exception(f"Failed to parse XML: {p!s}")
                raise

        return pl.concat(lfs) if lfs else pl.LazyFrame()

    def from_df(self, df: pl.LazyFrame | pl.DataFrame, output_path: Path | str) -> None:
        raise NotImplementedError("Streaming XML write is not supported by Polars.")

    def write(self, data: bytes, output_path: Path | str):
        with self.fs.open(output_path, "wb") as f:
            # Cast f to an IO[bytes] so Ty knows .write() accepts bytes
            cast("IO[bytes]", f).write(data)
