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
    """
    Handles reading and writing data in XML format.

    Includes self-healing logic to strip illegal ASCII control characters
    that frequently appear in legacy XML exports and cause parsing failures.
    """

    def discover(self, input_path: Path | str, pattern: str | None = None) -> set[str]:
        """
        Expands a path into a list of XML files.

        Args:
            input_path: The base path or directory to search.
            pattern: Optional glob pattern to filter discovered files.

        Returns:
            set[str]: A set of fully qualified paths to discovered XML files.
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

        pattern = f"{path_str.rstrip('/')}/**/*.xml" if not pattern else path_str
        return {
            str(self.fs.unstrip_protocol(str(p)))
            for p in self.fs.glob(pattern)
            if self.fs.isfile(p)
        }

    def _sanitize(self, path: str, **kwargs: Any):
        """
        Internal helper to strip illegal XML characters from a file.

        Args:
            path: The path to the file to sanitize.
            **kwargs: Additional options, including 'encoding' (default 'utf-8').

        Returns:
            bytes: The sanitized XML content encoded as UTF-8.
        """
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
        """
        Reads raw XML bytes and removes illegal control characters.

        Filters out ASCII characters 0-31 (except tab, newline, and carriage
        return) to prevent 'xml.parsers.expat.ExpatError'.

        Args:
            input_path: The path to the file(s) to read.
            **kwargs: Additional options passed to the sanitizer.

        Returns:
            io.BytesIO: An in-memory buffer containing sanitized XML bytes.
        """
        paths = self.discover(input_path)
        if not paths:
            LOG.warning(f"No files discovered for path: {input_path}")
            return io.BytesIO(b"")

        combined = b""
        for p in paths:
            combined += self._sanitize(p, **kwargs)
        return io.BytesIO(combined)

    def to_df(self, input_path: Path | str, **kwargs: Any) -> pl.LazyFrame:
        """
        Reads XML files into a unified Polars LazyFrame.

        Uses xmltodict to bridge XML into a dictionary before loading into
        a Polars DataFrame. Note that this process is currently eager.

        Args:
            input_path: The path to the file(s) or directory to read.
            **kwargs: Additional reading and sanitization options.

        Returns:
            pl.LazyFrame: A Polars LazyFrame representing the combined data.
        """
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
        """
        Writes data to XML format.

        Args:
            df: The Polars DataFrame or LazyFrame to write.
            output_path: The destination path for the file.

        Raises:
            NotImplementedError: Currently not supported by Polars natively.
        """
        raise NotImplementedError("Streaming XML write is not supported by Polars.")

    def write(self, data: bytes, output_path: Path | str):
        """
        Writes raw bytes directly to the specified output path.

        Args:
            data: The bytes object to write.
            output_path: The destination path for the file.
        """
        with self.fs.open(output_path, "wb") as f:
            # Cast f to an IO[bytes] so Ty knows .write() accepts bytes
            cast("IO[bytes]", f).write(data)
