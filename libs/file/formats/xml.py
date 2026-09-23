"""XML format handler."""

import io
import logging
import re
from typing import Any

import polars as pl
from lxml import etree  # type: ignore
from upath import UPath

from .base import FormatHandler

LOG = logging.getLogger(__name__)


@FormatHandler.register("xml")
class XMLHandler(FormatHandler):
    """Handler for XML files."""

    def discover(self, path: UPath | str, pattern: str | None = None) -> set[UPath]:
        """Discover XML files.

        Args:
            path: The path to the XML file.
            pattern: The pattern to match.

        Returns:
            set[str]: Set of discovered XML files.
        """
        return self._glob_files(path, pattern, "**/*.xml")

    def _parse_xml_stream(
        self, buffer: io.BytesIO, row_tag: str
    ) -> list[dict[str, str]]:
        """
        Streams XML using C-level lxml.etree.iterparse.
        Clears element trees dynamically to keep RAM usage near 0 MB.
        """
        records = []
        # Fast C-level streaming parser
        context = etree.iterparse(buffer, events=("end",), tag=row_tag)

        for _, elem in context:
            record = {}
            for child in elem:
                if child.text:
                    record[child.tag] = child.text.strip()
            if record:
                records.append(record)

            # CRITICAL: Clear C memory allocations as we stream
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

        return records

    def to_df(self, path: UPath | str, **kwargs: Any) -> pl.LazyFrame:
        """Convert XML files to Polars LazyFrame.

        Args:
            path: The path to the XML file.
            **kwargs: Additional keyword arguments.

        Returns:
            pl.LazyFrame: The LazyFrame containing the XML data.
        """

        row_tag = kwargs.get("row_tag") or self.options.get("row_tag")
        if not row_tag:
            raise ValueError("XMLHandler requires 'row_tag' option")

        files = self.discover(path)
        if not files:
            return pl.LazyFrame()

        dfs = []
        for f in files:
            buffer = self.read_raw(f)
            records = self._parse_xml_stream(buffer, row_tag=row_tag)
            if records:
                dfs.append(pl.DataFrame(records))

        return pl.concat([df.lazy() for df in dfs]) if dfs else pl.LazyFrame()

    def from_df(self, df: pl.LazyFrame | pl.DataFrame, path: UPath | str) -> None:
        raise NotImplementedError("XML write not supported")

    def read_raw(self, path: UPath | str, **kwargs: Any) -> io.BytesIO:
        """Read raw XML data from path.

        Args:
            path: The path to the XML file.
            **kwargs: Additional keyword arguments.

        Returns:
            io.BytesIO: The raw XML data.
        """
        encoding = kwargs.get("encoding", "utf-8")
        files = self.discover(path)
        if not files:
            return io.BytesIO(b"")

        combined = b""
        for f in files:
            raw = f.read_bytes().decode(encoding, errors="ignore")
            cleaned = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", raw)
            combined += cleaned.encode(encoding)

        return io.BytesIO(combined)

    def write_raw(self, data: bytes, path: UPath | str) -> None:
        UPath(path).write_bytes(data)
