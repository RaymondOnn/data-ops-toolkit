import io
import re
from pathlib import Path
from typing import IO, Any, cast

import polars as pl
import xmltodict

from libs.file.formats.base import FormatHandler


class XMLHandler(FormatHandler):
    def read_mem(self, input_file: Path | str, **kwargs: Any) -> io.BytesIO:
        """Removes illegal ASCII control characters."""
        encoding = kwargs.get("encoding", "utf-8")
        with self.fs.open(input_file, "rb") as f:
            raw = f.read().decode(encoding, errors="ignore")
            # Regex Repair: Strip chars 0-31 except \t, \n, \r
            clean = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", raw)
            return io.BytesIO(clean.encode("utf-8"))

    def to_df(self, input_file: Path | str, **kwargs: Any) -> pl.LazyFrame:
        buffer = self.read_mem(input_file, **kwargs)
        # XML to Polars bridge
        data = xmltodict.parse(buffer.read())
        return pl.DataFrame(data).lazy()

    def from_df(self, df: pl.LazyFrame | pl.DataFrame, output_file: Path | str) -> None:
        raise NotImplementedError("Streaming XML write is not supported by Polars.")

    def write_file(self, data: bytes, output_file: Path | str):
        with self.fs.open(output_file, "wb") as f:
            # Cast f to an IO[bytes] so Ty knows .write() accepts bytes
            cast("IO[bytes]", f).write(data)
