"""Base classes for file format handling."""

import io
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Self

import fsspec
import polars as pl

LOG = logging.getLogger(__name__)


class FormatHandler(ABC):
    """Base handler for reading/writing specific file formats."""

    def __init__(
        self,
        fs: fsspec.AbstractFileSystem | None = None,
        options: dict[str, Any] | None = None,
    ) -> None:
        self.fs = fs or fsspec.filesystem("file")
        self.options = options or {}

    @property
    def splittable(self) -> bool:
        """Whether format supports lazy/parallel reading."""
        return False

    def count_rows(self, path: Path | str) -> int:
        """Count rows using format-specific optimization."""
        result = self.to_df(path).select(pl.len()).collect()
        return int(result.item())

    def _glob_files(
        self, path: Path | str, pattern: str | None, default_glob: str
    ) -> set[str]:
        """Discover files matching pattern or default glob."""
        base_raw = self.fs._strip_protocol(str(path))
        if isinstance(base_raw, list | tuple):
            base_raw = base_raw[0] if base_raw else ""
        base = str(base_raw).rstrip("/")
        leaf = pattern.lstrip("/") if pattern else ""
        search = f"{base}/{leaf}" if base and leaf else base or leaf or ""

        if "*" in search:
            return {
                str(self.fs.unstrip_protocol(str(p)))
                for p in self.fs.glob(search)
                if self.fs.isfile(p)
            }

        if search and self.fs.isfile(search):
            return {str(self.fs.unstrip_protocol(search))}

        final = f"{search.rstrip('/')}/{default_glob}" if search else default_glob
        return {
            str(self.fs.unstrip_protocol(str(p)))
            for p in self.fs.glob(final)
            if self.fs.isfile(p)
        }

    @abstractmethod
    def discover(self, path: Path | str, pattern: str | None = None) -> set[str]:
        """Discover files matching format."""
        pass

    @abstractmethod
    def to_df(self, path: Path | str, **kwargs: Any) -> pl.LazyFrame:
        """Read to Polars LazyFrame."""
        pass

    @abstractmethod
    def from_df(self, df: pl.LazyFrame | pl.DataFrame, path: Path | str) -> None:
        """Write from Polars DataFrame."""
        pass

    @abstractmethod
    def read_raw(self, path: Path | str, **kwargs: Any) -> io.BytesIO:
        """Read raw bytes with optional healing."""
        pass

    @abstractmethod
    def write_raw(self, data: bytes, path: Path | str) -> None:
        """Write raw bytes."""
        pass

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if hasattr(self.fs, "close"):
            close_method = getattr(self.fs, "close", None)
            if callable(close_method):  # Fixed: check callable
                close_method()
