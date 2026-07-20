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
        """Discover files matching pattern or default glob using centralized client logic."""
        # Convert path to string cleanly
        search_path = str(path)

        # If there's no wildcard and it isn't an explicit file, fall back to default glob
        if (
            "*" not in search_path
            and not pattern
            and not self.fs.isfile(self.fs._strip_protocol(search_path))
        ):
            pattern = default_glob

        # If your handler already has access to a FileSystemClient instance, call it directly:
        # return set(self.client.glob(search_path, pattern=pattern))

        # Direct fallback leveraging the internal handler fs mapping:
        search = (
            f"{search_path.rstrip('/')}/{pattern.lstrip('/')}"
            if pattern
            else search_path
        )

        if "*" not in search and self.fs.isfile(self.fs._strip_protocol(search)):
            return {str(self.fs.unstrip_protocol(self.fs._strip_protocol(search)))}

        return {
            str(self.fs.unstrip_protocol(p))
            for p in self.fs.glob(search)
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
