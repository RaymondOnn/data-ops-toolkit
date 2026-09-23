"""Base classes for file format handling."""

import io
import logging
from abc import ABC, abstractmethod
from typing import Any, Self

import polars as pl
from upath import UPath

from libs.metaclasses.draft import ClassRegistry, load_package_modules

LOG = logging.getLogger(__name__)


class FormatHandler(
    ABC,
    ClassRegistry,
    registry_name="FormatHandlerRegistry",
    auto_key=False,
    package_paths="libs.file.formats",
):
    """Base handler for reading/writing specific file formats using UPath."""

    def __init__(self, options: dict[str, Any] | None = None) -> None:
        self.options = options or {}

    @classmethod
    def supported_extensions(cls) -> list[str]:
        """List all registered file extensions."""
        if not cls.keys():
            import libs.file.formats as formatters_pkg

            load_package_modules(formatters_pkg)

        return list(cls.keys())

    def count_rows(self, path: UPath | str) -> int:
        """Count rows using format-specific optimization."""
        result = self.to_df(path).select(pl.len()).collect()
        return int(result.item())

    def _glob_files(
        self, path: UPath | str, pattern: str | None, default_glob: str
    ) -> set[UPath]:
        """Discover files matching pattern or default glob using UPath."""
        upath = UPath(path)

        # 1. Single File Match
        if upath.is_file():
            return {upath}

        # 2. Pattern Match
        if pattern:
            search_pattern = pattern.lstrip("/")
            return {p for p in upath.glob(search_pattern) if p.is_file()}

        # 3. Path contains wildcards directly
        if "*" in str(path):
            parent = upath.parent
            return {p for p in parent.glob(upath.name) if p.is_file()}

        # 4. Fallback to default recursive glob on directory
        if upath.is_dir():
            return {p for p in upath.glob(default_glob) if p.is_file()}

        return set()

    @abstractmethod
    def discover(self, path: UPath | str, pattern: str | None = None) -> set[UPath]:
        """Discover files matching format."""

    @abstractmethod
    def to_df(self, path: UPath | str, **kwargs: Any) -> pl.LazyFrame:
        """Read to Polars LazyFrame."""

    @abstractmethod
    def from_df(self, df: pl.LazyFrame | pl.DataFrame, path: UPath | str) -> None:
        """Write from Polars DataFrame."""

    @abstractmethod
    def read_raw(self, path: UPath | str, **kwargs: Any) -> io.BytesIO:
        """Read raw bytes."""

    @abstractmethod
    def write_raw(self, data: bytes, path: UPath | str) -> None:
        """Write raw bytes."""

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit point. Override in subclasses if cleanup is required."""
        return
