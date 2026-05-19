import io
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Self

import fsspec
import polars as pl

LOG = logging.getLogger(__name__)


class FormatHandler(ABC):
    def __init__(
        self,
        fs: fsspec.AbstractFileSystem | None = None,
        storage_options: dict[str, Any] | None = None,
    ) -> None:
        """
        Decision: Use fsspec for cloud abstraction.
        storage_options are passed directly to Polars/PyArrow for high-speed I/O.
        """

        self.fs = fs or fsspec.filesystem("file")
        self.opts = storage_options or {}

    @abstractmethod
    def discover(self, input_path: Path | str) -> set[str]:
        """Expands a path into a list of Parquet files."""
        pass

    @abstractmethod
    def to_df(self, input_path: Path | str, **kwargs: Any) -> pl.LazyFrame:
        """High-level: Streaming/Repair -> LazyFrame"""
        pass

    @abstractmethod
    def from_df(self, df: pl.LazyFrame | pl.DataFrame, output_path: Path | str) -> None:
        """High-level: LazyFrame -> File (Streaming)"""
        pass

    @abstractmethod
    def read(self, input_path: Path | str, **kwargs: Any) -> io.BytesIO:
        """Low-level: Read + Repair -> Memory Buffer"""
        pass

    @abstractmethod
    def write(self, data: bytes, output_path: Path | str) -> None:
        """Low-level: Raw Bytes -> Storage"""
        pass

    def __enter__(self) -> Self:
        """
        Decision: Open a persistent connection context if the
        filesystem requires it (e.g., S3 session).
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """
        Decision: Explicitly close buffers or sessions.
        Crucial for 50M row jobs to prevent lingering file descriptors
        during long-running streaming sinks.
        """
        # Close fs sessions if supported, otherwise pass
        close_method = getattr(self.fs, "close", None)
        if callable(close_method):
            close_method()
