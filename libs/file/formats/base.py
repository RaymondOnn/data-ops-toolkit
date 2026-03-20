import io
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import fsspec
import polars as pl

from libs.file.formats import (
    CSVHandler,
    JSONHandler,
    ParquetHandler,
    XMLHandler,
)

LOG = logging.getLogger(__name__)


class FormatHandler(ABC):
    def __init__(self, fs=None, storage_options: dict[str, Any] | None = None) -> None:
        """
        Decision: Use fsspec for cloud abstraction.
        storage_options are passed directly to Polars/PyArrow for high-speed I/O.
        """

        self.fs = fs or fsspec.filesystem("file")
        self.opts = storage_options or {}

    @abstractmethod
    def to_df(self, input_file: Path, **kwargs: Any) -> pl.LazyFrame:
        """High-level: Streaming/Repair -> LazyFrame"""
        pass

    @abstractmethod
    def from_df(self, df: pl.LazyFrame | pl.DataFrame, output_file: Path) -> None:
        """High-level: LazyFrame -> File (Streaming)"""
        pass

    @abstractmethod
    def read_mem(self, input_file: Path, **kwargs: Any) -> io.BytesIO:
        """Low-level: Read + Repair -> Memory Buffer"""
        pass

    @abstractmethod
    def write_file(self, data: bytes, output_file: Path) -> None:
        """Low-level: Raw Bytes -> Storage"""
        pass

    def __enter__(self):
        """
        Decision: Open a persistent connection context if the
        filesystem requires it (e.g., S3 session).
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Decision: Explicitly close buffers or sessions.
        Crucial for 50M row jobs to prevent lingering file descriptors
        during long-running streaming sinks.
        """
        # Close fs sessions if supported, otherwise pass
        if hasattr(self.fs, "close"):
            self.fs.close()


class HandlerFactory:
    @staticmethod
    def get_handler(
        ext: str,
        fs: fsspec.AbstractFileSystem | None = None,
        storage_options: dict[str, Any] | None = None,
    ) -> FormatHandler:
        storage_options = storage_options or {}
        fs = fs or fsspec.filesystem("file")
        mapping = {
            "json": JSONHandler,
            "jsonl": JSONHandler,
            "ndjson": JSONHandler,
            "csv": CSVHandler,
            "parquet": ParquetHandler,
            "xml": XMLHandler,
        }
        return mapping[ext](fs, storage_options)
