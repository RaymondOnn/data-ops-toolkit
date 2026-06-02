import io
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Self

import fsspec
import polars as pl

LOG = logging.getLogger(__name__)


class FormatHandler(ABC):
    """
    Abstract base class for handling various file formats.

    This class defines the interface for reading, writing, and discovering
    files of a specific format, abstracting away the underlying filesystem
    details. It integrates with `fsspec` for cloud storage abstraction
    and `polars` for efficient data manipulation.
    """

    def __init__(
        self,
        fs: fsspec.AbstractFileSystem | None = None,
        storage_options: dict[str, Any] | None = None,
    ) -> None:
        """
        Initializes the FormatHandler with a filesystem and storage options.

        Args:
            fs: An optional `fsspec.AbstractFileSystem` instance. If None,
                a local filesystem (`fsspec.filesystem("file")`) is used.
            storage_options: A dictionary of options passed directly to
                `fsspec` and Polars/PyArrow for high-speed I/O.
        """
        self.fs = fs or fsspec.filesystem("file")
        self.opts = storage_options or {}

    @property
    def is_splittable(self) -> bool:
        """
        Indicates if the file format supports splittable reading.

        Splittable formats (e.g., Parquet, CSV) allow for lazy metadata
        reading or row-counting without loading the entire file into memory,
        which is crucial for parallel processing and memory efficiency.
        Non-splittable formats (e.g., standard JSON, XML) typically require
        eager loading or specialized parsing.

        Returns:
            bool: True if the format is splittable, False otherwise.
        """
        return False

    @abstractmethod
    def discover(self, input_path: Path | str, pattern: str | None = None) -> set[str]:
        """Expands a path into a list of Parquet files."""
        pass

    @abstractmethod
    def to_df(self, input_path: Path | str, **kwargs: Any) -> pl.LazyFrame:
        """
        Reads data from the given path(s) into a Polars LazyFrame.

        This method handles format-specific parsing and can include
        self-healing logic (e.g., fixing malformed JSON). It returns a
        LazyFrame to enable predicate pushdown and efficient query
        optimization.

        Args:
            input_path: The path to the file(s) or directory to read.
            **kwargs: Additional format-specific reading options.

        Returns:
            pl.LazyFrame: A Polars LazyFrame representing the data.
        """
        pass

    @abstractmethod
    def from_df(self, df: pl.LazyFrame | pl.DataFrame, output_path: Path | str) -> None:
        """
        Writes a Polars DataFrame or LazyFrame to the specified output path.

        This method handles format-specific writing, optimizing for streaming
        and memory efficiency when dealing with LazyFrames.

        Args:
            df: The Polars DataFrame or LazyFrame to write.
            output_path: The destination path for the output file(s).
        """
        pass

    @abstractmethod
    def read(self, input_path: Path | str, **kwargs: Any) -> io.BytesIO:
        """
        Reads raw bytes from the given path into an in-memory buffer.

        This low-level method can include self-healing logic for raw data
        (e.g., stripping BOM, removing invalid characters) before returning
        the content as a `io.BytesIO` object.

        Args:
            input_path: The path to the file to read.
            **kwargs: Additional format-specific reading options.

        Returns:
            io.BytesIO: An in-memory buffer containing the file's raw bytes.
        """
        pass

    @abstractmethod
    def write(self, data: bytes, output_path: Path | str) -> None:
        """
        Writes raw bytes directly to the specified output path.

        Args:
            data: The bytes object to write.
            output_path: The destination path for the file.
        """
        pass

    def __enter__(self) -> Self:
        """
        Enters the runtime context for the FormatHandler.

        This method can be used to open persistent connections or sessions
        if the underlying filesystem client requires it (e.g., S3 sessions).

        Returns:
            Self: The FormatHandler instance.
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """
        Exits the runtime context for the FormatHandler.

        This method is crucial for ensuring that any open buffers, file
        handles, or filesystem sessions are explicitly closed. This prevents
        resource leaks, especially in long-running streaming jobs or when
        dealing with large datasets.

        Args:
            exc_type: The type of exception raised, if any.
            exc_val: The exception instance raised, if any.
            exc_tb: The traceback object, if an exception was raised.
        """
        # Close fs sessions if supported, otherwise pass
        close_method = getattr(self.fs, "close", None)
        if callable(close_method):
            close_method()
