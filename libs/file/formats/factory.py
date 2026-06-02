from typing import Any, ClassVar

import fsspec

from .base import FormatHandler
from .csv import CSVHandler
from .json import JSONHandler
from .parquet import ParquetHandler
from .xml import XMLHandler


class FormatFactory:
    """Factory for instantiating format-specific handlers.

    Decision: Centralized Registry.
    By maintaining a central mapping of extensions to handlers, we allow
    the platform to be 'Contract-Aware'. Mixins and Services can query the
    factory to find supported formats without hardcoding extension lists.
    """

    _MAPPING: ClassVar[dict[str, type[FormatHandler]]] = {
        "json": JSONHandler,
        "jsonl": JSONHandler,
        "ndjson": JSONHandler,
        "csv": CSVHandler,
        "parquet": ParquetHandler,
        "xml": XMLHandler,
    }

    @staticmethod
    def get_handler(
        ext: str,
        fs: fsspec.AbstractFileSystem | None = None,
        storage_options: dict[str, Any] | None = None,
    ) -> FormatHandler:
        """
        Returns an initialized FormatHandler based on the file extension.

        Args:
            ext: The file extension (e.g., 'csv', 'parquet', 'json').
            fs: An optional fsspec filesystem instance. Defaults to local.
            storage_options: Connection/driver configuration for the handler.

        Returns:
            FormatHandler: A concrete implementation of a format handler.

        Raises:
            ValueError: If the provided extension is not supported.
        """
        storage_options = storage_options or {}
        fs = fs or fsspec.filesystem("file")

        handler_cls = FormatFactory._MAPPING.get(ext)
        if not handler_cls:
            raise ValueError(f"Unsupported file extension: {ext}")
        return handler_cls(fs, storage_options)

    @classmethod
    def get_supported_extensions(cls) -> list[str]:
        """Returns a list of all registered file extensions.

        Returns:
            list[str]: A list of extensions (e.g. ['csv', 'parquet']).

        Decision: Introspection.
        This allows the Ingestion Mixins to dynamically filter for data files
        in a landing zone based only on what the platform is capable of reading.
        """
        return list(cls._MAPPING.keys())
