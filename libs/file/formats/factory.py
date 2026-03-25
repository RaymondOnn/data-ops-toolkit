from typing import Any

import fsspec

from .base import FormatHandler
from .csv import CSVHandler
from .json import JSONHandler
from .parquet import ParquetHandler
from .xml import XMLHandler


class FormatFactory:
    @staticmethod
    def get_handler(
        ext: str,
        fs: fsspec.AbstractFileSystem | None = None,
        storage_options: dict[str, Any] | None = None,
    ) -> FormatHandler:
        storage_options = storage_options or {}
        fs = fs or fsspec.filesystem("file")
        mapping: dict[str, type[FormatHandler]] = {
            "json": JSONHandler,
            "jsonl": JSONHandler,
            "ndjson": JSONHandler,
            "csv": CSVHandler,
            "parquet": ParquetHandler,
            "xml": XMLHandler,
        }
        handler_cls = mapping.get(ext)
        if not handler_cls:
            raise ValueError(f"Unsupported file extension: {ext}")
        return handler_cls(fs, storage_options)
