"""Format handler factory."""

from typing import TYPE_CHECKING, Any, ClassVar

from .base import FormatHandler
from .csv import CSVHandler
from .json import JSONHandler
from .parquet import ParquetHandler
from .xml import XMLHandler

if TYPE_CHECKING:
    from libs.file.base import FileSystemClient


class FormatFactory:
    """Factory for creating format-specific handlers."""

    _HANDLERS: ClassVar[dict[str, type[FormatHandler]]] = {
        "csv": CSVHandler,
        "parquet": ParquetHandler,
        "json": JSONHandler,
        "jsonl": JSONHandler,
        "ndjson": JSONHandler,
        "xml": XMLHandler,
    }

    @staticmethod
    def get(
        ext: str,
        fs: "FileSystemClient",
        options: dict[str, Any] | None = None,
    ) -> FormatHandler:
        """Get handler for file extension.

        Args:
            ext: The file extension.
            fs: The file system.
            options: The options.

        Returns:
            FormatHandler: The handler for the file extension.
        """
        ext_clean = ext.casefold().lstrip(".")
        handler_cls = FormatFactory._HANDLERS.get(ext_clean)
        if not handler_cls:
            raise ValueError(f"Unsupported extension: {ext}")
        return handler_cls(fs, options)

    @classmethod
    def supported_extensions(cls) -> list[str]:
        """List all supported extensions."""
        return list(cls._HANDLERS.keys())
