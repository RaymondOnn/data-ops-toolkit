from collections.abc import Generator
from pathlib import Path
from typing import Any

from apps.ingestion.src.services.base import Source

from .base import ExtractContext, Extractor


# For ingestion of flat files
class FileIngest(Extractor[Source]):
    """Simplified extractor for basic file ingestion."""

    def extract(
        self, service: Source, context: ExtractContext, target_folder: Path
    ) -> Generator[dict[str, Any], None, None]:
        """Basic implementation of extract for flat files.

        Note: This is a placeholder implementation.
        """
        raise NotImplementedError("FileIngest.extract not yet implemented")

    def get_units(self, service: Source, context: ExtractContext) -> set[str]:
        """Get the list of files to ingest.

        Args:
            service: The source service.
            context: The extraction context.
        """
        return service.list_files(context.resource)
