from apps.ingestion.src.services.base import Service

from .base import Reader, ReaderContext


# For ingestion of flat files
class FileIngest(Reader):
    def get_units(self, service: Service, context: ReaderContext) -> set[str]:
        # context.source_path provides the directory or bucket to scan
        return service.list_files(context.source_path)
