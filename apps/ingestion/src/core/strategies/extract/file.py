from src.core.strategies.extract import Reader, ReaderContext
from src.services.base import Service


# For ingestion of flat files
class FileIngest(Reader):
    def get_units(self, service: Service, context: ReaderContext) -> list[str]:
        # context.source_path provides the directory or bucket to scan
        return service.list_files(context.source_path)
