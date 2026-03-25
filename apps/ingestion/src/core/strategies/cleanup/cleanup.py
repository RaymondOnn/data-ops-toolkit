from abc import ABC, abstractmethod

import structlog

from apps.ingestion.src.core.contexts.job import JobContext

LOG = structlog.get_logger(__name__)


# delete / move / keep
class CleanupStrategy(ABC):
    @abstractmethod
    def execute(self, context: JobContext):
        pass


class RetainCleanupStrategy(CleanupStrategy):
    def execute(self, context: JobContext):
        # Logic to move/delete local CSVs/Parquet
        if context.custom_params.get("regression_mode"):
            LOG.info("Purging local regression files")
            # shutil.rmtree(...)


class PurgeCleanupStrategy(CleanupStrategy):
    def execute(self, context: JobContext):
        # Usually a no-op, or perhaps clearing a local cache/tmp json
        pass


class ArchiveCleanupStrategy(CleanupStrategy):
    def execute(self, context: JobContext):
        # Logic for dropping shadow/temp tables in regression
        pass
