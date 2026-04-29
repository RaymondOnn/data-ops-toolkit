from abc import ABC, abstractmethod

from core.contexts.task import TaskContext
from loguru import logger

LOG = logger


# delete / move / keep
class CleanupStrategy(ABC):
    @abstractmethod
    def execute(self, context: TaskContext):
        pass


class RetainCleanupStrategy(CleanupStrategy):
    def execute(self, context: TaskContext):
        # Logic to move/delete local CSVs/Parquet
        if context.custom_params.get("regression_mode"):
            LOG.info("Purging local regression files")
            # shutil.rmtree(...)


class PurgeCleanupStrategy(CleanupStrategy):
    def execute(self, context: TaskContext):
        # Usually a no-op, or perhaps clearing a local cache/tmp json
        pass


class ArchiveCleanupStrategy(CleanupStrategy):
    def execute(self, context: TaskContext):
        # Logic for dropping shadow/temp tables in regression
        pass
        pass
