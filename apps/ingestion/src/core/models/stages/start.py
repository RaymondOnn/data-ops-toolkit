from typing import TYPE_CHECKING

from apps.ingestion.src.core.models.task.manifest import StagePayload
from apps.ingestion.src.utils.constants import STRIP_TZ_FOR_DB
from libs.utils.dates import current_timestamp
from loguru import logger

from .base import ExecutionStage
from .enums import Stage
from .utils import stage

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task import Task


LOG = logger


@stage(Stage.START.value)
class StartStage(ExecutionStage):
    requires_disk_space: bool = False

    def execute(self, task: "Task") -> str:
        # persist job-start metadata using engine helper
        start_ts = current_timestamp(naive=STRIP_TZ_FOR_DB).isoformat(sep=" ")
        try:
            # 4. Create Payload
            payload = StagePayload(
                start_time=start_ts,
                # worker_id=task.worker_id,
            )
            # 5. Finalize (using the generic helper we discussed)
            # Note: Pass the Struct directly if checkpoint() handles to_builtins
            self.checkpoint(task, payload=payload)
            LOG.info(
                "Task initialized",
                stage=self.name,
                job_id=task.job_id,
                run_id=task.run_id,
            )
            return self._next_stage()
        except Exception as e:
            # Ensure we capture the traceback in the manifest
            self.checkpoint(task, error=e)
            raise
