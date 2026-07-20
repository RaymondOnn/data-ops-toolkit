from typing import TYPE_CHECKING

from apps.ingestion.src.core.models.task.manifest import StagePayload
from apps.ingestion.src.utils.constants import (
    CONFIG_FILENAME,
    MANIFEST_FILENAME,
    STRIP_TZ_FOR_DB,
)
from libs.utils.dates import current_timestamp
from loguru import logger

from .base import ExecutionStage
from .enums import Stage
from .utils import stage

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task import Task


LOG = logger


@stage(Stage.START.value)
class StartStage(ExecutionStage[None]):
    requires_disk_space: bool = False

    def pre_flight(self, task: "Task") -> None:
        """Verifies the physical integrity of the task workspace."""
        task_folder = task.workspace.path

        # 1. Verify Task Folder
        if not task_folder.exists():
            raise FileNotFoundError(f"Isolated task directory missing: {task_folder}")

        # 2. Verify manifest.json (The state source of truth)
        if not (task_folder / MANIFEST_FILENAME).exists():
            raise FileNotFoundError(
                f"Manifest missing at {task_folder / MANIFEST_FILENAME}"
            )

        # 3. Verify config.json (The runtime instructions)
        if not (task_folder / CONFIG_FILENAME).exists():
            raise FileNotFoundError(
                f"Runtime config missing at {task_folder / CONFIG_FILENAME}"
            )

        LOG.debug("Physical task artifacts verified", run_id=task.run_id)

    def _execute(self, task: "Task") -> str:
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
