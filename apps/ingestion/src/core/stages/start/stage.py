from typing import TYPE_CHECKING

from libs.utils.dates import current_timestamp
from loguru import logger

from src.core.stages.contracts.stage import ExecutionStage
from src.core.stages.types import Stage
from src.utils.constants import (
    CONFIG_FILENAME,
    MANIFEST_FILENAME,
    STRIP_TZ_FOR_DB,
)

from .enums import StartPayload

if TYPE_CHECKING:
    from src.core.models.task import TaskManifest, TaskWorkspace
    from src.core.stages.types import StageContext
    from src.services.health.system import SystemMonitor

LOG = logger


@ExecutionStage.register(key=Stage.START.value)
class StartStage(ExecutionStage[None]):
    requires_disk_space: bool = False

    def pre_flight(
        self,
        system: "SystemMonitor",
        ctx: "StageContext",
        workspace: "TaskWorkspace",
        manifest: "TaskManifest",
    ) -> None:
        """Verifies the physical integrity of the task workspace."""
        task_folder = workspace.path

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

        LOG.debug("Physical task artifacts verified", run_id=ctx.run_id)

    def _execute(
        self,
        ctx: "StageContext",
        workspace: "TaskWorkspace",
        manifest: "TaskManifest",
    ) -> str:
        # persist job-start metadata using engine helper
        start_ts = current_timestamp(naive=STRIP_TZ_FOR_DB).isoformat(sep=" ")
        try:
            # 4. Create Payload
            payload = StartPayload(
                step_id="start",
                start_time=start_ts,
                # worker_id=task.worker_id,
            )
            # 5. Finalize (using the generic helper we discussed)
            # Note: Pass the Struct directly if save_stage_outcome() handles to_builtins
            self.save_stage_outcome(
                workspace=workspace, manifest=manifest, payload=payload
            )
            LOG.info(
                "Task initialized",
                stage=self.name,
                job_id=ctx.job_id,
                run_id=ctx.run_id,
            )
            return self._next_step(ctx)
        except Exception as e:
            # Ensure we capture the traceback in the manifest
            self.save_stage_outcome(workspace=workspace, manifest=manifest, error=e)
            raise
