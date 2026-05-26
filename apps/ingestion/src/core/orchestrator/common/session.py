from typing import TYPE_CHECKING

from apps.ingestion.src.core.models.task import ExecutionStatus, Task
from apps.ingestion.src.core.orchestrator.enums import TaskRef
from apps.ingestion.src.utils.common import setup_logger
from apps.ingestion.src.utils.exceptions import RewindTask
from loguru import logger

if TYPE_CHECKING:
    from apps.ingestion.src.core.orchestrator.common.executor import Executor
    from loguru import Logger


class TaskSession:
    """
    A class-based context manager for managing task execution lifecycles.
    Safer for Ray pickling than function decorators.
    """

    def __init__(self, executor: "Executor", task_ref: TaskRef, log: "Logger"):
        self.executor = executor
        self.task_ref = task_ref
        self.log = log
        self.handler_id: int | None = None
        self.task: Task | None = None

    def __enter__(self) -> Task:
        # 1. Rehydrate Identity
        self.task = Task(
            task_ref=self.task_ref.with_updates(status=ExecutionStatus.RUNNING.value),
            worker_id=self.executor.worker_id,
            exec_ctx=self.executor.exec_ctx,
        )

        # 2. Validate Workbench
        if not self.task.workspace.exists():
            raise FileNotFoundError(f"Task workbench missing: {self.task.folder}")

        # 3. Dedicated Logging
        self.handler_id = setup_logger(
            log_dir=self.executor.exec_ctx.workspace_dir / "logs",
            is_prod=self.executor.exec_ctx.is_prod,
            is_debug=self.executor.exec_ctx.is_debug,
            filename=f"{self.task.id}_{self.task.run_id}.jsonl".replace(":", "_"),
            enqueue=True,
        )

        # 4. Start Handshake
        self.task.check_in(self.task_ref.stage)
        self.task.workspace.remove_marker(".retrying")
        self.task.workspace.remove_marker(".blocked")

        self.log.info("Task session initialized", stage=self.task_ref.stage)
        return self.task

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Ensure finalization happens even if payload execution fails
        if self.task and not isinstance(exc_val, RewindTask):
            self.executor.finalize_task_execution(self.task, runtime_exception=exc_val)

        if self.handler_id is not None:
            logger.remove(self.handler_id)

        self.executor.is_busy = False
