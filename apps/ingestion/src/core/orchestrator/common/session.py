import time
from typing import TYPE_CHECKING

from apps.ingestion.src.core.models.task import ExecutionStatus, Task
from apps.ingestion.src.core.models.task.enums import TaskRef
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
        """
        Initializes the TaskSession.

        Args:
            executor: The compute executor running the task.
            task_ref: The identity and routing handle for the task.
            log: The logger instance for the execution context.
        """
        self.executor = executor
        self.task_ref = task_ref
        self.log = log
        self.handler_id: int | None = None
        self.task: Task | None = None
        self.start_time: float = 0

    def __enter__(self) -> Task:
        """
        Prepares the environment for task execution.

        Rehydrates the Task model, validates the physical workspace,
        initializes dedicated file-based logging, and performs the
        start-of-stage check-in.

        Returns:
            Task: The initialized and checked-in task instance.

        Raises:
            FileNotFoundError: If the task workspace directory does not exist.
        """
        # 1. Rehydrate Identity
        self.start_time = time.perf_counter()
        self.task = Task(
            task_ref=self.task_ref.with_updates(status=ExecutionStatus.RUNNING),
            worker_id=self.executor.worker_id,
            exec_ctx=self.executor.exec_ctx,
        )

        # 2. Validate Workbench
        if not self.task.workspace.exists():
            raise FileNotFoundError(
                f"Task workbench missing: {self.task.workspace.run_path}"
            )

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

        self.log.info(
            "ExecutionStage {stage} started", stage=self.task_ref.stage.upper()
        )
        return self.task

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Finalizes the task execution and cleans up resources.

        Closes the dedicated log handler, updates the task manifest
        based on the success or failure of the execution, and notifies
        the executor that the worker is no longer busy.

        Args:
            exc_type: The type of exception raised during execution.
            exc_val: The exception instance raised.
            exc_tb: The traceback for the exception.
        """
        duration = time.perf_counter() - self.start_time

        # Ensure finalization happens even if payload execution fails
        if self.task and not isinstance(exc_val, RewindTask):
            self.executor.finalize_task_execution(self.task, runtime_exception=exc_val)

        if exc_val:
            if isinstance(exc_val, RewindTask):
                self.log.warning(
                    "ExecutionStage {stage} finished (REWIND)",
                    stage=self.task_ref.stage.upper(),
                    duration_sec=round(duration, 4),
                )
            else:
                self.log.error(
                    "ExecutionStage {stage} failed",
                    stage=self.task_ref.stage.upper(),
                    duration_sec=round(duration, 4),
                    error=str(exc_val),
                )
        else:
            self.log.info(
                "ExecutionStage {stage} finished",
                stage=self.task_ref.stage.upper(),
                duration_sec=round(duration, 4),
            )

        if self.handler_id is not None:
            logger.remove(self.handler_id)

        self.executor.is_busy = False
