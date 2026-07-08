"""Task execution session manager with lifecycle hooks."""

import time

from apps.ingestion.src.core.contexts import ExecutionContext
from apps.ingestion.src.core.models.task import ExecutionStatus, Task
from apps.ingestion.src.core.models.task.enums import TaskRef
from apps.ingestion.src.utils.exceptions import RollbackRequired
from loguru import logger


class TaskSession:
    """Context manager for managing the lifecycle of a single task stage execution.

    This class handles task initialization, dedicated logging setup, and
    ensures proper cleanup and state conclusion upon exit.
    """

    def __init__(
        self, worker_id: str, exec_ctx: ExecutionContext, task_ref: TaskRef, log
    ):
        """Initializes the task session.

        Args:
            worker_id: The unique identifier for the current worker.
            exec_ctx: Global execution context.
            task_ref: The reference to the task being executed.
            log: A Loguru logger instance for this session.

        Decision: Contextual Logging.
        Each TaskSession gets a dedicated logger handler that writes to a
        specific file for that task, ensuring isolated and searchable logs.
        """
        self.worker_id = worker_id
        self.exec_ctx = exec_ctx
        self.task_ref = task_ref
        self.log = log
        self.task: Task | None = None
        self._handler_id: int | None = None
        self._context_manager = None
        self._start_time: float = 0

    def __enter__(self) -> Task:
        """Enters the runtime context for the task stage.

        Returns:
            Task: The initialized Task object for the current stage.

        Raises:
            FileNotFoundError: If the task's workspace directory is missing.

        Decision: Fail-Fast Workspace Validation.
        We verify the existence of the task workspace immediately upon entry.
        This prevents later operations from failing due to missing directories.
        """
        self._start_time = time.perf_counter()

        self._context_manager = logger.contextualize(
            run_id=self.task_ref.identity.run_id,
            job_id=self.task_ref.identity.job_id,
            stage=self.task_ref.stage,
        )
        self._context_manager.__enter__()

        log_dir = self.exec_ctx.workspace_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_name = log_dir / (
            f"{self.task_ref.identity.task_key}_"
            f"{self.task_ref.identity.run_id}.jsonl".replace(":", "_")
        )
        self._handler_id = logger.add(
            str(log_name),
            level="DEBUG",
            serialize=True,  # JSON format for structured logging
            enqueue=True,
            # Don't rotate per run - it's a single file per run
        )

        self.log.info(f"Session initialized for task: {self.task_ref.identity.run_id}")

        self.task = Task(
            task_ref=self.task_ref.with_updates(status=ExecutionStatus.RUNNING),
            worker_id=self.worker_id,
            exec_ctx=self.exec_ctx,
        )

        # Verify workspace exists
        if not self.task.workspace.exists():
            raise FileNotFoundError(
                f"Task workspace missing: {self.task.workspace.path}"
            )

        # Initialize
        self.task.check_in(self.task_ref.stage)

        if (self.task.workspace.path / ".retrying").exists():
            self.task.update_manifest(
                {"retry_count": self.task.manifest.retry_count + 1}
            )
            self.task.workspace.remove_marker(".retrying")

        self.task.workspace.remove_marker(".blocked")
        self.log.info(f"Stage {self.task_ref.stage.upper()} started")

        return self.task

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exits the runtime context, handling exceptions and finalizing the task.

        Args:
            exc_type: The type of the exception raised, or None if no exception.
            exc_val: The exception instance, or None.
            exc_tb: The traceback object, or None.

        Decision: Rollback Exception Handling.
        `RollbackRequired` is a special exception that signals a non-terminal
        failure, allowing the task to be rewound without being marked as
        permanently FAILED.
        """
        duration = time.perf_counter() - self._start_time

        # Log completion
        if exc_val:
            if isinstance(exc_val, RollbackRequired):
                self.log.warning(
                    f"Stage {self.task_ref.stage.upper()} rewound ({duration:.2f}s)"
                )
            else:
                self.log.error(f"Stage {self.task_ref.stage.upper()} failed: {exc_val}")
        else:
            self.log.info(
                f"Stage {self.task_ref.stage.upper()} completed ({duration:.2f}s)"
            )

        # Cleanup
        if self._handler_id:
            logger.remove(self._handler_id)

        # Exit the logging context
        if self._context_manager:
            self._context_manager.__exit__(exc_type, exc_val, exc_tb)
