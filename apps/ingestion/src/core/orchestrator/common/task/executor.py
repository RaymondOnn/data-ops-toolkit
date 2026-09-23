"""Task execution engine for running individual pipeline stages."""

# Start: 469 -> 320

import os
import subprocess

import ray
from libs.utils.exceptions import (
    install_exception_hooks,
)
from loguru import logger

from src.core.contexts import ExecutionContext
from src.core.models.task import (
    ExecutionStatus,
    TaskManifest,
    TaskManifestFile,
    TaskWorkspace,
)
from src.core.orchestrator.common.task.timeout import (
    TimeoutContext,
)
from src.core.orchestrator.common.task.types import TaskMetadata, TaskRef
from src.core.stages.contracts.stage import ExecutionStage
from src.core.stages.types import StageContext
from src.services.factory import ServiceFactory
from src.services.health.monitor import ServiceMonitor
from src.services.health.system import SystemMonitor
from src.utils.constants import CACHE_TASK_NAMESPACE

from .cache import TaskCache
from .queue import TaskQueue
from .session import TaskSession

LOG = logger


class Executor:
    """The primary execution engine for processing individual pipeline stages.

    This class handles the lifecycle of a stage run, including state
    transitions in the hot cache, workspace session management, and
    delegation to either internal logic or external PEX processes.
    """

    def __init__(self, worker_id: str, exec_ctx: ExecutionContext):
        """Initializes the executor with local worker metadata.

        Args:
            worker_id: Unique identifier for this Ray worker process.
            exec_ctx: The global execution context for environment settings.

        Decision: Resource Locality.
        We re-initialize the ServiceMonitor and Provider within the
        constructor to ensure that database and secret connections are
        established locally on the Ray worker node, avoiding the
        serialization of active network handles.
        """
        LOG.trace(
            "[DISPATCH] executor init",
            worker_id=worker_id,
            workspace=str(exec_ctx.workspace_dir),
        )
        self.worker_id = worker_id
        self.exec_ctx = exec_ctx
        self.is_busy = False

        cache_config = self.exec_ctx.cache_config
        self.cache = TaskCache(
            cache_config=cache_config, prefix=f"{CACHE_TASK_NAMESPACE}:"
        )
        self.queue = TaskQueue(self.exec_ctx.task_queue_config)
        self.current_metadata: TaskMetadata | None = None
        self.system = SystemMonitor(self.exec_ctx.workspace_dir)

        ServiceMonitor.setup(
            signal_dir=self.exec_ctx.signal_path,
            cache_config=self.exec_ctx.cache_config,
        )
        ServiceFactory.get_provider(self.exec_ctx.provider_config)
        LOG.trace("[DISPATCH] executor ready", worker_id=worker_id)

    def execute_stage(self, cache_key: str) -> None:
        """Coordinates the execution of a specific stage defined by a cache key.

        Args:
            cache_key: The formatted string identifier for the task in the cache.
            msg_id: The FlashQ message ID to ACK upon completion.

        Note:
        - We transition the task state to RUNNING before entering the
        TaskSession. This prevents the TaskManager from re-dispatching
        the same task if the maintenance loop ticks while the worker
        is still bootstrapping its local environment.
        """
        cache_key = self.cache._sanitize_key(cache_key)
        task_ref = TaskRef.from_key(cache_key)
        LOG.trace(
            "[DISPATCH] execute start",
            run_id=task_ref.identity.run_id,
            step_id=task_ref.step_id,
            cache_key=cache_key,
        )
        metadata = self.cache.get(cache_key)

        # Cache it onto the executor context for state outcome handlers to consume
        self.current_metadata = metadata
        # Move task to RUNNING status
        self.cache.transition_state(self.current_metadata, ExecutionStatus.RUNNING)

        task = None
        try:
            self.is_busy = True
            # 2. Execution Runtime Phase
            LOG.trace("[DISPATCH] execute running", run_id=task_ref.identity.run_id)

            # 1. Instantiate workspace & initialize manifest ONCE
            workspace = TaskWorkspace(
                job_id=metadata.job_id,
                dataset_id=metadata.dataset_id,
                partition_date=metadata.partition_date,
                run_id=metadata.run_id,
                exec_ctx=self.exec_ctx,
            )

            # Load or create skeleton manifest pointing to actual step_id
            manifest_path = TaskManifestFile.resolve_path(workspace)
            if not manifest_path.is_file():
                manifest = TaskManifest(
                    job_id=metadata.job_id,
                    run_id=metadata.run_id,
                    dataset_id=metadata.dataset_id,
                    current_step_id=task_ref.step_id,  # Uses 'extract' instead of fallback 'start'
                    status=ExecutionStatus.UNKNOWN,
                )
                TaskManifestFile.save(manifest, workspace)
            else:
                manifest = TaskManifestFile.load(workspace)

            with (
                TimeoutContext(
                    step_id=metadata.current_step_id,
                    timeout_state=metadata.timeout_state,
                ) as t_ctx,
                TaskSession(
                    self.worker_id,
                    exec_ctx=self.exec_ctx,
                    task_ref=task_ref,
                    log=LOG,
                    manifest=manifest,
                ) as session_task,
            ):
                task = session_task
                LOG.trace(
                    "[DISPATCH] execute session started",
                    run_id=task_ref.identity.run_id,
                    workspace=self.exec_ctx.get_run_path(task_ref.identity, "ACTIVE"),
                )
                if self._should_use_pex():
                    LOG.trace(
                        "[DISPATCH] execute using PEX",
                        pex_path=str(self.exec_ctx.code_pex_path),
                    )
                    self._run_via_pex(task_ref.step_id, metadata, t_ctx)
                else:
                    LOG.trace("[DISPATCH] execute direct", step=task_ref.step_id)

                    # 1. Instantiate execution dependencies for direct run
                    workspace = TaskWorkspace(
                        job_id=metadata.job_id,
                        dataset_id=metadata.dataset_id,
                        partition_date=metadata.partition_date,
                        run_id=metadata.run_id,
                        exec_ctx=self.exec_ctx,
                    )
                    manifest = TaskManifestFile.load(workspace)

                    step = task.context.get_step(task_ref.step_id)
                    stage_ctx = StageContext(
                        job_id=metadata.job_id,
                        dataset_id=metadata.dataset_id,
                        run_id=metadata.run_id,
                        partition_date=metadata.partition_date,
                        step_id=task_ref.step_id,
                        next_step_id=task.context.get_next_step_id(task_ref.step_id)
                        or "",
                        # config=step.config,
                        data_dir=workspace.get_data_path(task_ref.step_id),
                        workspace_dir=workspace.path,
                        task_context=task.context,
                    )

                    # 2. Execute stage directly
                    stage = ExecutionStage.create(step.stage, step=step)
                    stage.pre_flight(
                        system=self.system,
                        ctx=stage_ctx,
                        workspace=workspace,
                        manifest=manifest,
                    )
                    stage.execute(ctx=stage_ctx, workspace=workspace, manifest=manifest)

                LOG.trace(
                    "[DISPATCH] execute stage completed",
                    run_id=task_ref.identity.run_id,
                    step=task_ref.step_id,
                )

        except Exception:
            LOG.exception(
                "Failed to execute stage",
                run_id=task_ref.identity.run_id,
                step=task_ref.step_id,
            )
            raise
        finally:
            self.is_busy = False

    def _should_use_pex(self) -> bool:
        """Determines if the executor should invoke an external PEX binary.

        Returns:
            bool: True if a valid PEX path is configured and exists.

        Notes:
        - Using a PEX allows the execution of code in a completely isolated
        Python environment, which is critical for regression testing where
        the 'stable' baseline must run without contamination from
        the 'candidate' library changes.
        """
        return bool(
            self.exec_ctx.code_pex_path and self.exec_ctx.code_pex_path.exists()
        )

    def _run_via_pex(
        self,
        step_id: str,
        metadata: TaskMetadata,
        timeout_context: TimeoutContext | None = None,
    ) -> None:
        """Spawns a subprocess to execute the stage using a PEX binary.

        Args:
            task: The Task object representing the current execution.
            metadata: The TaskMetadata from the cache.
            timeout_context: Optional timeout context to enforce deadlines on the subprocess.

        """
        # Ensure that the subprocess uses the specified dependency PEX
        # to provide a isolated, reproducible runtime environment for
        # the stage execution.
        env = os.environ.copy()
        if self.exec_ctx.deps_pex_path:
            env["PEX_PATH"] = str(self.exec_ctx.deps_pex_path)

        cmd = [
            "python3",
            str(self.exec_ctx.code_pex_path),
            "run",
            metadata.partition_date,
            "--job-id",
            metadata.job_id,
            "--dataset",
            metadata.dataset_id,
            "--step-id",
            step_id,  # Clean string reference
        ]

        timeout_secs = timeout_context.get_remaining() if timeout_context else None

        # Capture stdout and stderr to stream them through our logging session
        process = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,  # Line buffered
        )

        try:
            # Read streams concurrently without blocking
            # (In production, a select loop or thread reader is preferred to avoid deadlocks)
            while True:
                if process.stdout is None:
                    break

                output = process.stdout.readline()
                if output == "" and process.poll() is not None:
                    break
                if output:
                    LOG.info(f"[PEX-STDOUT] {output.strip()}")

                rc = process.wait(timeout=timeout_secs)
                if rc != 0:
                    stderr_err = process.stderr.read() if process.stderr else ""
                    LOG.error(f"[PEX-STDERR] {stderr_err.strip()}")
                    raise subprocess.CalledProcessError(
                        rc, cmd, output=output, stderr=stderr_err
                    )

        except subprocess.TimeoutExpired as e:
            process.kill()
            raise TimeoutError(
                f"PEX subprocess timed out after {timeout_secs:.1f}s"
            ) from e


@ray.remote
def process_stage_task(
    worker_id: str,
    exec_ctx: ExecutionContext,
    cache_key: str,
):
    """Entry point for Ray task execution."""
    import gc
    import sys

    from loguru import logger

    # Setup worker isolated logging
    logger.remove()
    logger.add(
        sys.stdout,
        level="DEBUG",
        colorize=True,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    )
    install_exception_hooks()
    logger.info(f"Starting task processing for worker {worker_id}, key: {cache_key}")

    executor = Executor(worker_id, exec_ctx)

    try:
        executor.execute_stage(cache_key)
        # Success scenario: return updated metadata to the driver
        return {
            "run_id": (
                executor.current_metadata.run_id if executor.current_metadata else None
            ),
            "success": True,
            "error": None,
            "metadata": executor.current_metadata,
        }
    except Exception as runtime_error:
        # Failure scenario: pass the error back so the driver can process outcome logic
        return {
            "run_id": (
                executor.current_metadata.run_id if executor.current_metadata else None
            ),
            "success": False,
            "error": runtime_error,
            "metadata": executor.current_metadata,
        }
    finally:
        executor.is_busy = False
        gc.collect()
