import sys
from typing import TYPE_CHECKING, Any

import msgspec
from apps.ingestion.src.core.contexts import TaskContextBuilder
from apps.ingestion.src.core.models.task import (
    ExecutionStatus,
    Task,
    TaskSignal,
)
from apps.ingestion.src.core.models.task.enums import TaskIdentity
from apps.ingestion.src.core.orchestrator.enums import TaskRef
from apps.ingestion.src.services.factory import ServiceFactory
from apps.ingestion.src.utils.common import make_short_hash
from apps.ingestion.src.utils.constants import (
    CACHE_TASK_NAMESPACE,
    CONFIG_FILENAME,
    DISK_THRESHOLD_HALT,
    STRIP_TZ_FOR_DB,
)
from libs.utils.dates import get_current_timestamp
from libs.utils.system import get_disk_usage, get_system_vitals
from loguru import logger

from .janitor import Janitor
from .manager import TaskManager
from .signals import SignalProcessor
from .state import StateStore

if TYPE_CHECKING:
    from apps.ingestion.src.core.contexts.task import TaskContext


LOG = logger

# TODO: Check Disk Space
# - 85%+: Mark system DEGRADED, disable Extract stage
# - 90%+: Mark system CRITICAL, alert on-call
# - Load stage continues to clear backlog)

# TODO: Backfill Runs
# TODO: Regression Testing
# TODO: Feature Toggles
# TODO: Cancel Task
# TODO: Dynamic Stage Order (e.g. Archive before Write)


def generate_run_id() -> str:
    """Generates a unique run ID for a task."""

    timestamp = get_current_timestamp(strip_tz=STRIP_TZ_FOR_DB).strftime(
        "%Y%m%d-%H%M%S"
    )
    short_hash = make_short_hash(8)
    return f"{timestamp}-{short_hash}"


class Orchestrator:
    def __init__(
        self,
        builder: TaskContextBuilder,
        state_store: StateStore,
        task_manager: TaskManager,
        janitor: Janitor,
        signal_processor: SignalProcessor,
    ):
        self.builder = builder
        self.exec_ctx = builder.get_execution_context()
        self.state_store = state_store
        self.tasks = task_manager
        self.janitor = janitor
        self.signals = signal_processor

        self._check_system_health()

        LOG.info("Orchestrator initialized")

    def _perform_platform_preflight(self) -> None:
        """Validates critical shared infrastructure."""
        # 1. Verify connection to the State Tracking database.
        # This forces the lazy property to initialize, giving us "Fail-Fast" behavior
        # for long-running modes (Daemon / Sync Run) without slowing down CLI tools.
        LOG.info("Performing platform pre-flight check...")
        try:
            _ = self.state_store.db
        except Exception as e:
            LOG.critical("Pre-flight check failed: Database unreachable", error=str(e))
            raise e
        finally:
            self.state_store.close()  # Close connection after pre-flight

    def _check_system_health(self) -> None:
        """Monitors system vitals to apply backpressure."""
        vitals = get_system_vitals()
        usage = get_disk_usage(self.exec_ctx.workspace_dir)

        if usage.percent > DISK_THRESHOLD_HALT:
            LOG.critical("Disk full. Stopping orchestrator.")
            sys.exit(1)

        if vitals.mem_pct > 85:
            LOG.warning("High memory pressure detected", usage=vitals.mem_pct)
            # Degrade engine performance to prevent OOM
            self.tasks.is_degraded = True
        else:
            self.tasks.is_degraded = False

    def _init_services(self) -> Any:
        (self.exec_ctx.workspace_dir / "logs").mkdir(parents=True, exist_ok=True)

        try:
            self.exec_ctx.provider_config = self.builder.app_settings.get(
                "secret_provider", {}
            ).to_dict()
            ServiceFactory.get_provider(
                self.exec_ctx.env, self.exec_ctx.provider_config
            )
        except Exception as e:
            LOG.error(f"Service initialization failed: {e}")
            raise e

    def process_task_events(self, run_filter: set[str] | None = None) -> None:
        """AUTHORITATIVE HANDLER: Orchestrates actions based on detected signals."""
        # Process task-specific signals (.done, .fail, .sync)
        events = self.signals.collect_events(filter_run_ids=run_filter)
        if not events:
            return

        for event in events:
            run_id = event.identity.run_id
            if event.signal_type == ".done":
                LOG.success("Task Completion detected", run_id=run_id)
                if event.folder_path:
                    self.state_store.sync_from_folder(event.folder_path, deep_sync=True)
                    task = Task.from_folder(event.folder_path, self.exec_ctx)
                    self.janitor.cleanup_task(task)

            elif event.signal_type == ".fail":
                LOG.error("Task Failure detected", run_id=run_id)
                if event.folder_path:
                    # 1. Authoritative Sync: Pull the error details into the log buffer.
                    # We use deep_sync=True to ensure the FINAL_MANIFEST JSON is captured.
                    self.state_store.sync_from_folder(event.folder_path, deep_sync=True)

                    # 2. Quarantining: Physically move the folder using the Janitor
                    self.janitor.quarantine_task(event.folder_path, "FAILED")

            elif event.signal_type == ".sync":
                if event.folder_path:
                    self.state_store.sync_from_folder(
                        event.folder_path, deep_sync=False
                    )
                else:
                    self.state_store.update_run(run_id, {})

            # elif event.signal_type == ".expired":
            #     if run_record := self.state_store.active_registry.get(run_id):
            #         self.state_store.emit_expiry(
            #             run=run_record, context=None, reason="TTL Expired"
            #         )

        # Wake up the dispatch loop
        self.signals.notify()

    def _drive_engine(self) -> None:
        """Shared logic to process the task queue."""
        if self.tasks.cache.is_empty():
            return

        self.tasks._process_tasks()

    def _summarize_failures(self, state_store: Any, run_ids: set[str]):
        """
        Gathers failure details from the StateStore and prints a clean summary.
        """
        print("\n" + "═" * 60)
        print(" PIPELINE POST-MORTEM SUMMARY ".center(60, "═"))
        print("═" * 60)

        collected_exceptions = []

        for rid in run_ids:
            # Retrieve the manifest record synced from the Ray worker
            record = state_store.active_registry.get(rid)

            if record and ExecutionStatus(record.JOB_STATUS) == ExecutionStatus.FAILED:
                # Use metadata saved by FailedState.on_enter in executor.py
                error_type = getattr(record, "ERROR_TYPE", "RuntimeError")
                msg = getattr(record, "ERROR_MESSAGE", "Unknown Error")
                stage = getattr(record, "CURRENT_STAGE", "Unknown")

                # Create a descriptive message for the ExceptionGroup tree
                exc_msg = f"[Run: {rid} | Stage: {stage}] {msg}"

                # Attempt to find the matching Python built-in exception
                try:
                    import builtins

                    exc_class = getattr(builtins, error_type, RuntimeError)
                    # We create the exception instance without re-raising yet
                    collected_exceptions.append(exc_class(exc_msg))
                except Exception:
                    collected_exceptions.append(
                        RuntimeError(f"{error_type}: {exc_msg}")
                    )

        if collected_exceptions:
            # This is the "End of Logs" separator
            print("\n" + "═" * 60)
            print(" PIPELINE FAILURE SUMMARY ".center(60, "═"))
            print("═" * 60)

            # Raising this will trigger the built-in 3.11+ ExceptionGroup renderer
            raise ExceptionGroup(
                f"Pipeline encountered {len(collected_exceptions)} failures",
                collected_exceptions,
            )

        logger.success("All tasks completed successfully.")

        print("═" * 60 + "\n")

    def _trigger_job(
        self,
        job_id: str,
        dataset_id: str | None = None,
        partition_date_str: str | None = None,
        overrides: dict[str, Any] | None = None,
        run_id: str | None = None,
    ) -> set[str]:
        run_ids = set()

        # 1. Get the list of dataset configurations for this Task ID
        task_contexts: list[TaskContext] = self.builder.build(
            job_id=job_id,
            dataset_id=dataset_id,
            partition_date_str=partition_date_str,
            overrides=overrides,
        )

        for task_ctx in task_contexts:
            # 1. Create the TaskRef first - this is now the source of truth for identity
            run_id = run_id or generate_run_id()
            identity = TaskIdentity(
                job_id=task_ctx.job_id,
                dataset_id=task_ctx.dataset_id,
                partition_date=task_ctx.partition_date,
                run_id=run_id,
            )
            task_ref = TaskRef(
                namespace=CACHE_TASK_NAMESPACE,
                status=ExecutionStatus.PROVISIONED,
                stage=task_ctx.from_stage,
                identity=identity,
            )

            log = LOG.bind(
                run_id=task_ref.identity.run_id,
                job_id=task_ref.identity.job_id,
                dataset_id=task_ref.identity.dataset_id,
            )
            log.info(
                "Provisioning new run",
                run_id=task_ref.identity.run_id,
                identifier=task_ref.identity.identifier,
                from_stage=task_ref.stage,
            )

            # 2. Freeze the Task Context using the TaskRef identity
            config_path = (
                self.exec_ctx.active_path
                / f"{task_ref.identity.identifier}:{task_ref.identity.run_id}_{CONFIG_FILENAME}"
            )
            with config_path.open("wb") as f:
                f.write(msgspec.json.encode(task_ctx))

            # 3. Seed the State Store Registry using the key
            self.state_store.create_record(task_ref=task_ref)

            # 4. Workspace Provisioning (Using Task identity)
            task = Task(
                task_ref=task_ref,
                worker_id="orchestrator",
                exec_ctx=self.exec_ctx,
            )
            task.workspace.provision(config_path)

            # 5. Queue to Engine
            queued_ref = self.tasks.queue_tasks(
                task_ref, config_file_path=str(config_path)
            )

            # Update the state store with the actual TaskKey from the queue
            if queued_ref:
                self.state_store.update_run(
                    task_ref.identity.run_id,
                    {
                        "JOB_STATUS": queued_ref.status.value,
                        "CURRENT_STAGE": queued_ref.stage,
                    },
                )

            # 5. Optional: Update DB so it doesn't trigger again immediately
            # self.state_store.update_status(job_id, ExecutionStatus.QUEUED)
            run_ids.add(run_id)
            self.signals.notify()

        return run_ids

    def _terminate_job(
        self, task: Task, status: ExecutionStatus, reason: str | None = None
    ) -> None:
        """
        Controlled Crash Handler.
        Uses the Task's internal status updater to ensure consistency.
        """
        LOG.error("Terminating job", job_id=task.job_id, status=status, reason=reason)

        # 1. Update the Task state
        # We pass the reason as the payload so it gets serialized into the manifest
        # update_manifest handles the msgspec encoding and file write internally
        task.update_manifest(
            {
                "status": status,
                "current_stage": ExecutionStatus.CANCELLED,
            }
        )
        task.request_status_sync(TaskSignal.DONE)

        # 2. Sync the StateStore
        # Since request_status_sync created the .done file, we tell the StateStore
        # to perform its final deep sync to pull the failure details into the DB.
        self.state_store.sync_from_folder(task.workspace.run_path)

        # 3. Cleanup logic (Optional: move to failed or delete)
        if status == ExecutionStatus.EXPIRED:
            self.janitor.cleanup_task(task)


# def create_orchestrator(
#     app_cfg_path: str | None = None, env: str | None = None
# ) -> Orchestrator:
#     """
#     Bootstrap helper to initialize the Orchestrator with its dependencies.
#     Resolves configuration first to properly initialize services.
#     """
#     # 1. Initialize Builder (loads global app.yaml)
#     builder = (
#         TaskContextBuilder(app_cfg_path=app_cfg_path, env=env)
#         if env
#         else TaskContextBuilder(app_cfg_path=app_cfg_path)
#     )

#     # 2. Return fully wired Orchestrator (It will resolve its own services)
#     return Orchestrator(builder=builder)
