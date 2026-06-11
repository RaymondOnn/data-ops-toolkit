"""Main orchestrator for task execution lifecycle."""

import sys
from contextlib import suppress
from functools import partial
from typing import Any

import msgspec
from apps.ingestion.src.core.contexts import TaskContextBuilder
from apps.ingestion.src.core.models.task import ExecutionStatus, Task, TaskSignal
from apps.ingestion.src.core.models.task.enums import TaskIdentity
from apps.ingestion.src.core.orchestrator.enums import TaskRef
from apps.ingestion.src.services.factory import ServiceFactory
from apps.ingestion.src.utils.common import short_hash
from apps.ingestion.src.utils.constants import (
    CACHE_TASK_NAMESPACE,
    CONFIG_FILENAME,
    DISK_THRESHOLD_HALT,
    STRIP_TZ_FOR_DB,
)
from libs.utils.dates import current_timestamp
from libs.utils.system import get_disk_usage, get_system_vitals
from loguru import logger

from .janitor import Janitor
from .manager import TaskManager
from .signals import SignalEvent, SignalScanner
from .state import StateHub

LOG = logger


def generate_run_id() -> str:
    """Generate a unique run ID."""
    timestamp = current_timestamp(naive=STRIP_TZ_FOR_DB).strftime("%Y%m%d-%H%M%S")
    return f"{timestamp}-{short_hash(8)}"


class Orchestrator:
    """Main orchestrator managing task lifecycle, scheduling, and state."""

    def __init__(
        self,
        builder: TaskContextBuilder,
        state_hub: StateHub,
        task_manager: TaskManager,
        janitor: Janitor,
        scanner: SignalScanner,
    ):
        self.builder = builder
        self.exec_ctx = builder.build_execution_context()
        self.state = state_hub
        self.tasks = task_manager
        self.janitor = janitor
        self.signals = scanner

        self._check_system_health()
        self._init_services()
        self._register_signal_handlers()
        LOG.info("Orchestrator initialized")

    def _init_services(self) -> None:
        """Initialize service providers."""
        (self.exec_ctx.workspace_dir / "logs").mkdir(parents=True, exist_ok=True)

        try:
            self.exec_ctx.provider_config = self.builder.app_settings.get(
                "secret_provider", {}
            ).to_dict()
            ServiceFactory.get_provider(
                self.exec_ctx.env, self.exec_ctx.provider_config
            )
        except Exception:
            LOG.exception("Service initialization failed")
            raise

    def _check_system_health(self) -> None:
        """Monitor system health and apply backpressure."""
        vitals = get_system_vitals()
        disk = get_disk_usage(self.exec_ctx.workspace_dir)

        if disk.percent > DISK_THRESHOLD_HALT:
            LOG.critical("Disk full. Stopping orchestrator.")
            sys.exit(1)

        self.tasks.is_degraded = vitals.mem_pct > 85

    def preflight_check(self) -> None:
        """Verify critical infrastructure before starting."""
        LOG.info("Running pre-flight checks...")
        try:
            _ = self.state.sink.db
        except Exception as e:
            LOG.critical(f"Database unreachable: {e}")
            self.state.close()  # Clean up only on failure
            raise

    def process_signals(self, filter_run_ids: set[str] | None = None) -> None:
        """Process and dispatch signals."""
        self.signals.dispatch(filter_run_ids=filter_run_ids)

    def _register_signal_handlers(self) -> None:
        """Register signal handlers."""
        success_fn = partial(self._on_terminal_task, success=True)
        failure_fn = partial(self._on_terminal_task, success=False)

        self.signals.on(TaskSignal.DONE, success_fn)
        self.signals.on(TaskSignal.FAIL, failure_fn)
        self.signals.on(TaskSignal.SYNC, self._on_task_sync)

    def _on_terminal_task(self, event: SignalEvent, success: bool) -> None:
        """Unified handler for terminal task states."""
        log_fn = LOG.success if success else LOG.error

        error_context = ""
        traceback_str = None
        if event.folder_path:
            with suppress(Exception):
                task = Task.from_path(event.folder_path, self.exec_ctx)
                err = task.manifest.error
                if err:
                    error_context = (
                        f" | Stage: {err.stage.upper()} | Error: {err.message}"
                    )
                    traceback_str = err.traceback

        log_fn(
            f"Task {'completed' if success else 'failed'}: "
            f"{event.identity.run_id}{error_context}"
        )
        if not success and traceback_str:
            LOG.error(f"Full traceback for {event.identity.run_id}:\n{traceback_str}")

        if event.folder_path:
            self.state.sync_manifest(event.folder_path, deep_sync=True)
            if success:
                self.janitor._cleanup_task(
                    Task.from_path(event.folder_path, self.exec_ctx)
                )
            else:
                self.janitor.quarantine(event.folder_path, "FAILED")

    def _on_task_sync(self, event: SignalEvent) -> None:
        if event.folder_path:
            self.state.sync_manifest(event.folder_path, deep_sync=False)
        else:
            self.state.update_task(event.identity.run_id, {})

    def submit_tasks(self) -> None:
        """Run one dispatch cycle of the scheduler."""
        if not self.tasks.cache.is_empty():
            self.tasks.dispatch()

    def start_job(
        self,
        job_id: str,
        dataset_id: str | None = None,
        partition_date: str | None = None,
        overrides: dict[str, Any] | None = None,
        run_id: str | None = None,
    ) -> set[str]:
        """Start a new job or task."""
        run_ids = set()
        contexts = self.builder.build(
            job_id=job_id,
            dataset_id=dataset_id,
            partition_date=partition_date,
            overrides=overrides,
        )

        for ctx in contexts:
            run_id = run_id or generate_run_id()
            identity = TaskIdentity(
                job_id=ctx.job_id,
                dataset_id=ctx.dataset_id,
                partition_date=ctx.partition_date,
                run_id=run_id,
            )
            task_ref = TaskRef(
                namespace=CACHE_TASK_NAMESPACE,
                status=ExecutionStatus.PROVISIONED,
                stage=ctx.from_stage,
                identity=identity,
            )

            LOG.info(f"Provisioning task: {run_id}")

            # Persist config
            config_path = (
                self.exec_ctx.active_path
                / f"{identity.task_key}:{run_id}_{CONFIG_FILENAME}"
            )
            config_path.write_bytes(msgspec.json.encode(ctx))

            # Register and provision
            self.state.add_task(task_ref)
            task = Task(
                task_ref=task_ref, worker_id="orchestrator", exec_ctx=self.exec_ctx
            )
            task.workspace.create(config_path)

            # Queue for execution
            queued = self.tasks.enqueue(task_ref, str(config_path))
            if queued:
                self.state.update_task(
                    run_id,
                    {"JOB_STATUS": queued.status.value, "CURRENT_STAGE": queued.stage},
                )

            run_ids.add(run_id)
            self.signals.notify()

        return run_ids

    def abort_job(
        self, task: Task, status: ExecutionStatus, reason: str | None = None
    ) -> None:
        """Abort a running job."""
        LOG.error(f"Aborting job {task.job_id}: {status}", reason=reason)

        task.update_manifest(
            {"status": status, "current_stage": ExecutionStatus.CANCELLED}
        )
        task.send_signal(TaskSignal.DONE)
        self.state.sync_manifest(task.workspace.path)

        if status == ExecutionStatus.EXPIRED:
            self.janitor._cleanup_task(task)

    def summarize_failures(self, run_ids: set[str]) -> None:
        """Print failure summary for failed runs."""
        print("\n" + "═" * 60)
        print(" PIPELINE POST-MORTEM ".center(60, "═"))
        print("═" * 60)

        failures = []

        for run_id in run_ids:
            record = self.state.store.records.get(run_id)
            if record and ExecutionStatus(record.JOB_STATUS) == ExecutionStatus.FAILED:
                error_info = record.ERRORS or {}
                error_type = error_info.get("error_type", "RuntimeError")
                message = error_info.get("message", "Unknown Error")
                stage = error_info.get("stage", record.CURRENT_STAGE or "Unknown")
                failures.append(
                    f"[Run: {run_id} | Stage: {stage}] {error_type}: {message}"
                )

        if failures:
            print("\n" + "═" * 60)
            print(" FAILURE SUMMARY ".center(60, "═"))
            print("═" * 60)
            for failure in failures:
                print(f"  ❌ {failure}")
            print("═" * 60)
            sys.exit(1)

        LOG.success("All tasks completed successfully.")
