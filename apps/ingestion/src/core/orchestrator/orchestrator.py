import sys
import time
from collections import Counter
from copy import deepcopy
from datetime import datetime
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import msgspec
import structlog
from apps.ingestion.src.core.contexts import TaskContextBuilder
from apps.ingestion.src.core.models.job import ExecutionStatus, Task
from apps.ingestion.src.core.models.stages.enums import StageName
from apps.ingestion.src.services.factory import ServiceFactory
from apps.ingestion.src.utils.constants import CONFIG_FILENAME, DISK_THRESHOLD_HALT
from apscheduler.events import EVENT_JOB_ERROR, JobExecutionEvent
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from libs.resilience.heartbeat import Heartbeat
from libs.utils.system import get_disk_usage, get_system_vitals
from nanoid import generate

from .engine import IngestionEngine
from .enums import JobRecord, TaskMetadata
from .lifecycle import LifecycleManager
from .signals import SignalProcessor
from .state import StateStore
from .trigger import FileTriggerEvent, TimeTriggerEvent, TriggerEvent

if TYPE_CHECKING:
    from .enums import TaskMetadata

LOG = structlog.getLogger(__name__)

DEFAULT_SYNC_TIMEOUT_SECS = 1800  # 1 Hour default
INTERVAL_HEARTBEAT_SECS = 30
INTERVAL_DB_POLL_SECS = 60
INTERVAL_STATE_SYNC_SECS = 30
INTERVAL_ENGINE_SCAN_SECS = 5
INTERVAL_RECOVERY_SWEEP_SECS = 300


def generate_run_id() -> str:
    """Generates a unique run ID for a task."""
    timestamp = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
    short_hash = generate(alphabet="0123456789abcdef", size=6)
    return f"{timestamp}-{short_hash}"


# TODO: Check Disk Space
# - 85%+: Mark system DEGRADED, disable Extract stage
# - 90%+: Mark system CRITICAL, alert on-call
# - Load stage continues to clear backlog)

# TODO: Backfill Runs
# TODO: Regression Testing
# TODO: Feature Toggles
# TODO: Cancel Task


class Orchestrator:
    def __init__(self, builder: TaskContextBuilder):
        self.builder = builder
        self.exec_ctx = builder.get_execution_context()
        self.exec_ctx.provider_config = self.builder.app_settings.get(
            "secret_provider", {}
        ).to_dict()

        self.heartbeat = Heartbeat()
        self.engine = IngestionEngine(self.exec_ctx)

        # Service Discovery: Use the resolved app settings from the builder
        db_config = deepcopy(self.builder.app_settings.get("services.clickhouse", {}))
        service_name = db_config.pop("type")

        ServiceFactory.get_provider(self.exec_ctx.env, self.exec_ctx.provider_config)
        self.db_service = ServiceFactory.get_service(service_name, **db_config)
        self.state_store = StateStore(self.db_service, self.exec_ctx)

        # Initialize background scheduler
        # We limit max_workers to reduce DB contention and prevent thundering herd issues
        executors = {"default": ThreadPoolExecutor(max_workers=4)}
        self.scheduler = BackgroundScheduler(
            timezone=ZoneInfo(self.exec_ctx.timezone), executors=executors
        )
        self.scheduler.add_listener(self._on_job_error, EVENT_JOB_ERROR)

        # Component Injection
        self.signals = SignalProcessor(self.state_store, self.engine, self.exec_ctx)
        self.lifecycle = LifecycleManager(self.state_store, self.engine, self.exec_ctx)

        # 2. Wire the Signals to the Handlers (The Refactor Fix)
        self.signals.register_command("RECOVER_ALL.cmd", self.lifecycle.handle_recovery)
        self.signals.register_command("PURGE_EXPIRED.cmd", self.lifecycle.handle_expiry)
        # self.signals.register_command("RELOAD_CONFIG.cmd", self._reload_internal_config)

        self._perform_platform_preflight()
        LOG.info("Orchestrator initialized")

    def _perform_platform_preflight(self) -> None:
        """Validates critical shared infrastructure."""
        # 1. Verify connection to the State Tracking database
        try:
            with self.db_service.client.get_connection() as _:
                LOG.debug("Preflight: Database connectivity verified.")
        except Exception as e:
            LOG.critical("Preflight: Could not connect to database", error=str(e))
            sys.exit(1)

        # 2. Ensure signal directory is writable
        self.exec_ctx.signal_path.mkdir(parents=True, exist_ok=True)

        # 3. Enhanced System Health Check
        self._check_system_health()

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
            self.engine.is_degraded = True
        else:
            self.engine.is_degraded = False

    def run(
        self,
        job_id: str,
        dataset_id: str,
        partition_date_str: str | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> None:
        if self.exec_ctx.always_on:
            self._start_always_on_loop()
            self.mode = "ALWAYS_ON"
        else:
            self._run_synchronous_task(
                job_id,
                dataset_id=dataset_id,
                partition_date_str=partition_date_str,
                overrides=overrides,
            )
            self.mode = "TRIGGER"

    def _start_always_on_loop(self) -> None:
        """
        ARCHITECTURAL NOTE: We use a Polling Control Loop instead of an Event Watchdog.
        1. Portability: Works identically on EC2 (local disk) and K8S (EFS/NFS)
        where inotify events often fail to propagate across pods.
        2. Backpressure: Prevents 'thundering herd' spikes by batching signal
        processing into predictable 'ticks'.
        3. Self-Healing: Every tick performs a full state reconciliation,
        ensuring we recover from crashes automatically.
        """
        self.heartbeat.ready()

        # Register background maintenance tasks
        self.scheduler.add_job(
            self.heartbeat.ping, "interval", seconds=INTERVAL_HEARTBEAT_SECS
        )
        self.scheduler.add_job(
            self._poll_and_evaluate, "interval", seconds=INTERVAL_DB_POLL_SECS
        )
        self.scheduler.add_job(
            self.state_store.flush, "interval", seconds=INTERVAL_STATE_SYNC_SECS
        )
        self.scheduler.add_job(
            self._drive_engine, "interval", seconds=INTERVAL_ENGINE_SCAN_SECS
        )
        self.scheduler.add_job(
            self._perform_maintenance, "interval", seconds=INTERVAL_RECOVERY_SWEEP_SECS
        )

        self.scheduler.start()
        LOG.info("Background scheduler started")

        try:
            while True:
                self.signals._process_worker_signals()
                # Small sleep to prevent 100% CPU usage
                time.sleep(1)
        except KeyboardInterrupt:
            self.scheduler.shutdown()
            self.stop()

    def _on_job_error(self, event: JobExecutionEvent) -> None:
        """Listener to capture and log APScheduler job failures."""
        if event.exception:
            LOG.error(
                "Background job failed",
                job_id=event.job_id,
                exception=str(event.exception),
                # Traceback might be None if the exception was raised
                # outside the job execution
                traceback=str(event.traceback) if event.traceback else "N/A",
            )

    def _poll_and_evaluate(self) -> None:
        """Polls the DB view and evaluates triggers."""
        if self.exec_ctx.always_on:
            active_definitions = self.state_store.get_latest_state(force_refresh=True)
            self._evaluate_triggers(list(active_definitions.values()))

    def _drive_engine(self) -> None:
        """Drives the ingestion engine queues."""
        self.engine._process_jobs()

    def _perform_maintenance(self) -> None:
        """Performs scheduled recovery and expiry sweeps."""
        try:
            self.engine.scan_and_recover()
            self.lifecycle.handle_recovery()
            self.lifecycle.handle_expiry()
        except Exception as e:
            LOG.error("Maintenance sweep failed", error=str(e))

    def _run_synchronous_task(
        self,
        job_id: str,
        dataset_id: str,
        partition_date_str: str | None = None,
        overrides: dict[str, Any] | None = None,
        timeout: int = DEFAULT_SYNC_TIMEOUT_SECS,
    ) -> None:
        """The Dumb Trigger Mode logic"""
        if not overrides:
            raise ValueError("Dumb mode requires a valid TaskConfig.")

        run_ids: set[str] = self._trigger_job(
            job_id,
            dataset_id=dataset_id,
            partition_date_str=partition_date_str,
            overrides=overrides,
        )

        # In dumb trigger mode, we don't need to refresh the entire state store.
        # The job's status is tracked directly in the engine's cache.
        # The state_store is primarily for DB updates in this mode.

        # 2. Block until this specific job is finished
        LOG.info("Monitoring job until completion", job_id=job_id)

        start_time = time.time()

        while True:
            # Run the engine cycle to drive the job forward
            self.engine._process_jobs()
            self.signals._process_worker_signals(run_ids)

            # Check for global timeout
            if time.time() - start_time > timeout:
                LOG.critical(
                    "Synchronous task timed out", job_id=job_id, timeout_sec=timeout
                )
                raise TimeoutError(f"Task {job_id} exceeded sync timeout of {timeout}s")

            # 3. Categorize the status of all triggered runs
            run_stats = Counter()
            for rid in run_ids:
                status = self._get_run_status(
                    job_id, dataset_id, partition_date_str, rid
                )
                run_stats[status] += 1

            # 4. Exit condition: Total Terminal (Success + Failure) == Total Triggered
            terminal_count = run_stats["success"] + run_stats["failed"]
            if terminal_count == len(run_ids):
                LOG.info(
                    "Synchronous monitoring loop exited",
                    job_id=job_id,
                    total=len(run_ids),
                    success=run_stats["success"],
                    failed=run_stats["failed"],
                )
                break

            LOG.debug(
                f"Waiting for job to finish: {datetime.now().astimezone().isoformat()}"
            )
            time.sleep(2)

    def _get_run_status(
        self,
        job_id: str,
        dataset_id: str,
        partition_date: str | None,
        run_id: str | None = None,
    ) -> str:
        """
        Determines the detailed runtime status of a specific run.
        Returns: 'active' | 'success' | 'failed'
        """
        if not run_id:
            return "success"  # Unknown/Missing treated as done

        identifier = self.exec_ctx.get_task_identifier(
            job_id, dataset_id, partition_date or ""
        )
        for stage_enum in StageName:
            key = f"{stage_enum.label}:{identifier}:{run_id}"

            meta: TaskMetadata = self.engine.cache.get(key)
            if meta:
                if hasattr(meta, "status") and meta.status in [
                    ExecutionStatus.FAILED.value,
                    ExecutionStatus.EXPIRED.value,
                ]:
                    return "failed"
                return "active"

        # If not found in cache at all, it was successfully popped/finished
        return "success"

    def _evaluate_triggers(
        self,
        job_records: list[JobRecord],
    ) -> None:
        """
        Evaluates each job against its trigger type and
        fans out tables to the IngestionEngine.

        Misfire Policy is handled here based on GRACE_PERIOD_SECS:
        -1: Fire Immediately (Always True)
        0 : Skip (Always False if delay > 0)
        >0: Grace Period (True if within bounds)
        """
        now = datetime.now(ZoneInfo(self.exec_ctx.timezone))

        # Map of trigger types to their logic classes
        trigger_map: dict[str, TriggerEvent] = {
            "CRON": TimeTriggerEvent(),
            "FILE": FileTriggerEvent(),
            "MANUAL": TimeTriggerEvent(),
        }

        for record in job_records:
            # Only evaluate triggers for jobs that are currently PENDING
            if ExecutionStatus.PENDING.value != record.JOB_STATUS:
                continue

            scheduled_time = record.SCHEDULED_TIMESTAMP
            if scheduled_time.tzinfo is None:
                scheduled_time = scheduled_time.replace(
                    tzinfo=ZoneInfo(self.exec_ctx.timezone)
                )

            delay = (now - scheduled_time).total_seconds()
            grace_sec = record.MISFIRE_GRACE_SECS

            # --- 1. MISFIRE POLICY EVALUATION ---
            if delay > 0:
                # Case A: Force Fire (-1)
                if grace_sec == -1:
                    LOG.info(
                        "Force-triggering late job", job_id=record.JOB_ID, delay=delay
                    )

                # Case B: Within Grace Period (Catch-up)
                elif delay <= grace_sec:
                    LOG.info(
                        "Catch-up trigger (within grace period)",
                        job_id=record.JOB_ID,
                        delay=delay,
                        grace=grace_sec,
                    )

                # Case C: Expired (Beyond Grace)
                else:
                    LOG.warning(
                        "Trigger EXPIRED (Skipping)",
                        job_id=record.JOB_ID,
                        delay_sec=delay,
                        grace_sec=grace_sec,
                    )
                    self.state_store.skip_misfired_run(record.JOB_ID)
                    continue

            # --- 2. TRIGGER EVALUATION ---
            trigger_type = "FILE" if record.WATCH_FILE_PATH else record.TRIGGER_TYPE
            trigger = trigger_map.get(trigger_type, trigger_map["CRON"])

            if trigger.should_fire(record):
                LOG.info(
                    "Trigger condition met",
                    job_id=record.JOB_ID,
                    trigger=trigger_type,
                )
                self._trigger_job(
                    job_id=record.JOB_ID,
                    dataset_id=record.DATASET_ID,
                )

    def stop(self) -> None:
        """Graceful shutdown for Always-On"""
        print("Shutting down gracefully...")
        # Close DB connections, stop Ray actors, etc.
        sys.exit(0)

    # TODO: Check if able to trigger on a per job or per dataset basis.
    def _trigger_job(
        self,
        job_id: str,
        dataset_id: str,
        partition_date_str: str | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> set[str]:

        log = LOG.bind(job_id=job_id, dataset_id=dataset_id)
        run_ids = set()

        log.debug("Building task contexts", partition_date=partition_date_str)
        # 1. Get the list of dataset configurations for this Task ID
        # Uses the injected builder which already has app_settings loaded
        task_contexts = self.builder.build(
            job_id=job_id,
            dataset_id=dataset_id,
            partition_date_str=partition_date_str,
            overrides=overrides,
        )

        for task_ctx in task_contexts:
            # B. Generate the Unique Identity for this Run
            run_id = generate_run_id()
            identifier = self.exec_ctx.get_task_identifier(
                job_id=task_ctx.job_id,
                dataset_id=task_ctx.dataset_id,
                partition_date=task_ctx.partition_date,
            )
            prefix = f"{identifier}:{run_id}"

            # C. Create the Folder Structure (Composite Key + Run ID)
            log.info(
                "Provisioning new run",
                run_id=run_id,
                identifier=identifier,
                from_stage=task_ctx.from_stage,
            )
            # Path: storage/active/
            active_root = self.exec_ctx.active_path
            active_root.mkdir(parents=True, exist_ok=True)

            # E. Freeze the Task Context (The instructions for the workers)
            config_path = active_root / f"{prefix}_{CONFIG_FILENAME}"
            with config_path.open("wb") as f:
                f.write(msgspec.json.encode(task_ctx))

            # 4. Queue to Engine (Immediate move to DiskCache)
            self.engine.queue_jobs(
                identifier=identifier,
                run_id=run_id,
                config_file_path=str(config_path),
            )

            # 5. Optional: Update DB so it doesn't trigger again immediately
            self.state_store.update_status(job_id, ExecutionStatus.QUEUED)
            run_ids.add(run_id)

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
        task.request_status_sync(deep_sync=True)

        # 2. Sync the StateStore
        # Since request_status_sync created the .done file, we tell the StateStore
        # to perform its final deep sync to pull the failure details into the DB.
        self.state_store.sync_from_folder(task.folder)

        # 3. Cleanup logic (Optional: move to failed or delete)
        if status == ExecutionStatus.EXPIRED:
            self.lifecycle._cleanup_workspace(task.run_id)


def create_orchestrator(app_cfg_path: str | None = None) -> Orchestrator:
    """
    Bootstrap helper to initialize the Orchestrator with its dependencies.
    Resolves configuration first to properly initialize services.
    """
    # 1. Initialize Builder (loads global app.yaml)
    builder = TaskContextBuilder(app_cfg_path=app_cfg_path)

    # 2. Return fully wired Orchestrator (It will resolve its own services)
    return Orchestrator(builder=builder)
