import sys
import time
from collections import Counter
from copy import deepcopy
from datetime import datetime
from typing import TYPE_CHECKING, Any

import msgspec
import structlog
from apps.ingestion.src.core.contexts import TaskContextBuilder
from apps.ingestion.src.core.models.job import ExecutionStatus, Task
from apps.ingestion.src.core.models.stages.enums import StageName
from apps.ingestion.src.services.factory import ServiceFactory
from apps.ingestion.src.utils.constants import ALWAYS_ON_MODE, DISK_THRESHOLD_HALT, CONFIG_FILENAME
from libs.resilience.heartbeat import Heartbeat
from libs.utils.system import get_disk_usage
from nanoid import generate

from .engine import IngestionEngine
from .lifecycle import LifecycleManager
from .signals import SignalProcessor
from .state import StateStore
from .trigger import FileTriggerEvent, TimeTriggerEvent, TriggerEvent

if TYPE_CHECKING:
    from .enums import TaskMetadata

LOG = structlog.getLogger(__name__)
MISFIRE_GRACE_PERIOD_SECS = 3600
DEFAULT_SYNC_TIMEOUT_SECS = 1800  # 1 Hour default
INTERVAL_HEARTBEAT_SECS = 30
INTERVAL_DB_POLL_SECS = 60
INTERVAL_STATE_SYNC_SECS = 30
INTERVAL_ENGINE_SCAN_SECS = 10
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

        ServiceFactory.get_provider(self.exec_ctx.provider_config)
        self.db_service = ServiceFactory.get_service(service_name, **db_config)
        self.state_store = StateStore(self.db_service, self.exec_ctx)
        # State timers
        self.timers = {
            "heartbeat": float(0),
            "engine_scan": float(0),
            "job_trigger": float(0),
            "db_poll": float(0),
            "recovery_sweep": float(0),
            "state_sync": float(0),
        }

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
        self.db_service.client.connect()
        # 2. Ensure signal directory is writable
        self.exec_ctx.signal_path.mkdir(parents=True, exist_ok=True)

        # 3. Check for Disk Pressure (Safety Threshold: 95%)
        usage = get_disk_usage(self.exec_ctx.workspace_dir)
        if usage.percent > DISK_THRESHOLD_HALT:
            LOG.critical(
                "System storage full. Orchestrator cannot start safely.",
                extra={"disk_usage_pct": round(usage.percent, 2)},
            )
            sys.exit(1)

    def run(
        self,
        job_id: str,
        dataset_id: str,
        run_date_str: str | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> None:
        if ALWAYS_ON_MODE:
            self._start_always_on_loop()
            self.mode = "ALWAYS_ON"
        else:
            self._run_synchronous_task(
                job_id,
                dataset_id=dataset_id,
                run_date_str=run_date_str,
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

        try:
            while True:
                now = time.time()
                self.signals._process_worker_signals()

                # --- 1. Systemd Heartbeat (Every 30s) ---
                if now - self.timers["heartbeat"] > INTERVAL_HEARTBEAT_SECS:
                    self.heartbeat.ping()
                    self.timers["heartbeat"] = int(now)

                # --- 2. Database Polling (Every 60s) ---
                if (
                    now - self.timers["db_poll"] > INTERVAL_DB_POLL_SECS
                    and ALWAYS_ON_MODE
                ):
                    # state_store.refresh() queries Postgres for active job definitions
                    active_definitions = self.state_store.get_latest_state(
                        force_refresh=True
                    )
                    self._evaluate_triggers(set(active_definitions.values()))
                    self.timers["db_poll"] = int(now)

                # 2. NEW: Sync the StateStore to Postgres
                # This writes all buffered 'RUNNING', 'HELD', or 'COMPLETED' updates
                if now - self.timers["state_sync"] > INTERVAL_STATE_SYNC_SECS:
                    self.state_store.flush()
                    self.timers["state_sync"] = int(now)

                # # --- 3. Task Triggering (Every 10s) ---
                # if now - self.last_job_trigger > 10:
                #     self.last_job_trigger = now

                # --- 4. Engine Driving (Every Loop - High Priority) ---
                # This drives the actual work (Ray workers/Recovery)
                if now - self.timers["engine_scan"] > INTERVAL_ENGINE_SCAN_SECS:
                    self.engine.scan_and_recover()
                    self.engine._process_jobs()
                    self.timers["engine_scan"] = int(now)

                # 3. Maintenance: Run recovery sweep every 5 minutes
                if now - self.timers["recovery_sweep"] > INTERVAL_RECOVERY_SWEEP_SECS:
                    self.lifecycle.handle_recovery()
                    self.lifecycle.handle_expiry()
                    self.timers["recovery_sweep"] = int(now)

                # Small sleep to prevent 100% CPU usage
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def _run_synchronous_task(
        self,
        job_id: str,
        dataset_id: str,
        run_date_str: str | None = None,
        overrides: dict[str, Any] | None = None,
        timeout: int = DEFAULT_SYNC_TIMEOUT_SECS,
    ) -> None:
        """The Dumb Trigger Mode logic"""
        if not overrides:
            raise ValueError("Dumb mode requires a valid TaskConfig.")

        run_ids: set[str] = self._trigger_job(
            job_id,
            dataset_id=dataset_id,
            run_date_str=run_date_str,
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
                status = self._get_run_status(job_id, dataset_id, run_date_str, rid)
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

            print(
                f"Waiting for job to finish: {datetime.now().astimezone().isoformat()}"
            )
            time.sleep(2)

    def _get_run_status(
        self,
        job_id: str,
        dataset_id: str,
        run_date: str | None,
        run_id: str | None = None,
    ) -> str:
        """
        Determines the detailed runtime status of a specific run.
        Returns: 'active' | 'success' | 'failed'
        """
        if not run_id:
            return "success"  # Unknown/Missing treated as done

        identifier = self.exec_ctx.get_task_identifier(
            job_id, dataset_id, run_date or ""
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
        job_records: set[dict[str, Any]],
    ) -> None:
        """
        Evaluates each job against its trigger type and
        fans out tables to the IngestionEngine.

        Misfire Policy is handled here based on GRACE_PERIOD_SECS:
        -1: Fire Immediately (Always True)
        0 : Skip (Always False if delay > 0)
        >0: Grace Period (True if within bounds)
        """

        def provision_run(record: dict[str, Any]) -> None:

            # Map of trigger types to their logic classes
            trigger_map: dict[str, TriggerEvent] = {
                "CRON": TimeTriggerEvent(),
                "FILE": FileTriggerEvent(),
                "MANUAL": TimeTriggerEvent(),  # Ad-hoc always fires
            }

            trigger_type = record.get("trigger_type", "CRON")
            if trigger_map[trigger_type].should_fire(record):
                LOG.info(
                    "Trigger condition met",
                    job_id=record["job_id"],
                    trigger=trigger_type,
                )
                self._trigger_job(
                    job_id=record["job_id"],
                    dataset_id=record["dataset_id"],
                )

        now = time.time()

        for record in job_records:
            # If the job was JUST recovered, its next_scheduled_time was set to 'now'
            # by TerminalStep.recover(), so delay will be ~0.
            scheduled_time = record.get("next_scheduled_time", now)
            if not scheduled_time:
                continue

            # Resolve grace period: Config override > System Default
            # -1 = Always Fire | 0 = Strict Skip | >0 = Threshold
            grace_sec = record.get(
                "misfire_grace_sec",
            )
            delay = now - scheduled_time

            # --- MISFIRE POLICY EVALUATION ---
            # CASE 1: The job is LATE (delay > 0)
            if delay > 0:
                # A: If grace_sec is -1, it's a "Manual Resume" or "Force Run"
                # Policy -1 means 'run no matter how late we are'
                if MISFIRE_GRACE_PERIOD_SECS == -1:
                    LOG.info(
                        "Triggering FORCE_RUN (Late)",
                        job_id=record["job_id"],
                        delay_sec=delay,
                        policy=-1,
                    )
                    provision_run(record)
                    continue

                # B: If delay is within the grace period (delay < grace_sec)
                if delay <= MISFIRE_GRACE_PERIOD_SECS:
                    LOG.info(
                        "Triggering CATCH_UP (Grace Period)",
                        job_id=record["job_id"],
                        delay_sec=delay,
                        grace_sec=grace_sec,
                    )
                    provision_run(record)
                    continue

                # C: If delay is beyond the grace period
                if delay > MISFIRE_GRACE_PERIOD_SECS:
                    LOG.warning(
                        "Trigger EXPIRED (Skipping)",
                        job_id=record["job_id"],
                        delay_sec=delay,
                        grace_sec=grace_sec,
                    )
                    # We tell the StateStore to update the next run time
                    # without executing
                    self.state_store.skip_misfired_run(record["job_id"])
                    continue

            # CASE 2: The job is ON TIME (Standard Trigger Logic)
            provision_run(record)

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
        run_date_str: str | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> set[str]:

        log = LOG.bind(job_id=job_id, dataset_id=dataset_id)
        run_ids = set()

        log.debug("Building task contexts", run_date=run_date_str)
        # 1. Get the list of dataset configurations for this Task ID
        # Uses the injected builder which already has app_settings loaded
        task_contexts = self.builder.build(
            job_id=job_id,
            dataset_id=dataset_id,
            run_date_str=run_date_str,
            overrides=overrides,
        )

        for task_ctx in task_contexts:
            # B. Generate the Unique Identity for this Run
            run_id = generate_run_id()
            identifier = self.exec_ctx.get_task_identifier(
                job_id=task_ctx.job_id,
                dataset_id=task_ctx.dataset_id,
                run_date=run_date_str or "",
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
            self.timers["job_trigger"] = time.time()
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

        # jo 2. Sync the StateStore
        # Since request_status_sync created the .done file, we tell the StateStore
        # to perform its final deep sync to pull the failure details into the DB.
        self.state_store.sync_from_folder(task.job_id, task.run_id)

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
