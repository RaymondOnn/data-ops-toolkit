import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import msgspec
import structlog
from apps.ingestion.src.core.contexts import JobContextBuilder
from apps.ingestion.src.core.models.job import Job, JobStatus
from apps.ingestion.src.core.orchestrator.engine import IngestionEngine
from apps.ingestion.src.core.orchestrator.lifecycle import LifecycleManager
from apps.ingestion.src.core.orchestrator.signals import SignalProcessor
from apps.ingestion.src.core.orchestrator.state import StateStore
from apps.ingestion.src.core.orchestrator.trigger import (
    FileTriggerEvent,
    TimeTriggerEvent,
    TriggerEvent,
)
from apps.ingestion.src.services.factory import ServiceFactory
from apps.ingestion.src.utils.common import find_path
from apps.ingestion.src.utils.constants import ALWAYS_ON_MODE
from libs.resilience.heartbeat import Heartbeat
from nanoid import generate

LOG = structlog.getLogger(__name__)
PID_FILE = Path(".daemon.pid")
MISFIRE_GRACE_PERIOD_SECS = 3600


def generate_run_id() -> str:
    """Generates a unique run ID for a job."""
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
# TODO: Cancel Job


class Orchestrator:
    def __init__(self, builder: JobContextBuilder):
        self.builder = builder
        self.exec_ctx = builder.get_execution_context()

        self.heartbeat = Heartbeat()
        self.engine = IngestionEngine(self.exec_ctx)

        # Service Discovery: Use the resolved app settings from the builder
        print(dict(self.builder.app_settings))
        service_name = "clickhouse_db"
        db_config = self.builder.app_settings.get("services", {}).get(service_name, {})
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
        self.signals.register_command("RELOAD_CONFIG.cmd", self._reload_internal_config)

        LOG.info("Orchestrator initialized")

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
                if now - self.timers["heartbeat"] > 30:
                    self.heartbeat.ping()
                    self.timers["heartbeat"] = int(now)

                # --- 2. Database Polling (Every 60s) ---
                if now - self.timers["db_poll"] > 60 and ALWAYS_ON_MODE:
                    # state_store.refresh() queries Postgres for active job definitions
                    active_definitions = self.state_store.get_latest_state(
                        force_refresh=True
                    )
                    self._evaluate_triggers(list(active_definitions.values()))
                    self.timers["db_poll"] = int(now)

                # 2. NEW: Sync the StateStore to Postgres
                # This writes all buffered 'RUNNING', 'HELD', or 'COMPLETED' updates
                if now - self.timers["state_sync"] > 30:
                    self.state_store.flush()
                    self.timers["state_sync"] = int(now)

                # # --- 3. Job Triggering (Every 10s) ---
                # if now - self.last_job_trigger > 10:
                #     self.last_job_trigger = now

                # --- 4. Engine Driving (Every Loop - High Priority) ---
                # This drives the actual work (Ray workers/Recovery)
                if now - self.timers["engine_scan"] > 10:
                    self.engine.scan_and_recover()
                    self.engine._process_jobs()
                    self.timers["engine_scan"] = int(now)

                # 3. Maintenance: Run recovery sweep every 5 minutes
                if now - self.timers["recovery_sweep"] > 300:
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
    ) -> None:
        """The Dumb Trigger Mode logic"""
        if not overrides:
            raise ValueError("Dumb mode requires a valid JobConfig.")

        self._trigger_job(
            job_id,
            dataset_id=dataset_id,
            run_date_str=run_date_str,
            overrides=overrides,
        )

        # 2. Block until this specific job is finished
        LOG.info("Monitoring job until completion", job_id=job_id)

        while True:
            # Run the engine cycle to drive the job forward
            self.engine._process_jobs()

            # Check if our specific job is in the 'complete' prefix or marked COMPLETED
            # Note: We check all stage prefixes for the key

            if self._is_job_finished(job_id):
                LOG.info("Job finished successfully", job_id=job_id)
                break

            time.sleep(2)

    def _is_job_finished(self, job_id: str) -> bool:
        """
        Checks if all tables associated with a job_id have cleared the pipeline.
        Decision: Enum-driven Lifecycle Check.
        A job is finished if no associated runs are in an 'Active' state.
        """
        # 1. Physical Workspace Check (The primary breadcrumb)
        active_path = self.exec_ctx.active_path
        return bool(find_path(active_path, job_id))

        # 2. Logical State Check via StateStore's active records
        # We look for any run associated with this job_id that is still in
        # an 'Active' state
        active_runs = [
            run
            for run in self.state_store.active_records
            if run["job_id"] == job_id and run["status"] in JobStatus.active_statuses()
        ]

        return len(active_runs) == 0

    def _evaluate_triggers(
        self,
        job_records: list[dict[str, Any]],
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
    ) -> None:

        log = LOG.bind(job_id=job_id, dataset_id=dataset_id)

        log.debug("Building job contexts", run_date=run_date_str)
        # 1. Get the list of dataset configurations for this Job ID
        # Uses the injected builder which already has app_settings loaded
        job_contexts = self.builder.build(
            job_id=job_id,
            dataset_id=dataset_id,
            run_date_str=run_date_str,
            overrides=overrides,
        )

        for job_ctx in job_contexts:
            # B. Generate the Unique Identity for this Run
            run_id = generate_run_id()
            composite_key = f"{job_ctx.job_id}:{job_ctx.dataset_id}"
            prefix = f"{composite_key}_{run_date_str}_{run_id}"

            # C. Create the Folder Structure (Composite Key + Run ID)
            log.info(
                "Provisioning new run",
                run_id=run_id,
                composite_key=composite_key,
                from_step=job_ctx.from_step,
            )
            # Path: storage/active/
            active_root = self.exec_ctx.active_path
            active_root.mkdir(parents=True, exist_ok=True)

            # E. Freeze the Job Context (The instructions for the workers)
            config_path = active_root / f"{prefix}_config.json"
            with config_path.open("wb") as f:
                f.write(msgspec.json.encode(job_ctx))

            # 4. Queue to Engine (Immediate move to DiskCache)
            self.engine.queue_jobs(
                composite_key=composite_key,
                run_id=run_id,
                config_file_path=str(config_path),
            )

            # 5. Optional: Update DB so it doesn't trigger again immediately
            self.state_store.update_status(job_id, JobStatus.QUEUED)
            self.timers["job_trigger"] = time.time()

    def _terminate_job(
        self, job: Job, status: JobStatus, reason: str | None = None
    ) -> None:
        """
        Controlled Crash Handler.
        Uses the Job's internal status updater to ensure consistency.
        """
        LOG.error("Terminating job", job_id=job.id, status=status, reason=reason)

        # 1. Update the Job state
        # We pass the reason as the payload so it gets serialized into the manifest
        # update_manifest handles the msgspec encoding and file write internally
        job.update_manifest(
            {
                "job_status": status,
                "current_step": JobStatus.CANCELLED,
            }
        )
        job.request_status_sync(deep_sync=True)

        # 2. Sync the StateStore
        # Since request_status_sync created the .done file, we tell the StateStore
        # to perform its final deep sync to pull the failure details into the DB.
        self.state_store.sync_from_folder(job.id, job.run_id)

        # 3. Cleanup logic (Optional: move to failed or delete)
        if status == JobStatus.EXPIRED:
            self.lifecycle._cleanup_workspace(job.run_id)


def create_orchestrator(app_cfg_path: str | None = None) -> Orchestrator:
    """
    Bootstrap helper to initialize the Orchestrator with its dependencies.
    Resolves configuration first to properly initialize services.
    """
    # 1. Initialize Builder (loads global app.yaml)
    builder = JobContextBuilder(app_cfg_path=app_cfg_path)

    # 2. Return fully wired Orchestrator (It will resolve its own services)
    return Orchestrator(builder=builder)
