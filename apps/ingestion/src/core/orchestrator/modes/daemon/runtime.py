"""
Daemon Mode Runtime Orchestrator.

This module manages the 'Always-On' lifecycle of the ingestion engine,
handling background scheduling, signal processing, and surgical
task recovery (resumes).
"""

import sys
import threading
import time  # Moved time import here for consistency
from collections.abc import Callable
from contextlib import suppress
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import msgspec
import pendulum
import ray
from apps.ingestion.src.core.contexts.execution import ExecutionContext
from apps.ingestion.src.core.models.task import Task
from apscheduler.events import EVENT_JOB_ERROR, JobExecutionEvent
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from libs.resilience.heartbeat import Heartbeat
from libs.utils.dates import get_current_timestamp
from loguru import logger

if TYPE_CHECKING:
    from apps.ingestion.src.core.orchestrator.common.orchestrator import Orchestrator
    from apps.ingestion.src.core.orchestrator.enums import JobRecord, TaskMetadata

    from .commands import CommandProcessor
    from .janitor import DaemonJanitor
    from .state import DaemonStateStore
    from .trigger import TriggerDecision


LOG = logger

INTERVAL_HEARTBEAT_SECS = 30
INTERVAL_DB_POLL_SECS = 60
INTERVAL_STATE_SYNC_SECS = 30
INTERVAL_RECOVERY_SWEEP_SECS = 300
INTERVAL_PROBE_SECS = 3600


class ResumePayload(msgspec.Struct):
    """Structured payload for surgical task recovery."""

    run_id: str
    from_stage: str | None = None
    overrides: dict[str, Any] = {}


class DaemonRuntime:
    """Always-On manager for background threads and reactive polling loops.

    Decision: Reactive Polling.
    The daemon uses a combination of time-based polling (DB) and
    event-driven notification (Signals) to minimize latency while
    ensuring the authoritative state in ClickHouse is always respected.
    """

    def __init__(
        self,
        exec_ctx: ExecutionContext,
        orchestrator: "Orchestrator",
        daemon_state: "DaemonStateStore",
        daemon_janitor: "DaemonJanitor",
        command_processor: "CommandProcessor",
        trigger_job_fn: Callable[[list["JobRecord"]], list["TriggerDecision"]],
    ) -> None:
        """Initializes the Daemon components and registers system commands.

        Args:
            exec_ctx: The global execution context.
            orchestrator: The primary engine orchestrator.
            daemon_state: Observer for DB state synchronization.
            daemon_janitor: Facility manager for cleanup and recovery.
            command_processor: Parser for filesystem-based command signals.
            trigger_job_fn: Strategy for evaluating job start conditions.
        """
        self.exec_ctx = exec_ctx
        self.orchestrator = orchestrator

        # Specialized Daemon Components
        self.state_monitor = daemon_state
        self.janitor = daemon_janitor
        self.commands = command_processor
        self.trigger_job_fn = trigger_job_fn

        # Aliases for readability
        self.tasks = orchestrator.tasks
        self.signals = orchestrator.signals

        # Register local handlers for system commands
        self.commands.register_handler("STOP", self._handle_stop_command)
        self.commands.register_handler("ADHOC_RUN", self._handle_adhoc_run)
        self.commands.register_handler("RESUME", self._handle_resume)

        self.heartbeat = Heartbeat()

    def run(self, overrides: dict[str, Any] | None = None) -> None:
        """Bootstraps background jobs and starts the engine reactive loop.

        Decision: Multi-threaded Isolation.
        The Scheduler runs on its own thread to ensure heartbeats and
        polling continue even if the Engine Loop is blocked by Ray
        GCS initialization or heavy metadata processing.
        """
        self.orchestrator._perform_platform_preflight()

        # 1. Scheduler Initialization
        tz_name = self.exec_ctx.timezone or pendulum.local_timezone().name
        self.scheduler = BackgroundScheduler(
            timezone=ZoneInfo(tz_name),
            executors={"default": ThreadPoolExecutor(max_workers=4)},
        )
        self.scheduler.add_listener(self._on_job_error, EVENT_JOB_ERROR)

        # 2. Job Registration
        self.scheduler.add_job(
            self.heartbeat.ping, "interval", seconds=INTERVAL_HEARTBEAT_SECS
        )
        self.scheduler.add_job(
            self._poll_and_evaluate, "interval", seconds=INTERVAL_DB_POLL_SECS
        )
        self.scheduler.add_job(
            self.orchestrator.state_store.flush,
            "interval",
            seconds=INTERVAL_STATE_SYNC_SECS,
        )
        self.scheduler.add_job(
            self._perform_maintenance, "interval", seconds=INTERVAL_RECOVERY_SWEEP_SECS
        )

        # 3. Startup
        self.scheduler.start()
        self.heartbeat.ready()

        self._engine_thread = threading.Thread(
            target=self._engine_loop,
            name="EngineReactiveLoop",
            daemon=True,
        )
        self._engine_thread.start()

        self._poll_and_evaluate()

        LOG.info(
            "Background daemon started",
            env=self.exec_ctx.env,
            tz=self.exec_ctx.timezone,
        )

        try:
            while True:
                if self.exec_ctx.stop_at_ts:
                    if self.scheduler.running:
                        self.scheduler.pause()  # Stop polling while draining
                    if not self._engine_thread.is_alive():
                        break
                time.sleep(1)
        except KeyboardInterrupt:
            pass

        self.stop()

    def _check_external_signals(self) -> None:
        """Handles external signals and drains the CommandProcessor queue.

        Decision: Sequential Processing.
        Commands are processed before the Engine Tick to ensure that 'STOP'
        or 'RESUME' signals take effect before the next batch of tasks
        is dispatched.
        """
        # 1. Drain the command queue (e.g., ADHOC_RUN, STOP, RECOVER_ALL)
        command_queue = self.commands.process_commands()
        for handler, payload in command_queue:
            handler(payload)

        if command_queue:
            self.signals.notify()

    def _handle_stop_command(self, payload: Any) -> None:
        """Abstracted handler for the STOP.cmd signal.

        Decision: Graceful Draining (ADR 014).
        Instead of immediate termination, 'STOP' sets a future timestamp.
        The engine continues to process active Ray tasks but stops
        dispatching new ones, preventing data corruption during shutdown.
        """
        content = str(payload).lower() if payload else ""

        if content == "force":
            LOG.warning("🛑 FORCE STOP detected. Terminating loop.")
            self.exec_ctx.stop_at_ts = time.time()
        else:
            # Standard Graceful Drain
            timeout = self.exec_ctx.drain_timeout_secs
            stop_ts = time.time() + timeout
            self.exec_ctx.stop_at_ts = stop_ts

            deadline = (
                pendulum.from_timestamp(stop_ts)
                .in_tz(self.exec_ctx.timezone)
                .format("HH:mm:ss")
            )
            LOG.warning(
                f"⏳ DRAIN detected. (Deadline: {deadline}, Timeout: {timeout}s)"
            )

    def _handle_adhoc_run(self, payload: dict[str, Any] | None) -> None:
        """Handles triggering an adhoc job from JSON payload.

        Decision: Ad-hoc Seeding.
        Ad-hoc runs are treated as manually triggered jobs. They bypass the
        standard trigger evaluation but still use the StateStore to
        establish a RUN_ID for lineage tracking.
        """
        if not payload:
            LOG.error("ADHOC_RUN command received with empty payload")
            return

        job_id = payload.get("job_id")
        partition_date = payload.get("partition_date")
        dataset_id = payload.get("dataset_id")  # Optional

        if not job_id or not partition_date:
            LOG.error("ADHOC_RUN missing required fields", payload=payload)
            return

        LOG.info("Triggering adhoc run via command", job_id=job_id, dataset=dataset_id)
        # This will call _trigger_job which seeds the StateStore (IS_SCHEDULED=0)
        self.orchestrator._trigger_job(
            job_id=job_id,
            dataset_id=dataset_id,
            partition_date_str=partition_date,
        )

    def _handle_resume(self, payload: Any) -> None:
        """Handles surgical recovery of a specific run."""
        if not payload:
            LOG.error("RESUME command received with empty payload")
            return

        try:
            # Refactor: Convert raw dictionary to structured ResumePayload
            data = msgspec.convert(payload, ResumePayload)
        except msgspec.ValidationError as e:
            LOG.error(f"RESUME payload validation failed: {e}")
            return

        run_id = data.run_id
        from_stage = data.from_stage
        # overrides = data.overrides

        if not run_id:
            LOG.error("RESUME missing required field: run_id")
            return

        folder = self.orchestrator.state_store.resolve_task_path(run_id)
        if not folder:
            LOG.error("Resume failed: Run ID %s not found in FAILED/HOLD.", run_id)
            return

        try:
            # 1. Lookup in Hot Cache
            # We find the metadata to check history and update it
            cache_keys = list(self.tasks.cache.iterkeys(pattern=f"*:*:{run_id}"))
            if not cache_keys:
                LOG.error(f"Resume rejected: Run {run_id} has no active cache entry.")
                return

            key = cache_keys[0]
            meta: TaskMetadata = self.tasks.cache.get(key)

            # 2. Reset Runtime Slate
            # Decision: Human-Driven Reset.
            # A manual RESUME command implies human intervention. We clear the
            # rewind_history and reset retry counts in the hot cache to allow
            # the new attempt to proceed with a clean slate.
            meta.rewind_history = {}
            meta.retry_count = 0
            if from_stage:
                meta.current_stage = from_stage

            self.tasks.cache[key] = meta

            # 3. Apply Overrides to Manifest (Physical)
            task = Task.from_folder(folder, self.exec_ctx)
            updates = {"current_stage": from_stage} if from_stage else {}
            # if overrides:
            #     updates["custom_overrides"] = overrides
            if updates:
                task.update_manifest(updates)

            # 4. Update Cache (Logical)
            if from_stage:
                meta.rewind_history[from_stage] = get_current_timestamp().isoformat()
                meta.current_stage = from_stage

            self.tasks.cache[key] = meta

        except Exception:
            LOG.exception("Failed to apply overrides during resume for %s", run_id)
            return

        LOG.info(
            "Resuming task: %s (Rewind to: %s)", run_id, from_stage or "Last Failure"
        )
        self.janitor.janitor.recover_task_by_path(folder)

    def _engine_loop(self) -> None:
        """Daemon-only loop that wakes up on resource changes or signals.

        Decision: Ray-Optimized Waiting.
        We use ray.wait with a short timeout to wake up the loop as soon
        as a single task completes, rather than polling at fixed intervals.
        This significantly improves throughput for short-lived tasks.
        """
        last_drain_log = 0
        while True:
            if self.exec_ctx.stop_at_ts is not None:  # Use self.exec_ctx
                if time.time() - last_drain_log > 10:  # Use self.exec_ctx
                    LOG.info(
                        f"Draining: {len(self.tasks._active_tasks)} tasks remaining..."
                    )
                    last_drain_log = time.time()

                if (
                    not self.tasks._active_tasks
                    or time.time() >= self.exec_ctx.stop_at_ts
                ):  # Use self.exec_ctx
                    break

            try:
                active_refs = list(self.tasks._active_tasks.keys())
                if not active_refs:
                    self.signals.wait_for_change(timeout=60.0)
                else:
                    ready, _ = ray.wait(active_refs, num_returns=1, timeout=0.5)
                    if not ready and not self.signals.wait_for_change(timeout=0.1):
                        continue

                self.signals.wait_for_change(timeout=0)
                self._check_external_signals()
                self.orchestrator.process_task_events(None)
                self.orchestrator._drive_engine()
            except Exception:
                LOG.exception("Critical error in Daemon Engine Loop")
                time.sleep(1)

    def stop(self) -> None:
        """Gracefully shuts down background services and Ray handles."""
        LOG.info("Shutting down Orchestrator services...")
        with suppress(Exception):
            self.orchestrator.state_store.close()

        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)

        if ray.is_initialized():
            active_count = len(self.tasks._active_tasks)
            if active_count > 0:
                LOG.warning(f"Shutting down Ray with {active_count} tasks orphaned.")
            ray.shutdown()

        LOG.success("Daemon shutdown complete.")
        sys.exit(0)

    def _on_job_error(self, event: JobExecutionEvent) -> None:
        if event.exception:
            LOG.error(
                "Daemon background job failed",
                job_id=event.job_id,
                error=str(event.exception),
            )

    def _poll_and_evaluate(self) -> None:
        """Polls the DB view and evaluates triggers with batched notification."""
        self.orchestrator.builder._settings_cache.clear()
        self.orchestrator.state_store.flush()

        active_definitions = self.state_monitor.get_latest_state(force_refresh=True)
        decisions = self.trigger_job_fn(list(active_definitions.values()))

        needs_wakeup = False
        for decision in decisions:
            if decision.action == "purge":
                self.janitor.process_expired_run(decision.record, decision.context)
                needs_wakeup = True
            elif decision.action == "trigger":
                self.orchestrator._trigger_job(
                    job_id=decision.record.JOB_ID,
                    dataset_id=decision.record.DATASET_ID,
                    partition_date_str=decision.record.PARTITION_DATE,
                    run_id=decision.record.RUN_ID,
                )

        if needs_wakeup:
            LOG.debug("Notifying engine of state changes from purge actions")
            self.signals.notify()

    def _perform_maintenance(self) -> None:
        """Performs scheduled recovery and zombie detection."""
        try:
            if not self.orchestrator.state_store.active_registry:
                return
            self.orchestrator.state_store._flush_buffer_to_stream(force=True)
            self.tasks.recover_zombie_tasks()
        except Exception:
            LOG.exception("Daemon maintenance sweep failed")
