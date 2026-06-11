"""
Daemon Mode Runtime Orchestrator.

This module manages the 'Always-On' lifecycle of the ingestion engine,
handling background scheduling, signal processing, and surgical
task recovery (resumes).
"""

import sys
import threading
import time
from contextlib import suppress
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import msgspec
import pendulum
import ray
from apps.ingestion.src.core.models.task import Task
from apscheduler.events import EVENT_JOB_ERROR, JobExecutionEvent
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from libs.resilience.heartbeat import Heartbeat
from loguru import logger

if TYPE_CHECKING:
    from apps.ingestion.src.core.orchestrator.common.orchestrator import (
        Orchestrator,
    )
    from apps.ingestion.src.core.orchestrator.enums import TaskMetadata

    from .commands import CommandProcessor
    from .janitor import DaemonJanitor
    from .state import DaemonState
    from .trigger import TriggerManager

LOG = logger

# Scheduler intervals (seconds)
HEARTBEAT_INTERVAL = 30
DB_POLL_INTERVAL = 60
STATE_FLUSH_INTERVAL = 30
RECOVERY_INTERVAL = 300


class ResumeRequest(msgspec.Struct):
    """Request to resume a failed task."""

    run_id: str
    from_stage: str | None = None
    overrides: dict[str, Any] = {}


class DaemonRuntime:
    """Always-on daemon for background scheduling and reactive polling."""

    def __init__(
        self,
        orchestrator: "Orchestrator",
        state: "DaemonState",
        janitor: "DaemonJanitor",
        command_processor: "CommandProcessor",
        trigger_manager: "TriggerManager",
    ):
        self.orchestrator = orchestrator
        self.exec_ctx = orchestrator.exec_ctx
        self.state = state
        self.janitor = janitor
        self.commands = command_processor
        self.trigger = trigger_manager

        # Shortcuts
        self.tasks = orchestrator.tasks
        self.signals = orchestrator.signals

        # Register command handlers
        self.commands.register_handler("STOP", self._handle_stop)
        self.commands.register_handler("ADHOC_RUN", self._handle_adhoc_run)
        self.commands.register_handler("RESUME", self._handle_resume)

        self.heartbeat = Heartbeat()
        self._scheduler: BackgroundScheduler | None = None
        self._engine_thread: threading.Thread | None = None

    def run(self) -> None:
        """Start the daemon runtime."""
        self.orchestrator.preflight_check()

        # Setup scheduler
        tz_name = self.exec_ctx.timezone or pendulum.local_timezone().name
        self._scheduler = BackgroundScheduler(
            timezone=ZoneInfo(tz_name),
            executors={"default": ThreadPoolExecutor(max_workers=4)},
        )
        self._scheduler.add_listener(self._on_job_error, EVENT_JOB_ERROR)

        # Register background jobs
        self._scheduler.add_job(
            self.heartbeat.ping, "interval", seconds=HEARTBEAT_INTERVAL
        )

        # X secs before DB poll
        flush_start = pendulum.now(tz=tz_name).add(seconds=DB_POLL_INTERVAL - 15)
        self._scheduler.add_job(
            self.orchestrator.state.flush,
            "interval",
            seconds=DB_POLL_INTERVAL,
            start_date=flush_start,
        )

        self._scheduler.add_job(
            self._poll_and_trigger, "interval", seconds=DB_POLL_INTERVAL
        )
        self._scheduler.add_job(
            self._recovery_sweep, "interval", seconds=RECOVERY_INTERVAL
        )

        # Start
        self._scheduler.start()
        self.heartbeat.ready()

        self._engine_thread = threading.Thread(
            target=self._control_loop,
            name="EngineLoop",
            daemon=True,
        )
        self._engine_thread.start()

        # Initial poll
        self._poll_and_trigger()

        LOG.info(f"Daemon started | env={self.exec_ctx.env}")

        try:
            while True:
                if self.exec_ctx.stop_at_ts:
                    if self._scheduler.running:
                        self._scheduler.pause()
                    if not self._engine_thread.is_alive():
                        break
                time.sleep(1)
        except KeyboardInterrupt:
            pass

        self.stop()

    def stop(self) -> None:
        """Gracefully shut down the daemon."""
        LOG.info("Shutting down daemon...")

        with suppress(Exception):
            self.orchestrator.state.close()

        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown(wait=False)

        if ray.is_initialized():
            active = len(self.tasks._active_tasks)
            if active > 0:
                LOG.warning(f"Shutting down with {active} orphaned tasks")
            ray.shutdown()

        LOG.success("Daemon shutdown complete")
        sys.exit(0)

    def _process_commands(self) -> None:
        """Process pending command files."""
        commands = self.commands.process_commands()
        for handler, payload in commands:
            handler(payload)

        if commands:
            self.signals.notify()

    def _handle_stop(self, payload: Any) -> None:
        """Handle STOP command."""
        content = str(payload).lower() if payload else ""

        if content == "force":
            LOG.warning("Force stop - terminating immediately")
            self.exec_ctx.stop_at_ts = time.time()
        else:
            timeout = self.exec_ctx.drain_timeout_secs
            self.exec_ctx.stop_at_ts = time.time() + timeout
            LOG.warning(f"Graceful drain initiated (timeout={timeout}s)")

    def _handle_adhoc_run(self, payload: dict[str, Any] | None) -> None:
        """Handle ADHOC_RUN command."""
        if not payload:
            LOG.error("ADHOC_RUN: empty payload")
            return

        job_id = payload.get("job_id")
        partition_date = payload.get("partition_date")
        dataset_id = payload.get("dataset_id")

        if not job_id or not partition_date:
            LOG.error(f"ADHOC_RUN missing required fields: {payload}")
            return

        LOG.info(f"Triggering adhoc run: job={job_id}, dataset={dataset_id}")
        self.orchestrator.start_job(
            job_id=job_id,
            dataset_id=dataset_id,
            partition_date=partition_date,
        )

    def _handle_resume(self, payload: Any) -> None:
        """Handle RESUME command for task recovery."""
        if not payload:
            LOG.error("RESUME: empty payload")
            return

        try:
            request = msgspec.convert(payload, ResumeRequest)
        except msgspec.ValidationError:
            LOG.exception("RESUME: invalid payload")
            return

        if not request.run_id:
            LOG.error("RESUME: missing run_id")
            return

        folder = self.orchestrator.state.find_task_path(request.run_id)
        if not folder:
            LOG.error(f"RESUME: run {request.run_id} not found")
            return

        # Update cache state
        cache_keys = list(self.tasks.cache.iterkeys(pattern=f"*:*:{request.run_id}"))
        if cache_keys:
            key = cache_keys[0]
            metadata: TaskMetadata = self.tasks.cache.get(key)
            metadata.rewind_history = {}
            metadata.retry_count = 0
            if request.from_stage:
                metadata.current_stage = request.from_stage
            self.tasks.cache[key] = metadata

        # Update manifest
        task = Task.from_path(folder, self.exec_ctx)
        if request.from_stage:
            task.update_manifest({"current_stage": request.from_stage})

        LOG.info(
            f"Resuming task {request.run_id} -> stage={request.from_stage or 'last'}"
        )
        self.janitor.janitor.recover_task(folder)

    def _control_loop(self) -> None:
        """Main engine dispatch loop."""
        last_drain_log = 0

        while True:
            if self.exec_ctx.stop_at_ts:
                if time.time() - last_drain_log > 10:
                    LOG.info(
                        f"Draining: {len(self.tasks._active_tasks)} tasks remaining"
                    )
                    last_drain_log = time.time()

                if (
                    not self.tasks._active_tasks
                    or time.time() >= self.exec_ctx.stop_at_ts
                ):
                    break

            try:
                active_refs = list(self.tasks._active_tasks.keys())
                if not active_refs:
                    self.signals.wait(timeout=60.0)
                else:
                    ready, _ = ray.wait(active_refs, num_returns=1, timeout=0.5)
                    if not ready and not self.signals.wait(timeout=0.1):
                        continue

                self.signals.wait(timeout=0)
                self._process_commands()
                self.orchestrator.process_signals()
                self.orchestrator.submit_tasks()
            except Exception:
                LOG.exception("Engine loop error")
                time.sleep(1)

    def _poll_and_trigger(self) -> None:
        """Poll database and evaluate triggers."""
        self.orchestrator.builder._settings_cache.clear()
        self.orchestrator.state.flush()

        active_runs = self.state.refresh()
        decisions = self.trigger.evaluate(list(active_runs.values()))

        needs_wake = False
        for decision in decisions:
            if decision.action == "purge":
                self.janitor.evict_expired_run(decision.record, decision.context)
                needs_wake = True
            elif decision.action == "trigger":
                self.orchestrator.start_job(
                    job_id=decision.record.JOB_ID,
                    dataset_id=decision.record.DATASET_ID,
                    partition_date=decision.record.PARTITION_DATE,
                    run_id=decision.record.RUN_ID,
                )

        if needs_wake:
            self.signals.notify()

    def _recovery_sweep(self) -> None:
        """Periodic recovery and zombie detection."""
        try:
            if not self.orchestrator.state.store.records:
                return
            self.orchestrator.state.sink._flush_to_disk(force=True)
            self.tasks.recover_zombie_tasks()
        except Exception:
            LOG.exception("Recovery sweep failed")

    def _on_job_error(self, event: JobExecutionEvent) -> None:
        """Handle scheduler job errors."""
        if event.exception:
            LOG.error(f"Background job failed: {event.job_id} - {event.exception}")
