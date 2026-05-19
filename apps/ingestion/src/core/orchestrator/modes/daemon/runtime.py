import sys
import threading
import time  # Moved time import here for consistency
from collections.abc import Callable
from contextlib import suppress
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import pendulum
import ray
from apps.ingestion.src.core.contexts.execution import ExecutionContext
from apscheduler.events import EVENT_JOB_ERROR, JobExecutionEvent
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from libs.resilience.heartbeat import Heartbeat
from loguru import logger

if TYPE_CHECKING:
    from apps.ingestion.src.core.orchestrator.common.orchestrator import Orchestrator
    from apps.ingestion.src.core.orchestrator.enums import JobRecord

    from .janitor import DaemonJanitor
    from .state import DaemonStateStore
    from .trigger import TriggerDecision


LOG = logger

INTERVAL_HEARTBEAT_SECS = 30
INTERVAL_DB_POLL_SECS = 60
INTERVAL_STATE_SYNC_SECS = 30
INTERVAL_RECOVERY_SWEEP_SECS = 300
INTERVAL_PROBE_SECS = 3600


class DaemonRuntime:
    """
    Always-On: Manages background threads, schedulers, and a reactive polling loop.
    """

    def __init__(
        self,
        exec_ctx: ExecutionContext,
        orchestrator: "Orchestrator",
        daemon_state: "DaemonStateStore",
        daemon_janitor: "DaemonJanitor",
        process_commands_fn: Callable[[], bool],
        trigger_job_fn: Callable[[list["JobRecord"]], list["TriggerDecision"]],
    ) -> None:
        self.exec_ctx = exec_ctx
        self.orchestrator = orchestrator

        # Specialized Daemon Components
        self.state_monitor = daemon_state
        self.janitor = daemon_janitor
        self.process_commands_fn = process_commands_fn
        self.trigger_job_fn = trigger_job_fn

        # Aliases for readability
        self.tasks = orchestrator.tasks
        self.signals = orchestrator.signals

        self.heartbeat = Heartbeat()

    def run(self, overrides: dict[str, Any] | None = None) -> None:
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
        """Handles daemon-specific signals like STOP.cmd and CommandProcessor calls."""
        # 1. Process custom commands (e.g., RECOVER_ALL.cmd)
        if self.process_commands_fn():
            self.signals.notify()

        # 2. Handle Shutdown Signals
        stop_cmd = self.exec_ctx.signal_path / "STOP.cmd"
        if stop_cmd.exists():
            content = stop_cmd.read_text().strip().lower()
            if content == "force":
                LOG.warning("🛑 FORCE STOP detected. Terminating loop.")
                self.exec_ctx.stop_at_ts = time.time()
            else:
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

            stop_cmd.unlink()
            self.signals.notify()

    def _engine_loop(self) -> None:
        """Daemon-only loop that wakes up on resource changes or signals."""
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
            self.janitor.recover_failed_tasks()
        except Exception:
            LOG.exception("Daemon maintenance sweep failed")
