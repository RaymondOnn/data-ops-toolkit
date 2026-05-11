import sys
import threading
import time
from collections import Counter
from copy import deepcopy
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import msgspec
import pendulum
import ray
from apps.ingestion.src.core.contexts import TaskContextBuilder
from apps.ingestion.src.core.contexts.execution import ExecutionContext
from apps.ingestion.src.core.models.task import (
    ExecutionStatus,
    Task,
    TaskSignal,
)
from apps.ingestion.src.core.orchestrator.enums import TaskRef
from apps.ingestion.src.services.factory import ServiceFactory
from apps.ingestion.src.utils.common import make_short_hash
from apps.ingestion.src.utils.constants import (
    CACHE_TASK_NAMESPACE,
    CONFIG_FILENAME,
    DISK_THRESHOLD_HALT,
    STRIP_TZ_FOR_DB,
)
from apscheduler.events import JobExecutionEvent
from libs.utils.dates import get_current_timestamp
from libs.utils.system import get_disk_usage, get_system_vitals
from loguru import logger

if TYPE_CHECKING:
    from apps.ingestion.src.core.contexts.task import TaskContext


LOG = logger

DEFAULT_SYNC_TIMEOUT_SECS = 1800  # 1 Hour default
INTERVAL_HEARTBEAT_SECS = 30
INTERVAL_DB_POLL_SECS = 60
INTERVAL_STATE_SYNC_SECS = 30
INTERVAL_ENGINE_SCAN_SECS = 1
INTERVAL_RECOVERY_SWEEP_SECS = 300
INTERVAL_PROBE_SECS = 3600


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
    from libs.utils.dates import get_current_timestamp

    timestamp = get_current_timestamp(strip_tz=STRIP_TZ_FOR_DB).strftime(
        "%Y%m%d-%H%M%S"
    )
    short_hash = make_short_hash(8)
    return f"{timestamp}-{short_hash}"


class Orchestrator:
    def __init__(self, builder: TaskContextBuilder):
        self.builder = builder
        self.exec_ctx = builder.get_execution_context()

        # Initialize the Global Registry with the environment's cache configuration
        # ServiceRegistry.configure(
        #     self.exec_ctx.workspace_dir, self.exec_ctx.cache_config
        # )
        self._perform_platform_preflight()
        self._setup_components(exec_ctx=self.exec_ctx)
        self._check_system_health()

        self._new_work_event = threading.Event()

        LOG.info("Orchestrator initialized")

    def _perform_platform_preflight(self) -> None:
        """Validates critical shared infrastructure."""
        # 1. Verify connection to the State Tracking database
        try:
            self._init_services(self.exec_ctx)
        except Exception as e:
            LOG.critical("Failed to initialize services", error=str(e))
            raise e

        # 2. Ensure signal directory is writable
        self.exec_ctx.signal_path.mkdir(parents=True, exist_ok=True)
        self.exec_ctx.active_path.mkdir(parents=True, exist_ok=True)
        self.exec_ctx.data_path.mkdir(parents=True, exist_ok=True)

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

    def _init_services(self, exec_ctx: ExecutionContext):
        try:
            exec_ctx.provider_config = self.builder.app_settings.get(
                "secret_provider", {}
            ).to_dict()
            ServiceFactory.get_provider(exec_ctx.env, exec_ctx.provider_config)

            db_config = deepcopy(
                self.builder.app_settings.get("services.clickhouse", {}).to_dict()
            )
            self.db_service = ServiceFactory.get_service(
                service_type=db_config.pop("type"), **db_config
            )

        except Exception as e:
            LOG.error(f"Failed to initialize services: {e}")
            raise e

    def _setup_components(self, exec_ctx: ExecutionContext) -> None:
        from .commands import CommandProcessor
        from .janitor import Janitor
        from .manager import TaskManager
        from .signals import SignalProcessor
        from .state import StateStore
        from .trigger import TriggerManager

        self.triggers = TriggerManager(exec_ctx)
        self.signals = SignalProcessor(exec_ctx)

        self.state_store = StateStore(self.db_service, exec_ctx)
        self.tasks = TaskManager(exec_ctx, self.state_store)

        self.janitor = Janitor(self.state_store, self.tasks, exec_ctx)
        self.commands = CommandProcessor(
            exec_ctx, self.janitor, self.tasks, self.state_store
        )

        # 1. Initialize the Signal
        # self._on_task_queued = threading.Event()
        # 2. Start the dedicated Engine Thread
        self._engine_thread = threading.Thread(
            target=self._start_task_event_loop, name="EngineReactiveLoop", daemon=True
        )
        self._engine_thread.start()

    def _handle_system_signals(self, run_filter: set[str] | None = None) -> None:
        """AUTHORITATIVE HANDLER: Orchestrates actions based on detected signals."""
        # 1. Process commands first (e.g., RECOVER_ALL.cmd)
        commands_processed = self.commands.process_commands()
        if commands_processed:
            # If commands were processed, they might have changed state, so notify.
            # This ensures the engine loop re-evaluates immediately.
            self.signals.notify()

        # 2. Process task-specific signals
        events = self.signals.collect_events(filter_run_ids=run_filter)
        if not events:
            return

        for event in events:
            run_id = event.task_ref.run_id
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

                    # 2. Quarantining: Physically move the folder for analysis
                    from apps.ingestion.src.core.models.states import FailedState

                    task = Task.from_folder(event.folder_path, self.exec_ctx)
                    task.move_to_folder(FailedState.folder_name)

            elif event.signal_type == ".sync":
                if event.folder_path:
                    self.state_store.sync_from_folder(
                        event.folder_path, deep_sync=False
                    )
                else:
                    self.state_store.update_run(run_id, {})

            elif event.signal_type == ".expired":
                if run_record := self.state_store.active_registry.get(run_id):
                    self.state_store.emit_expiry(
                        run=run_record, context=None, reason="TTL Expired"
                    )

        # Wake up the dispatch loop
        self.signals.notify()

    def _start_task_event_loop(self) -> None:
        """
        The main engine loop. It only wakes up when:
        1. A worker finishes (ray.wait)
        2. A new job is queued (self._on_task_queued)
        3. A safety timeout occurs (60s)
        """
        while True:
            try:
                # 1. Identify currently running Ray workers
                active_refs = list(self.tasks._active_tasks.keys())

                # 2. WAIT PHASE: Block until something happens
                if not active_refs:
                    # SCENARIO A: Engine is idle.
                    # Block indefinitely until signals.notify() is called (new job or .cmd)
                    LOG.debug("TaskManager idle: awaiting signal or 60s heartbeat")
                    self.signals.wait_for_change(timeout=60.0)
                else:
                    # SCENARIO B: Workers are busy.
                    # Block until EITHER a worker finishes OR a signal/new job arrives.
                    # ray.wait returns if any task in 'active_refs' completes.
                    ready, _ = ray.wait(active_refs, num_returns=1, timeout=0.5)

                    # If workers are still busy, check the Bus for new jobs/files.
                    # We use a short timeout here to keep the loop tight.
                    if not ready and not self.signals.wait_for_change(timeout=0.1):
                        continue

                # 3. ACTION PHASE: Drive the state machine
                # We reset the bus signal before processing to catch new events during execution
                # wait_for_change(timeout=0) clears the event and returns immediately.
                self.signals.wait_for_change(timeout=0)

                # This method now performs the actual work
                self._handle_system_signals()
                self._drive_engine()

            except Exception:
                LOG.exception("Critical error in Reactive Engine Loop")
                time.sleep(1)  # Prevent rapid-fire logging on persistent errors

    def _setup_scheduler(self):
        from apscheduler.events import EVENT_JOB_ERROR
        from apscheduler.executors.pool import ThreadPoolExecutor
        from apscheduler.schedulers.background import BackgroundScheduler

        # We limit max_workers to reduce DB contention and
        # prevent thundering herd issues
        tz_name = self.exec_ctx.timezone or pendulum.local_timezone().name
        self.scheduler = BackgroundScheduler(
            timezone=ZoneInfo(tz_name),
            executors={"default": ThreadPoolExecutor(max_workers=4)},
        )
        self.scheduler.add_listener(self._on_job_error, EVENT_JOB_ERROR)

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
        # self.scheduler.add_job(
        #     self._drive_engine, "interval", seconds=INTERVAL_ENGINE_SCAN_SECS
        # )
        self.scheduler.add_job(
            self._perform_maintenance, "interval", seconds=INTERVAL_RECOVERY_SWEEP_SECS
        )
        self.scheduler.add_job(
            self._probe_services, "interval", seconds=INTERVAL_PROBE_SECS
        )

        self.scheduler.start()

    def run(
        self,
        job_id: str,
        dataset_id: str,
        partition_date_str: str | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> None:
        if self.exec_ctx.always_on:
            self._start_always_on_loop()
        else:
            self._run_synchronous_task(
                job_id,
                dataset_id=dataset_id,
                partition_date_str=partition_date_str,
                overrides=overrides,
            )

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
        from libs.resilience.heartbeat import Heartbeat

        self.heartbeat = Heartbeat()
        self.heartbeat.ready()

        self._setup_scheduler()

        # Trigger initial poll immediately so we don't wait for the first interval
        self._poll_and_evaluate()
        LOG.info(
            "Background scheduler started",
            env=self.exec_ctx.env,
            timezone=self.exec_ctx.timezone,
        )

        try:
            while True:
                # self.signals._process_worker_signals()
                # self._drive_engine()
                # Small sleep to prevent 100% CPU usage
                time.sleep(1)
        except KeyboardInterrupt:
            self.scheduler.shutdown()
            self.stop()

    # Scheduler Job
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

    # Scheduler Job
    def _poll_and_evaluate(self) -> None:
        """Polls the DB view and evaluates triggers with batched notification."""
        self.builder._settings_cache.clear()

        # 1. Sync local and global state
        self.state_store.flush()
        active_definitions = self.state_store.get_latest_state(force_refresh=True)

        # 2. Decision Hub
        decisions = self.triggers.evaluate(list(active_definitions.values()))

        # Track if we need to wake up the engine
        needs_notification = False

        # 3. Action Coordination
        for decision in decisions:
            if decision.action == "purge":
                if not self.exec_ctx.is_dry_run:
                    self.janitor.process_expired_run(decision.record, decision.context)
                    needs_notification = True  # State changed via purge

            elif decision.action == "trigger":
                if not self.exec_ctx.is_dry_run:
                    # _trigger_job already calls notify() internally
                    self._trigger_job(
                        decision.record.JOB_ID,
                        decision.record.DATASET_ID,
                        partition_date_str=decision.record.PARTITION_DATE,
                        run_id=decision.record.RUN_ID,
                    )
                    # No need to set needs_notification here as _trigger_job handled it

        # 4. Final Safety Wake-up
        # If we purged anything but didn't trigger a new job,
        # we still notify to let the engine re-check concurrency slots.
        if needs_notification:
            LOG.debug("Notifying engine of state changes from purge actions")
            self.signals.notify()

    # Scheduler Job
    def _drive_engine(self) -> None:
        """Drives the ingestion engine queues."""
        # Only drive the engine if there are actually tasks in the queue.
        # DiskCache len() is an O(1) operation.
        if self.tasks.cache.is_empty():
            return

        self.tasks._process_tasks()

    # Scheduler Job
    def _perform_maintenance(self) -> None:
        """Performs scheduled recovery and expiry sweeps."""
        try:
            # Skip recovery and expiry logic if no active records are tracked.
            if not self.state_store.active_registry:
                return

            self.state_store._flush_buffer_to_stream(force=True)
            self.tasks.recover_zombie_tasks()
            self.janitor.recover_failed_tasks()
        except Exception:
            LOG.exception("Maintenance sweep failed")

    # Scheduler Job
    def _probe_services(self) -> None:
        """Periodic call to check if downed services have recovered."""
        # Scan signals directory for .source_down files
        for signal in self.exec_ctx.signal_path.glob("*.source_down"):
            service_name = signal.name.replace(".source_down", "")

            # Define how to probe (e.g. ping the database service)
            # For now, we use a generic probe if the service is known
            LOG.debug("Probing service for recovery", service=service_name)

            # Logic inside registry handles resetting the circuit if probe_fn returns True
            # ServiceRegistry.probe(service_name, probe_fn=...)

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
            raise ValueError("Trigger mode requires a valid TaskConfig.")

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
            self.tasks._process_tasks()
            self._handle_system_signals(run_ids)
            self.state_store.flush()

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
            total_terminal = sum(
                count for status, count in run_stats.items() if status.is_terminal
            )
            if total_terminal == len(run_ids):
                LOG.info(
                    "Synchronous monitoring loop exited",
                    job_id=job_id,
                    total=len(run_ids),
                    success=run_stats[ExecutionStatus.SUCCESS],
                    failed=run_stats[ExecutionStatus.FAILED],
                )
                break

            now = get_current_timestamp(strip_tz=True)
            LOG.debug(f"Waiting for job to finish: {now}")
            time.sleep(0.5)  # Reduced from 2s to 0.5s for faster local execution

    def _get_run_status(
        self,
        job_id: str,
        dataset_id: str,
        partition_date: str | None,
        run_id: str | None = None,
    ) -> ExecutionStatus:
        """
        Determines the detailed runtime status of a specific run.
        """
        if not run_id:
            return ExecutionStatus.SUCCESS  # Missing treated as done

        identifier = self.exec_ctx.get_task_identifier(
            job_id, dataset_id, partition_date or ""
        )

        # 1. Check TaskManager Cache (Hot State)
        # Format: task:{status}:{stage}:{identifier}:{run_id}
        with self.tasks.lock:
            pattern = f"{CACHE_TASK_NAMESPACE}:*:*:{identifier}:{run_id}"
            for key in self.tasks.cache.iterkeys(pattern=pattern):
                # If it's in the cache, it's still being managed (Active)
                if run_id in key:
                    return ExecutionStatus.RUNNING

        # 2. Check StateStore Registry (Database Mirror)
        # If it's gone from the task cache, it has been popped.
        # We check the registry to see if it was popped because of failure.
        record = self.state_store.active_registry.get(run_id)
        if record:
            status_val = ExecutionStatus(record.JOB_STATUS)
            if status_val.is_failure:
                return ExecutionStatus.FAILED

        # If not found in cache at all, it was successfully popped/finished
        return ExecutionStatus.SUCCESS

    def _trigger_job(
        self,
        job_id: str,
        dataset_id: str,
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
            task_ref = TaskRef(
                namespace=CACHE_TASK_NAMESPACE,
                status=ExecutionStatus.PROVISIONED.value,
                stage=task_ctx.from_stage,
                job_id=task_ctx.job_id,
                dataset_id=task_ctx.dataset_id,
                partition_date=task_ctx.partition_date,
                run_id=run_id,
            )

            log = LOG.bind(
                run_id=task_ref.run_id,
                job_id=task_ref.job_id,
                dataset_id=task_ref.dataset_id,
            )
            log.info(
                "Provisioning new run",
                run_id=task_ref.run_id,
                identifier=task_ref.identifier,
                from_stage=task_ref.stage,
            )

            # 2. Freeze the Task Context using the TaskRef identity
            config_path = (
                self.exec_ctx.active_path
                / f"{task_ref.identifier}:{task_ref.run_id}_{CONFIG_FILENAME}"
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
            # We pass the task_ref directly. queue_tasks will handle the transition to WAITING.
            queued_ref = self.tasks.queue_tasks(
                task_ref, config_file_path=str(config_path)
            )

            # Update the state store with the actual TaskKey from the queue
            if queued_ref:
                self.state_store.update_run(
                    task_ref.run_id,
                    {
                        "JOB_STATUS": queued_ref.status,
                        "CURRENT_STAGE": queued_ref.stage,
                    },
                )

            # 5. Optional: Update DB so it doesn't trigger again immediately
            # self.state_store.update_status(job_id, ExecutionStatus.QUEUED)
            run_ids.add(run_id)
            self.signals.notify()

        return run_ids

    def stop(self) -> None:
        """Graceful shutdown for Always-On"""
        print("Shutting down gracefully...")
        # Close DB connections, stop Ray actors, etc.
        sys.exit(0)

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
        self.state_store.sync_from_folder(task.folder)

        # 3. Cleanup logic (Optional: move to failed or delete)
        if status == ExecutionStatus.EXPIRED:
            self.janitor.cleanup_task(task)


def create_orchestrator(app_cfg_path: str | None = None) -> Orchestrator:
    """
    Bootstrap helper to initialize the Orchestrator with its dependencies.
    Resolves configuration first to properly initialize services.
    """
    # 1. Initialize Builder (loads global app.yaml)
    builder = TaskContextBuilder(app_cfg_path=app_cfg_path)

    # 2. Return fully wired Orchestrator (It will resolve its own services)
    return Orchestrator(builder=builder)
