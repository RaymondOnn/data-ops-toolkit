from apps.ingestion.src.core.contexts import ExecutionContext, TaskContextBuilder
from apps.ingestion.src.services.factory import ServiceFactory

from .common import Janitor, Orchestrator, SignalScanner, StateHub, TaskManager
from .modes import DaemonRuntime, TriggerRuntime


def assemble_runtime(
    exec_ctx: ExecutionContext, builder: TaskContextBuilder
) -> DaemonRuntime | TriggerRuntime:
    """
    Assembles the Orchestrator components for testing or advanced use cases.
    This allows for manual wiring of dependencies and is not intended for general use.
    """
    exec_ctx.provider_config = builder.app_settings.get("secret_provider", {}).to_dict()

    ServiceFactory.get_provider(exec_ctx.env, exec_ctx.provider_config)
    db_config = builder.app_settings.get("services.clickhouse", {}).to_dict()

    # 1. Build common baseline infrastructure
    signal_processor = SignalScanner(exec_ctx=exec_ctx)
    base_state_store = StateHub(exec_ctx=exec_ctx, db_config=db_config)

    if exec_ctx.always_on:
        from .modes.daemon import (
            CommandProcessor,
            DaemonJanitor,
            DaemonRuntime,
            DaemonState,
            DenyDuplicateAdmission,
            ProactiveMaintenance,
            TriggerManager,
        )

        # 2. Build Daemon-specific variations
        triggers = TriggerManager(exec_ctx)
        daemon_state_store = DaemonState(hub=base_state_store, exec_ctx=exec_ctx)
        task_manager = TaskManager(
            exec_ctx,
            state_store=base_state_store,
            admission_policy=DenyDuplicateAdmission(),
            maintenance_policy=ProactiveMaintenance(),
        )
        base_janitor = Janitor(
            exec_ctx=exec_ctx,
            enqueue_fn=task_manager.enqueue,
            active_tasks_fn=lambda: task_manager.active_tasks,
        )
        daemon_janitor = DaemonJanitor(
            janitor=base_janitor,
            state=daemon_state_store,
        )
        commands = CommandProcessor(
            exec_ctx,
            janitor=daemon_janitor,
            state_store=daemon_state_store,
        )

        # 3. Assemble the core engine Facade
        orchestrator = Orchestrator(
            builder=builder,
            state_hub=base_state_store,
            task_manager=task_manager,
            janitor=base_janitor,
            scanner=signal_processor,
        )

        # 4. Wrap it in the Daemon process shell and return it
        return DaemonRuntime(
            orchestrator=orchestrator,
            state=daemon_state_store,
            janitor=daemon_janitor,
            command_processor=commands,
            trigger_manager=triggers,
        )

    from .modes.trigger import NoOpMaintenance, OverwriteAdmission, TriggerRuntime

    # 2. Build Transient/Trigger-specific variations
    task_manager = TaskManager(
        exec_ctx,
        state_store=base_state_store,
        admission_policy=OverwriteAdmission(),
        maintenance_policy=NoOpMaintenance(),
        # policy=EvictDuplicatePolicy()
    )
    base_janitor = Janitor(
        exec_ctx=exec_ctx,
        enqueue_fn=task_manager.enqueue,
        active_tasks_fn=lambda: task_manager.active_tasks,
    )

    # 3. Assemble the core engine Facade
    orchestrator = Orchestrator(
        builder=builder,
        task_manager=task_manager,
        state_hub=base_state_store,
        janitor=base_janitor,
        scanner=signal_processor,
    )

    # 4. Wrap it in the transient run-to-completion shell and return it
    return TriggerRuntime(exec_ctx=exec_ctx, orchestrator=orchestrator)
