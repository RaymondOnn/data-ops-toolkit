from typing import TYPE_CHECKING

import niquests
from loguru import logger

from .enums import HookOnFailure, HookType

if TYPE_CHECKING:
    from apps.ingestion.src.core.contexts.task import HookAction
    from apps.ingestion.src.core.models.task import Task
LOG = logger


class HookRunner:
    """Executes hook actions for pipeline stages.

    Design: Follows the Observer pattern — stages delegate to HookRunner
    rather than hard-coding side effects.
    """

    @staticmethod
    def run_hooks(
        task: "Task",
        stage_name: str,
        phase: str,  # "pre" or "post"
    ) -> None:
        """Execute all hooks for a given stage and phase."""
        hooks_config = task.context.hooks.get(stage_name)
        if not hooks_config:
            return
        actions: list[HookAction] = getattr(hooks_config, phase, [])
        if not actions:
            return
        LOG.info(f"Running {len(actions)} {phase}-hooks for stage '{stage_name}'")
        for i, action in enumerate(actions):
            try:
                HookRunner._execute_action(task, action)
                LOG.info(f"  ✅ Hook {i+1}/{len(actions)} ({action.type}) succeeded")
            except Exception as e:
                match action.on_failure:
                    case HookOnFailure.ABORT:
                        LOG.error(f"  ❌ Hook {i+1} failed (abort): {e}")
                        raise
                    case HookOnFailure.WARN:
                        LOG.warning(f"  ⚠️ Hook {i+1} failed (warn): {e}")
                    case HookOnFailure.SKIP:
                        LOG.debug(f"  ⏭️ Hook {i+1} failed (skip): {e}")

    @staticmethod
    def _execute_action(task: "Task", action: "HookAction") -> None:
        """Dispatch to the appropriate handler based on action type."""
        match action.type:
            case HookType.QUERY:
                HookRunner._run_query(task, action)
            case HookType.HTTP:
                HookRunner._run_http(task, action)
            case HookType.SCRIPT:
                HookRunner._run_script(task, action)
            case _:
                raise ValueError(f"Unknown hook type: {action.type}")

    @staticmethod
    def _run_query(task: "Task", action: "HookAction") -> None:
        """Execute a SQL query hook."""
        from apps.ingestion.src.services.factory import ServiceFactory

        if not action.query or not action.connection:
            raise ValueError("Query hook requires 'query' and 'connection'")
        # Template substitution
        query = action.query.format(
            partition_date=task.partition_date,
            run_id=task.run_id,
            job_id=task.job_id,
            dataset_id=task.dataset_id,
        )
        # Support file:// references
        if query.startswith("file://"):
            from pathlib import Path

            query = Path(query.removeprefix("file://")).read_text()
        sink = ServiceFactory.get_sink(action.connection)
        sink.command(query)

    @staticmethod
    def _run_http(task: "Task", action: "HookAction") -> None:
        """Execute an HTTP webhook hook."""
        if not action.url:
            raise ValueError("HTTP hook requires 'url'")
        payload = {
            "job_id": task.job_id,
            "run_id": task.run_id,
            "stage": task.target_stage,
            "partition_date": task.partition_date,
        }
        niquests.post(action.url, json=payload, timeout=action.timeout_seconds)

    @staticmethod
    def _run_script(task: "Task", action: "HookAction") -> None:
        """Execute a shell script hook."""
        import subprocess

        if not action.command:
            raise ValueError("Script hook requires 'command'")
        subprocess.run(
            action.command,
            shell=True,
            check=True,
            timeout=action.timeout_seconds,
            env={
                "TASK_RUN_ID": task.run_id,
                "TASK_JOB_ID": task.job_id,
                "TASK_PARTITION_DATE": task.partition_date,
            },
        )
