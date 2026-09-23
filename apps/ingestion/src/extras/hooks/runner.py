from typing import TYPE_CHECKING, Any

import msgspec
from libs.utils.template import TemplateEngine
from loguru import logger
from simpleeval import simple_eval

from .enums import HookOnFailure
from .hooks import Hook

if TYPE_CHECKING:
    from src.core.contexts.step import StepContext
    from src.core.models.task import TaskManifest, TaskWorkspace
    from src.core.stages.types import StageContext
    from src.extras.hooks.enums import HookAction

LOG = logger


class HookCheckFailed(Exception):
    """Raised when a CHECK hook's condition evaluates to False."""


class HookRunner:
    """Executes hook actions for pipeline stages.

    Design: Follows the Observer pattern — stages delegate to HookRunner
    rather than hard-coding side effects.
    """

    def __init__(
        self,
        ctx: "StageContext",
        workspace: "TaskWorkspace",
        manifest: "TaskManifest",
    ):
        self.ctx = ctx
        self.workspace = workspace
        self.manifest = manifest
        # Initialize the global runtime maps
        self.state_map: dict[str, Any] = {}
        self.store_map: dict[str, Any] = {}

    @property
    def engine(self) -> TemplateEngine:
        """Builds a generic TemplateEngine loaded with task telemetry and runtime state."""
        manifest_data = msgspec.to_builtins(self.manifest)
        context_data = msgspec.to_builtins(self.ctx.task_context)
        ctx = {
            "partition_date": self.ctx.partition_date,
            "run_id": self.ctx.run_id,
            "job_id": self.ctx.job_id,
            "dataset_id": self.ctx.dataset_id,
            "workspace_dir": self.ctx.workspace_dir,
            "state": self.state_map,
            "store": self.store_map,
            "manifest": manifest_data,
            "context": context_data,
        }
        return TemplateEngine(context=ctx)

    def run_hooks(self, step: "StepContext", phase: str) -> None:
        """Executes hooks associated with a given step and trigger ('pre' or 'post')."""
        # step_context = self.task.context.get_step(step_id)
        hooks_config = None

        if step and step.hooks:
            hooks_config = step.hooks
        # else:
        #     # 2. Fall back to task.context.hooks keyed by step_id
        #     hooks_config = self.task.context.hooks.get(step_id)
        if not hooks_config:
            return

        actions: list[HookAction] = getattr(hooks_config, phase, [])
        if not actions:
            return

        LOG.info(f"Executing {len(actions)} {phase}-hooks for step '{step.id}'...")

        for i, action in enumerate(actions):
            if not self._should_run(action):
                LOG.debug(f"Skipping hook {i+1} due to condition: {action.if_}")
                continue

            try:
                result = self._execute_action(action)
                if action.id:
                    self.state_map[action.id] = {"status": "success", "result": result}
            except Exception as e:
                if action.id:
                    self.state_map[action.id] = {"status": "failed", "error": str(e)}

                match action.on_failure:
                    case HookOnFailure.ABORT:
                        LOG.exception(f"  ❌ Hook {i+1} failed (abort)")
                        raise
                    case HookOnFailure.WARN:
                        LOG.warning(f"  ⚠️ Hook {i+1} failed (warn): {e}")
                    case HookOnFailure.SKIP:
                        LOG.debug(f"  ⏭️ Hook {i+1} failed (skip): {e}")

    def _should_run(self, action: "HookAction") -> bool:
        if action.if_:
            try:
                # Evaluates condition via simpleeval using the engine context namespace
                return bool(simple_eval(action.if_, names=self.engine.context))
            except Exception:
                LOG.exception(f"Error evaluating expression '{action.if_}'")
                raise
        return True

    def _execute_action(self, action: "HookAction", _depth: int = 0) -> Any:
        """Dispatch execution to the registered strategy handler after template rendering."""
        # 1. Convert struct to builtins dict
        raw_action = msgspec.to_builtins(action)

        # 2. Render all template placeholders using TemplateEngine
        rendered_action_dict = self.engine.render(raw_action)

        # 3. Re-convert back to a validated HookAction object
        rendered_action = msgspec.convert(rendered_action_dict, type=type(action))

        # 4. Dispatch directly via Hook gateway
        return Hook.run(action.type, runner=self, action=rendered_action, _depth=_depth)
