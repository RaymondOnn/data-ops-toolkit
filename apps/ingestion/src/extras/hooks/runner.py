from typing import TYPE_CHECKING, Any

from libs.utils.dict import flatten_dict
from loguru import logger
from simpleeval import simple_eval

from .enums import HookOnFailure
from .hooks import HOOK_STRATEGIES

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task import Task
    from apps.ingestion.src.extras.hooks.enums import HookAction

LOG = logger


class HookCheckFailed(Exception):
    """Raised when a CHECK hook's condition evaluates to False."""


class HookRunner:
    """Executes hook actions for pipeline stages.

    Design: Follows the Observer pattern — stages delegate to HookRunner
    rather than hard-coding side effects.
    """

    def __init__(self, task: "Task"):
        self.task = task
        # Initialize the global runtime maps
        self.state_map: dict[str, Any] = {}
        self.store_map: dict[str, Any] = {}

    @property
    def _eval_names(self) -> dict[str, Any]:
        """Builds a nested dictionary namespace compatible with simpleeval."""
        return {
            "partition_date": str(self.task.partition_date),
            "run_id": str(self.task.run_id),
            "job_id": str(self.task.job_id),
            "dataset_id": str(self.task.dataset_id),
            "state": self.state_map,
            "store": self.store_map,
            # Expose raw structures directly for evaluation: e.g. "manifest.extract.file_count > 0"
            "manifest": self.task.manifest,
            "context": self.task.context,
        }

    @property
    def _template_vars(self) -> dict[str, Any]:
        """Common template variables for hook string interpolation."""
        import msgspec

        # 1. Base metadata fields
        template_dict = {
            "partition_date": str(self.task.partition_date),
            "run_id": str(self.task.run_id),
            "job_id": str(self.task.job_id),
            "dataset_id": str(self.task.dataset_id),
        }

        # 2. Dynamically flatten and inject state.* mapping context
        for hook_id, telemetry in self.state_map.items():
            flattened_telemetry = flatten_dict(telemetry, parent_key=f"state.{hook_id}")
            template_dict.update(flattened_telemetry)

        # 3. Inject store.* variables
        for key, val in self.store_map.items():
            template_dict[f"store.{key}"] = str(val) if val is not None else ""

        # 4. Convert and flatten task manifest configurations
        manifest_builtins = msgspec.to_builtins(self.task.manifest)
        template_dict.update(flatten_dict(manifest_builtins, parent_key="manifest"))

        # 5. Convert and flatten task runtime context configurations
        context_builtins = msgspec.to_builtins(self.task.context)
        template_dict.update(flatten_dict(context_builtins, parent_key="context"))

        return template_dict

    def run_hooks(
        self,
        stage_name: str,
        phase: str,  # "pre" or "post"
    ) -> None:
        """Execute all hooks for a given stage and phase."""
        hooks_config = self.task.context.hooks.get(stage_name)
        if not hooks_config:
            return

        actions: list[HookAction] = getattr(hooks_config, phase, [])
        if not actions:
            return

        LOG.info(f"Running {len(actions)} {phase}-hooks for stage '{stage_name}'")
        for i, action in enumerate(actions):
            if not self._should_run(action):
                LOG.info(
                    f"  ⏭️ Hook {i+1}/{len(actions)} ({action.type}) "
                    "skipped via conditional expression."
                )
                if action.id:
                    self.state_map[action.id] = {"status": "skipped"}
                continue

            try:
                # 1. Execute the action
                result = self._execute_action(action)

                # 2. Record success in state mapping if hook has an ID
                if action.id:
                    self.state_map[action.id] = {"status": "success", **(result or {})}

                LOG.success(f"  ✅ Hook {i+1}/{len(actions)} ({action.type}) succeeded")
            except Exception as e:
                # 2. Record success in state mapping if hook has an ID
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

    def _should_run(self, action: "HookAction"):
        if action.if_:
            try:
                names_context = self._eval_names
                return simple_eval(action.if_, names=names_context)

            except Exception as eval_err:
                LOG.exception(
                    f"  ❌ Error evaluating expression '{action.if_}': {eval_err}"
                )
                raise
        return True

    def _execute_action(self, action: "HookAction", _depth: int = 0) -> Any:
        """Dispatch execution to the registered strategy strategy handler."""
        strategy_func = HOOK_STRATEGIES.get(action.type)
        if not strategy_func:
            raise ValueError(f"Unsupported hook type: {action.type}")

        # Pass the runner instance, action schema, and current traversal depth context
        return strategy_func(runner=self, action=action, _depth=_depth)
