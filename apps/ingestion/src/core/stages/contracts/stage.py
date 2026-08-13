"""Base classes for pipeline execution stages."""

import traceback
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, Generic, Self, TypeVar, cast

import msgspec
from loguru import logger

from src.core.contexts.step import StepConfig
from src.core.models.task.status import ExecutionStatus
from src.core.stages.contracts.payload import BasePayload, ErrorInfo
from src.core.stages.enums import NO_MORE_STAGES
from src.extras.hooks import HookRunner
from src.services.health.system import SystemMonitor
from src.utils.exceptions import OutOfDiskSpace

if TYPE_CHECKING:
    from src.core.models.task.base import Task
    from src.core.stages.types import StageConfig

LOG = logger

T = TypeVar("T", bound="StageConfig | None")


class ExecutionStage(ABC, Generic[T]):
    """Base class for all pipeline stages."""

    requires_disk_space: bool = True
    config_class: type[T] | None = None

    def __init__(self, step: StepConfig):
        self.step: StepConfig = step
        self.step_id: str = (
            step.id
        )  # ID priority (fallback to stage name in StepConfig)
        self.name: str = step.stage
        self._config: T | None = None

        # if isinstance(stage.value, str):
        #     self.name: str = stage.value
        #     self.bitmask = stage.bitmask
        #     self._config: T | None = None

    @classmethod
    def get_disk_free_stages(cls) -> list[type[Self]]:
        """Get all subclasses that don't require disk space."""

        def collect_subclasses(klass: type[Self]) -> list[type[Self]]:
            result: list[type[Self]] = []
            for subclass in klass.__subclasses__():
                # Type check: ensure subclass is a subclass of ExecutionStage
                if issubclass(subclass, ExecutionStage):
                    result.append(subclass)
                    result.extend(collect_subclasses(subclass))
            return result

        return [
            stage for stage in collect_subclasses(cls) if not stage.requires_disk_space
        ]

    def _bind_config(self, task: "Task") -> None:
        """Bind stage config from task context."""
        if self.step.config is None:
            raise RuntimeError(
                f"Step '{self.step_id}' (stage: '{self.name}') has no configuration bound."
            )

        # If subclass defines config_class, we check it at runtime
        if self.config_class is not None:
            if not isinstance(self.step.config, self.config_class):
                raise TypeError(
                    f"Step '{self.step_id}' expected config of type {self.config_class.__name__}, "
                    f"got {type(self.step.config).__name__}"
                )
            self._config = self.step.config
        else:
            # Type ignore tell pyright/mypy we know this assignment matches T in concrete classes
            self._config = cast("T", self.step.config)

    @property
    def config(self) -> T:  # Returns T directly, guaranteed not None
        """Get stage config (validated)."""
        if self._config is None:
            raise RuntimeError(f"Config not bound for stage '{self.name}'")
        return self._config

    def _check_disk_space(self, task: "Task") -> None:
        """Verify sufficient disk space before execution."""
        system = SystemMonitor(task.exec_ctx.workspace_dir)

        if system.is_disk_blocked():
            health = system.report
            if not health:
                raise ValueError("No system health report found!")
            raise OutOfDiskSpace(
                message=f"Disk at {health.disk_usage_pct:.1f}% - aborting stage {self.name}",
                disk_usage=health.disk_usage_pct,
            )

    def pre_flight(self, task: "Task") -> None:
        """
        Pre-execution checks and configuration binding.

        Decision: Config Binding.
        Since TaskContext now supports optional stage blocks, we must assert
        that the specific block required for this stage exists before starting
        execution. Binding it to self.stage_config simplifies subclass logic.
        """
        if self.requires_disk_space:
            self._check_disk_space(task)
        self._bind_config(task)

    # @abstractmethod
    def execute(self, task: "Task") -> str:
        """Execute stage logic. Returns next stage name."""
        runner = HookRunner(task)
        runner.run_hooks(self.step_id, "pre")
        next_stage = self._execute(task)
        runner.run_hooks(self.step_id, "post")
        return next_stage

    @abstractmethod
    def _execute(self, task: "Task") -> str:
        """Execute stage logic. Returns next stage name."""

    def _next_step(self, task: "Task") -> str:
        """Get next stage in pipeline."""
        next_step_id = task.context.get_next_step_id(self.step_id)
        if not next_step_id:
            return NO_MORE_STAGES
        return next_step_id

    def checkpoint(
        self,
        task: "Task",
        data_folder: Path | None = None,
        payload: BasePayload | None = None,
        error: Exception | None = None,
    ) -> None:
        """
        Save stage results and update manifest.

        Decision: Terminal Error Recording.
        If an error is provided, we explicitly record the traceback and
        set the manifest status to FAILED. This ensures the Task execution
        loop halts immediately and the Orchestrator can provide a
        detailed post-mortem.
        """
        if error:
            err_payload = ErrorInfo(
                stage=self.name,
                step_id=self.step_id,
                error_type=type(error).__name__,
                message=str(error),
                traceback=traceback.format_exc(),
            )
            # Record failure immediately
            task.update_manifest(
                {
                    "error": msgspec.to_builtins(err_payload),
                    "status": ExecutionStatus.FAILED.value,
                }
            )
            return

        if not payload:
            raise ValueError("Payload must be provided for successful checkpoints.")

        # Create symlink to data folder if provided
        if data_folder:
            task.workspace.create_symlink(self.step_id, data_folder)

        # Update manifest with results
        payloads = msgspec.to_builtins(task.manifest.payloads)
        payloads.append(msgspec.to_builtins(payload))
        task.update_manifest(
            {
                "payloads": msgspec.to_builtins(payloads),
            }
        )


DISK_FREE_STAGES = set(ExecutionStage.get_disk_free_stages())


class ExecutionStageRegistry:
    """Encapsulates stage class registration and lookup logic."""

    _stages: ClassVar[dict[str, type[ExecutionStage]]] = {}

    @classmethod
    def register(cls, stage_name: str):
        """Decorator to register a stage class in the registry."""

        def decorator(stage_cls: type[ExecutionStage]):
            cls._stages[stage_name] = stage_cls
            return stage_cls

        return decorator

    @classmethod
    def _discover(cls) -> None:
        """Discover and import all stage modules to populate _stages."""
        import importlib

        stages_dir = Path(__file__).parent.parent
        # apps/ingestion root dir (parent of src)
        app_root = stages_dir.parent.parent.parent

        for py_file in stages_dir.rglob("*.py"):
            try:
                rel_path = py_file.relative_to(app_root)
                mod_parts = rel_path.with_suffix("").parts
                leaf_name = mod_parts[-1]
                if leaf_name not in (
                    "base",
                    "utils",
                    "contracts",
                    "__init__",
                    "enums",
                    "config",
                    "payload",
                ):
                    modname = ".".join(mod_parts)
                    importlib.import_module(modname)
            except Exception as e:
                logger.warning(f"Failed to discover stage module {py_file}: {e}")

    @classmethod
    def get(cls, stage_name: str) -> type[ExecutionStage]:
        """Given a stage name, return the corresponding ExecutionStage class."""
        if stage_name not in cls._stages:
            cls._discover()

        if stage_name not in cls._stages:
            raise ValueError(f"Unknown stage: {stage_name}: {list(cls._stages.keys())}")

        return cls._stages[stage_name]

    @classmethod
    def clear(cls) -> None:
        """Clear registered stages (primarily for testing)."""
        cls._stages.clear()

    @classmethod
    def registered_stages(cls) -> list[str]:
        """Return list of currently registered stage names."""
        return list(cls._stages.keys())


# Aliases for backwards compatibility
stage = ExecutionStageRegistry.register
register_stage = ExecutionStageRegistry.register
