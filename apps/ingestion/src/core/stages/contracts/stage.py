"""Base classes for pipeline execution stages."""

import traceback
from abc import abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

import msgspec
from libs.metaclasses.draft import ClassRegistry
from loguru import logger

from src.core.models.task.manifest import (
    TaskManifest,
    TaskManifestFile,
    TaskManifestView,
)
from src.core.models.task.status import ExecutionStatus
from src.core.stages.contracts.payload import BasePayload, ErrorInfo
from src.core.stages.types import NO_MORE_STAGES
from src.extras.hooks import HookRunner
from src.services.health.system import SystemMonitor
from src.utils.exceptions import OutOfDiskSpace, RollbackRequired

if TYPE_CHECKING:
    from src.core.contexts.step import StepContext
    from src.core.models.task.workspace import TaskWorkspace
    from src.core.stages.types import StageConfig, StageContext

LOG = logger

T = TypeVar("T", bound="StageConfig | None")


class ExecutionStage(
    ClassRegistry,
    Generic[T],
    registry_name="ExecutionStageRegistry",
    auto_key=True,
    package_paths="src.core.stages",
):
    """Base class for all pipeline stages."""

    requires_disk_space: bool = True
    config_class: type[T] | None = None

    def __init__(
        self,
        step: "StepContext",
    ):
        self.step = step
        self.step_id: str = (
            step.id
        )  # ID priority (fallback to stage name in StepContext)
        self.name: str = step.stage
        self._config: T | None = None
        self._meta_repo = None

    @classmethod
    def get_disk_free_stages(cls) -> list[type["ExecutionStage"]]:
        """Get all stage classes that don't require disk space."""
        stages = []
        for stage_key in set(cls.keys()):
            stage_cls = cls.get_class(stage_key)
            if (
                issubclass(stage_cls, ExecutionStage)
                and not stage_cls.requires_disk_space
            ):
                stages.append(stage_cls)
        return stages

    def _bind_config(self) -> None:
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

    def get_meta_repo(self, workspace: "TaskWorkspace") -> Any:
        """Retrieves or reuses the process-cached MetadataRepository for the task."""
        if self._meta_repo is not None:
            return self._meta_repo
        from src.services.repo.metadata import MetadataRepository

        # ServiceFactory hashes metadata_config and reuses the worker-level instance
        return MetadataRepository(**workspace.exec_ctx.metadata_db_config)

    def _check_disk_space(self, system: SystemMonitor) -> None:
        """Verify sufficient disk space before execution."""
        # system: Any = self._health_checker or SystemMonitor(task.exec_ctx.workspace_dir)

        if system.is_disk_blocked():
            health = system.report
            if not health:
                raise ValueError("No system health report found!")
            raise OutOfDiskSpace(
                message=f"Disk at {health.disk_usage_pct:.1f}% - aborting stage {self.name}",
                disk_usage=health.disk_usage_pct,
            )

    def pre_flight(
        self,
        system: SystemMonitor,
        ctx: "StageContext",
        workspace: "TaskWorkspace",
        manifest: "TaskManifest",
    ) -> None:
        """
        Pre-execution checks and configuration binding.

        Decision: Config Binding.
        Since TaskContext now supports optional stage blocks, we must assert
        that the specific block required for this stage exists before starting
        execution. Binding it to self.stage_config simplifies subclass logic.
        """
        if self.requires_disk_space:
            self._check_disk_space(system)
        self._bind_config()

    @staticmethod
    def render_placeholders(target: str, manifest: "TaskManifest", step_id: str) -> Any:
        """Renders string templates, expressions, and environment variables within `target`."""
        from libs.utils.template import TemplateEngine

        # Populate steps dictionary: { "steps": { "<step_id>": payload_dict } }
        view = TaskManifestView(manifest)
        steps_ctx = {
            p.step_id: msgspec.to_builtins(p) for p in manifest.payloads if p.step_id
        }

        # 2. Resolve 'upstream' payload relative to current step
        upstream_ctx = {}
        if upstream_id := view.get_upstream_step_id(step_id):
            upstream_ctx = steps_ctx.get(upstream_id, {})

        ctx = {"steps": steps_ctx, "upstream": upstream_ctx}
        engine = TemplateEngine(context=ctx)
        return engine.render(target)

    # @abstractmethod
    def execute(
        self,
        ctx: "StageContext",
        workspace: "TaskWorkspace",
        manifest: "TaskManifest",
    ) -> str:
        """Execute stage logic. Returns next stage name."""
        runner = HookRunner(ctx=ctx, workspace=workspace, manifest=manifest)
        runner.run_hooks(self.step, "pre")
        next_stage = self._execute(ctx=ctx, workspace=workspace, manifest=manifest)
        runner.run_hooks(self.step, "post")
        return next_stage

    @abstractmethod
    def _execute(
        self,
        ctx: "StageContext",
        workspace: "TaskWorkspace",
        manifest: "TaskManifest",
    ) -> str:
        """Execute stage logic. Returns next stage name."""

    def validate_row_count(
        self, manifest: "TaskManifest", row_count: int, dep_step_id: str | None = None
    ) -> bool:
        """Validate stage row count.

        If row_count == 0 on the first attempt, raises RollbackRequired to force a re-attempt.
        If row_count == 0 persists after a re-attempt, sets task.manifest.is_empty_result_set = True
        and returns True.

        Returns:
            bool: True if empty result set confirmed, False if rows exist.
        """
        if row_count > 0:
            return False

        target_step = dep_step_id or self.step_id
        rollback_count = manifest.rollback_stack.count(target_step)

        if rollback_count == 0:
            LOG.warning(
                f"0 rows detected on first attempt for step '{target_step}'. Triggering re-attempt."
            )
            raise RollbackRequired(
                target_step,
                "Zero rows detected on first attempt; triggering re-attempt",
            )

        LOG.info(
            f"0 rows confirmed on re-attempt for step '{target_step}'. Flagging TaskManifest.is_empty_result_set=True."
        )
        manifest.is_empty_result_set = True
        return True

    def _next_step(self, ctx: "StageContext") -> str:
        """Get next stage in pipeline."""
        next_step_id = ctx.next_step_id
        if not next_step_id:
            return NO_MORE_STAGES
        return next_step_id

    def save_stage_outcome(
        self,
        workspace: "TaskWorkspace",
        manifest: "TaskManifest",
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
                step_id=self.step_id,
                stage=self.step.stage,
                error_type=type(error).__name__,
                message=str(error),
                traceback=traceback.format_exc(),
            )
            # Record failure immediately
            TaskManifestFile.update(
                workspace=workspace,
                updates={
                    "error": msgspec.to_builtins(err_payload),
                    "status": ExecutionStatus.FAILED.value,
                },
            )
            return

        if not payload:
            raise ValueError("Payload must be provided for successful checkpoints.")

        # Create symlink to data folder if provided
        if data_folder:
            workspace.create_symlink(self.step_id, data_folder)

        # 1. Update manifest using clean object references
        updated_payloads = [*manifest.payloads, payload]
        TaskManifestFile.update(
            workspace=workspace,
            updates={
                "payloads": updated_payloads,
            },
        )

        # 2. IMMEDIATE VALIDATION GATE:
        # Re-load manifest from disk right away to ensure the schema and
        # tagged payload union structures deserialize without error.
        TaskManifestFile.load(workspace)


DISK_FREE_STAGES = set(ExecutionStage.get_disk_free_stages())
