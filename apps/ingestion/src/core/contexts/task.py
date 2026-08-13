"""Task configuration and context models for pipeline execution."""

from pathlib import Path
from typing import Any

import msgspec
from loguru import logger
from msgspec import Struct, field

from src.core.contexts.step import StepConfig
from src.core.stages.enums import Stage
from src.extras.flags import FeatureFlags
from src.extras.hooks.enums import StageHooks
from src.utils.constants import CONFIG_FILENAME

LOG = logger

# Cannot find implementation or library stub for module

# =============================================================================
# Step Configuration
# =============================================================================


class TaskContext(Struct, kw_only=True):
    """Complete pipeline configuration for a task run."""

    # Ordered list of steps for this dataset (new canonical structure)
    steps: list[StepConfig] = field(default_factory=list)
    hooks: dict[str, StageHooks] = field(default_factory=dict)

    # Identity
    job_id: str
    dataset_id: str
    partition_date: str
    run_id: str

    # Execution boundaries — step-based (preferred) and stage-based (legacy)
    from_step: str = "start"  # step id to start from (inclusive)
    to_step: str = ""  # step id to stop at (inclusive)
    mode: str | None = None  # append | incremental | truncate | CDC
    partition_on: list[str] = field(default_factory=list)
    primary_keys: list[str] = field(default_factory=list)

    # Paths
    output_path: str = ""

    # Metadata
    audit_columns: list[str] = field(
        default_factory=lambda: ["_partition", "_run_id", "_source"]
    )
    validation_command: str = "validation-app"
    expires_at: float | None = None
    overrides: dict[str, Any] = field(default_factory=dict)
    extras: dict[str, Any] = field(default_factory=dict)
    flags: "FeatureFlags" = field(default_factory=FeatureFlags)

    def __post_init__(self) -> None:
        """Validates and normalizes execution boundaries after initialization."""
        all_ids = self.get_step_ids()

        # 1. Default empty boundaries
        if not self.from_step:
            self.from_step = "start"
        if not self.to_step:
            self.to_step = all_ids[-1]

        # 2. Strict presence validation
        if self.from_step not in all_ids:
            raise ValueError(
                f"Invalid from_step '{self.from_step}'. Valid steps: {all_ids}"
            )
        if self.to_step not in all_ids:
            raise ValueError(
                f"Invalid to_step '{self.to_step}'. Valid steps: {all_ids}"
            )

        # 3. Order validation
        if all_ids.index(self.from_step) > all_ids.index(self.to_step):
            raise ValueError(
                f"Invalid boundary range: from_step '{self.from_step}' "
                f"occurs after to_step '{self.to_step}'."
            )

    def get_step(self, step_id: str) -> "StepConfig | None":
        """Look up a step by its id."""
        return next((s for s in self.steps if s.id == step_id), None)

    def get_step_ids(
        self,
        from_step: str | None = None,
        to_step: str | None = None,
    ) -> list[str]:
        """Return step ids in execution order filtered by inclusive boundaries.

        Args:
            from_step: Start step ID (inclusive). Defaults to self.from_step if set.
            to_step: End step ID (inclusive). Defaults to self.to_step if set.
        """
        all_ids = ["start"] + [s.id for s in self.steps]

        start_boundary = from_step if from_step is not None else self.from_step
        stop_boundary = to_step if to_step is not None else self.to_step

        # Resolve inclusive start index
        start_idx = (
            all_ids.index(start_boundary)
            if start_boundary and start_boundary in all_ids
            else 0
        )

        # Resolve inclusive stop index (+1 to include the end step)
        end_idx = (
            all_ids.index(stop_boundary) + 1
            if stop_boundary and stop_boundary in all_ids
            else len(all_ids)
        )

        # Handle invalid/reversed ranges
        if start_idx >= end_idx:
            return []

        return all_ids[start_idx:end_idx]

    # def get_active_steps(self) -> list[StepConfig]:
    #     """Return steps within from_step..to_step boundaries (inclusive).

    #     Falls back to all steps if no boundaries are set.
    #     """
    #     if not self.steps:
    #         return []
    #     ids = self.get_step_ids()
    #     start = (
    #         ids.index(self.from_step) if self.from_step and self.from_step in ids else 0
    #     )
    #     end = (
    #         ids.index(self.to_step) + 1
    #         if self.to_step and self.to_step in ids
    #         else len(ids)
    #     )
    #     return self.steps[start:end]

    # def get_step_index(self, step_id: str) -> int | None:
    #     """Return the 0-based index of a step by its ID."""
    #     for idx, step in enumerate(self.steps):
    #         if step.id == step_id:
    #             return idx
    #     return None

    # def get_prev_step(self, step_id: str) -> StepConfig | None:
    #     """Get the step executing immediately before the current step_id."""
    #     idx = self.get_step_index(step_id)
    #     if idx is not None and idx > 0:
    #         return self.steps[idx - 1]
    #     return None

    def get_next_step_id(self, current_step_id: str) -> str | None:
        """Get the ID of the step executing immediately after current_step_id

        Respects the active run boundaries (from_step .. to_step).
        """
        active_ids = self.get_step_ids()
        if current_step_id in active_ids:
            idx = active_ids.index(current_step_id)
            if idx + 1 < len(active_ids):
                return active_ids[idx + 1]
        return None

    def resolve_stage(self, step_id: str) -> str:
        """Derives the system Stage enum value for a given step_id."""
        if step_id == "start":
            return Stage.START.value

        step = self.get_step(step_id)
        if not step:
            raise ValueError(
                f"Step '{step_id}' not found in TaskContext configuration."
            )
        return step.stage

    @classmethod
    def from_path(
        cls, folder_path: Path | str | None = None, filepath: Path | str | None = None
    ):
        config_file = Path()
        if filepath and (path := Path(filepath)).is_file():
            config_file = path
        elif folder_path and (path := Path(folder_path)).is_dir():
            config_file = path / CONFIG_FILENAME

        if not config_file.exists():
            raise FileNotFoundError(f"Failed to load config file: {path}")

        return msgspec.json.decode(config_file.read_bytes(), type=TaskContext)

    # def __post_init__(self):
    #     if self.extract and not self.primary_keys:
    #         LOG.error(f"{self.extract=} {self.primary_keys=}")
    #         raise ValueError(
    #             "No primary key defined for source dataset. "
    #             "At least one column must be marked as primary_key."
    #         )


# =============================================================================
# Loading Utility
# =============================================================================


def load_context(folder: Path) -> "TaskContext":
    """Load pipeline context from workspace folder."""
    config_path = folder / CONFIG_FILENAME
    if not config_path.exists():
        raise FileNotFoundError(f"Missing {CONFIG_FILENAME} in {folder}")

    with config_path.open("rb") as f:
        return msgspec.json.decode(f.read(), type=TaskContext)
