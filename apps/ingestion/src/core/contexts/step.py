from msgspec import Struct

from src.core.stages.types import StageConfig
from src.extras.hooks.enums import StageHooks


class StepConfig(Struct, kw_only=True):
    """Configuration for a single pipeline step as declared in job.yaml.

    A step binds a logical id (e.g., 'normalize') to a pipeline stage
    (e.g., 'transform') and carries stage-specific config and hooks.
    Multiple steps can share the same stage (e.g., two extract steps).
    """

    id: str  # logical name, used as workspace folder name and template key
    stage: str  # maps to Stage enum value (extract, transform, write, ...)

    # Stage-specific configs — at most one will be set per step
    config: StageConfig | None = None

    # Per-step hooks
    hooks: StageHooks | None = None
