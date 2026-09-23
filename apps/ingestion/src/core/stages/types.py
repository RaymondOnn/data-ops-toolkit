from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

from .archive.config import ArchiveConfig
from .archive.enums import ArchivePayload
from .extract.config import ExtractConfig
from .extract.enums import ExtractPayload
from .publish.config import PublishConfig
from .publish.enums import PublishPayload
from .start.enums import StartPayload
from .transform.config import TransformConfig
from .transform.enums import TransformPayload
from .write.config import WriteConfig
from .write.enums import WritePayload

if TYPE_CHECKING:
    from src.core.contexts.task import TaskContext

StagePayload = (
    StartPayload
    | ExtractPayload
    | TransformPayload
    | WritePayload
    | PublishPayload
    | ArchivePayload
)


StageConfig = (
    ExtractConfig | TransformConfig | WriteConfig | PublishConfig | ArchiveConfig
)


# Sentinel value indicating the pipeline has reached its terminal conclusion
NO_MORE_STAGES = "FINISH"
STAGE_ORDER = [
    "start",
    "extract",
    "transform",
    "write",
    "publish",
    "archive",
]


class Stage(StrEnum):
    """Pipeline stages - enum order defines bitmask values."""

    START = "start"
    EXTRACT = "extract"
    TRANSFORM = "transform"
    WRITE = "write"
    # AUDIT = "audit"
    PUBLISH = "publish"
    ARCHIVE = "archive"

    # Execution order (can be different from enum order)

    # @cached_property
    # def bitmask(self) -> int:
    #     """Bitmask based on enum definition order (STABLE)."""
    #     return 1 << list(Stage).index(self)

    @property
    def token(self) -> str:
        """3-letter token."""
        return {
            "start": "STA",
            "extract": "EXT",
            "transform": "TFR",
            "write": "WRT",
            "publish": "PUB",
            "archive": "ARC",
        }.get(self.value, self.value[:3].upper())

    @classmethod
    def from_string(cls, value: str) -> "Stage":
        """Convert string to Stage enum member (case-insensitive)."""
        try:
            return cls(value.lower())
        except ValueError:
            valid = ", ".join([s.value for s in cls])
            raise ValueError(
                f"Invalid stage '{value}'. Valid stages: {valid}"
            ) from None

    @classmethod
    def _ordered(cls) -> list["Stage"]:
        """Return stages in execution order."""
        order_map = {name: idx for idx, name in enumerate(STAGE_ORDER)}
        return sorted(cls, key=lambda s: order_map[s.value])

    @property
    def _index(self) -> int:
        """Position in execution order."""
        return self._ordered().index(self)

    def next(self) -> "Stage | None":
        """Next stage in execution order."""
        ordered = self._ordered()
        idx = self._index
        return ordered[idx + 1] if idx + 1 < len(ordered) else None

    def prev(self) -> "Stage | None":
        """Previous stage in execution order."""
        ordered = self._ordered()
        idx = self._index
        return ordered[idx - 1] if idx > 0 else None

    @classmethod
    def first(cls) -> "Stage":
        return cls._ordered()[0]

    @classmethod
    def last(cls) -> "Stage":
        return cls._ordered()[-1]


ALL_STAGES = tuple(Stage)  # All stages in enum definition order


class StageContext(msgspec.Struct):
    job_id: str
    dataset_id: str
    run_id: str
    partition_date: str
    step_id: str
    next_step_id: str
    data_dir: Path
    workspace_dir: Path
    task_context: "TaskContext"
