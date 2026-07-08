from enum import IntFlag, StrEnum
from functools import cached_property

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

    @cached_property
    def bitmask(self) -> int:
        """Bitmask based on enum definition order (STABLE)."""
        return 1 << list(Stage).index(self)

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


# If you still need StageBitmask for type hints
class StageBitmask(IntFlag):
    """Auto-generated from Stage enum."""

    NONE = 0

    @classmethod
    def generate(cls) -> None:
        """Generate bitmask members from Stage enum."""
        for stage in Stage:
            setattr(cls, stage.name, stage.bitmask)

    @classmethod
    def all(cls) -> int:
        """Bitmask with all stages set."""
        return (1 << len(Stage)) - 1


# Generate StageBitmask members
StageBitmask.generate()
