from enum import IntFlag, StrEnum, auto
from typing import Self

# Sentinel value indicating the pipeline has reached its terminal conclusion
STAGE_TERMINAL_SENTINEL = "FINISH"


class StageBitmask(IntFlag):
    """
    Class representing the bitmask for job stages.
    """

    NONE = 0
    START = auto()
    EXTRACT = auto()
    TRANSFORM = auto()
    WRITE = auto()
    # AUDIT = auto()
    PUBLISH = auto()
    ARCHIVE = auto()

    @classmethod
    def all_done(cls) -> "StageBitmask":
        """
        Dynamically calculates the sum of all flags.
        Useful for checking if the 50M row pipeline is 100% complete.
        """
        mask = cls.NONE
        for member in cls:
            mask |= member
        return mask

    def is_fully_complete(self) -> bool:
        """Helper to check if the current instance matches all_done."""
        return self == self.all_done()


class StageName(StrEnum):
    START = "start"
    EXTRACT = "extract"
    TRANSFORM = "transform"
    WRITE = "write"
    # AUDIT = "audit"
    PUBLISH = "publish"
    ARCHIVE = "archive"

    @classmethod
    def from_label(cls, label: str) -> "StageName":
        """Robust lookup that handles case-insensitive labels."""
        try:
            return cls(label.casefold())
        except ValueError as exc:
            raise ValueError(f"'{label}' is not a valid StageName") from exc

    @classmethod
    def _members(cls):
        return list(cls)

    @property
    def label(self) -> str:
        return self.value.casefold()

    @property
    def bitmask(self) -> StageBitmask:
        # Map the stage to the IntFlag
        mapping = {
            StageName.START: StageBitmask.START,
            StageName.EXTRACT: StageBitmask.EXTRACT,
            StageName.TRANSFORM: StageBitmask.TRANSFORM,
            StageName.WRITE: StageBitmask.WRITE,
            # StageName.AUDIT: StageBitmask.AUDIT,
            StageName.PUBLISH: StageBitmask.PUBLISH,
            StageName.ARCHIVE: StageBitmask.ARCHIVE,
        }
        return mapping[self]

    @property
    def token(self) -> str:
        """Returns a 3-letter token for the stage."""
        mapping = {
            StageName.START: "INI",
            StageName.EXTRACT: "EXT",
            StageName.TRANSFORM: "TRN",
            StageName.WRITE: "WRI",
            # StageName.AUDIT: "AUD",
            StageName.PUBLISH: "PUB",
            StageName.ARCHIVE: "ARC",
        }
        return mapping.get(self, "UNK")

    @classmethod
    def next(cls, current_stage: str) -> str:
        """Finds the next stage label or returns the terminal sentinel."""
        members = list(cls)
        try:
            current_member = cls(current_stage)
            idx = members.index(current_member)
            return (
                members[idx + 1] if idx + 1 < len(members) else STAGE_TERMINAL_SENTINEL
            )
        except ValueError:
            return STAGE_TERMINAL_SENTINEL

    @classmethod
    def prev(cls, current_stage: str) -> Self | None:
        """Finds the next stage in the sequence based on a string label."""
        members = list(cls)
        try:
            current_member = cls(current_stage)
            idx = members.index(current_member)
            return members[idx - 1] if idx > 0 else None
        except ValueError:
            return None

    @classmethod
    def first(cls) -> Self:
        return cls._members()[0]

    @classmethod
    def last(cls) -> Self:
        return cls._members()[-1]

    # Comparison for "Ordered" behavior
    def __lt__(self, other):
        if type(self) is type(other):
            return self._members().index(self) < self._members().index(other)
        return NotImplemented


# Using a Tuple of StageName objects ensures type safety and immutability.
EXEC_STAGES: tuple[StageName, ...] = tuple(StageName)
