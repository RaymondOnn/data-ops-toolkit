from enum import IntFlag, StrEnum, auto
from typing import Self


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
    COMPLETE = auto()

    @classmethod
    def ALL_DONE(cls) -> "StageBitmask":
        """
        Dynamically calculates the sum of all flags.
        Useful for checking if the 50M row pipeline is 100% complete.
        """
        mask = cls.NONE
        for member in cls:
            mask |= member
        return mask

    def is_fully_complete(self) -> bool:
        """Helper to check if the current instance matches ALL_DONE."""
        return self == self.ALL_DONE()


class StageName(StrEnum):
    START = "start"
    EXTRACT = "extract"
    TRANSFORM = "transform"
    WRITE = "write"
    # AUDIT = "audit"
    PUBLISH = "publish"
    COMPLETE = "complete"

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
            StageName.COMPLETE: StageBitmask.COMPLETE,
        }
        return mapping[self]

    @classmethod
    def next(cls, current_stage: str) -> Self | None:
        """Finds the next stage in the sequence based on a string label."""
        members = list(cls)
        try:
            current_member = cls(current_stage)
            idx = members.index(current_member)
            return members[idx + 1] if idx + 1 < len(members) else None
        except ValueError:
            return None

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


EXEC_STAGES = [stage.label for stage in StageName]
