from enum import IntEnum, IntFlag, auto
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
    AUDIT = auto()
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


class StageName(IntEnum):
    START = 0
    EXTRACT = auto()
    TRANSFORM = auto()
    WRITE = auto()
    # AUDIT = auto()
    PUBLISH = auto()
    COMPLETE = auto()

    @property
    def label(self) -> str:
        return self.name.casefold()

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
    def next(cls, current_label: str) -> Self | None:
        """Finds the next stage in the sequence based on a string label."""
        current_enum = cls[current_label.upper()]
        try:
            return cls(current_enum.value + 1)
        except ValueError:
            return None  # We have reached the end of the pipeline

    @classmethod
    def prev_stage(cls, current_label: str) -> Self | None:
        """Finds the next stage in the sequence based on a string label."""
        current_enum = cls[current_label.upper()]
        try:
            return cls(current_enum.value - 1)
        except ValueError:
            return None  # We have reached the start of the pipeline

    @classmethod
    def first(cls) -> Self:
        return cls(0)

    @classmethod
    def last(cls) -> Self:
        return cls(len(cls) - 1)


EXEC_STAGES = [stage.label for stage in sorted(StageName)]
