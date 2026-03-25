from enum import IntEnum, IntFlag, auto
from typing import Self


class JobBitmask(IntFlag):
    """
    Class representing the bitmask for job steps.
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
    def ALL_DONE(cls) -> "JobBitmask":
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


class JobSteps(IntEnum):
    START = 0
    EXTRACT = 1
    TRANSFORM = 2
    WRITE = 3
    AUDIT = 4
    PUBLISH = 5
    COMPLETE = 6

    @property
    def label(self) -> str:
        return self.name.casefold()

    @property
    def bitmask(self) -> JobBitmask:
        # Map the step to the IntFlag
        mapping = {
            JobSteps.START: JobBitmask.START,
            JobSteps.EXTRACT: JobBitmask.EXTRACT,
            JobSteps.TRANSFORM: JobBitmask.TRANSFORM,
            JobSteps.WRITE: JobBitmask.WRITE,
            JobSteps.AUDIT: JobBitmask.AUDIT,
            JobSteps.PUBLISH: JobBitmask.PUBLISH,
            JobSteps.COMPLETE: JobBitmask.COMPLETE,
        }
        return mapping[self]

    @classmethod
    def next_step(cls, current_label: str) -> Self | None:
        """Finds the next step in the sequence based on a string label."""
        current_enum = cls[current_label.upper()]
        try:
            return cls(current_enum.value + 1)
        except ValueError:
            return None  # We have reached the end of the pipeline

    @classmethod
    def prev_step(cls, current_label: str) -> Self | None:
        """Finds the next step in the sequence based on a string label."""
        current_enum = cls[current_label.upper()]
        try:
            return cls(current_enum.value - 1)
        except ValueError:
            return None  # We have reached the start of the pipeline

    @classmethod
    def first_step(cls) -> Self:
        return cls(0)

    @classmethod
    def last_step(cls) -> Self:
        return cls(len(cls) - 1)


STEP_ORDER = [step.label for step in sorted(JobSteps)]
