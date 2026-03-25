from .base import JobStep
from .enums import STEP_ORDER, JobSteps


def get_step_class_by_name(name: str) -> JobStep:
    """
    Given a step name, returns the corresponding JobStep class.

    Iterates through all subclasses of JobStep and checks if the name
    attribute matches the given name.
    If no match is found, raises a ValueError.
    """
    for step_class in JobStep.__subclasses__():
        # If you have nested subclasses, you may want a recursive walk here.
        if getattr(step_class, "name", None) == name:
            idx = STEP_ORDER.index(name)
            step = JobSteps(idx)
            return step_class(step=step)
    raise ValueError(f"Unknown step name: {name}")
