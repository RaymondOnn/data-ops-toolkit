# trigger.py
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any


class TriggerEvent(ABC):
    @abstractmethod
    def should_fire(self, job_record: dict[str, Any]) -> bool:
        """Evaluate if the condition for this job is met."""
        pass


class TimeTriggerEvent(TriggerEvent):
    def should_fire(self, job_record: dict[str, Any]) -> bool:
        # job_record comes from your DB poll
        next_run = job_record.get("next_run_time")  # Unix timestamp
        if not next_run:
            return False
        return bool(datetime.now().timestamp() >= next_run)


class FileTriggerEvent(TriggerEvent):
    def should_fire(self, job_record: dict[str, Any]) -> bool:
        # Check if the expected file exists in the landing zone
        path_pattern = job_record.get("expected_path")
        if not path_pattern:
            return False
        return any(Path().glob(path_pattern))
