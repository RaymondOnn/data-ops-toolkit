# trigger.py
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

from .enums import JobRecord


class TriggerEvent(ABC):
    @abstractmethod
    def should_fire(self, job_record: JobRecord) -> bool:
        """Evaluate if the condition for this job is met."""
        pass


class TimeTriggerEvent(TriggerEvent):
    def should_fire(self, job_record: JobRecord) -> bool:
        # job_record comes from your DB poll
        # We compare localized datetimes to ensure 'now' matches the schedule's timezone
        from zoneinfo import ZoneInfo

        # Using the context timezone is safer than system local
        tz = job_record.SCHEDULED_TIMESTAMP.tzinfo or ZoneInfo("Asia/Singapore")
        now = datetime.now(tz)
        return bool(now >= job_record.SCHEDULED_TIMESTAMP)


class FileTriggerEvent(TriggerEvent):
    def should_fire(self, job_record: JobRecord) -> bool:
        # Check if the expected file exists in the landing zone
        path_pattern = job_record.WATCH_FILE_PATH
        if not path_pattern:
            return False
        return any(Path().glob(path_pattern))
