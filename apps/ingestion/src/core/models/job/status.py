from enum import StrEnum


class JobStatus(StrEnum):
    """
    Shows the current health of the job.

    Independent of Job Step for easier maintenance
    """

    # Initial State
    PENDING = "PENDING"  # Created, waiting for schedule
    QUEUED = "QUEUED"  # Picked up by Orchestrator, waiting for Worker

    # Active States
    PROVISIONING = "PROVISIONING"  # Worker initialized, manifest created
    RUNNING = "RUNNING"  # Actively processing a step

    # Terminal States (End of the road)
    SUCCESS = "SUCCESS"  # Fully finished (Complete step passed)
    FAILED = "FAILED"  # Hard stop, requires manual intervention
    CANCELLED = "CANCELLED"  # Manual kill
    EXPIRED = "EXPIRED"  # TTL reached, data purged, no recovery needed

    # Wait States (The "Breadcrumb" triggers)
    DEFERRED = "DEFERRED"  # Transient error, Orchestrator will retry later
    BLOCKED = "BLOCKED"  # Manual HOLD or dependency missing

    UNKNOWN = "UNKNOWN"

    @classmethod
    def active_statuses(cls) -> set["JobStatus"]:
        return {cls.QUEUED, cls.PENDING, cls.RUNNING, cls.PROVISIONING}

    @classmethod
    def terminal_statuses(cls) -> set["JobStatus"]:
        return {cls.SUCCESS, cls.FAILED, cls.CANCELLED}
