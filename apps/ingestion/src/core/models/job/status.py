from enum import StrEnum


class ExecutionStatus(StrEnum):
    """
    Shows the current health of the job.

    Independent of Task Step for easier maintenance
    """

    # Initial State
    PENDING = "PENDING"  # Created, waiting for schedule
    QUEUED = "QUEUED"  # Picked up by Orchestrator, waiting for Worker

    # Active States
    PROVISIONING = "PROVISIONING"  # Worker initialized, manifest created
    RUNNING = "RUNNING"  # Actively processing a stage

    # Terminal States (End of the road)
    SUCCESS = "SUCCESS"  # Fully finished (Complete stage passed)
    FAILED = "FAILED"  # Hard stop, requires manual intervention
    CANCELLED = "CANCELLED"  # Manual kill
    EXPIRED = "EXPIRED"  # TTL reached, data purged, no recovery needed

    # Wait States (The "Breadcrumb" triggers)
    DEFERRED = "DEFERRED"  # Transient error, Orchestrator will retry later
    BLOCKED = "BLOCKED"  # Manual HOLD or dependency missing

    UNKNOWN = "UNKNOWN"

    @classmethod
    def active_statuses(cls) -> set["ExecutionStatus"]:
        return {cls.QUEUED, cls.PENDING, cls.RUNNING, cls.PROVISIONING}

    @classmethod
    def terminal_statuses(cls) -> set["ExecutionStatus"]:
        return {cls.SUCCESS, cls.FAILED, cls.CANCELLED}

    @classmethod
    def dispatched_statuses(cls) -> set["ExecutionStatus"]:
        return {cls.PROVISIONING, cls.RUNNING}
