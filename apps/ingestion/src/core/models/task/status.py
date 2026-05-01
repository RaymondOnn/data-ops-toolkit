from enum import StrEnum


class ExecutionStatus(StrEnum):
    """
    Shows the current health of the job.

    Independent of Task Step for easier maintenance
    """

    # Initial State
    PENDING = "PENDING"  # Created, waiting for schedule
    PROVISIONED = "PROVISIONED"  # Instructions and folder created on disk
    QUEUED = "QUEUED"  # Accepted by the TaskManager, waiting for a Ray Worker

    # Active States
    RUNNING = "RUNNING"  # Actively processing a stage

    # Terminal States (End of the road)
    SUCCESS = "SUCCESS"  # Fully finished (Complete stage passed)
    FAILED = "FAILED"  # Hard stop, requires manual intervention
    CANCELLED = "CANCELLED"  # Manual kill
    EXPIRED = "EXPIRED"  # TTL reached, data purged, no recovery needed

    # Wait States (The "Breadcrumb" triggers)
    DEFERRED = "DEFERRED"  # Transient error, Orchestrator will retry later
    BLOCKED = "BLOCKED"  # Manual HOLD or dependency missing
    RETRY = "RETRY"  # Waiting for scheduled retry after failure

    UNKNOWN = "UNKNOWN"

    @property
    def is_terminal(self) -> bool:
        """Returns True if the status represents a final state."""
        return self in self.terminal_statuses()

    @property
    def is_failure(self) -> bool:
        """Returns True if the status represents a non-successful terminal state."""
        return self in {self.FAILED, self.CANCELLED, self.EXPIRED}

    @property
    def is_success(self) -> bool:
        """Returns True if the status represents a successful completion."""
        return self == self.SUCCESS

    @classmethod
    def active_statuses(cls) -> set["ExecutionStatus"]:
        return {cls.QUEUED, cls.PENDING, cls.RUNNING, cls.PROVISIONED}

    @classmethod
    def terminal_statuses(cls) -> set["ExecutionStatus"]:
        return {cls.SUCCESS, cls.FAILED, cls.CANCELLED, cls.EXPIRED}

    @classmethod
    def dispatched_statuses(cls) -> set["ExecutionStatus"]:
        return {cls.RUNNING}
