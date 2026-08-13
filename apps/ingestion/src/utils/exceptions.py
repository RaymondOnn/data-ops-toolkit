"""Control flow exceptions for task recovery mechanisms.

These exceptions are not errors themselves, but signals from stages to the
executor indicating what recovery action should be taken.
"""


class TryAgainLater(Exception):
    """
    Signals a transient failure that requires retrying after a delay.

    Raised by a stage when a temporary issue occurs (e.g., network timeout,
    resource contention, service unavailability). The executor will wait
    for the specified duration before retrying the stage.
    """

    def __init__(
        self, reason: str, wait_seconds: int = 30, service_name: str | None = None
    ):
        """
        Initialize the retry signal.

        Args:
            reason: The reason for requiring a retry.
            wait_seconds: Duration to wait before retrying (default 30).
            service_name: Optional name of the service that failed.
        """
        self.reason = reason
        self.wait_seconds = wait_seconds
        self.service_name = service_name
        super().__init__(self.reason)


class RollbackRequired(Exception):
    """
    Signals that a prerequisite is missing and execution must rollback.

    Raised by a stage when it detects that a required artifact from a
    previous stage is missing or incomplete. The executor will jump back
    to the specified stage to recreate the missing dependency.
    """

    def __init__(self, target_step_id: str, reason: str):
        """
        Initialize the rollback signal.

        Args:
            target_step_id: The ID of the step to rollback to.
            reason: The reason for the rollback.
        """
        self.target_step_id = target_step_id
        self.reason = reason
        super().__init__(self.reason)


class OutOfDiskSpace(Exception):
    """
    Signals that the system is out of disk space and requires recovery actions.

    This exception is raised when the available disk space drops below the
    configured threshold. It triggers the orchestrator's self-healing
    mechanism to clean up old tasks and free up space.
    """

    def __init__(
        self,
        message: str,
        disk_usage: float | None = None,
        required_space: int | None = None,
        available_space: int | None = None,
        wait_seconds: int = 60,
    ):
        self.message = message
        self.disk_usage = disk_usage
        self.required_space = required_space
        self.available_space = available_space
        self.wait_seconds = wait_seconds
        self.service_name = (
            "DISK_PRESSURE"  # For compatibility with service outage handling
        )
        super().__init__(message)
