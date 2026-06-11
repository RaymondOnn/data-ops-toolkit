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

    def __init__(self, target_stage: str, reason: str):
        """
        Initialize the rollback signal.

        Args:
            target_stage: The name of the stage to rollback to.
            reason: The reason for the rollback.
        """
        self.target_stage = target_stage
        self.reason = reason
        super().__init__(self.reason)
