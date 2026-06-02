class RetryTask(Exception):
    """
    Raised by a stage to signal a transient failure that requires a retry.
    """

    def __init__(
        self, reason: str, wait_seconds: int = 30, service_name: str | None = None
    ):
        """
        Initializes the RetryTask exception.

        Args:
            reason: The reason for the retry.
            wait_seconds: The duration to wait before retrying (default 30).
            service_name: Optional name of the service that failed.
        """
        self.reason = reason
        self.wait_seconds = wait_seconds
        self.service_name = service_name
        super().__init__(self.reason)


class RewindTask(Exception):
    """
    Raised to signal that a prerequisite artifact is missing and
    the task must jump back to a previous stage.
    """

    def __init__(self, target_stage: str, reason: str):
        """
        Initializes the RewindTask exception.

        Args:
            target_stage: The name of the stage to rewind to.
            reason: The reason for the rewind.
        """
        self.target_stage = target_stage
        self.reason = reason
        super().__init__(self.reason)
