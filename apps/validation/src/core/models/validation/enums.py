
from enum import StrEnum



class ExecutionStatus(StrEnum):
    """
    Status of a validation execution.
    """
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    RUNNING = "RUNNING"
    ERROR = "ERROR"
    
class ValidationOutcome(StrEnum):
    """
    Outcome of a validation tier.
    """
    PASSED = "PASSED"
    FAILED = "FAILED"
    WARN = "WARN". # Passed with warnings i.e. Some failures
    