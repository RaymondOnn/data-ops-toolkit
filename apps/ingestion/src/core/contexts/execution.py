class ExecutionMode(StrEnum):
    NORMAL = "normal"
    DEBUG = "debug"
    TEST = "test"


@dataclass
class ExecutionContext:
    mode: ExecutionMode = ExecutionMode.NORMAL
