import enum

import msgspec


class HookType(str, enum.Enum):
    """Types of hooks to execute."""

    QUERY = "query"
    HTTP = "http"
    SCRIPT = "script"
    COPY = "copy"
    DELETE = "delete"
    CHECK = "check"


class HookOnFailure(str, enum.Enum):
    """Actions to take when a hook fails."""

    ABORT = "abort"
    WARN = "warn"
    SKIP = "skip"


class HookAction(msgspec.Struct, frozen=True):
    """A single hook action to execute."""

    type: HookType  # Extensible
    connection: str | None = None  # For query type
    query: str | None = None  # SQL or file:// path
    url: str | None = None  # For http type
    command: str | None = None  # For script type
    on_failure: HookOnFailure = HookOnFailure.ABORT
    timeout_seconds: int = 300


class StageHooks(msgspec.Struct, frozen=True):
    """Hooks for a single stage."""

    pre: list[HookAction] = []
    post: list[HookAction] = []
