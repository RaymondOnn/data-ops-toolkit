from enum import StrEnum
from typing import Any

import msgspec


class HookType(StrEnum):
    """Types of hooks to execute."""

    QUERY = "query"
    HTTP = "http"
    SCRIPT = "script"
    COPY = "copy"
    DELETE = "delete"
    ROUTINE = "routine"
    STORE = "store"


class HookOnFailure(StrEnum):
    """Actions to take when a hook fails."""

    ABORT = "abort"
    WARN = "warn"
    SKIP = "skip"


class HookAction(msgspec.Struct, frozen=True):
    """A single hook action to execute.

    Fields are optional and validated at runtime based on `type`.
    Uses Python-safe names (e.g., `from_path` instead of `from`).
    """

    type: HookType

    # Shared
    id: str | None = None
    if_: str | None = msgspec.field(
        default=None, name="if"
    )  # Maps YAML "if" to safe python attr
    connection: dict[str, Any] = {}  # For query, delete, check types
    on_failure: HookOnFailure = HookOnFailure.ABORT
    timeout_seconds: int = 300

    # Query hook
    query: str | None = None  # SQL or file:// path

    # HTTP hook
    url: str | None = None
    method: str | None = None  # GET | POST
    headers: str | None = None
    output: str | None = None

    # Script hook
    command: str | None = None

    # Copy hook
    from_path: str | None = None  # Source path/URI (Sling: "from")
    to_path: str | None = None  # Destination path/URI (Sling: "to")
    from_connection: dict[str, Any] = {}  # Source storage connection
    to_connection: dict[str, Any] = {}  # Destination storage connection
    single_file: bool = False  # Copy single file vs directory

    # Delete hook
    location: str | None = None  # Path to delete
    recursive: bool = False

    # Routine hook
    routine_path: str | None = None  # Path to routine YAML file

    # Store hook fields
    key: str | None = None  # The key to set in the store (e.g., "my_variable")
    value: str | None = None  # The value to store (supports template rendering!)
    clear: bool = False

    into: str | None = None


class StageHooks(msgspec.Struct, frozen=True):
    """Hooks for a single stage."""

    pre: list[HookAction] = []
    post: list[HookAction] = []
