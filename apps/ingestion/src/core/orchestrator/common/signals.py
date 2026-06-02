import threading
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

from apps.ingestion.src.core.models.task.enums import (
    SUPPORTED_SIGNAL_EXTENSIONS,
    TaskIdentity,
)
from apps.ingestion.src.utils.common import find_path
from loguru import logger

if TYPE_CHECKING:
    from apps.ingestion.src.core.contexts import ExecutionContext


LOG = logger


class InternalEventBus:
    """
    Thread-safe event signaling mechanism for orchestrator synchronization.

    Used to bridge asynchronous worker events with the synchronous polling tick.
    """

    def __init__(self):
        """Initializes the event bus with a fresh threading event."""
        self._subscribers = []
        self._change_event = threading.Event()

    def notify(self):
        """Wakes up any threads waiting for a system state change."""
        self._change_event.set()

    def wait_for_change(self, timeout: float | None = None) -> bool:
        """
        Blocks the calling thread until a notification is received.

        Args:
            timeout: Maximum time to wait in seconds.

        Returns:
            bool: True if the event was set, False if the timeout occurred.
        """
        signaled = self._change_event.wait(timeout=timeout)
        self._change_event.clear()
        return signaled


class SignalEvent(NamedTuple):
    """
    Immutable data representing a detected task system event.

    Carries the task identity and the nature of the signal found on disk.
    """

    identity: TaskIdentity
    signal_type: str  # .done, .fail, .sync, etc.
    folder_path: Path | None


class SignalProcessor(InternalEventBus):
    """
    The 'Sensor' for filesystem-based events.

    It parses zero-byte signal files into high-level event objects.
    """

    def __init__(
        self,
        exec_ctx: "ExecutionContext",
    ):
        """
        Initializes the SignalProcessor.

        Args:
            exec_ctx: The global execution context providing signal paths.
        """
        super().__init__()
        self.exec_ctx = exec_ctx

        # Decentralized: SignalProcessor owns the signals directory
        self.exec_ctx.signal_path.mkdir(parents=True, exist_ok=True)

    def collect_events(
        self, filter_run_ids: set[str] | None = None
    ) -> list[SignalEvent]:
        """
        Scans the signal directory and converts file markers into SignalEvents.

        Cleans up (deletes) processed or invalid signal files to prevent
        duplicate processing in subsequent ticks.

        Args:
            filter_run_ids: Optional set of run IDs to limit discovery.

        Returns:
            list[SignalEvent]: A list of detected task signals.
        """
        signal_path = self.exec_ctx.signal_path
        events = []

        for file in signal_path.iterdir():
            if not file.is_file():
                continue

            # Only process known signal types, ignore .cmd files here
            if file.suffix not in SUPPORTED_SIGNAL_EXTENSIONS:
                file.unlink()  # Delete unknown/unhandled files
                continue

            try:
                # Decouple parsing logic: Get Immutable Identity from the filename
                identity = self.exec_ctx.get_task_id(file.stem)

                if filter_run_ids and identity.run_id not in filter_run_ids:
                    continue

                events.append(
                    SignalEvent(
                        identity=identity,
                        signal_type=file.suffix,
                        folder_path=find_path(
                            self.exec_ctx.workspace_dir, identity.run_id
                        ),
                    )
                )

            except Exception:
                LOG.exception("Failed to parse signal file", filename=file.name)
            else:
                file.unlink()

        return events
