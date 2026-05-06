import threading
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

from apps.ingestion.src.utils.common import find_path
from loguru import logger

if TYPE_CHECKING:
    from apps.ingestion.src.core.contexts import ExecutionContext


LOG = logger


class InternalEventBus:
    def __init__(self):
        self._subscribers = []
        self._change_event = threading.Event()

    def notify(self):
        """Wake up everyone waiting for a change."""
        self._change_event.set()

    def wait_for_change(self, timeout=None):
        """Block until someone calls notify()."""
        signaled = self._change_event.wait(timeout=timeout)
        self._change_event.clear()
        return signaled


class SignalEvent(NamedTuple):
    """Immutable data representing a detected system event."""

    run_id: str
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
        super().__init__()
        self.exec_ctx = exec_ctx

    def collect_events(
        self, filter_run_ids: set[str] | None = None
    ) -> list[SignalEvent]:
        """Scans signals/ and returns a list of detected events."""
        signal_path = self.exec_ctx.signal_path
        events = []

        for file in signal_path.iterdir():
            if not file.is_file():
                continue

            # Only process known signal types, ignore .cmd files here
            if file.suffix not in [".sync", ".done", ".fail", ".retry", ".expired"]:
                file.unlink()  # Delete unknown/unhandled files
                continue

            try:
                _, _, _, run_id = self.exec_ctx.parse_identifier(file.stem)
                if filter_run_ids and run_id not in filter_run_ids:
                    continue

                events.append(
                    SignalEvent(
                        run_id=run_id,
                        signal_type=file.suffix,
                        folder_path=find_path(self.exec_ctx.workspace_dir, run_id),
                    )
                )

            except Exception:
                LOG.exception("Failed to parse signal file", filename=file.name)
            else:
                file.unlink()

        return events
