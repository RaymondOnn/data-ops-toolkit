"""Filesystem-based event detection for task orchestration."""

import threading
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple, Protocol

from loguru import logger

from src.core.models.task import TaskSignal
from src.core.models.task.enums import (
    SUPPORTED_SIGNAL_EXTENSIONS,
    TaskIdentity,
)
from src.utils.common import find_path

if TYPE_CHECKING:
    from src.core.contexts import ExecutionContext

LOG = logger


class EventBus:
    """Thread-safe event bus for orchestrator synchronization."""

    def __init__(self):
        self._event = threading.Event()

    def notify(self) -> None:
        """Wake up waiting threads."""
        self._event.set()

    def wait(self, timeout: float | None = None) -> bool:
        """Wait for trigger or timeout."""
        signaled = self._event.wait(timeout=timeout)
        self._event.clear()
        return signaled


class SignalEvent(NamedTuple):
    """Detected task filesystem event."""

    identity: TaskIdentity
    signal_type: str  # .done, .fail, .sync
    folder_path: Path | None


class SignalHandler(Protocol):
    def __call__(self, event: SignalEvent) -> None: ...


class SignalScanner(EventBus):
    """Scans signal directory for task marker files."""

    def __init__(self, exec_ctx: "ExecutionContext"):
        super().__init__()
        self.exec_ctx = exec_ctx
        self._handlers: dict[TaskSignal, SignalHandler] = {}
        self.exec_ctx.signal_path.mkdir(parents=True, exist_ok=True)

    def on(self, signal_type: TaskSignal, handler: SignalHandler) -> None:
        """Register a handler for a signal type."""
        self._handlers[signal_type] = handler

    def dispatch(self, filter_run_ids: set[str] | None = None) -> None:
        """Scan and dispatch signals to registered handlers."""
        events_dispatched = False
        for marker in self.exec_ctx.signal_path.iterdir():
            if not marker.is_file():
                continue

            if marker.suffix not in SUPPORTED_SIGNAL_EXTENSIONS:
                marker.unlink()
                continue

            try:
                signal_type = TaskSignal(marker.suffix.casefold().strip("."))
                handler = self._handlers.get(signal_type)

                if not handler:
                    LOG.warning(f"No handler for signal: {signal_type}")
                    marker.unlink()
                    continue

                identity = self.exec_ctx.parse_task_id(marker.stem)
                if filter_run_ids and identity.run_id not in filter_run_ids:
                    continue

                event = SignalEvent(
                    identity=identity,
                    signal_type=signal_type,
                    folder_path=find_path(self.exec_ctx.workspace_dir, identity.run_id),
                )

                handler(event)
                events_dispatched = True

            except ValueError:
                LOG.exception(f"Unrecognized signal file: '{marker.suffix}'")
            except Exception:
                LOG.exception("Failed to process signal", file=marker.name)
            finally:
                marker.unlink()

        if events_dispatched:
            self.notify()  # Wake up the engine
