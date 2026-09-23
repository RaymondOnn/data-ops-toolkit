import threading


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
