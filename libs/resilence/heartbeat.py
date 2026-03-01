import os
import sdnotify

class Heartbeat:
    """A simple wrapper around systemd's watchdog notifications."""

    def __init__(self) -> None:
        """Initializes the SystemdWatchdog.

        Checks if the NOTIFY_SOCKET environment variable is set
        by systemd.
        """
        self.notifier = sdnotify.SystemdNotifier()
        self.is_systemd = bool(os.getenv("NOTIFY_SOCKET"))

    def ping(self) -> None:
        """Sends the watchdog heartbeat.

        If the NOTIFY_SOCKET environment variable is set, sends a
        watchdog notification to systemd.
        """
        if self.is_systemd:
            self.notifier.notify("WATCHDOG=1")

    def ready(self) -> None:
        """Tells systemd the app has finished starting up.

        If the NOTIFY_SOCKET environment variable is set, sends a
        ready notification to systemd.
        """
        if self.is_systemd:
            self.notifier.notify("READY=1")
