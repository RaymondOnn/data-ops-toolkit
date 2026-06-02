import os

import sdnotify


class Heartbeat:
    """A simple wrapper around systemd's watchdog notifications."""

    def __init__(self) -> None:
        """Initializes the SystemdWatchdog.

        Checks the environment for the NOTIFY_SOCKET variable to determine
        if the process is running under systemd management.
        """
        self.notifier = sdnotify.SystemdNotifier()
        self.is_systemd = bool(os.getenv("NOTIFY_SOCKET"))

    def ping(self) -> None:
        """Sends the watchdog heartbeat notification.

        If the process is running under systemd, sends 'WATCHDOG=1' to
        the notification socket.
        """
        if self.is_systemd:
            self.notifier.notify("WATCHDOG=1")

    def ready(self) -> None:
        """Tells systemd the application has finished starting up.

        If the process is running under systemd, sends 'READY=1' to
        the notification socket.
        """
        if self.is_systemd:
            self.notifier.notify("READY=1")
