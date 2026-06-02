from unittest.mock import MagicMock, patch

from libs.resilience.heartbeat import Heartbeat


class TestHeartbeat:
    """Unit tests for the Heartbeat utility."""

    @patch("os.getenv")
    def test_detects_systemd_correctly(self, mock_getenv):
        """
        GIVEN the NOTIFY_SOCKET is present in the environment
        THEN is_systemd should be True
        WHEN the Heartbeat is initialized
        """
        mock_getenv.return_value = "/path/to/socket"
        hb = Heartbeat()
        assert hb.is_systemd is True

    @patch("os.getenv")
    def test_detects_missing_systemd(self, mock_getenv):
        """
        GIVEN the NOTIFY_SOCKET is absent from the environment
        THEN is_systemd should be False
        WHEN the Heartbeat is initialized
        """
        mock_getenv.return_value = None
        hb = Heartbeat()
        assert hb.is_systemd is False

    @patch("os.getenv")
    def test_ping_behavior_under_systemd(self, mock_getenv):
        """
        GIVEN a Heartbeat instance running under systemd
        THEN it should notify systemd with WATCHDOG=1
        WHEN ping is called
        """
        mock_getenv.return_value = "socket"
        hb = Heartbeat()
        # Inject a mock notifier to verify interaction
        hb.notifier = MagicMock()

        hb.ping()
        hb.notifier.notify.assert_called_once_with("WATCHDOG=1")

    @patch("os.getenv")
    def test_ping_behavior_standalone(self, mock_getenv):
        """
        GIVEN a Heartbeat instance NOT running under systemd
        THEN it should not attempt to notify
        WHEN ping is called
        """
        mock_getenv.return_value = None
        hb = Heartbeat()
        hb.notifier = MagicMock()

        hb.ping()
        hb.notifier.notify.assert_not_called()

    @patch("os.getenv")
    def test_ready_notification(self, mock_getenv):
        """
        GIVEN a Heartbeat instance running under systemd
        THEN it should notify systemd with READY=1
        WHEN ready is called
        """
        mock_getenv.return_value = "socket"
        hb = Heartbeat()
        hb.notifier = MagicMock()

        hb.ready()
        hb.notifier.notify.assert_called_once_with("READY=1")
