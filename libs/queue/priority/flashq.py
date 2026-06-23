from pathlib import Path
from typing import Any

import msgspec
from loguru import logger

from .base import PriorityQueue, TaskMessage

LOG = logger


class FlashQQueue(PriorityQueue):
    """FlashQ implementation of PriorityQueue."""

    def __init__(
        self,
        filepath: Path,
        auto_ack: bool = False,
        enable_dashboard: bool = True,
        dashboard_port: int = 8000,
    ):
        """Initialize FlashQ queue.

        Args:
            filepath: Path to queue database
            auto_ack: Auto-acknowledge messages on pop
            enable_dashboard: Enable web dashboard
            dashboard_port: Port for dashboard (default: 8000)
        """
        from flashq import FlashQ

        self.path = filepath.expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)

        self.instance = FlashQ(db_path=str(self.path))
        self.instance.auto_ack = auto_ack

        # Store dashboard config
        self.dashboard_enabled = enable_dashboard
        self.dashboard_port = dashboard_port
        self._dashboard_started = False

        # Log the dashboard URL
        if enable_dashboard:
            dashboard_url = f"http://localhost:{dashboard_port}"
            LOG.info(
                f"FlashQ initialized | Path: {self.path} | "
                f"Dashboard: {dashboard_url} | auto_ack={auto_ack}"
            )

            # Optionally start the dashboard automatically
            # Note: This would need to run in a background thread
            # self._start_dashboard()
        else:
            LOG.info(f"FlashQ initialized | Path: {self.path} | Dashboard: disabled")

    @classmethod
    def from_dict(cls, config: dict) -> "FlashQQueue":
        """Create FlashQQueue from dictionary."""
        return cls(
            filepath=Path(config["filepath"]),
            auto_ack=config.get("auto_ack", False),
            enable_dashboard=config.get("enable_dashboard", True),
            dashboard_port=config.get("dashboard_port", 8000),
        )

    def _start_dashboard(self) -> None:
        """Start the FlashQ web dashboard in a background thread."""
        if self._dashboard_started:
            return

        import threading

        from flashq.server import run_server

        def start_server():
            run_server(
                db_path=str(self.path),
                host="0.0.0.0",
                port=self.dashboard_port,
                debug=False,
            )

        thread = threading.Thread(target=start_server, daemon=True)
        thread.start()
        self._dashboard_started = True
        LOG.info(f"FlashQ dashboard started on port {self.dashboard_port}")

    def push(
        self,
        data: Any,
        priority: int,
        group: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Push task to FlashQ."""
        # Convert data to dict if it's a Pydantic/msgspec model
        if hasattr(data, "to_dict"):
            data = data.to_dict()
        elif hasattr(data, "__dataclass_fields__"):
            data = msgspec.to_builtins(data)

        self.instance.push(
            data=data, priority=priority, group=group, metadata=metadata or {}
        )

    def pop(self, visibility_timeout: int = 300) -> TaskMessage | None:
        """Pop highest priority task."""
        msg = self.instance.pop(visibility_timeout=visibility_timeout)
        if not msg:
            return None

        # Wrap in TaskMessage for consistent interface
        return TaskMessage(
            id_=msg.id, data=msg.data, metadata=getattr(msg, "metadata", {})
        )

    def ack(self, msg_id: str) -> None:
        """Acknowledge task completion."""
        self.instance.ack(msg_id)

    def size(self) -> int:
        """Return approximate queue size."""
        # FlashQ doesn't provide direct size, but we can estimate
        # This is a limitation - consider caching or periodic counting
        return 0  # Placeholder

    def cleanup(self) -> None:
        """Clean up completed tasks."""
        # FlashQ automatically handles cleanup
        pass

    def shutdown(self) -> None:
        """Shutdown FlashQ."""
        # FlashQ doesn't require explicit shutdown
        pass

    def __repr__(self) -> str:
        return f"FlashQQueue(path={self.path})"
