"""Filesystem-based command processor for daemon control."""

from collections.abc import Callable
from enum import StrEnum
from typing import Any

import msgspec
from loguru import logger

LOG = logger


class CommandPriority(StrEnum):
    """Command priority levels (lower number = higher priority)."""

    STOP = "0"
    CANCEL_RUN = "0"
    RELOAD_CONFIG = "1"
    RECOVER_ALL = "2"
    RESUME = "2"
    ADD = "3"


class CommandProcessor:
    """Processes filesystem-based command signals."""

    def __init__(self, exec_ctx, janitor, state_store):
        """Initializes the CommandProcessor with core orchestration components.

        Args:
            exec_ctx (ExecutionContext): The global execution context
            janitor (Janitor): The maintenance service for task recovery and cleanup.
            state_store (StateStore): The persistent state storage for job telemetry.

        Notes:
            Decision: Handler-Based Dispatch.
            By mapping command types to callable handlers, the processor can easily be
            extended with new command types (e.g., dynamic log level adjustment) without
            modifying the core polling loop.
        """
        self.exec_ctx = exec_ctx
        self.janitor = janitor
        self.state_store = state_store
        self._handlers: dict[str, Callable] = {
            "RECOVER_ALL": lambda _: self.janitor.recover_failed_tasks(),
            "CANCEL_RUN": self._cancel_run,
            "RELOAD_CONFIG": lambda _: LOG.info("Config reload not implemented"),
        }

    def register_handler(self, command: str, handler: Callable) -> None:
        """Registers a custom handler for a specific command type.

        Args:
            command (str): The base name of the command (e.g., 'RESUME').
            handler (Callable): A callable that accepts the command payload as its
                only argument.

        Raises:
            AttributeError: If the handler is not a callable object or does not
                support the required argument signature.
        """
        self._handlers[command.upper().replace(".CMD", "")] = handler

    def process_commands(self) -> list[tuple[Callable, Any]]:
        """Scans the signal directory for command files and processes them.

        Returns:
            list[tuple[Callable, Any]]: A list of (handler, payload) pairs for
                commands that were successfully identified and parsed from the
                filesystem.

        Notes:
            Decision: Priority Processing via Filesystem Time.
            Command files are sorted by modification time (`st_mtime`),
            ensuring that older commands (e.g., a STOP signal sent minutes ago)
            are processed before newer ones in the same polling tick.

            Decision: Atomic Command Execution.
            Command files are unlinked (deleted) immediately after being read into
            memory. This ensures that each command is processed exactly once and
            prevents accidental re-triggering if the orchestrator crashes mid-execution
            and restarts.
        """
        signal_dir = self.exec_ctx.signal_path
        if not signal_dir.exists():
            return []

        commands = []

        for cmd_file in sorted(
            signal_dir.glob("*.cmd"), key=lambda p: p.stat().st_mtime
        ):
            cmd_name = cmd_file.stem.upper()
            base_name = self._get_command_type(cmd_name)

            handler = self._handlers.get(base_name)
            if not handler:
                LOG.warning(f"Unknown command: {cmd_name}")
                cmd_file.unlink()
                continue

            payload = self._load_payload(cmd_file)
            commands.append((handler, payload))
            cmd_file.unlink()

        return commands

    def _get_command_type(self, cmd_name: str) -> str:
        """Extracts the base command type from a full command filename.

        Args:
            cmd_name: The full name of the command file (e.g., 'ADD_12345.cmd').

        Returns:
            str: The base command type (e.g., 'ADD', 'STOP').

        Notes:
            Decision: Prefix-Based Matching.
            By iterating through `CommandPriority` enums, we can identify the
            base command type even if the file has a suffix (e.g.,
            'CANCEL_RUN_uuid.cmd'), allowing multiple instances of the same
            command type to exist in the queue simultaneously.

        Raises:
            ValueError: If the command name is empty or invalid.
        """
        for known in CommandPriority:
            if cmd_name.startswith(known):
                return known
        return cmd_name

    def _load_payload(self, cmd_file) -> Any:
        """Loads and decodes the payload from a command file.

        Args:
            cmd_file (Path): The path to the command file.

        Returns:
            Any: The decoded JSON object or raw string content.

        Notes:
            Decision: Flexible Payload Encoding.
            Supports both JSON-encoded dictionaries and plain text payloads. This allows
            for simple commands (like 'STOP') and complex commands (like 'RESUME' with
            a full configuration override) to use the same IPC channel.

        Raises:
            UnicodeDecodeError: If the file contains non-UTF-8 binary data.
        """
        if cmd_file.stat().st_size == 0:
            return None

        content = cmd_file.read_bytes()
        try:
            return msgspec.json.decode(content)
        except msgspec.DecodeError:
            return content.decode("utf-8").strip()

    def _cancel_run(self, payload: Any) -> None:
        """Handles the CANCEL_RUN command by updating the job status.

        Args:
            payload: The command payload, expected to contain a 'run_id'.

        Notes:
            Decision: State-Store Driven Cancellation.
            Instead of directly calling `ray.cancel()`, which can leave external handles
            dangling, the command updates the job status in the StateStore. The
            orchestrator's tick loop detects this status change and triggers a
            coordinated shutdown of the associated worker group.

        Raises:
            RuntimeError: If the state store is unreachable or the update fails.
        """
        run_id = payload.get("run_id") if isinstance(payload, dict) else payload
        if not run_id:
            LOG.error("CANCEL_RUN requires run_id")
            return

        LOG.warning(f"Cancelling run: {run_id}")
        self.state_store.store.update_run(run_id, {"JOB_STATUS": "CANCELLED"})
