import shutil
from collections.abc import Callable

import structlog
from apps.ingestion.src.core.contexts import ExecutionContext
from apps.ingestion.src.core.orchestrator.engine import IngestionEngine
from apps.ingestion.src.core.orchestrator.state import StateStore
from apps.ingestion.src.utils.common import find_path

LOG = structlog.getLogger(__name__)


# TODO: Stray signals
class SignalProcessor:
    def __init__(
        self,
        state_store: StateStore,
        engine: IngestionEngine,
        exec_ctx: ExecutionContext,
    ) -> None:
        self.state_store = state_store
        self.engine = engine
        self.exec_ctx = exec_ctx
        self._command_registry: dict[str, Callable[[], None]] = {}

    def register_command(self, cmd_name: str, callback: Callable[[], None]) -> None:
        """Registers a method to be called when a .cmd file appears."""
        self._command_registry[cmd_name] = callback

    def _process_worker_signals(self, run_ids: set[str] | None = None) -> None:
        """
        Scans the flat signals directory for any {run_id}.signal files.

        The worker writes the manifest.json first, then "drops" the signal file.
        This prevents the Orchestrator from reading a manifest that the worker
        is still writing.
        """
        run_ids = run_ids or set()  # If None, we process all signals in the directory

        signal_dir = self.exec_ctx.signal_path
        if not signal_dir.exists():
            return

        # Define our signals and whether they require a deep manifest sync
        # .sync = Light heartbeat | .done = Final deep audit
        signals = {"*.sync": False, "*.done": True, "*.fail": True}

        # iterdir() returns a generator, which is memory efficient
        # This glob automatically ignores files starting with "."
        for pattern, is_deep_sync in signals.items():
            for signal in signal_dir.glob(pattern):
                identifier = None
                try:
                    # 1. Parse metadata from filename
                    # Format: {job_id}:{dataset_id}:{run_date}:{run_id}
                    (job_id, dataset_id, run_date, run_id_from_file) = (
                        self.exec_ctx.parse_identifier(signal.stem)
                    )

                    # If we are filtering (Dumb Mode), skip signals not
                    # belonging to our triggered run(s)
                    if run_ids and run_id_from_file not in run_ids:
                        continue

                    identifier = self.exec_ctx.get_task_identifier(
                        job_id, dataset_id, run_date
                    )
                    record = self.state_store.active_records.get(identifier)

                    # If not in cache, leave it for the next iteration.
                    # This handles the gap between worker start and DB flush.
                    if not record:
                        LOG.info(
                            "Creating task record",
                            job_id=job_id,
                            dataset_id=dataset_id,
                            run_date=run_date,
                        )

                        self.state_store.create_record(
                            job_id=job_id,
                            dataset_id=dataset_id,
                            run_date=run_date,
                        )

                    # 2. Resolve the path dynamically
                    # The folder might have been moved to FAILED/ or HOLD/ by the worker
                    # just before/after dropping the signal.
                    # We search the whole workspace.
                    task_dir = find_path(self.exec_ctx.workspace_dir, run_id_from_file)

                    if not task_dir or not task_dir.exists():
                        LOG.warning(
                            "Signal received but task directory not found",
                            run_id=run_id_from_file,
                        )
                        continue

                    # 3. Sync manifest -> DB
                    self.state_store.sync_from_folder(task_dir, deep_sync=is_deep_sync)

                    # 4. Final Metadata Cleanup (Zero-Footprint)
                    # If this was a .done signal, the Orchestrator performs the final cleanup
                    # now that the manifest has been successfully synced to the DB.
                    if ".done" in signal.name:
                        LOG.info("Final sync complete.", run_id=run_id_from_file)
                        shutil.rmtree(task_dir, ignore_errors=True)

                    # 4. Remove the signal
                    signal.unlink(missing_ok=True)
                    LOG.debug("Signal processed", path=signal.name)

                except (OSError, ValueError) as e:
                    LOG.error(
                        "Failed to process signal", signal=signal.name, error=str(e)
                    )
                except Exception:
                    LOG.exception(
                        "Unexpected error processing signal", signal=signal.name
                    )

    def _check_for_manual_commands(self) -> None:
        """
        Checks for 'Command Files' dropped by CLI users/scripts.
        This acts as our inter-process communication (IPC).
        """
        cmd_dir = self.exec_ctx.signal_path

        for cmd_file, callback in self._command_registry.items():
            cmd_path = cmd_dir / cmd_file
            if cmd_path.exists():
                LOG.info("Manual command received", command=cmd_file)
                try:
                    # 1. Execute the command
                    callback()
                    # 2. 'Eat' the command file so it doesn't run again next tick
                    cmd_path.unlink()
                except OSError as e:
                    LOG.error(
                        "Failed to cleanup manual command file", cmd=cmd_file, error=e
                    )
                except Exception:
                    LOG.exception(
                        "Unexpected error executing manual command", cmd=cmd_file
                    )
