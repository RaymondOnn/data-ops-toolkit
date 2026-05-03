import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task import Task

LOG = logger


class CleanupPolicy(ABC):
    """
    Base class for the Policy Design Pattern.
    Encapsulates both the criteria evaluation and the resulting execution.
    """

    @abstractmethod
    def apply(self, task: "Task") -> None:
        pass

    def __and__(self, other: "CleanupPolicy") -> "CleanupPolicy":
        # Refined to flatten chains automatically
        policies = []
        for p in [self, other]:
            if isinstance(p, CompositePolicy):
                policies.extend(p.policies)
            else:
                policies.append(p)
        return CompositePolicy(*policies)


class CompositePolicy(CleanupPolicy):
    """
    Chains multiple policies together, executing them in sequence.
    """

    def __init__(self, *policies: CleanupPolicy):
        self.policies = policies

    def apply(self, task: "Task") -> None:
        for policy in self.policies:
            try:
                policy.apply(task)
            except Exception as e:
                LOG.error(
                    f"Cleanup step {policy.__class__.__name__} failed",
                    run_id=task.run_id,
                    error=str(e)
                )


class MetadataCleanupPolicy(CleanupPolicy):
    """Purges the temporary metadata workspace."""

    def apply(self, task: "Task") -> None:
        LOG.debug("MetadataCleanupPolicy: Purging workspace", run_id=task.run_id)
        # Assuming task.purge_metadata handles path existence internally.
        task.purge_metadata()


class VaultCleanupPolicy(CleanupPolicy):
    """Protects Data Vaults if in regression, test, or dry-run modes."""

    def apply(self, task: "Task") -> None:
        # Check for safety flags in the execution context.
        is_test = getattr(task.exec_ctx, "is_test", False)
        is_dry = getattr(task.exec_ctx, "is_dry_run", False)
        is_reg = task.context.custom_params.get("regression_mode", False)

        if any([is_test, is_dry, is_reg]):
            LOG.info(
                "VaultCleanupPolicy: Skipping purge (Preservation mode active).",
                run_id=task.run_id,
            )
            return

        LOG.debug("VaultCleanupPolicy: Purging physical data vaults", run_id=task.run_id)
        task.purge_data_vaults()


class ExternalSourceCleanupPolicy(CleanupPolicy):
    """
    Cleans up external source files/directories based on custom parameters.
    """

    def apply(self, task: "Task") -> None:
        is_test = getattr(task.exec_ctx, "is_test", False)
        params = task.context.custom_params
        
        # Only proceed if explicitly requested and not in test mode.
        if not params.get("purge_external_source") or is_test:
            return

        source_id = task.context.extract.source_identifier
        if not source_id:
            return

        path = Path(source_id)
        if not path.exists():
            return

        mode = params.get("source_cleanup_mode", "file")

        if mode == "file" and path.is_file():
            LOG.info(f"Cleanup: Unlinking source file {path}")
            path.unlink(missing_ok=True)

        elif mode == "directory":
            parent = path.parent if path.is_file() else path
            LOG.info(f"Cleanup: Removing source directory {parent}")
            shutil.rmtree(parent, ignore_errors=True)

        elif mode == "directory_if_empty":
            parent = path.parent if path.is_file() else path
            if path.is_file():
                path.unlink(missing_ok=True)

            if parent.is_dir() and not any(parent.iterdir()):
                LOG.info(f"Cleanup: Removing empty source directory {parent}")
                parent.rmdir()


class CleanupCoordinator:
    """
    Coordinates the cleanup process using a default or custom policy chain.
    """

    def apply(self, task: "Task", policy: CleanupPolicy | None = None) -> None:
        """Applies the cleanup chain to the provided task."""
        if policy is None:
            # Construct default chain using the flattened __and__ logic.
            policy = (
                MetadataCleanupPolicy()
                & VaultCleanupPolicy()
                & ExternalSourceCleanupPolicy()
            )
        
        policy.apply(task)
