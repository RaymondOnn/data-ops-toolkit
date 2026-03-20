import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.services.base import Service
from src.services.factory import ServiceFactory
from src.services.registry import protect_service

from libs.clients.base import ClientCantConnect
from libs.file import FileSystemClient, FileSystemSkills
from libs.resilience.circuit_breaker import CircuitBreaker

if TYPE_CHECKING:
    from libs.auth.models import Secret

breaker = CircuitBreaker(
    failure_threshold=3,
    recovery_timeout=300,
    expected_exceptions=(ClientCantConnect, ConnectionError, TimeoutError),
)


class StorageService(Service):
    """
    Registry-aware Service that manages any Filesystem.
    """

    def __init__(
        self,
        name: str,
        url: str,
        capabilities: list[FileSystemSkills],
        storage_options: dict[str, Any],
        **config: Any,
    ) -> None:

        super().__init__(name, **config)
        self.url = url
        self.capabilities = capabilities
        self.opts = storage_options
        self.client = self._init_client(**config)

    def _init_client(self, **config: Any) -> FileSystemClient:
        from libs.file.base import create_fs_client

        # 1. Resolve Secret (Password/Keys)
        # Assuming 'password' is the Secret object containing S3/Azure keys
        secret: Secret = config["password"]
        config["password"] = secret.resolve(sanitize=True) if secret else {}

        # 2. Merge Credentials with Storage Options
        # Patch: Create a new dict instead of using .update() which returns None
        merged_opts = {**self.opts, **config}

        return create_fs_client(
            url=self.url, capabilities=self.capabilities, storage_options=merged_opts
        )

    @protect_service(breaker)
    def get_work_units(self, target: str, num_partitions: int) -> list[dict[str, Any]]:
        """
        Uses the internal client to split 50M rows.
        Works across S3, Azure, GCS, or Local.
        """
        if not getattr(self.client, "fs", None):
            raise RuntimeError("Filesystem client is not initialized")

        path = self.client.resolve_path(target)
        files = self.client.fs.glob(f"{path}/**/*")
        return [{"files": files[i::num_partitions]} for i in range(num_partitions)]

    @protect_service(breaker)
    def stage_data(self, source_dir: Path, target_table: str) -> str:
        """
        Phase 1: Organize Parquet files into a staging directory.
        Returns the path to the staged folder.
        """
        # Create a unique staging path: e.g., tmp/staging/orders_1710123456/
        staging_path = f"tmp/staging/{target_table}_{int(time.time())}"

        # We use the client's 'copy_dir' which should be a server-side
        # operation (S3-to-S3) to avoid pulling 50M rows into our 2GB RAM.
        self.client.copy_dir(source_dir, staging_path)

        return staging_path

    @protect_service(breaker)
    def promote_data(
        self,
        staging_table: str,
        target_table: str,
        partition_col: str,
        partition_val: str,
    ) -> None:
        """
        Phase 2: Idempotent swap for File Systems.
        Equivalent to 'REPLACE PARTITION'.
        """
        # Target path: e.g., data/orders/dt=2026-03-10/
        final_path = f"{target_table}/{partition_col}={partition_val}"

        # 1. Idempotency: Remove existing data for this partition
        if self.client.exists(final_path):
            self.client.delete_dir(final_path)

        # 2. Atomic Move: Move the staged folder to the production path
        # On S3, this is a metadata-only rename or a fast copy/delete
        self.client.move_dir(staging_table, final_path)


# --- Role 1: Reading Flat Files (Landing Zone) ---
@ServiceFactory.register("flat_file")
class FlatFileService(StorageService):
    """Specifically for reading source data from landing zones."""

    def __init__(self, name: str, **config: Any) -> None:
        super().__init__(
            name=name,
            url=config.get("url", "file:///tmp/landing"),
            capabilities=[FileSystemSkills.FILE],
            storage_options=config.get("storage_options", {}),
            **config,
        )


# --- Role 2: Standard Archival (Moving artifacts) ---
@ServiceFactory.register("standard_archive")
class StandardArchiveService(StorageService):
    """Standard archival for job artifacts and logs."""

    def __init__(self, name: str, **config: Any) -> None:
        super().__init__(
            name=name,
            url=config.get("url", "s3://archive-bucket"),
            capabilities=[FileSystemSkills.ARCHIVE],
            storage_options=config.get("storage_options", {}),
            **config,
        )


# --- Role 3: CAS Archival (Immutable/Hashed storage) ---
@ServiceFactory.register("cas_archive")
class CASArchive(StorageService):
    """Content Addressable Storage for immutable records."""

    def __init__(self, name: str, account_id: str, **config: Any) -> None:
        # Vaults often use specific storage classes (e.g., Glacier or WORM)
        super().__init__(
            name=name,
            url=config.get("url", "s3://cas-vault"),
            capabilities=[FileSystemSkills.CAS],
            storage_options=config.get(
                "storage_options", {"s3_storage_class": "GLACIER"}
            ),
            account_id=account_id,
            **config,
        )
