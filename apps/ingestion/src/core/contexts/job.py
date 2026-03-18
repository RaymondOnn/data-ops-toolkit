
from typing import Any, Literal
from enum import StrEnum

import structlog
import msgspec

from src.core.models.steps import JobSteps


LOG = structlog.getLogger(__name__)


class ExecutionMode(StrEnum):
    NORMAL = "normal"
    DEBUG = "debug"
    TEST = "test"
    DRYRUN = "dry_run"



class ExtractConfig(msgspec.Struct):
    """Configuration for data extraction/ingestion."""
    source_type: str               # e.g. "postgres", "s3", "local"
    source_identifier: str         # path, table, or API endpoint
    num_partitions: int = 10       # parallelism level
    load_mode: Literal["snapshot", "delta"] = "snapshot"
    source_config: dict[str, Any] = {} # connection / credentials
    source_params: dict[str, Any] = {} # extraction-specific options (filters, etc.)
    schema_file: str | None = None
    schema_items: list[dict[str, Any]] = []

class TransformConfig(msgspec.Struct):
    """Configuration for data transformation."""
    script: str | None = None
    params: dict[str, Any] = {}

class LoadConfig(msgspec.Struct):
    """Configuration for data loading/sinking."""
    sink_type: str          # e.g. "clickhouse", "snowflake"
    sink_identifier: str    # target table name or path
    sink_config: dict[str, Any] = {}
    load_params: dict[str, Any] = {}
    partition_col: str | None = None
    partition_value: str | None = None

class ArchiveConfig(msgspec.Struct):
    """Configuration for data governance and archival."""
    enabled: bool = True
    retention_days: int = 2555
    base_path: str = "/mnt/archive/ingestion"
    type: str = "s3"
    config: dict[str, Any] = {}

class JobContext(msgspec.Struct):
    # Sub-Configurations (Must come first as they don't have defaults)
    extract: ExtractConfig
    transform: TransformConfig
    load: LoadConfig
    archive: ArchiveConfig

    # Core identifiers
    job_id: str
    dataset_id: str
    run_date: str

    # Paths
    output_path: str

    # Runtime Details
    execution_mode: ExecutionMode
    from_step: str = JobSteps.first_step().label 
    to_step: str = JobSteps.last_step().label

    # Logic-wide Metadata
    audit_cols: list[str] = ["_ingested_at", "_partition_key", "_job_id", "_row_hash"]
    validation_cmd: str = "validation-app"
    expires_at: float | None = None
    custom_overrides: dict[str, Any] = {}
    extras: dict[str, Any] = {}
        
    def is_debug(self) -> bool:
        return self.execution_mode == ExecutionMode.DEBUG

    def is_test(self) -> bool:
        return self.execution_mode == ExecutionMode.TEST

    def is_normal(self) -> bool:
        return self.execution_mode == ExecutionMode.NORMAL

