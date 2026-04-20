from typing import Any, Literal

import msgspec
from apps.ingestion.src.core.models.stages.enums import StageName


class ExtractConfig(msgspec.Struct):
    """Configuration for data extraction/ingestion."""

    source_type: str  # e.g. "postgres", "s3", "local"
    source_identifier: str  # path, table, or API endpoint
    num_workers: int = 10  # parallelism level
    load_mode: Literal["snapshot", "delta"] = "snapshot"
    source_config: dict[str, Any] = msgspec.field(
        default_factory=dict
    )  # connection / credentials
    source_params: dict[str, Any] = msgspec.field(
        default_factory=dict
    )  # extraction-specific options (filters, etc.)
    schema_file: str | None = None
    schema_items: list[dict[str, Any]] = msgspec.field(default_factory=list)


class TransformConfig(msgspec.Struct):
    """Configuration for data transformation."""

    transform_type: str  # e.g. "default", "bitmask", "custom"
    source_dir: str | None = None  # directory to be used for regression testing
    transform_params: dict[str, Any] = msgspec.field(default_factory=dict)


class LoadConfig(msgspec.Struct):
    """Configuration for data loading/sinking."""

    sink_type: str  # e.g. "clickhouse", "snowflake"
    sink_identifier: str  # target table name or path
    partition_col: str
    partition_value: str
    sink_config: dict[str, Any] = msgspec.field(default_factory=dict)
    load_params: dict[str, Any] = msgspec.field(default_factory=dict)


class ArchiveConfig(msgspec.Struct, omit_defaults=True):
    """Configuration for data governance and archival."""

    enabled: bool
    retention_days: int | None
    base_path: str | None
    type: str | None
    config: dict[str, Any] | None = msgspec.field(default_factory=dict)


class TaskContext(msgspec.Struct):
    # Sub-Configurations (Must come first as they don't have defaults)
    extract: ExtractConfig
    transform: TransformConfig
    load: LoadConfig
    archive: ArchiveConfig

    # Core identifiers
    job_id: str
    dataset_id: str
    partition_date: str

    # Paths
    output_path: str

    # Runtime Details

    from_stage: str = StageName.first().label
    to_stage: str = StageName.last().label

    # Logic-wide Metadata
    audit_cols: list[str] = msgspec.field(
        default_factory=lambda: ["_partition", "_run_id", "_source"]
    )
    validation_cmd: str = "validation-app"
    expires_at: float | None = None
    custom_overrides: dict[str, Any] = msgspec.field(default_factory=dict)
    extras: dict[str, Any] = msgspec.field(default_factory=dict)
