from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import msgspec
from apps.ingestion.src.core.models.stages.enums import StageName
from apps.ingestion.src.extras.flags import FeatureFlags
from apps.ingestion.src.utils.constants import CONFIG_FILENAME

if TYPE_CHECKING:
    from apps.ingestion.src.core.orchestrator.enums import JobRecord


class ExtractConfig(msgspec.Struct):
    """Configuration for data extraction/ingestion."""

    source_type: str  # e.g. "postgres", "s3", "local"
    source_identifier: str | None # path, table, or API endpoint
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
    # Feature Flags: The 'One Spot' to manage toggles
    feature_flags: FeatureFlags = msgspec.field(default_factory=FeatureFlags)

    @classmethod
    def create_placeholder(cls, run: "JobRecord") -> "TaskContext":
        """Creates a synthetic context for audit logging of untriggered/expired jobs."""
        return cls(
            job_id=run.JOB_ID,
            dataset_id=run.DATASET_ID,
            partition_date=str(run.PARTITION_DATE),
            output_path="",
            extract=ExtractConfig(source_type="N/A", source_identifier="N/A"),
            transform=TransformConfig(transform_type="N/A"),
            load=LoadConfig(
                sink_type="N/A",
                sink_identifier="N/A",
                partition_col="N/A",
                partition_value="N/A",
            ),
            archive=ArchiveConfig(
                enabled=False, retention_days=None, base_path=None, type=None
            ),
            feature_flags=FeatureFlags(),
        )


def load_task_context(folder: Path) -> TaskContext:
    """
    Standardized utility to load a TaskContext from a physical workspace folder.
    """
    config_path = folder / CONFIG_FILENAME
    if not config_path.exists():
        raise FileNotFoundError(f"Missing {CONFIG_FILENAME} in {folder}")

    with config_path.open(mode="rb") as f:
        return msgspec.json.decode(f.read(), type=TaskContext)
