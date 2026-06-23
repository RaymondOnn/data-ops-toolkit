"""Task configuration and context models for pipeline execution."""

from pathlib import Path
from typing import Any, Literal, Self

import msgspec
from apps.ingestion.src.core.models.stages.enums import Stage
from apps.ingestion.src.extras.flags import FeatureFlags
from apps.ingestion.src.utils.constants import CONFIG_FILENAME
from loguru import logger

LOG = logger


# =============================================================================
# Schema Definition
# =============================================================================


class ColumnMapping(msgspec.Struct):
    """Column mapping and transformation rule."""

    target_col: str
    target_type: str = "string"
    target_length: Any = None
    target_scale: Any = None
    source_col: str | None = None
    source_type: str | None = None
    source_length: Any = None
    source_scale: Any = None
    masking: str | None = None
    internal: bool = False
    is_primary_key: bool = False

    @classmethod
    def from_csv_row(cls, row: dict[str, str]) -> "ColumnMapping":
        """Create ColumnMapping from CSV row."""
        return cls(
            source_col=cls._none_if_empty(row.get("source_col")),
            target_col=row.get("target_col", ""),
            source_type=cls._none_if_empty(row.get("source_dtype")),
            target_type=row.get("target_dtype", ""),
            source_length=cls._to_int(row.get("source_length")),
            source_scale=cls._to_int(row.get("source_scale")),
            target_length=cls._to_int(row.get("target_length")),
            target_scale=cls._to_int(row.get("target_scale")),
            masking=cls._none_if_empty(row.get("masking")),
            internal=cls._to_bool(row.get("internal_flag", "false")),
            is_primary_key=cls._to_bool(row.get("primary_key", "false")),
        )

    @staticmethod
    def _none_if_empty(value: str | None) -> str | None:
        return None if not value or value.lower() == "none" else value

    @staticmethod
    def _to_int(value: str | None) -> int | None:
        if not value or value.lower() == "none":
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _to_bool(value: str) -> bool:
        return value.lower().strip() in ("true", "1", "t", "yes", "y")

    def to_dict(self) -> dict[str, Any]:
        """Convert config to dictionary (excludes None values)."""
        return msgspec.to_builtins(self)


# =============================================================================
# Stage Configurations
# =============================================================================


class BaseConfig(msgspec.Struct):
    """Base class for all config classes."""

    @classmethod
    def from_params(cls, *args: Any, **kwargs: Any) -> Any:
        """Abstract factory method for creating configuration from raw parameters."""
        raise NotImplementedError(f"from_params must be implemented by {cls.__name__}")

    def to_dict(self) -> dict[str, Any]:
        """Convert config to dictionary (excludes None values)."""
        return msgspec.to_builtins(self)


class ExtractConfig(BaseConfig):
    """Configuration for data extraction."""

    type: str  # "postgres", "s3", "local", etc.
    resource: str  # Table name, path, or endpoint
    num_workers: int = 10
    load_mode: Literal["snapshot", "delta"] = "snapshot"
    service: dict[str, Any] = msgspec.field(default_factory=dict)
    params: dict[str, Any] = msgspec.field(default_factory=dict)
    schema: list[ColumnMapping] = msgspec.field(default_factory=list)

    def __post_init__(self) -> None:
        """Validate schema has primary key."""
        if not self.resource or self.resource == "N/A" or not self.schema:
            return
        if not any(col.is_primary_key for col in self.schema):
            raise ValueError(
                f"No primary key defined for dataset '{self.resource}'. "
                "At least one column must be marked as primary_key."
            )

    @classmethod
    def from_params(
        cls,
        source_params: dict[str, Any],
        service: dict[str, Any],
        schema: list[ColumnMapping],
        **overrides,
    ) -> Self:
        """Create ExtractConfig from raw source parameters."""
        params = source_params.copy()
        source_type = params.pop("type")

        # Resolve resource based on source type
        if source_type == "database":
            resource = params.pop("table_name", "")
        elif source_type == "api":
            resource = params.pop("endpoint", "")
        elif source_type in ("flat_file", "file"):
            resource, file_pattern = cls._resolve_file_resource(params)
            if file_pattern is not None:
                params["file_pattern"] = file_pattern
        else:
            resource = ""
            LOG.warning(f"Unknown source type: {source_type}")

        LOG.debug(
            f"Creating ExtractConfig: type={source_type}, resource={resource}, params={params}"
        )
        return cls(
            type=service["type"].casefold(),
            resource=resource,
            num_workers=overrides["num_workers"],
            load_mode=overrides["load_mode"],
            service=service,
            params=params,
            schema=schema,
        )

    @staticmethod
    def _resolve_file_resource(params: dict) -> tuple[str, str | None]:
        """Resolve resource for file sources.

        Case 1: file_path with wildcards
            Input: file_path = './mock_data/*.parquet', archive=None
            Output: folder='./mock_data', pattern='*.parquet'

        Case 2: direct file path
            Input: file_path = './mock_data/sample_orders.csv', archive=None
            Output: folder='./mock_data', pattern='sample_orders.csv'

        Case 3: folder only (no file_path, just context.resource)
            Input: context.resource = './mock_data', file_path=None
            Output: folder='./mock_data', pattern='' or None? Let's say pattern=None

        Returns:
            tuple[str, str | None]: (resource_path, file_pattern)
        """
        file_pattern = params.pop("file_pattern", {})
        archive = file_pattern.get("archive")
        glob_pattern = file_pattern.get("glob", "")

        if archive:
            # Archive file: archive is the resource
            return archive, glob_pattern if glob_pattern else None

        if not glob_pattern:
            return "", None

        # Check if it's a pattern with wildcards
        if "*" in glob_pattern or "?" in glob_pattern:
            # Pattern - split into directory and pattern
            path = Path(glob_pattern)
            resource = str(path.parent) if path.parent != path else "."
            return resource, path.name

        # Direct file
        return glob_pattern, None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = super().to_dict()
        # Convert schema items to dicts if needed
        if self.schema:
            result["schema"] = [
                col.to_dict() if hasattr(col, "to_dict") else col for col in self.schema
            ]
        return result


class TransformConfig(BaseConfig):
    """Configuration for data transformation."""

    type: str = "default"  # "default", "bitmask", "custom"
    params: dict[str, Any] = msgspec.field(default_factory=dict)
    source_dir: str | None = None  # For regression testing

    @classmethod
    def from_params(
        cls,
        transform_type: str,
        transform_params: dict[str, Any],
        **overrides,
    ) -> Self:
        """Create TransformConfig from raw parameters."""
        LOG.debug(
            f"Creating TransformConfig: type={transform_type}, params={transform_params}, overrides={overrides}"
        )
        return cls(
            type=transform_type,
            params=transform_params,
            source_dir=overrides.get("source_dir"),
        )


class LoadConfig(BaseConfig):
    """Configuration for data loading."""

    type: str  # "clickhouse", "snowflake", etc.
    destination: str  # Target table or path
    partition_by: str
    partition_value: str
    service: dict[str, Any] = msgspec.field(default_factory=dict)
    params: dict[str, Any] = msgspec.field(default_factory=dict)

    @classmethod
    def from_params(
        cls,
        sink_params: dict[str, Any],
        service: dict[str, Any],
        **overrides,
    ) -> Self:
        """Create LoadConfig from raw parameters."""
        params = sink_params.copy()

        # Resolve destination from params
        partition_by = overrides.get("partition_by") or params.pop("partition_by", "")
        partition_value = overrides.get("partition_value") or params.pop(
            "partition_value", ""
        )

        return cls(
            type=service["type"].casefold(),
            destination=params["destination"],
            partition_by=partition_by,
            partition_value=partition_value,
            service=service,
            params=params,
        )


class ArchiveConfig(BaseConfig, omit_defaults=True):
    """Configuration for data archival."""

    enabled: bool
    retention_days: int | None = None
    base_path: str | None = None
    type: str | None = None
    service: dict[str, Any] = msgspec.field(default_factory=dict)

    @classmethod
    def from_params(
        cls,
        archive_params: dict[str, Any],
        service: dict[str, Any],
        archive_enabled: bool = False,
        **overrides,
    ) -> Self:
        """Create ArchiveConfig from raw parameters."""
        params = archive_params.copy()
        archive_type = overrides.get("type") or params.pop("type", None)

        if not archive_enabled:
            return cls(
                enabled=archive_enabled,
                retention_days=None,
                base_path=None,
                type=archive_type,
                service=service,
            )

        return cls(
            enabled=archive_enabled,
            retention_days=overrides.get("retention_days"),
            base_path=service["url"],
            type=service["type"].casefold(),
            service=service,
        )


# =============================================================================
# Pipeline Context
# =============================================================================


class TaskContext(msgspec.Struct, kw_only=True):
    """Complete pipeline configuration for a task run."""

    # Stage configurations
    extract: ExtractConfig | None = None
    transform: TransformConfig | None = None
    load: LoadConfig | None = None
    archive: ArchiveConfig | None = None

    # Identity
    job_id: str
    dataset_id: str
    partition_date: str

    # Execution boundaries
    from_stage: str = Stage.first().value
    to_stage: str = Stage.last().value

    # Paths
    output_path: str = ""

    # Metadata
    audit_columns: list[str] = msgspec.field(
        default_factory=lambda: ["_partition", "_run_id", "_source"]
    )
    validation_command: str = "validation-app"
    expires_at: float | None = None
    overrides: dict[str, Any] = msgspec.field(default_factory=dict)
    extras: dict[str, Any] = msgspec.field(default_factory=dict)
    flags: FeatureFlags = msgspec.field(default_factory=FeatureFlags)


# =============================================================================
# Loading Utility
# =============================================================================


def load_context(folder: Path) -> TaskContext:
    """Load pipeline context from workspace folder."""
    config_path = folder / CONFIG_FILENAME
    if not config_path.exists():
        raise FileNotFoundError(f"Missing {CONFIG_FILENAME} in {folder}")

    with config_path.open("rb") as f:
        return msgspec.json.decode(f.read(), type=TaskContext)
