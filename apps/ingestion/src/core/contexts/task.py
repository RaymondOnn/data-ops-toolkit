"""Task configuration and context models for pipeline execution."""

from pathlib import Path
from typing import Any, Literal, Self

import msgspec
from apps.ingestion.src.core.models.stages.enums import Stage
from apps.ingestion.src.extras.flags import FeatureFlags
from apps.ingestion.src.extras.hooks.enums import StageHooks
from apps.ingestion.src.utils.constants import CONFIG_FILENAME
from loguru import logger

LOG = logger


# =============================================================================
# Schema Definition
# =============================================================================


# class ColumnMapping(msgspec.Struct):
#     """Column mapping and transformation rule."""

#     target_col: str
#     target_type: str = "string"
#     target_length: Any = None
#     target_scale: Any = None
#     source_col: str | None = None
#     source_type: str | None = None
#     source_length: Any = None
#     source_scale: Any = None
#     masking: str | None = None
#     internal: bool = False
#     is_primary_key: bool = False

#     @classmethod
#     def from_csv_row(cls, row: dict[str, str]) -> "ColumnMapping":
#         """Create ColumnMapping from CSV row."""
#         return cls(
#             source_col=cls._none_if_empty(row.get("source_col")),
#             target_col=row.get("target_col", ""),
#             source_type=cls._none_if_empty(row.get("source_dtype")),
#             target_type=row.get("target_dtype", ""),
#             source_length=cls._to_int(row.get("source_length")),
#             source_scale=cls._to_int(row.get("source_scale")),
#             target_length=cls._to_int(row.get("target_length")),
#             target_scale=cls._to_int(row.get("target_scale")),
#             masking=cls._none_if_empty(row.get("masking")),
#             internal=cls._to_bool(row.get("internal_flag", "false")),
#             is_primary_key=cls._to_bool(row.get("primary_key", "false")),
#         )

#     @staticmethod
#     def _none_if_empty(value: str | None) -> str | None:
#         return None if not value or value.lower() == "none" else value

#     @staticmethod
#     def _to_int(value: str | None) -> int | None:
#         if not value or value.lower() == "none":
#             return None
#         try:
#             return int(float(value))
#         except (ValueError, TypeError):
#             return None

#     @staticmethod
#     def _to_bool(value: str) -> bool:
#         return value.lower().strip() in ("true", "1", "t", "yes", "y")

#     def to_dict(self) -> dict[str, Any]:
#         """Convert config to dictionary (excludes None values)."""
#         return msgspec.to_builtins(self)


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


class ExtractConfig(msgspec.Struct):
    """Configuration for data extraction."""

    type: str  # "postgres", "s3", "local", etc.
    resource: str  # Table name, path, or endpoint
    num_workers: int = 10
    load_mode: Literal["append", "delta"] = "append"
    connection: dict[str, Any] = msgspec.field(default_factory=dict)
    select: list[str] = msgspec.field(default_factory=list)
    where: str | None = None
    limit: int | None = None
    sql: str | None = None
    columns: dict[str, str] = msgspec.field(default_factory=dict)
    null_if: list[str] = msgspec.field(default_factory=list)
    batch_size: int | None = None
    flatten: int = -1

    # ---- file only attributes ----
    compression: str | None = None
    glob: str | None = None
    header: bool = True
    skip_blank_lines: bool = True

    def __post_init__(self) -> None:
        """Validate schema has primary key."""
        if not self.resource or self.resource == "N/A":
            return

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


def parse_extract_config(config) -> ExtractConfig:
    """Create ExtractConfig from raw source parameters."""
    return ExtractConfig(
        type=config["connection"]["key"].casefold(),
        resource=config["object"],
        num_workers=config["num_workers"],
        load_mode=config["mode"],
        connection=config["connection"],
        select=config["select"],
    )


class TransformConfig(msgspec.Struct):
    """Configuration for data transformation."""

    type: str = "default"  # "default", "bitmask", "custom"
    params: dict[str, Any] = msgspec.field(default_factory=dict)
    source_dir: str | None = None  # For regression testing


def parse_transform_config(config) -> TransformConfig:
    """Create TransformConfig from raw parameters."""
    # LOG.debug(
    #     f"Creating TransformConfig: type={transform_type}, params={transform_params}, overrides={overrides}"
    # )
    return TransformConfig(
        type=config["transform_type"],
        params=config["transform_params"],
        source_dir=config.get("source_dir"),
    )


class LoadConfig(msgspec.Struct):
    """Configuration for data loading."""

    type: str  # "clickhouse", "snowflake", etc.
    destination: str  # Target table or path
    partition_on: str
    partition_value: str
    connection: dict[str, Any] = msgspec.field(default_factory=dict)
    params: dict[str, Any] = msgspec.field(default_factory=dict)


def parse_load_config(config) -> LoadConfig:
    """Create LoadConfig from raw parameters."""
    params = config["sink_params"].copy()

    # Resolve destination from params
    partition_on = config.get("partition_on") or params.pop("partition_on", "")
    partition_value = config.get("partition_value") or params.pop("partition_value", "")

    return LoadConfig(
        type=config["connection"]["key"].casefold(),
        destination=params["destination"],
        partition_on=partition_on,
        partition_value=partition_value,
        connection=config["connection"],
        params=params,
    )


class ArchiveConfig(msgspec.Struct, kw_only=True, omit_defaults=True):
    enabled: bool = False
    type: str = ""  # Serializer drops key if it is ""
    base_path: str = ""  # Serializer drops key if it is ""
    connection: dict[str, Any] = msgspec.field(default_factory=dict)
    retention_days: int = 2555


def parse_archive_config(config) -> ArchiveConfig:
    """Create ArchiveConfig from raw parameters."""

    if not config["archive_enabled"]:
        return ArchiveConfig(enabled=False)

    return ArchiveConfig(
        enabled=True,
        retention_days=config.get("retention_days", 2555),
        base_path=config["connection"]["url"],
        type=config["connection"]["key"].casefold(),
        connection=config["connection"],
    )


# =============================================================================
# Pipeline Context
# =============================================================================


class TaskContext(msgspec.Struct, kw_only=True):
    """Complete pipeline configuration for a task run."""

    # Stage configurations
    extract: ExtractConfig | None = None
    transform: TransformConfig | None = None
    write: LoadConfig | None = None
    archive: ArchiveConfig | None = None
    hooks: dict[str, StageHooks] = msgspec.field(default_factory=dict)

    # Identity
    job_id: str
    dataset_id: str
    partition_date: str

    # Execution boundaries
    from_stage: str = Stage.first().value
    to_stage: str = Stage.last().value
    mode: str | None = None  # append | incremental | truncate | CDC
    partition_on: list[str] = msgspec.field(default_factory=list)
    primary_keys: list[str] = msgspec.field(default_factory=list)

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

    @classmethod
    def from_params(cls, raw_config: dict[str, Any]) -> Self:
        return cls(
            # Stage configurations
            extract=parse_extract_config(raw_config.get("extract", {})),
            transform=parse_transform_config(raw_config.get("transform", {})),
            write=parse_load_config(raw_config.get("write", {})),
            archive=parse_archive_config(raw_config.get("archive", {})),
            # Identity
            job_id=raw_config["job_id"],
            dataset_id=raw_config["dataset_id"],
            partition_date=raw_config["partition_date"],
            # Execution boundaries
            from_stage=raw_config.get("from_stage", Stage.first().value),
            to_stage=raw_config.get("to_stage", Stage.last().value),
            mode=raw_config.get("mode"),
            partition_on=raw_config.get("partition_on", []),
            primary_keys=raw_config.get("primary_keys", []),
            # Paths
            # output_path = str(raw_config["output_path"]) or None,
            # Metadata
            audit_columns=raw_config.get(
                "audit_columns", ["_partition", "_run_id", "_source"]
            ),
            expires_at=raw_config.get("expires_at"),
            overrides=raw_config.get("overrides", {}),
            extras=raw_config.get("extras", {}),
            flags=raw_config.get("flags", FeatureFlags()),
            hooks=raw_config.get("hooks", {}),
        )

    @classmethod
    def from_path(
        cls, folder_path: Path | str | None = None, filepath: Path | str | None = None
    ):
        config_file = Path()
        if filepath and (path := Path(filepath)).is_file():
            config_file = path
        elif folder_path and (path := Path(folder_path)).is_dir():
            config_file = path / CONFIG_FILENAME

        if not config_file.exists():
            raise FileNotFoundError(f"Failed to load config file: {path}")

        return msgspec.json.decode(config_file.read_bytes(), type=TaskContext)

    def __post_init__(self):
        if self.extract and not self.primary_keys:
            LOG.error(f"{self.extract=} {self.primary_keys=}")
            raise ValueError(
                "No primary key defined for source dataset. "
                "At least one column must be marked as primary_key."
            )


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
