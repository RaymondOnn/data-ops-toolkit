from collections.abc import Callable
from pathlib import Path
from typing import Any

import msgspec
from libs.database.sql import SQLContext
from msgspec import field

from src.core.stages.contracts.config import BaseStageConfig


class ExtractConfig(BaseStageConfig, tag="extract"):
    """Configuration for data extraction."""

    resource: str  # Table name, path, or endpoint
    num_workers: int = 10
    # load_mode: Literal["append", "delta"] = "append"
    connection: dict[str, Any] = field(default_factory=dict)

    sql_context: SQLContext = field(default_factory=SQLContext)
    # select: list[str] = field(default_factory=list)
    # where: str | None = None
    # limit: int | None = None
    # sql: str | None = None

    columns: dict[str, str] = field(default_factory=dict)
    null_if: list[str] = field(default_factory=list)
    batch_size: int | None = None
    flatten: int = -1

    # ---- file only attributes ----
    # compression: str | None = None
    glob: str | None = None
    # header: bool = True
    # skip_blank_lines: bool = True

    @property
    def select(self) -> list[str]:
        return (self.sql_context.main.select if self.sql_context.main else None) or []

    @property
    def where(self) -> Any | None:
        return self.sql_context.main.where if self.sql_context.main else None

    @property
    def limit(self) -> int | None:
        return self.sql_context.main.limit if self.sql_context.main else None

    @property
    def sql(self) -> str | None:
        if self.sql_context.sql:
            return self.sql_context.sql
        return self.sql_context.main.sql if self.sql_context.main else None

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


def parse_extract_config(
    step: dict[str, Any], get_val: Callable[[str, Any], Any]
) -> ExtractConfig:
    """Parse extract step into ExtractConfig with hierarchical fallbacks."""
    # 1. Enforce top-level dictionary structure strictly
    if "resource" not in step or not isinstance(step["resource"], dict):
        raise ValueError(f"Step configuration missing required dict 'resource': {step}")

    resource = step["resource"]

    # 2. Extract strictly required keys or fail fast with clear messages
    if "object" not in resource:
        raise KeyError(
            f"Missing required field 'object' inside step['resource']: {resource}"
        )
    if "connection" not in resource:
        raise KeyError(
            f"Missing required field 'connection' inside step['resource']: {resource}"
        )

    resource_obj = str(resource["object"])

    # Directly build main query payload for SQLContext
    sql_ctx = {
        "from": resource_obj,
        "select": step.get("select", get_val("select", None)),
        "where": step.get("where", get_val("where", None)),
        # "group_by": step.get("group_by", get_val("group_by", None)),
        # "having": step.get("having", get_val("having", None)),
        # "qualify": step.get("qualify", get_val("qualify", None)),
        # "order_by": step.get("order_by", get_val("order_by", None)),
        "limit": step.get("limit", get_val("limit", None)),
        "sql": step.get("sql", get_val("sql", None)),
    }

    # 3. Construct payload using explicit values + fallbacks
    payload = {
        "resource": resource_obj,
        "connection": resource["connection"],
        "glob": resource.get("glob", get_val("glob", None)),
        "num_workers": step.get("num_workers", get_val("num_workers", 10)),
        "sql_context": {
            "main": sql_ctx,
        },
        # "select": step.get("select", get_val("select", [])),
        # "where": step.get("where", get_val("where", None)),
        # "limit": step.get("limit", get_val("limit", None)),
        # "sql": step.get("sql", get_val("sql", None)),
        "columns": step.get("columns", get_val("columns", {})),
        "null_if": step.get("null_if", get_val("null_if", [])),
        "flatten": step.get("flatten", get_val("flatten", -1)),
    }

    # msgspec validates types and required fields automatically
    return msgspec.convert(payload, type=ExtractConfig)
