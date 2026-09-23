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
    columns: dict[str, str] = field(default_factory=dict)
    null_if: list[str] = field(default_factory=list)
    batch_size: int | None = None
    flatten: int = -1

    # ---- file only attributes ----
    # compression: str | None = None
    glob: str | None = None
    # header: bool = True
    # skip_blank_lines: bool = True

    mode: str | None = None
    primary_keys: str | list[str] = field(default_factory=list)
    update_key: str | None = None

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

    # @staticmethod
    # def _resolve_file_resource(params: dict) -> tuple[str, str | None]:
    #     """Resolve resource for file sources.

    #     Case 1: file_path with wildcards
    #         Input: file_path = './mock_data/*.parquet', archive=None
    #         Output: folder='./mock_data', pattern='*.parquet'

    #     Case 2: direct file path
    #         Input: file_path = './mock_data/sample_orders.csv', archive=None
    #         Output: folder='./mock_data', pattern='sample_orders.csv'

    #     Case 3: folder only (no file_path, just context.resource)
    #         Input: context.resource = './mock_data', file_path=None
    #         Output: folder='./mock_data', pattern='' or None? Let's say pattern=None

    #     Returns:
    #         tuple[str, str | None]: (resource_path, file_pattern)
    #     """
    #     file_pattern = params.pop("file_pattern", {})
    #     archive = file_pattern.get("archive")
    #     glob_pattern = file_pattern.get("glob", "")

    #     if archive:
    #         # Archive file: archive is the resource
    #         return archive, glob_pattern if glob_pattern else None

    #     if not glob_pattern:
    #         return "", None

    #     # Check if it's a pattern with wildcards
    #     if "*" in glob_pattern or "?" in glob_pattern:
    #         # Pattern - split into directory and pattern
    #         path = Path(glob_pattern)
    #         resource = str(path.parent) if path.parent != path else "."
    #         return resource, path.name

    #     # Direct file
    #     return glob_pattern, None


def parse_extract_config(
    step: dict[str, Any], context: dict[str, Any]
) -> ExtractConfig:
    """Parse extract step parameters into ExtractConfig."""

    step_params = step["with"]

    # 1. Resolve object (check step_with -> step_params -> context)
    resource_obj = step_params["object"]
    if not resource_obj:
        raise KeyError(
            f"Missing required extraction field 'object' in step config: {step_params}"
        )

    resource_str = str(resource_obj)

    # 2. Resolve connection (check step_with -> step_params -> source dict -> context)
    source_cfg = context.get("source", {})
    source_conn = source_cfg.get("connection") if isinstance(source_cfg, dict) else None

    if not (connection := source_conn):
        raise ValueError(
            f"Missing required 'connection' for extract step '{step_params.get('id', resource_str)}'."
        )

    # 3. Build SQL Context
    sql_ctx = {
        "from": resource_str,
        "select": step_params.get("select"),
        "where": step_params.get("where"),
        "limit": step_params.get("limit"),
        "sql": step_params.get("sql"),
    }

    # 4. Construct payload
    raw_payload = {
        "resource": resource_str,
        "connection": connection,
        "glob": step_params.get("glob"),
        "num_workers": step_params.get("num_workers"),
        "sql_context": {"main": sql_ctx},
        "columns": step_params.get("columns"),
        "null_if": step_params.get("null_if"),
        "flatten": step_params.get("flatten"),
        "mode": context["mode"],
        "primary_keys": context.get("primary_keys", []),
        "update_key": context.get("update_key"),
    }

    payload = {k: v for k, v in raw_payload.items() if v is not None}
    return msgspec.convert(payload, type=ExtractConfig)
