from typing import Any

import msgspec
from msgspec import field

from src.core.stages.contracts.config import BaseStageConfig

from .enums import CaseStyle, CaseType, WriteStrategy


class WriteConfig(BaseStageConfig, tag="write"):
    """Configuration for data loading."""

    target: str  # Target table or path
    data_dir_ref: str  # Folder containing data to be loaded
    expected_count_ref: str  # To be resolved right before execution

    connection: dict[str, Any] = field(default_factory=dict)
    add_new_columns: bool = False  # alter table to add new cols if true
    create_table_ddl: str | None = None
    write_strategy: WriteStrategy = WriteStrategy.STAGING
    working_table: str | None = None
    meta_columns: dict[str, str | bool] = field(default_factory=dict)
    column_case_type: CaseType = CaseType.UPPER
    column_case_style: CaseStyle = CaseStyle.SNAKE
    column_types: dict[str, Any] = field(default_factory=dict)  # schema overrides
    soft_delete_missing: bool = False
    update_key: str | None = None

    def __post_init__(self):
        self._validate_column_format_options()

    def _validate_column_format_options(self):
        if self.column_case_style == CaseStyle.CAMEL and self.column_case_type not in (
            CaseType.CAPS,
            CaseType.NONE,
        ):
            raise ValueError(
                "For column_case_style='camel', the available options for "
                "column_case_type are ('none', 'caps')"
            )


def parse_write_config(step: dict[str, Any], context: dict[str, Any]) -> WriteConfig:
    """Parse write stage configuration cleanly using step_params and context."""

    step_params = step["with"]

    # 1. Target resolution: check step_with -> step_params -> context
    target = step_params.get("target")
    if not target:
        raise KeyError(
            f"Missing required field 'target' (or 'destination') for write step: {step_params}"
        )

    # 2. Connection resolution
    context_target = context.get("target", {})
    context_target_conn = (
        context_target.get("connection") if isinstance(context_target, dict) else None
    )

    connection = context_target_conn
    if not connection:
        raise ValueError(
            f"Missing required 'connection' for write step '{step.get('id', target)}'."
        )

    # 3. Build payload
    raw_payload: dict[str, Any] = {
        "target": str(target),
        "connection": connection,
        "data_dir_ref": str(step_params["object"]),
        "expected_count_ref": str(step_params["expected_count"]),
        "soft_delete_missing": context.get("soft_delete_missing"),
        "meta_columns": context.get("meta_columns"),
        "update_key": context.get("update_key"),
    }

    # 4. Optional fields mapping
    for field_name in (
        "add_new_columns",
        "create_table_ddl",
        "write_strategy",
        "working_table",
        "column_case_type",
        "column_case_style",
        "column_types",
    ):
        val = step_params.get(field_name)
        if val is not None:
            if field_name in (
                "write_strategy",
                "column_case_type",
                "column_case_style",
            ) and isinstance(val, str):
                raw_payload[field_name] = val.casefold()
            else:
                raw_payload[field_name] = val

    # 5. Strip all None values so msgspec field defaults apply cleanly
    payload = {k: v for k, v in raw_payload.items() if v is not None}
    return msgspec.convert(payload, type=WriteConfig)
