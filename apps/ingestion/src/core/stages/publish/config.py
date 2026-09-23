from typing import Any

import msgspec
from msgspec import field

from src.core.stages.contracts.config import BaseStageConfig
from src.core.stages.models import WriteMode

from .enums import DeltaMergeType


class PublishConfig(BaseStageConfig, tag="publish"):
    """Configuration for data loading."""

    target: str  # Target table or path
    mode: WriteMode
    staging_artifact_ref: str
    expected_count_ref: str
    primary_keys: str | list[str]

    connection: dict[str, Any] = field(default_factory=dict)
    update_key: str | None = None

    # delete_insert, update_insert, update
    delta_merge_type: DeltaMergeType = DeltaMergeType.DELETE_INSERT
    soft_delete_missing: bool = True  # Notimplemented
    soft_delete_column: str = "_is_deleted"  # parse from meta_columns


# soft_delete_missing: UPDATE target_table SET _sling_deleted = TRUE
# WHERE primary_key NOT IN (SELECT primary_key FROM temp_stage)
# AND _sling_deleted = FALSE;


def parse_publish_config(
    step: dict[str, Any], context: dict[str, Any]
) -> PublishConfig:
    """Parse publish stage config using step_params and dataset/job context."""

    step_params = step["with"]

    # 1. Target resolution
    target = step_params["target"]
    if not target:
        raise KeyError(
            f"Missing required field 'target' (or 'destination') for publish step: {step_params}"
        )

    target_name = target.get("name", target) if isinstance(target, dict) else target

    # 2. Connection resolution
    context_target = context.get("target", {})
    context_target_conn = (
        context_target.get("connection") if isinstance(context_target, dict) else None
    )

    connection = context_target_conn
    if not connection:
        raise ValueError(
            f"Missing required 'connection' for publish step '{step_params.get('id', target_name)}'."
        )

    # 3. Dynamic artifact & count references
    staging_location = step_params["staging_location"]
    if not staging_location:
        raise KeyError(
            f"Missing required field 'staging_location' for publish step: {step_params}"
        )

    expected_count_ref = step_params["expected_count"]

    # 4. Mode & Primary Keys
    mode = context.get("mode")
    if not mode:
        raise KeyError(
            f"Missing required 'mode' for publish step in dataset/job context: {step_params}"
        )

    primary_keys = context.get("primary_keys", [])

    # 5. Base payload construction
    payload: dict[str, Any] = {
        "target": str(target_name),
        "connection": connection,
        "mode": WriteMode(mode) if isinstance(mode, str) else mode,
        "staging_artifact_ref": str(staging_location),
        "expected_count_ref": str(expected_count_ref) if expected_count_ref else "",
        "primary_keys": primary_keys,
        "update_key": context.get("update_key"),
    }

    # 6. Optional overrides
    merge_type = step_params.get("delta_merge_type")
    if merge_type is not None:
        payload["delta_merge_type"] = (
            DeltaMergeType(merge_type.casefold())
            if isinstance(merge_type, str)
            else merge_type
        )

    soft_delete = context.get("soft_delete_missing")
    if soft_delete is not None:
        payload["soft_delete_missing"] = soft_delete

    return msgspec.convert(payload, type=PublishConfig)
