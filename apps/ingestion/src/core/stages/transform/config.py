from enum import StrEnum
from typing import Any

import msgspec
from libs.database.sql import Join, SQLContext
from msgspec import Struct, field

from src.core.stages.contracts.config import BaseStageConfig


class TransformType(StrEnum):
    """Enumeration of transformation types."""

    SQL = "sql"
    PYTHON = "python"


class TransformStep(Struct, kw_only=True):
    """Configuration for an individual transformation sub-step."""

    id: str | None = None
    type: TransformType = TransformType.SQL

    # Maps internal alias -> step_id output (e.g., {"raw_extract": "extract_orders_data"})
    sources: dict[str, str] = field(default_factory=dict)

    # --- SQL Transform attributes ---
    # cte: list[SqlCTEConfig] = field(default_factory=list)
    sql_context: SQLContext = field(default_factory=SQLContext)

    # --- Custom Python Transform attributes ---
    module_path: str | None = None  # e.g., "my_project.processors.ml.detect_fraud"
    params: dict[str, Any] = field(default_factory=dict)


class TransformConfig(BaseStageConfig, tag="transform", kw_only=True):
    """Top-level configuration for the Transform Stage."""

    steps: list[TransformStep] = field(default_factory=list)
    inputs: dict[str, str] = field(default_factory=dict)


def _normalize_cte_dict(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalizes YAML syntax nuances (e.g. 'from' -> 'from_table', 'join' -> 'joins', dict/string to list) for msgspec."""
    cte = raw.copy()

    # 1. Map YAML 'from' or 'from_table' to SelectQueryContext 'from_table'
    if "from" in cte and "from_table" not in cte:
        cte["from_table"] = cte.pop("from")

    # 2. Normalize 'join' or 'joins' (dict or list) into a list of join dicts
    raw_join = cte.pop("join", None) or cte.get("joins")
    if raw_join:
        joins_list = [raw_join] if isinstance(raw_join, dict) else raw_join
        cte["joins"] = [
            {
                "to_table": j.get("to_table") or j.get("table"),
                "on": j["on"],
                "type": j.get("type", Join.INNER),
            }
            for j in joins_list
        ]

    # 3. Normalize single string list fields (select, group_by, order_by)
    for list_field in ("select", "group_by", "order_by"):
        if isinstance(cte.get(list_field), str):
            cte[list_field] = [cte[list_field]]

    return cte


def parse_transform_config(
    step: dict[str, Any], context: dict[str, Any]
) -> TransformConfig:
    """Parse transform stage configuration."""

    step_params = step["with"]
    stage_inputs = step.get("inputs")
    raw_steps: list[dict[str, Any]] = step_params.get("steps")

    prepared_steps = []
    for raw in raw_steps:
        raw_type = str(raw["type"]).casefold()
        transform_type = "python" if raw_type == "custom" else raw_type

        match transform_type:
            case "sql":
                prepared_ctes = []
                for raw_cte in raw.get("cte", []):
                    if "name" not in raw_cte:
                        raise KeyError(
                            f"Missing required key 'name' in transform CTE: {raw_cte}"
                        )
                    prepared_ctes.append(_normalize_cte_dict(raw_cte))

                sql_context_payload = {
                    "ctes": prepared_ctes,
                }

                prepared_steps.append(
                    {
                        "id": raw.get("id"),
                        "type": transform_type,
                        "sources": raw.get("sources", {}),
                        "sql_context": sql_context_payload,
                    }
                )
            case "python":
                prepared_steps.append(
                    {
                        "id": raw.get("id"),
                        "type": transform_type,
                        "module_path": raw.get("module_path"),
                        "params": raw.get("params", {}),
                    }
                )
            case _:
                raise ValueError(f"Unsupported transform type: {transform_type}")

    raw_payload = {
        "steps": prepared_steps,
        "inputs": stage_inputs,
    }

    # 5. Strip all None values so msgspec field defaults apply cleanly
    payload = {k: v for k, v in raw_payload.items() if v is not None}
    return msgspec.convert(payload, type=TransformConfig)
