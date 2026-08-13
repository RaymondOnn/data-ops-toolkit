from collections.abc import Callable
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
    source_dir: str | None = None

    def get_external_deps(self, current_step_id: str, context: Any) -> set[str]:
        """Gathers all external step_ids required by any sub-step in this stage."""
        external_deps = set()
        all_pipeline_step_ids = context.get_step_ids()

        for sub_step in self.steps:
            for source_step_id in sub_step.sources.values():
                # Only treat as external dependency if it comes from OUTSIDE this transform step
                if (
                    source_step_id != current_step_id
                    and source_step_id in all_pipeline_step_ids
                ):
                    external_deps.add(source_step_id)

        return external_deps


# def parse_single_transform(
#     raw_transform: dict[str, Any],
#     get_val: Callable[[str, Any], Any]
# ) -> TransformStep:
#     """Parse an individual transformation sub-step."""
#     transform_type = str(raw_transform.get("type") or get_val("type", "sql")).casefold()

#     ctes = []
#     if transform_type == "sql":
#         for raw_cte in raw_transform.get("cte", []):
#             join_cfg = None
#             if raw_join := raw_cte.get("join"):
#                 join_cfg = SqlJoinConfig(
#                     to_table=raw_join.get("to_table") or raw_join.get("table", ""),
#                     on=raw_join["on"],
#                     type=raw_join.get("type", Join.INNER),
#                 )

#             ctes.append(
#                 SqlCTEConfig(
#                     name=raw_cte["name"],
#                     select=raw_cte.get("select", []),
#                     from_table=raw_cte.get("from"),
#                     join=join_cfg,
#                 )
#             )

#     return TransformStep(
#         id=raw_transform.get("id"),
#         type=TransformType(transform_type),
#         sources=raw_transform.get("sources", {}),
#         cte=ctes,
#         module_path=raw_transform.get("module_path"),
#         params=raw_transform.get("params", {}),
#     )


def _normalize_cte_dict(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalizes YAML syntax nuances (e.g. dict-to-list, string-to-list) for msgspec."""
    cte = raw.copy()

    # 1. Normalize 'join' or 'joins' dict -> list & 'table' -> 'to_table'
    raw_join = cte.pop("join", None) or cte.get("joins")
    if raw_join:
        joins_list = [raw_join] if isinstance(raw_join, dict) else raw_join
        cte["joins"] = [
            {
                "to_table": j.get("to_table") or j.get("table"),
                "on": j["on"],
                "type": j.get("type", "inner"),
            }
            for j in joins_list
        ]

    # 2. Normalize single string list fields (select, group_by, order_by)
    for list_field in ("select", "group_by", "order_by"):
        if isinstance(cte.get(list_field), str):
            cte[list_field] = [cte[list_field]]

    return cte


def parse_transform_config(
    config: dict[str, Any], get_val: Callable[[str, Any], Any]
) -> TransformConfig:
    """Parse transform stage config strictly using msgspec.convert to fail fast on invalid schema."""

    raw_steps = config.get("steps", [])

    # Handle both multi-step list configuration and single flat transform specs
    steps_list = raw_steps if raw_steps else [config]

    prepared_steps = []
    for raw in steps_list:
        transform_type = str(raw.get("type")).casefold()

        # Build raw CTE dicts for strict structural parsing
        match transform_type:
            case "sql":
                prepared_ctes = []
                for raw_cte in raw.get("cte", []):
                    # Fail fast if required CTE keys are missing
                    if "name" not in raw_cte:
                        raise KeyError(
                            f"Missing required key 'name' in transform CTE: {raw_cte}"
                        )

                    joins_list = None
                    if raw_join := (raw_cte.get("join") or raw_cte.get("joins")):
                        joins_raw = (
                            [raw_join] if isinstance(raw_join, dict) else raw_join
                        )
                        joins_list = []
                        for j in joins_raw:
                            to_table = j.get("to_table") or j.get("table")
                            if not to_table or "on" not in j:
                                raise KeyError(
                                    f"Invalid join config in CTE '{raw_cte['name']}': {j}"
                                )
                            joins_list.append(
                                {
                                    "to_table": to_table,
                                    "on": j["on"],
                                    "type": j.get("type", Join.INNER),
                                }
                            )

                    prepared_ctes.append(
                        {
                            "name": raw_cte["name"],
                            # Maps YAML 'from' or 'from_table' -> SelectQueryContext.from_table
                            "from_table": raw_cte.get("from")
                            or raw_cte.get("from_table"),
                            "select": raw_cte.get("select"),
                            "joins": joins_list,  # Note: plural 'joins' matching SelectQueryContext
                            "where": raw_cte.get("where"),
                            "group_by": raw_cte.get("group_by"),
                            "having": raw_cte.get("having"),
                            "qualify": raw_cte.get("qualify"),
                            "order_by": raw_cte.get("order_by"),
                            "limit": raw_cte.get("limit"),
                            "sql": raw_cte.get("sql"),
                        }
                    )

                # Set payload AFTER prepared_ctes is populated
                sql_context_payload = {
                    "ctes": prepared_ctes,
                    "sql": raw.get("raw_sql"),
                }

            case "python":
                sql_context_payload = {}

            case _:
                raise ValueError(f"Unsupported transform type: {transform_type}")

        prepared_steps.append(
            {
                "id": raw.get("id"),
                "type": transform_type,
                "sources": raw.get("sources", {}),
                "sql_context": sql_context_payload,
                "module_path": raw.get("module_path"),
                "params": raw.get("params", {}),
            }
        )

    payload = {
        "steps": prepared_steps,
        "source_dir": config.get("source_dir") or get_val("source_dir", None),
    }

    # msgspec recursively converts and validates nested structs
    return msgspec.convert(payload, type=TransformConfig)
