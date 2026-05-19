from pathlib import Path
from typing import Any

import msgspec
import polars as pl
from apps.ingestion.src.core.contexts import TaskContext, TaskContextBuilder
from apps.ingestion.src.services.factory import ServiceFactory
from apps.ingestion.src.utils.constants import APP_CONFIG_ROOT
from loguru import logger

LOG = logger
CLONE_SUFFIX = "_clone"


def get_task_ctx(job_id: str, dataset_id: str, env: str = "local") -> TaskContext:
    builder = TaskContextBuilder(env=env)
    return next(iter(builder.build(job_id=job_id, dataset_id=dataset_id)))


def run_skeleton_clone(
    job_id: str, dataset_id: str, env: str = "local", suffix: str = CLONE_SUFFIX
):
    """Execution logic for cloning."""

    ctx = get_task_ctx(job_id, dataset_id, env=env)

    source_ident = ctx.load.sink_identifier
    target_ident = f"{source_ident}{suffix}"

    service = ServiceFactory.get_service(
        service_type=ctx.load.sink_type,
        **ctx.load.sink_config,
    )

    if hasattr(service, "clone"):
        service.clone(reference=source_ident, other=target_ident)


def run_cleanup(
    job_id: str, dataset_id: str, env: str = "local", suffix: str = CLONE_SUFFIX
):
    """Removes the shadow sink created for regression testing."""
    ctx = get_task_ctx(job_id, dataset_id, env=env)
    target_ident = f"{ctx.load.sink_identifier}{suffix}"

    service = ServiceFactory.get_service(
        service_type=ctx.load.sink_type,
        **ctx.load.sink_config,
    )

    if hasattr(service, "drop"):
        service.drop(target_ident)


def run_comparison(
    job_id: str,
    dataset_id: str,
    env: str = "local",
    suffix: str = CLONE_SUFFIX,
    ignore_cols: set[str] | None = None,
    detailed: bool = False,
) -> bool:
    """
    Execution logic for equality check.
    Detailed mode provides a column-level breakdown for ClickHouse sinks.
    """
    ctx = get_task_ctx(job_id, dataset_id, env=env)
    ref_table = ctx.load.sink_identifier
    other_table = f"{ref_table}{suffix}"
    ignore_cols = ignore_cols or set()

    service = ServiceFactory.get_service(
        service_type=ctx.load.sink_type, **ctx.load.sink_config
    )

    if detailed:
        return _run_detailed_sql_report(service, ref_table, other_table, ignore_cols)

    return service.is_equal(
        reference=ref_table,
        other=other_table,
        exclude_columns=ignore_cols,
    )


# TODO: What if dataset has no primary key or natural join key? Can we do some kind of hash-based bucketing approach for comparison?
def _run_detailed_sql_report(
    service: Any, ref: str, other: str, ignore_cols: set[str]
) -> bool:
    """
    Generates a DataCompy-style report using database compute.
    Uses standard SQL (SUM CASE) to remain compatible across engines.
    """
    LOG.info(f"📊 Generating SQL regression report for {ref}")

    # 1. Generic Column Discovery via Polars (Works for any DB)
    res_ref = pl.from_arrow(service.fetch(f"SELECT * FROM {ref} LIMIT 0"))
    res_other = pl.from_arrow(service.fetch(f"SELECT * FROM {other} LIMIT 0"))

    ref_df = res_ref if isinstance(res_ref, pl.DataFrame) else res_ref.to_frame()
    other_df = (
        res_other if isinstance(res_other, pl.DataFrame) else res_other.to_frame()
    )

    common_cols = [
        c for c in ref_df.columns if c in other_df.columns and c not in ignore_cols
    ]

    if not common_cols:
        LOG.error("No common columns found for comparison.")
        return False

    # 2. Basic Statistics
    count_ref = service.get_row_count(ref)
    count_other = service.get_row_count(other)
    divergent_rows = service.minus(ref, other, ignore_cols)

    # 3. Value-Level Mismatch Analysis (ANSI SQL approach)
    # Heuristic: assume first common column is a Join Key
    join_key = common_cols[0]

    # Construction of a cross-DB count expression
    mismatch_exprs = ", ".join(
        [
            f"SUM(CASE WHEN A.{c} != B.{c} THEN 1 ELSE 0 END) AS {c}_mismatch"
            for c in common_cols
            if c != join_key
        ]
    )

    sql_vals = f"SELECT {mismatch_exprs} FROM {ref} AS A INNER JOIN {other} AS B ON A.{join_key} = B.{join_key}"

    val_results = service.fetch(sql_vals)[0]
    mismatches = dict(zip([c for c in common_cols if c != join_key], val_results))

    # 4. Render Report
    output = [
        "\n" + "═" * 60,
        " DATA REGRESSION REPORT ".center(60, "═"),
        "═" * 60,
        f"Reference:      {ref} ({count_ref} rows)",
        f"Shadow:         {other} ({count_other} rows)",
        f"Join Key:       {join_key}",
        "─" * 60,
        "Row Statistics:",
        f"  Total Matches:  {count_ref - divergent_rows}",
        f"  Divergent Rows: {divergent_rows}",
        "─" * 60,
    ]

    if divergent_rows > 0:
        output.append("Column-Level Mismatch Breakdown:")
        for col, count in mismatches.items():
            if count > 0:
                output.append(f"  - {col.ljust(25)}: {count} differences")
    else:
        output.append("✅ PERFECT MATCH: No data divergence detected.")

    output.append("═" * 60 + "\n")
    print("\n".join(output))

    return divergent_rows == 0 and count_ref == count_other


class TransformSpec(msgspec.Struct, rename="lower"):
    type: str
    name: str | None = None


class DatasetSpec(msgspec.Struct, rename="lower"):
    transform: TransformSpec | None = None


class TaskSpec(msgspec.Struct, rename="lower"):
    default: DatasetSpec | None = None
    datasets: dict[str, DatasetSpec] = {}


def find_affected_peers(
    target_job_id: str,
    target_dataset_id: str,
    env: str = "local",
    config_root: Path = APP_CONFIG_ROOT,
) -> list[dict[str, str]]:

    builder = TaskContextBuilder(env=env)
    peers = []

    try:
        target_ctx = get_task_ctx(target_job_id, target_dataset_id, env=env)
        target_transform_type = target_ctx.transform.transform_type.casefold()
        target_transform_name = (
            target_ctx.transform.transform_params.get("name") or ""
        ).casefold()
        LOG.info(
            f"Target transformation signature: Type='{target_transform_type}', "
            f"Name='{target_transform_name}'"
        )
    except Exception as e:
        LOG.error(f"Failed to resolve target job/dataset context: {e}")
        return []

    # 2. Glob and Scan
    if not config_root.exists():
        LOG.warning(f"Configuration root not found: {config_root}")
        return []

    for job_dir in config_root.iterdir():
        if not job_dir.is_dir():
            continue

        job_id = job_dir.name
        config_path = job_dir / "config.yaml"
        if not config_path.exists():
            continue

        try:
            # Build all contexts for this job_id to get all datasets
            all_contexts_for_job = builder.build(job_id=job_id)

            for peer_ctx in all_contexts_for_job:
                # Skip the target itself
                if (
                    peer_ctx.job_id == target_job_id
                    and peer_ctx.dataset_id == target_dataset_id
                ):
                    continue

                peer_transform_type = peer_ctx.transform.transform_type.casefold()
                peer_transform_name = (
                    peer_ctx.transform.transform_params.get("name") or ""
                ).casefold()

                # Compare transformation signatures
                if (
                    peer_transform_type == target_transform_type
                    and peer_transform_name == target_transform_name
                ):
                    peers.append(
                        {"job_id": peer_ctx.job_id, "dataset_id": peer_ctx.dataset_id}
                    )
        except Exception as e:
            LOG.warning(f"Skipping job '{job_id}' due to configuration error: {e}")
            continue

    return peers
