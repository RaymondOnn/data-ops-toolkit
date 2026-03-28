from pathlib import Path

import msgspec
import structlog

from apps.ingestion.src.core.contexts import TaskContext, TaskContextBuilder
from apps.ingestion.src.services.factory import ServiceFactory
from apps.ingestion.src.utils.constants import APP_CONFIG_ROOT

LOG = structlog.getLogger(__name__)


def get_task_ctx(job_id: str, dataset_id: str) -> TaskContext:
    builder = TaskContextBuilder()
    return next(iter(builder.build(job_id=job_id, dataset_id=dataset_id)))


def run_skeleton_clone(job_id: str, dataset_id: str, target_path: str):
    """Execution logic for cloning."""

    ctx = get_task_ctx(job_id, dataset_id)

    # The factory uses the config ALREADY loaded in ctx
    cloned_identifier = f"{ctx.load.sink_identifier}_clone"
    service = ServiceFactory.get_sink(ctx.load.sink_type, **ctx.load.sink_config)
    service.clone(reference=ctx.load.sink_identifier, other=cloned_identifier)


def run_comparison(
    job_id: str, dataset_id: str, feature_path: str, ignore_cols: set[str]
) -> bool:
    """Execution logic for equality check."""

    ctx = get_task_ctx(job_id, dataset_id)

    cloned_identifier = f"{ctx.load.sink_identifier}_clone"
    service = ServiceFactory.get_sink(ctx.load.sink_type, **ctx.load.sink_config)

    # We pass the master path from the config, and the feature path from the CLI
    is_match, report = service.is_equal(
        reference=f"{ctx.load.sink_identifier}_clone",
        other=cloned_identifier,
        exclude_columns=ignore_cols,
    )
    return is_match


class TransformSpec(msgspec.Struct, rename="lower"):
    type: str
    name: str | None = None


class DatasetSpec(msgspec.Struct, rename="lower"):
    transform: TransformSpec | None = None


class TaskSpec(msgspec.Struct, rename="lower"):
    default: DatasetSpec | None = None
    datasets: dict[str, DatasetSpec] = {}


def find_affected_peers(
    target_job: str, target_ds: str, config_root: Path = APP_CONFIG_ROOT
):
    builder = TaskContextBuilder()

    # 1. Resolve Target Identity via Builder
    target_ctx = builder.build(job_id=target_job, dataset_id=target_ds)
    t_type = target_ctx.transform_config.type.casefold()
    t_name = (target_ctx.transform_config.name or "").casefold()

    peers = []

    # 2. Glob and Scan
    for config_path in config_root.glob("**/config.yaml"):
        try:
            raw_data = config_path.read_bytes()

            # Fast byte-level pre-filter
            if t_type.encode() not in raw_data.lower() and (
                t_name and t_name.encode() not in raw_data.lower()
            ):
                continue

            # 3. Schema-constrained Decode
            # msgspec ignores keys not defined in TaskConfig
            cfg = msgspec.yaml.decode(raw_data, type=TaskSpec)

            job_id = config_path.parent.name

            # Check every dataset in this config
            for ds_id, ds_cfg in cfg.datasets.items():
                if job_id == target_job and ds_id == target_ds:
                    continue

                # Determine if this dataset potentially uses the target logic
                # (Checking both local dataset transform and job-level default)
                effective_trans = ds_cfg.transform or cfg.default.transform

                if not effective_trans:
                    continue

                if (
                    effective_trans.type.casefold() == t_type
                    and (effective_trans.name or "").casefold() == t_name
                ):
                    # 4. Final Verification
                    # Inflate the full context to ensure local env overrides
                    # don't change the transformer at the last second
                    peer_ctx = builder.build(job_id=job_id, dataset_id=ds_id)
                    peers.append(
                        {"job_id": job_id, "dataset_id": ds_id, "ctx": peer_ctx}
                    )

        except Exception:
            continue

    return peers
    return peers
    return peers
    return peers
    return peers
