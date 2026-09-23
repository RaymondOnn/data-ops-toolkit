from datetime import datetime
from pathlib import Path
from typing import Any

from libs.utils.dict import flatten_dict, set_nested_key
from libs.utils.file import is_path_like
from libs.utils.template import SafeDict, TemplateEngine

from src.services.factory import SECRET_PROTOCOL

PATH_PREFIX_BLACKLIST = (SECRET_PROTOCOL, "http://", "https://", "s3://", "arn:")


class TemplateContext(SafeDict):
    """Structured mapping context for string template resolution."""

    def __init__(
        self,
        workspace_dir: str | Path = "",
        job_id: str | None = None,
        dataset_id: str | None = None,
        run_id: str | None = None,
        partition_date: str | None = None,
        now: datetime | None = None,
    ):
        super().__init__()
        ts = now or datetime.now()

        self["workspace_dir"] = str(workspace_dir)
        self["YYYY"] = ts.strftime("%Y")
        self["MM"] = ts.strftime("%m")
        self["DD"] = ts.strftime("%d")

        task_vars = {
            "job_id": job_id,
            "dataset_id": dataset_id,
            "run_id": run_id,
            "partition_date": partition_date,
        }
        for k, v in task_vars.items():
            if v is not None:
                self[k] = v

    def register_step(self, step_id: str, stage: str, output_path: str | Path) -> None:
        """Helper to dynamically register step output metadata."""
        self[f"{step_id}.output_data"] = str(output_path)
        self[f"{step_id}.step_id"] = step_id
        self[f"{step_id}.stage"] = stage


def resolve_strings(
    ctx_data: dict[str, Any],
    context_variables: dict[str, Any],
    exclude_prefixes: tuple[str, ...] = PATH_PREFIX_BLACKLIST,
) -> dict[str, Any]:
    """Resolves templates and environment variables via TemplateEngine, then normalizes path-like strings.

    Args:
        ctx_data: Data structure (dict, list, or primitive) to resolve.
        context_variables: Dictionary of context variables passed to TemplateEngine.
        exclude_prefixes: Tuple of URL/Secret schemes to bypass during path evaluation.

    Returns:
        The fully interpolated and path-resolved configuration structure.
    """
    if not ctx_data:
        return ctx_data

    # Step 1: Use generic TemplateEngine to render all expressions, {vars}, and $ENV_VARS
    engine = TemplateEngine(context=context_variables)
    rendered_data = engine.render(ctx_data)

    # Step 2: Traverse and convert remaining path-like strings to absolute paths
    flat_configs = (
        flatten_dict(rendered_data) if isinstance(rendered_data, dict) else {}
    )
    resolved_data = (
        rendered_data.copy() if isinstance(rendered_data, dict) else rendered_data
    )

    for path, value in flat_configs.items():
        if isinstance(value, str) and is_path_like(
            value, exclude_prefixes=exclude_prefixes
        ):
            abs_path = str(Path(value).expanduser().resolve().absolute())
            if abs_path != value:
                resolved_data = set_nested_key(
                    resolved_data, path=path, new_value=abs_path
                )

    return resolved_data


def build_template_scope(
    job_id: str,
    dataset_id: str,
    run_id: str,
    partition_date: str,
    workspace_dir: str,
    dataset_cfg: dict[str, Any] | None = None,
    job_cfg: dict[str, Any] | None = None,
) -> TemplateContext:
    """Creates the base TemplateContext populated with App, Job, and Dataset scopes."""
    context_vars = TemplateContext(
        job_id=job_id,
        dataset_id=dataset_id,
        run_id=run_id,
        partition_date=partition_date,
        workspace_dir=workspace_dir,
    )

    # Directly scope dataset attributes excluding execution containers
    raw_cfg = dataset_cfg or {}
    context_vars["dataset"] = {
        k: v for k, v in raw_cfg.items() if k not in {"steps", "calls"}
    }

    # Directly scope job attributes excluding execution containers
    raw_job = job_cfg or {}
    context_vars["job"] = {k: v for k, v in raw_job.items() if k not in {"calls"}}
    return context_vars
