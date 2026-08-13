from datetime import datetime
from pathlib import Path


class TemplateContext(dict):
    """Structured, safe mapping context for string template resolution."""

    def __init__(
        self,
        workspace_dir: str | Path = "",
        job_id: str | None = None,
        dataset_id: str | None = None,
        run_id: str | None = None,
        partition_date: str | None = None,
        now: datetime | None = None,
    ):
        ts = now or datetime.now()

        # Always-available system variables
        mapping: dict[str, str] = {
            "workspace_dir": str(workspace_dir),
            "YYYY": ts.strftime("%Y"),
            "MM": ts.strftime("%m"),
            "DD": ts.strftime("%d"),
        }

        # Only register task-specific variables if explicitly supplied
        task_vars = {
            "job_id": job_id,
            "dataset_id": dataset_id,
            "run_id": run_id,
            "partition_date": partition_date,
        }
        for k, v in task_vars.items():
            if v is not None:
                mapping[k] = v

        super().__init__(mapping)

    def register_step(self, step_id: str, stage: str, output_path: str | Path) -> None:
        """Helper to dynamically register step output metadata."""
        self[f"{step_id}.output_data"] = str(output_path)
        self[f"{step_id}.step_id"] = step_id
        self[f"{step_id}.stage"] = stage

    def __missing__(self, key: str) -> str:
        """Preserve unpopulated template strings like {missing_key} without throwing KeyError."""
        return f"{{{key}}}"
