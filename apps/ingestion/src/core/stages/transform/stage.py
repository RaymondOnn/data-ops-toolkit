"""Transform stage for data processing."""

from typing import TYPE_CHECKING

import polars as pl
from libs.utils.dates import current_timestamp
from loguru import logger

from src.core.models.task import Task
from src.core.stages.contracts.stage import ExecutionStage, ExecutionStageRegistry
from src.core.stages.enums import Stage
from src.utils.exceptions import RollbackRequired

from .config import TransformConfig
from .enums import TransformPayload
from .execution import (
    TransformContext,
    TransformFactory,
)

if TYPE_CHECKING:
    from src.core.stages.types import StagePayload

LOG = logger
OUTPUT_FORMAT = "parquet"


@ExecutionStageRegistry.register(Stage.TRANSFORM.value)
class TransformStage(ExecutionStage[TransformConfig]):
    """Stage for transforming extracted data."""

    config_class = TransformConfig
    payload: "StagePayload"

    def pre_flight(self, task: Task) -> None:
        """Verify extract artifacts exist."""
        super().pre_flight(task)

        current_step_id = task.task_ref.step_id
        step_config = task.context.get_step(current_step_id)

        if not step_config or not isinstance(step_config.config, TransformConfig):
            return

        transform_cfg: TransformConfig = step_config.config

        # Automatically extracts 'extract_orders' and 'extract_users'
        deps_step_ids = transform_cfg.get_external_deps(current_step_id, task.context)

        for dep_step_id in deps_step_ids:
            if not task.manifest.get_payload_for_step(dep_step_id):
                raise RollbackRequired(dep_step_id, "Extraction metadata missing")

            output_dir = task.workspace.path / dep_step_id
            if not output_dir.exists():
                raise RollbackRequired(dep_step_id, "External source folder missing")

            if not list(output_dir.iterdir()):
                raise RollbackRequired(dep_step_id, "No data files found")

            if not any(f.stat().st_size > 0 for f in output_dir.glob("*.parquet")):
                raise RollbackRequired(
                    dep_step_id, "Physical artifacts missing or empty"
                )

    def _execute(self, task: Task) -> str:
        """Execute transformation using distributed executor."""
        start_time = current_timestamp(naive=True).isoformat(sep=" ")
        LOG.info(f"Starting transform: {self.step_id}")

        try:
            target_path = task.workspace.reset_data_dir(self.step_id)
            sub_steps_root = target_path / self.step_id / "_sub_steps"
            sub_steps_root.mkdir(parents=True, exist_ok=True)

            external_sources = self.config.get_external_deps(self.step_id, task.context)
            external_source_paths = {
                step_id: task.workspace.path / step_id for step_id in external_sources
            }

            sub_step_outputs: dict[str, str] = {}
            num_steps = len(self.config.steps)

            for idx, sub_step in enumerate(self.config.steps):
                is_final_step = idx == num_steps - 1
                sub_step_id = sub_step.id or f"step_{idx + 1}"

                output_dir = (
                    target_path if is_final_step else (sub_steps_root / sub_step_id)
                )
                output_dir.mkdir(parents=True, exist_ok=True)

                resolved_sources = {}
                for alias, src_id in sub_step.sources.items():
                    if src_id in sub_step_outputs:
                        resolved_sources[alias] = str(sub_step_outputs[src_id])
                    elif src_id in external_source_paths:
                        resolved_sources[alias] = str(external_source_paths[src_id])
                    else:
                        raise ValueError(
                            f"Source '{src_id}' for sub-step '{sub_step_id}' not found in previous outputs or external dependencies."
                        )

                ctx = TransformContext(
                    sources=resolved_sources,
                    target=output_dir,
                    format=OUTPUT_FORMAT,
                    sub_step=sub_step,
                )

                transformer = TransformFactory.get(context=ctx)
                transformer.transform()

                sub_step_outputs[sub_step_id] = str(output_dir)

            # Get row count safely
            lf = pl.scan_parquet(str(target_path / "*.parquet"))
            row_count = int(lf.select(pl.len()).collect()[0, 0])

            # Get schema
            sample = next(target_path.glob("*.parquet"), None)
            schema = {}
            if sample:
                schema = {k: str(v) for k, v in pl.read_parquet_schema(sample).items()}

            LOG.info(f"Transformation complete: {row_count:_} rows")

            payload = TransformPayload(
                step_id=self.step_id,
                output_count=row_count,
                artifact_folder=str(target_path),
                output_schema=schema,
                start_time=start_time,
            )

            self.checkpoint(task, data_folder=target_path, payload=payload)
            LOG.info(f"Transform complete: {row_count:_} rows -> {target_path.name}")
            return self._next_step(task)

        except Exception as e:
            self.checkpoint(task, error=e)
            raise
