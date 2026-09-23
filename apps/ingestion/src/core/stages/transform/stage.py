"""Transform stage for data processing."""

import time
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
from libs.database import TypeResolver
from libs.utils.dates import current_timestamp
from loguru import logger

from src.core.models.task import TaskManifestView
from src.core.stages.contracts.stage import ExecutionStage
from src.core.stages.types import Stage
from src.core.stages.utils import merge_schemas
from src.utils.exceptions import RollbackRequired

from .config import TransformConfig, TransformStep
from .enums import PartitionTransformed, TransformPayload
from .execution import (
    TransformContext,
    Transformer,
)

if TYPE_CHECKING:
    from src.core.models.task import TaskManifest, TaskWorkspace
    from src.core.stages.types import StageContext, StagePayload
    from src.services.health.system import SystemMonitor

LOG = logger
OUTPUT_FORMAT = "parquet"


@ExecutionStage.register(key=Stage.TRANSFORM)
class TransformStage(ExecutionStage[TransformConfig]):
    """Stage for transforming extracted data."""

    config_class = TransformConfig
    payload: "StagePayload"

    def _resolve_step_id_from_placeholder(
        self, template_str: str, manifest: "TaskManifest", step_id: str
    ) -> str | None:
        """Extracts target step_id directly from raw template string.

        Handles:
            - '{steps.extract_orders_data.artifact_folder}' -> 'extract_orders_data'
            - '{upstream.artifact_folder}'                  -> upstream step_id
        """
        from libs.utils.template import NAMED_TEMPLATE_PATTERN

        if not isinstance(template_str, str) or "{" not in template_str:
            return None

        # Find all template keys matching {key} or {nested.key} syntax used by TemplateEngine
        matches = NAMED_TEMPLATE_PATTERN.findall(template_str)
        for key in matches:
            parts = key.split(".")
            # Case 1: Explicit step reference e.g., steps.extract_orders_data.artifact_folder
            if len(parts) >= 2 and parts[0] == "steps":
                return parts[1]

            # Case 2: Upstream reference e.g., upstream.artifact_folder
            if len(parts) >= 2 and parts[0] == "upstream":
                view = TaskManifestView(manifest)
                return view.get_upstream_step_id(step_id=step_id)

        return None

    def pre_flight(
        self,
        system: "SystemMonitor",
        ctx: "StageContext",
        workspace: "TaskWorkspace",
        manifest: "TaskManifest",
    ) -> None:
        """Verify input artifacts exist and render stage inputs."""
        super().pre_flight(system, ctx, workspace, manifest)

        self.rendered_inputs = {}

        for key, raw_val in self.config.inputs.items():
            # 1. Parse step_id directly from raw placeholder template
            step_id = self._resolve_step_id_from_placeholder(
                raw_val, manifest=manifest, step_id=self.step_id
            )

            # 2. Render actual destination path
            rendered = self.render_placeholders(
                target=raw_val, manifest=manifest, step_id=self.step_id
            )
            rendered_path = str(rendered)
            self.rendered_inputs[key] = rendered_path

            data_folder = Path(rendered_path)

            # Helper to fail with RollbackRequired if step_id exists, else RuntimeError
            def fail(msg: str, s_id=step_id, k=key, p=rendered_path):
                if s_id:
                    raise RollbackRequired(s_id, f"Input '{k}' failed preflight: {msg}")
                raise RuntimeError(f"External input '{k}' path '{p}' invalid: {msg}")

            # 3. Perform preflight validations
            if not (data_folder.exists() or data_folder.iterdir()):
                fail("Data Files not available!")

    def _execute(
        self,
        ctx: "StageContext",
        workspace: "TaskWorkspace",
        manifest: "TaskManifest",
    ) -> str:
        """Execute transformation using distributed executor."""
        start_ts = current_timestamp(naive=True).isoformat(sep=" ")
        LOG.info(f"Starting transform: {self.step_id}")

        try:
            if manifest.is_empty_result_set:
                return self._handle_empty_manifest(
                    ctx=ctx, workspace=workspace, manifest=manifest, start_ts=start_ts
                )

            target_path = workspace.reset_data_dir(self.step_id)
            sub_steps_root = workspace.get_partition_dir(self.step_id, "_sub_steps")

            partition_dates = self._discover_partition_dates(self.rendered_inputs)
            LOG.info(f"Processing {len(partition_dates)} partitions: {partition_dates}")

            sub_step_outputs: dict[str, str] = {}
            partition_metrics: dict[str, PartitionTransformed] = {}
            partition_schemas: list[dict[str, pl.DataType] | pl.Schema] = []
            total_rows = 0

            num_steps = len(self.config.steps)
            for idx, sub_step in enumerate(self.config.steps):
                is_final_step = idx == num_steps - 1
                sub_step_id = sub_step.id or f"step_{idx + 1}"
                base_output_dir = (
                    target_path if is_final_step else (sub_steps_root / sub_step_id)
                )

                raw_sources = self._resolve_sub_step_sources(sub_step, sub_step_outputs)

                if partition_dates:
                    p_rows, p_schemas = self._process_sub_step_partitions(
                        sub_step=sub_step,
                        partition_dates=partition_dates,
                        raw_sources=raw_sources,
                        base_output_dir=base_output_dir,
                        is_final_step=is_final_step,
                        partition_metrics=partition_metrics,
                    )
                    if is_final_step:
                        total_rows += p_rows
                        partition_schemas.extend(p_schemas)

                sub_step_outputs[sub_step_id] = str(base_output_dir)

            final_schema = merge_schemas(partition_schemas)

            # Convert Polars DataTypes to canonical string representations
            output_schema_dict = {
                col_name: TypeResolver.polars_to_generic_type(dtype)
                for col_name, dtype in final_schema.items()
            }

            payload = TransformPayload(
                step_id=self.step_id,
                rows_processed=total_rows,
                artifact_folder=str(target_path),
                output_schema=output_schema_dict,
                start_time=start_ts,
                partitions=partition_metrics,
            )

            self.save_stage_outcome(
                workspace=workspace,
                manifest=manifest,
                data_folder=target_path,
                payload=payload,
            )

            LOG.info(
                f"Transformation complete: {total_rows:_} rows across {len(partition_metrics)} partitions"
            )
            return self._next_step(ctx)

        except Exception as e:
            self.save_stage_outcome(workspace=workspace, manifest=manifest, error=e)
            raise

    # =========================================================================
    # Helpers to reduce branch complexity in _execute
    # =========================================================================

    def _handle_empty_manifest(
        self,
        ctx: "StageContext",
        workspace: "TaskWorkspace",
        manifest: "TaskManifest",
        start_ts: str,
    ) -> str:
        LOG.info("Task manifest is_empty_result_set=True. Bypassing TransformStage.")
        payload = TransformPayload(
            step_id=self.step_id,
            rows_processed=0,
            start_time=start_ts,
            artifact_folder=None,
            output_schema={},
        )
        self.save_stage_outcome(workspace=workspace, manifest=manifest, payload=payload)
        return self._next_step(ctx)

    def _resolve_sub_step_sources(
        self, sub_step: TransformStep, sub_step_outputs: dict[str, str]
    ) -> dict[str, str]:
        raw_sources = {}
        for alias, src_id in sub_step.sources.items():
            if src_id in sub_step_outputs and sub_step_outputs:
                raw_sources[alias] = str(sub_step_outputs[src_id])

            if self.rendered_inputs and src_id in self.rendered_inputs:
                raw_sources[alias] = str(self.rendered_inputs[src_id])

            else:
                raise ValueError(
                    f"Source '{src_id}' for sub-step '{sub_step.id}' not found in previous outputs or external dependencies."
                )
        return raw_sources

    def _process_sub_step_partitions(
        self,
        *,
        sub_step: TransformStep,
        partition_dates: list[str],
        raw_sources: dict[str, str],
        base_output_dir: Path,
        is_final_step: bool,
        partition_metrics: dict[str, PartitionTransformed],
    ) -> tuple[int, list[dict[str, pl.DataType] | pl.Schema]]:
        total_rows_out = 0
        schemas = []

        for raw_p_date in partition_dates:
            p_folder = (
                raw_p_date
                if raw_p_date.startswith("partition_date=")
                else f"partition_date={raw_p_date}"
            )
            clean_date = raw_p_date.replace("partition_date=", "")

            p_start_time = time.perf_counter()
            partition_sources = {
                alias: str(Path(src_dir) / p_folder)
                for alias, src_dir in raw_sources.items()
                if (Path(src_dir) / p_folder).exists()
            }

            if not partition_sources:
                continue

            rows_in = self._count_input_rows(partition_sources)

            partition_target = base_output_dir / p_folder
            partition_target.mkdir(parents=True, exist_ok=True)

            ctx = TransformContext(
                sources=partition_sources,
                target=partition_target,
                format=OUTPUT_FORMAT,
                sub_step=sub_step,
            )
            transformer = Transformer.create(context=ctx)
            transformer.transform()

            p_duration = (time.perf_counter() - p_start_time) * 1000

            if is_final_step:
                part_files = list(partition_target.glob(f"*.{OUTPUT_FORMAT}"))
                if part_files:
                    lf_out = pl.scan_parquet(f"{partition_target}/*.{OUTPUT_FORMAT}")
                    rows_out = int(lf_out.select(pl.len()).collect()[0, 0])
                    total_rows_out += rows_out

                    schemas.append(pl.read_parquet_schema(part_files[0]))
                    partition_metrics[clean_date] = PartitionTransformed(
                        partition_date=clean_date,
                        rows_in=rows_in,
                        rows_out=rows_out,
                        files=[f.name for f in part_files],
                        execution_time_ms=round(p_duration, 2),
                    )

        return total_rows_out, schemas

    @staticmethod
    def _count_input_rows(partition_sources: dict[str, str]) -> int:
        rows_in = 0
        for src_path in partition_sources.values():
            files = list(Path(src_path).glob(f"*.{OUTPUT_FORMAT}"))
            if files:
                lf_in = pl.scan_parquet(f"{src_path}/*.{OUTPUT_FORMAT}")
                rows_in += int(lf_in.select(pl.len()).collect()[0, 0])
        return rows_in

    @staticmethod
    def _discover_partition_dates(sources: dict[str, str]) -> list[str]:
        """Scans resolved input source folders for partition_date=YYYY-MM-DD directories."""
        partition_dates: set[str] = set()

        for src_path_str in sources.values():
            src_path = Path(src_path_str)
            if not src_path.exists():
                continue

            # Find folders matching Hive format (e.g., partition_date=2026-08-30)
            for p_dir in src_path.glob("partition_date=*"):
                if p_dir.is_dir():
                    partition_dates.add(p_dir.name)

        return sorted(partition_dates)
