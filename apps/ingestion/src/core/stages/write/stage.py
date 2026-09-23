"""Write stage for loading transformed data to staging."""

from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
from libs.file import FormatHandler
from libs.utils.dates import current_timestamp
from loguru import logger

from src.core.stages.contracts.stage import ExecutionStage
from src.core.stages.types import Stage
from src.services.factory import ServiceFactory
from src.utils.exceptions import RollbackRequired

from .config import WriteConfig
from .enums import (
    DEFAULT_METADATA_COLS,
    CaseStyle,
    CaseType,
    MetadataColumnSpec,
    PartitionStaged,
    WritePayload,
)
from .execution.load import DataWriter, WriteContext

if TYPE_CHECKING:
    from src.core.models.task import TaskManifest, TaskWorkspace
    from src.core.stages.types import StageContext, StagePayload
    from src.services.health.system import SystemMonitor

LOG = logger
OUTPUT_FORMAT = "parquet"


def format_column_name(name: str, case_type: CaseType, case_style: CaseStyle) -> str:
    """Formats a column name according to specified CaseType and CaseStyle rules."""
    import re

    # 1. Insert space before capital letters in CamelCase (e.g., 'orderDate' -> 'order Date')
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", name)
    # Also handle acronym transitions (e.g., 'XMLParser' -> 'XML Parser')
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", s)

    # 2. Replace non-alphanumeric characters (underscores, hyphens, spaces) with space
    s = re.sub(r"[^a-zA-Z0-9]+", " ", s)

    # 3. Extract words
    words = [w for w in s.split() if w]
    if not words:
        words = [name]

    # 2. Apply CaseStyle formatting
    match case_style:
        case CaseStyle.SNAKE:
            formatted = "_".join(words)
        case CaseStyle.KEBAB:
            formatted = "-".join(words)
        case CaseStyle.CAMEL:
            formatted = words[0].lower() + "".join(w.capitalize() for w in words[1:])
        case CaseStyle.NONE:
            formatted = name

    # 3. Apply CaseType casing rules
    match case_type:
        case CaseType.LOWER:
            result = formatted.lower()
        case CaseType.UPPER:
            result = formatted.upper()
        case CaseType.CAPS:
            result = formatted.capitalize()
        case CaseType.TITLE:
            result = formatted.title()
        case CaseType.NONE:
            result = formatted

    LOG.trace(f"{name} -> {result}")
    return result


def _prepare_partitions(lf: pl.LazyFrame, update_key: str | None) -> pl.LazyFrame:
    """Ensures internal __partition column is present."""
    schema = lf.collect_schema()
    if "partition_date" in schema.names():
        return lf.rename({"partition_date": "__partition"})
    return lf


def _apply_casing(
    lf: pl.LazyFrame, case_type: CaseType, case_style: CaseStyle
) -> pl.LazyFrame:
    """Formats non-system/metadata user column names."""
    if case_type == CaseType.NONE and case_style == CaseStyle.NONE:
        return lf

    rename_map, seen = {}, set()
    for col in [c for c in lf.collect_schema().names() if not c.startswith("_")]:
        new_col = format_column_name(col, case_type, case_style)
        base, counter = new_col, 1
        while new_col in seen:
            new_col = f"{base}_{counter}"
            counter += 1
        seen.add(new_col)
        if col != new_col:
            rename_map[col] = new_col

    return lf.rename(rename_map) if rename_map else lf


def _apply_casts(lf: pl.LazyFrame, column_types: dict[str, Any] | None) -> pl.LazyFrame:
    """Applies type casting for user columns."""
    if not column_types:
        return lf

    from libs.database.dtypes import GENERIC_TO_TYPE_GROUP, TypeGroup, TypeResolver

    schema = set(lf.collect_schema().names())
    casts = [
        pl.col(c).cast(
            TypeResolver.group_to_polars(
                GENERIC_TO_TYPE_GROUP.get(t.strip().lower(), TypeGroup.TEXT)
            )
            if isinstance(t, str)
            else t
        )
        for c, t in column_types.items()
        if c in schema and not c.startswith("_")
    ]
    return lf.with_columns(casts) if casts else lf


def _inject_metadata(
    lf: pl.LazyFrame, manifest: "TaskManifest", meta_specs: list[MetadataColumnSpec]
) -> pl.LazyFrame:
    """Injects single-underscore system metadata columns."""
    if not meta_specs:
        return lf

    cols = set(lf.collect_schema().names())
    meta_exprs = [
        s.build_expr(manifest=manifest, existing_cols=cols)
        for s in meta_specs
        if s.target_name not in cols
    ]
    return lf.with_columns(meta_exprs) if meta_exprs else lf


@ExecutionStage.register(key=Stage.WRITE.value)
class WriteStage(ExecutionStage[WriteConfig]):
    """Stage for loading transformed data to staging area."""

    requires_disk_space: bool = False
    config_class = WriteConfig
    payload: "StagePayload"

    def pre_flight(
        self,
        system: "SystemMonitor",
        ctx: "StageContext",
        workspace: "TaskWorkspace",
        manifest: "TaskManifest",
    ) -> None:
        """Verify sink connectivity and transform artifacts."""
        super().pre_flight(system, ctx, workspace, manifest)

        if not self.config or not isinstance(self.config, WriteConfig):
            return

        data_dir = self.render_placeholders(
            self.config.data_dir_ref, manifest, self.step_id
        )
        if not data_dir:
            raise ValueError()

        self.data_dir = Path(data_dir)

        # Verify transform data exists
        if not (
            self.data_dir.exists()
            or self.data_dir.iterdir()
            or any(self.data_dir.glob("*.{OUTPUT_FORMAT}"))
        ):
            dep_step_id = self.data_dir.name
            raise RollbackRequired(dep_step_id, "Data Files not available!")

        self.sink = ServiceFactory.get_sink(**self.config.connection)

    def _execute(
        self,
        ctx: "StageContext",
        workspace: "TaskWorkspace",
        manifest: "TaskManifest",
    ) -> str:
        """Load transformed data to staging."""
        start_ts = current_timestamp(naive=True).isoformat(sep=" ")
        if manifest.is_empty_result_set:
            LOG.info("Task manifest is_empty_result_set=True. Bypassing WriteStage.")
            payload = WritePayload(
                step_id=self.step_id,
                destination=self.config.target,
                staging_artifact="",
                rows_processed=0,
                start_time=start_ts,
            )
            self.save_stage_outcome(
                workspace=workspace, manifest=manifest, payload=payload
            )
            return self._next_step(ctx)

        try:
            expected_count = int(
                self.render_placeholders(
                    self.config.expected_count_ref,
                    manifest=manifest,
                    step_id=self.step_id,
                )
            )

            meta_specs = self._resolve_meta_columns(
                meta_columns=self.config.meta_columns,
                soft_delete_missing=self.config.soft_delete_missing,
            )

            # 1. Build the complete LazyFrame pipeline via chaining / piping
            handler = FormatHandler.create(OUTPUT_FORMAT)
            lf = (
                handler.to_df(path=self.data_dir, hive_partitioning=True)
                .pipe(_prepare_partitions, update_key=self.config.update_key)
                .pipe(
                    _apply_casing,
                    case_type=self.config.column_case_type,
                    case_style=self.config.column_case_style,
                )
                .pipe(_apply_casts, column_types=self.config.column_types)
                .pipe(_inject_metadata, manifest=manifest, meta_specs=meta_specs)
            )
            # 2. Extract partition keys directly from the lazy plan
            partition_keys, is_datetime_key = self._extract_partition_keys(
                lf, update_key=self.config.update_key
            )

            # 3. Reconcile database schema (excluding __ internal processing columns)
            clean_incoming_schema = pl.Schema(
                {k: v for k, v in lf.collect_schema().items() if not k.startswith("__")}
            )

            writer = DataWriter()
            final_columns = writer.reconcile_schema(
                sink=self.sink,
                meta_repo=self.get_meta_repo(workspace),
                target=self.config.target,
                create_table_ddl=self.config.create_table_ddl,
                add_new_columns=self.config.add_new_columns,
                incoming_schema=clean_incoming_schema,
                metadata_columns={spec.target_name for spec in meta_specs},
                run_id=ctx.run_id,
            )

            total_rows, partition_metrics, last_staging_id = 0, [], ""

            # 4. Filter and materialize per partition
            for part_val in partition_keys:
                part_df = (
                    lf.pipe(
                        self._filter_partition,
                        val=part_val,
                        is_datetime=is_datetime_key,
                        update_key=self.config.update_key,
                    )
                    .select([pl.col(c) for c in final_columns])  # Strip __ columns here
                    .collect()
                )
                if part_df.height == 0:
                    continue

                # 4. Save clean data to Parquet for staging/loading
                data_dir = workspace.reset_data_dir(self.step_id)
                handler.from_df(part_df.lazy(), str(data_dir))

                write_ctx = WriteContext(
                    target=self.config.target,
                    incoming_data_dir=str(data_dir),
                    write_strategy=self.config.write_strategy,
                    working_table=self.config.working_table,
                    expected_count=expected_count,
                    file_format=OUTPUT_FORMAT,
                )
                last_staging_id, rows = writer.write(sink=self.sink, context=write_ctx)
                total_rows += rows
                partition_metrics.append(
                    PartitionStaged(partition_date=str(part_val), rows_processed=rows)
                )

            payload = WritePayload(
                step_id=self.step_id,
                destination=self.config.target,
                staging_artifact=last_staging_id,
                rows_processed=total_rows,
                start_time=start_ts,
                partitions=partition_metrics,
            )
            self.save_stage_outcome(
                workspace=workspace, manifest=manifest, payload=payload
            )
            return self._next_step(ctx)

        except Exception as e:
            self.save_stage_outcome(workspace=workspace, manifest=manifest, error=e)
            raise

    @staticmethod
    def _filter_partition(
        lf: pl.LazyFrame,
        val: Any,
        is_datetime: bool,
        update_key: str | None = None,
    ) -> pl.LazyFrame:
        """Applies predicate for target partition value."""
        if val is None:
            return lf

        schema = lf.collect_schema()

        if "__partition" not in schema.names():
            LOG.warning(
                "Internal column '__partition' not found in schema. Skipping partition filter."
            )
            return lf

        col_dtype = schema["__partition"]

        # Convert string val to Python date/datetime if target column is temporal
        target_val = val
        if isinstance(val, str):
            if col_dtype == pl.Date:
                target_val = date.fromisoformat(val)
            elif col_dtype == pl.Datetime:
                target_val = datetime.fromisoformat(val)

        # Apply date truncation if it's a datetime column being filtered by date
        col_expr = pl.col("__partition")
        if col_dtype in (pl.Datetime, pl.Date) and is_datetime:
            col_expr = col_expr.dt.date()

        return lf.filter(col_expr == target_val)

    @staticmethod
    def _extract_partition_keys(
        lf: pl.LazyFrame,
        update_key: str | None = None,
    ) -> tuple[list[Any], bool]:
        """Extracts unique partition values directly from Polars LazyFrame."""
        schema = lf.collect_schema()
        names = schema.names()

        has_update_key = bool(update_key and update_key in names)
        is_datetime = has_update_key and schema[update_key] in (pl.Datetime, pl.Date)

        if "__partition" in names:
            target_expr = pl.col("__partition")
        elif has_update_key:
            target_expr = (
                pl.col(update_key).dt.date() if is_datetime else pl.col(update_key)
            )
        else:
            return [None], False

        keys = lf.select(target_expr.alias("_k")).unique().collect()["_k"].to_list()
        return (keys or [None]), is_datetime

    @staticmethod
    def _resolve_meta_columns(
        meta_columns: dict[str, str | bool],
        soft_delete_missing: bool = False,
    ) -> list[MetadataColumnSpec]:
        """Resolves metadata column definitions and default values for target ingestion.

        This method maps standard metadata keys (`run_id`, `run_date`, `loaded_at`, `deleted`)
        to their runtime values and handles custom column renaming or feature overrides.

        Opt-In / Opt-Out & Renaming Behavior:
            - **Default Preset**: Passing `{"run_id": True, "loaded_at": True}` includes those
              metadata columns with their standard internal names (`_run_id`, `_loaded_at`).
            - **Renaming Columns**: Passing a string value renames the metadata column in the
              destination dataset (e.g., `{"run_id": "batch_id"}` yields a column named `batch_id`).
            - **Opting Out**: Omitting a key or setting its value to `False`/`None` excludes it
              from the target schema (e.g., `{"run_date": False}`).
            - **Soft Delete Override**: If `soft_delete_missing=True` and no `deleted` key is
              provided in `meta_columns`, the `deleted` metadata column (defaulting to `_is_deleted`)
              is automatically enabled and set to `None` (SQL NULL) for standard incoming rows.

        Args:
            task (Task): The executing pipeline task providing contextual metadata (e.g., run_id).
            meta_columns (dict[str, str | bool]): Configuration mapping indicating enabled status
                or custom names for metadata columns.
            soft_delete_missing (bool): Whether soft-delete reconciliation is active for missing
                source records. Defaults to False.

        Returns:
            dict[str, Any]: A mapping of target column names to their runtime values/literals.

        Examples:
            >>> # Standard defaults
            >>> _resolve_meta_columns(task, {"run_id": True, "loaded_at": True})
            {"_run_id": "run_123", "_loaded_at": "2026-08-24 17:50:00"}

            >>> # Custom column names
            >>> _resolve_meta_columns(
            ...     task, {"run_id": "execution_id", "deleted": "is_archived"}
            ... )
            {"execution_id": "run_123", "is_archived": None}

            >>> # Soft delete opt-in fallback
            >>> _resolve_meta_columns(task, {}, soft_delete_missing=True)
            {"_is_deleted": None}
        """

        effective_meta = dict(meta_columns)

        # _is_deleted added if soft_delete_missing is active
        if soft_delete_missing:
            effective_meta["deleted"] = True

        active_specs: list[MetadataColumnSpec] = []
        for key, val in effective_meta.items():
            if not val:
                continue

            if key not in DEFAULT_METADATA_COLS:
                raise ValueError(f"Unsuppported metadata column: {key}")

            base_spec = DEFAULT_METADATA_COLS[key]
            name_override = val if isinstance(val, str) else None

            # Reconstruct struct with runtime enabled flag & target override
            active_specs.append(
                MetadataColumnSpec(
                    key=base_spec.key,
                    default_name=base_spec.default_name,
                    is_enabled=True,
                    name_override=name_override,
                    expr_builder=base_spec.expr_builder,
                )
            )

        return active_specs
