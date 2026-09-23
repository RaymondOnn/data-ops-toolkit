import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
from libs.clients.base import ClientCantConnect
from libs.resilience.circuit_breaker import CircuitOpen
from libs.utils.dates import current_timestamp
from loguru import logger

from src.core.stages.contracts.stage import ExecutionStage
from src.core.stages.models import WriteMode
from src.core.stages.types import Stage
from src.core.stages.utils import merge_schemas
from src.extras.checkpoints.checkpoint import (
    Checkpoint,
    CheckpointType,
    build_incremental_filter,
)
from src.services.factory import ServiceFactory

from .config import ExtractConfig
from .enums import ExtractPayload, FileInfo, PartitionExtracted
from .execution import (
    ExtractContext,
    Extractor,
)

if TYPE_CHECKING:
    from src.core.models.task import TaskManifest, TaskWorkspace
    from src.core.stages.types import StageContext
    from src.services.health.system import SystemMonitor

LOG = logger


@ExecutionStage.register(key=Stage.EXTRACT.value)
class ExtractStage(ExecutionStage[ExtractConfig]):
    """Extract raw data partition-by-partition using a registered Extractor."""

    config_class = ExtractConfig

    def pre_flight(
        self,
        system: "SystemMonitor",
        ctx: "StageContext",
        workspace: "TaskWorkspace",
        manifest: "TaskManifest",
    ) -> None:
        super().pre_flight(system, ctx, workspace, manifest)
        try:
            self.source = ServiceFactory.get_source(**self.config.connection)
            LOG.info(
                f"PRE-FLIGHT: Service {self.config.connection['type']} initialized successfully"
            )
        except Exception:
            LOG.exception("PRE-FLIGHT: Failed to initialize source")
            raise

    def _execute(
        self,
        ctx: "StageContext",
        workspace: "TaskWorkspace",
        manifest: "TaskManifest",
    ) -> str:
        LOG.debug(f"EXECUTE: ExtractStage starting for task {ctx.run_id}")
        start_ts = current_timestamp(naive=True)

        try:
            meta_repo = self.get_meta_repo(workspace)
            if not meta_repo:
                raise Exception("Metadata Repository is required.")

            checkpoint = Checkpoint.load_latest(
                meta_repo=meta_repo,
                job_id=ctx.job_id,
                dataset_id=self.config.resource,
            )

            # 1. Determine target date partitions to extract (e.g. catch-up days)
            target_partitions = self._resolve_target_partitions(
                partition_date=ctx.partition_date,
                checkpoint=checkpoint,
            )
            LOG.info(f"Target partitions to process: {target_partitions}")

            extractor = Extractor.create(self.config.connection["type"])
            data_dir = workspace.reset_data_dir(self.step_id)

            partitions_payload: dict[str, PartitionExtracted] = {}
            all_file_infos: list[FileInfo] = []
            all_source_files: list[str] = []
            total_rows = 0
            schemas = []

            # 2. Extract partition-by-partition in a clean loop
            for p_date in target_partitions:
                LOG.info(f"▶ Processing partition_date: {p_date}")

                # Record starting watermark boundary before extraction
                partition_start = checkpoint.start_value or p_date

                # Build incremental predicate ONLY for incremental load modes (e.g. DELTA, CDC)
                # Skip for FULL_REFRESH and SNAPSHOT loads
                if self.config.mode in (WriteMode.FULL_REFRESH, WriteMode.SNAPSHOT):
                    incremental_predicate = None
                else:
                    incremental_predicate = build_incremental_filter(
                        checkpoint=checkpoint,
                        update_key=self.config.update_key,
                        glob_template=self.config.glob,
                        partition_date=p_date,
                    )

                extract_ctx = ExtractContext(
                    resource=self.config.resource,
                    src_connection=self.config.connection,
                    num_workers=self.config.num_workers,
                    run_id=ctx.run_id,
                    partition_date=p_date,
                    job_id=ctx.job_id,
                    workspace=str(ctx.workspace_dir),
                    monitor_params={
                        "signal_dir": str(workspace.exec_ctx.signal_path),
                        "cache_config": workspace.exec_ctx.cache_config,
                    },
                    sql_context=self.config.sql_context,
                    columns=self.config.columns,
                    batch_size=self.config.batch_size,
                    null_if=self.config.null_if,
                    glob=self.config.glob,
                    flatten=self.config.flatten,
                    update_key=self.config.update_key,
                    primary_keys=self.config.primary_keys,
                    incremental_predicate=incremental_predicate,
                )

                # Partition-specific staging folder
                partition_dir = workspace.get_partition_dir(
                    self.step_id, partition_date=p_date
                )
                partition_dir.mkdir(parents=True, exist_ok=True)

                p_file_infos: list[FileInfo] = []
                p_rows = 0
                p_min_val = None
                p_max_val = None

                for file_data in extractor.extract(
                    self.source, extract_ctx, partition_dir
                ):
                    path = file_data["path"]
                    rows = file_data["rows"]
                    chunk_min = file_data.get("min_checkpoint")
                    chunk_max = file_data.get("max_checkpoint")

                    if chunk_max:
                        checkpoint.update_end_value(chunk_max)
                        p_max_val = (
                            chunk_max
                            if p_max_val is None
                            else max(p_max_val, chunk_max)
                        )
                    if chunk_min:
                        p_min_val = (
                            chunk_min
                            if p_min_val is None
                            else min(p_min_val, chunk_min)
                        )

                    checksum = self._calculate_checksum(path)
                    schema = pl.read_parquet_schema(path)
                    schemas.append(schema)

                    file_info = FileInfo(
                        path=str(path),
                        checksum=checksum,
                        row_count=rows,
                        size_bytes=path.stat().st_size,
                        max_checkpoint=chunk_max,
                    )
                    p_file_infos.append(file_info)
                    all_file_infos.append(file_info)
                    p_rows += rows
                    total_rows += rows

                # Source files for this partition
                p_sources = getattr(extractor, "source_files", [])
                all_source_files.extend(p_sources)
                audit_identity = self.resolve_resource_identify(extractor)

                # Determine effective values for logging & checkpoint state
                effective_start = str(p_min_val or partition_start)
                effective_end = str(p_max_val or checkpoint.end_value or p_date)

                # Ensure checkpoint metadata is fully updated and typed for cold-starts/new datasets
                checkpoint = self.update_checkpoint(
                    checkpoint=checkpoint,
                    config=self.config,
                    partition_date=ctx.partition_date,
                    # start_value=target_partitions[0] if target_partitions else "",
                    # end_value=target_partitions[-1] if target_partitions else "",
                )

                # 3. Store PartitionExtracted metadata with actual boundary values
                # Checkpoint table will extract info from here
                partitions_payload[p_date] = PartitionExtracted(
                    partition_date=p_date,
                    row_processed=p_rows,
                    file_count=len(p_file_infos),
                    source_files=p_sources,
                    files=p_file_infos,
                    checkpoint_type=checkpoint.type.value,
                    checkpoint_start=effective_start,
                    checkpoint_end=effective_end,
                    checkpoint_state_payload=checkpoint.serialize_payload(),
                    resource=audit_identity,
                )

                # Prepare start/end values for subsequent iterations
                checkpoint.start_value = p_date
                checkpoint.end_value = p_date
                LOG.success(
                    f"✓ Completed partition {p_date}: {p_rows:_} rows, {len(p_file_infos)} files"
                )

            final_schema = merge_schemas(schemas)

            payload = ExtractPayload(
                step_id=self.step_id,
                artifact_folder=str(data_dir),
                rows_processed=total_rows,
                output_schema={k: str(v) for k, v in final_schema.items()},
                start_time=start_ts.isoformat(sep=" "),
                partitions=partitions_payload,
            )

            self.save_stage_outcome(
                workspace=workspace,
                manifest=manifest,
                data_folder=data_dir,
                payload=payload,
            )
            LOG.info(
                f"EXTRACT COMPLETED: {len(target_partitions)} partitions, {total_rows:_} rows"
            )
            return self._next_step(ctx)

        except (ClientCantConnect, CircuitOpen) as e:
            LOG.warning(f"Extraction halted: {e}")
            self.save_stage_outcome(workspace=workspace, manifest=manifest, error=e)
            raise
        except Exception as e:
            LOG.exception("Extract failed")
            self.save_stage_outcome(workspace=workspace, manifest=manifest, error=e)
            raise

    def _resolve_target_partitions(
        self, partition_date, checkpoint: Checkpoint
    ) -> list[str]:
        """Calculates the list of discrete date partitions to process."""
        end_date_str = partition_date

        # 1. Non-incremental modes always process a single target partition
        if self.config.mode != WriteMode.DELTA:
            return [end_date_str]

        # Incremental date catch-up
        start_date_str = checkpoint.start_value
        if not start_date_str:
            return [end_date_str]

        try:
            start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date_str, "%Y-%m-%d")
            if start_dt >= end_dt:
                return [end_date_str]

            dates = []
            curr = start_dt + timedelta(days=1)
            while curr <= end_dt:
                dates.append(curr.strftime("%Y-%m-%d"))
                curr += timedelta(days=1)
            return dates or [end_date_str]
        except ValueError:
            # Fallback if start_value was a full timestamp or non-date string
            return [end_date_str]

    @staticmethod
    def _calculate_checksum(path: Path) -> str:
        hasher = hashlib.md5()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    def resolve_resource_identify(self, extractor: "Extractor") -> str:
        source_files = getattr(extractor, "source_files", [])
        return self.source.resolve_identity(
            target=self.config.resource, items=source_files, glob=self.config.glob
        )

    def update_checkpoint(
        self, checkpoint: Checkpoint, config: ExtractConfig, **kwargs: Any
    ) -> Checkpoint:
        load_mode = config.mode
        conn_type = config.connection.get("type")
        update_key = config.update_key
        start_value = kwargs.get("start_value")
        end_value = kwargs.get("end_value")
        payload_dict = {}

        if load_mode == WriteMode.CDC:
            checkpoint_type = CheckpointType.LSN_OFFSET

        elif load_mode in [WriteMode.SNAPSHOT, WriteMode.FULL_REFRESH]:
            checkpoint_type = CheckpointType.UPDATE_KEY

        elif load_mode == WriteMode.DELTA:
            checkpoint_type = CheckpointType.UPDATE_KEY

            if conn_type == "api":
                checkpoint_type = CheckpointType.PAGE_OFFSET
            elif conn_type == "file":
                partition_date = kwargs.get("partition_date")
                if update_key == "partition_date" and partition_date:
                    end_value = str(partition_date)
            else:
                # Database sources (postgres, oracle, clickhouse, etc.)
                checkpoint_type = CheckpointType.UPDATE_KEY

        else:
            raise NotImplementedError(f"Load mode '{load_mode}' not supported.")

        match checkpoint_type:
            case CheckpointType.UPDATE_KEY:
                payload_dict = {"update_key": update_key}
            case CheckpointType.PAGE_OFFSET:
                payload_dict = {"limit": kwargs.get("limit")}
            case CheckpointType.LSN_OFFSET:
                pass

        return Checkpoint(
            type=checkpoint_type,
            start_value=start_value or checkpoint.start_value,
            end_value=end_value or checkpoint.end_value,
            state_payload=payload_dict or checkpoint.state_payload,
        )
