import hashlib
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
from apps.ingestion.src.core.contexts import ExtractConfig
from apps.ingestion.src.core.models.task.manifest import ExtractPayload, FileInfo
from apps.ingestion.src.core.strategies.extract import (
    ExtractContext,
    Extractor,
    ExtractorFactory,
)
from apps.ingestion.src.services.factory import ServiceFactory
from libs.clients.base import ClientCantConnect
from libs.resilience.circuit_breaker import CircuitOpen
from libs.utils.dates import current_timestamp, seconds_diff
from loguru import logger

from .base import ExecutionStage
from .enums import Stage
from .utils import stage

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task import Task

LOG = logger


@stage(Stage.EXTRACT.value)
class ExtractStage(ExecutionStage[ExtractConfig]):
    """Extract raw data using a registered Extractor."""

    config_attribute = "extract"

    def pre_flight(self, task: "Task") -> None:
        super().pre_flight(task)
        try:
            self.source = ServiceFactory.get_source(**self.config.connection)
            LOG.info(f"PRE-FLIGHT: Service {self.config.type} initialized successfully")
        except Exception:
            LOG.exception("PRE-FLIGHT: Failed to initialize source")
            raise

    def _execute(self, task: "Task") -> str:
        LOG.debug(f"EXECUTE: ExtractStage starting for task {task.run_id}")
        start_ts = current_timestamp(naive=True)

        try:
            ctx = ExtractContext(
                kind=self.config.type,
                resource=str(self.config.resource),
                src_connection=self.config.connection,
                num_workers=self.config.num_workers,
                run_id=task.run_id,
                partition_date=task.partition_date,
                job_id=task.job_id,
                workspace=str(task.exec_ctx.workspace_dir),
                monitor_params={
                    "signal_dir": str(task.exec_ctx.signal_path),
                    "cache_config": task.exec_ctx.cache_config,
                },
                select=self.config.select,
                where=self.config.where,
                limit=self.config.limit,
                columns=self.config.columns,
                batch_size=self.config.batch_size,
                null_if=self.config.null_if,
                compression=self.config.compression,
                glob=self.config.glob,
                header=self.config.header,
                skip_blank_lines=self.config.skip_blank_lines,
                flatten=self.config.flatten,
                task_folder=str(task.workspace.path),
                tmp_cleanup=False,
            )

            # Get extractor
            extractor = ExtractorFactory.get(self.config.type)
            LOG.info(f"  Extractor type: {type(extractor).__name__}")

            # Get data folder
            data_store = task.workspace.reset_data_dir(self.name)
            LOG.info(f"  Data store: {data_store}")

            # Extract and stream results
            LOG.info("  Starting extraction...")
            file_infos = []
            total_rows = 0
            schemas = []

            for i, file_data in enumerate(
                extractor.extract(self.source, ctx, data_store)
            ):
                path = file_data["path"]
                rows = file_data["rows"]
                LOG.info(f"  📄 File {i}: {path.name} ({rows:_} rows)")

                checksum = self._calculate_checksum(path)
                schema = pl.read_parquet_schema(path)
                schemas.append(schema)

                file_infos.append(
                    FileInfo(
                        path=str(path),
                        checksum=checksum,
                        row_count=rows,
                        size_bytes=path.stat().st_size,
                    )
                )
                total_rows += rows

                # Progress update every 5 files
                if i % 5 == 0:  # checkpoint every 5 files
                    elapsed = seconds_diff(start_ts, current_timestamp(naive=True))
                    rate = total_rows / elapsed if elapsed > 0 else 0
                    LOG.info(
                        f"  📊 Progress: {i} files, {total_rows:_} rows "
                        f"({rate:,.0f} rows/sec)"
                    )
                    task.update_manifest(
                        {
                            "extract": {
                                "source_count": total_rows,
                                "file_count": len(file_infos),
                            }
                        }
                    )
                LOG.info(
                    f"  ✅ Extraction complete: {len(file_infos):_} files, "
                    f"{total_rows:_} rows"
                )

            # Backup source files if configured (non-blocking)
            # self._backup_source_files(task, ctx)

            final_schema = self._merge_schemas(schemas)
            audit_identity = self.resolve_resource_identify(extractor)
            LOG.info(f"  Audit identity: {audit_identity}")

            payload = ExtractPayload(
                file_count=len(file_infos),
                files=file_infos,
                artifact_folder=str(data_store),
                source_files=getattr(extractor, "source_files", []),
                resource=audit_identity,
                source_count=total_rows,
                schema={k: str(v) for k, v in final_schema.items()},
                start_time=start_ts.isoformat(sep=" "),
            )

            self.checkpoint(task, data_folder=data_store, payload=payload)
            LOG.info(
                f"EXTRACT COMPLETED: {len(file_infos):_} files, {total_rows:_} rows"
            )
            return self._next_stage()

        except (ClientCantConnect, CircuitOpen) as e:
            LOG.warning(f"Extraction halted: {e}")
            self.checkpoint(task, error=e)
            raise
        except Exception as e:
            LOG.exception("Extract failed")
            self.checkpoint(task, error=e)
            raise

    @staticmethod
    def _calculate_checksum(path: Path) -> str:
        hasher = hashlib.md5()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    def _merge_schemas(
        self, schemas: list[dict[str, pl.DataType]]
    ) -> dict[str, pl.DataType]:
        LOG.debug("  Merging schemas...")
        if not schemas:
            return {}

        _priority = {
            pl.Utf8: 100,
            pl.Float64: 90,
            pl.Float32: 80,
            pl.Int64: 70,
            pl.Int32: 60,
            pl.Int16: 50,
            pl.Int8: 40,
            pl.UInt64: 35,
            pl.UInt32: 30,
            pl.UInt16: 25,
            pl.UInt8: 20,
        }

        def priority(dtype: pl.DataType) -> int:
            return (
                -1
                if isinstance(dtype, pl.Struct | pl.List | pl.Array)
                else _priority.get(type(dtype), 0)
            )

        def promote(t1: pl.DataType, t2: pl.DataType) -> pl.DataType:
            match (t1, t2):
                case (a, b) if a is b:
                    return a
                case (pl.String, _) | (_, pl.String) | (pl.Utf8, _) | (_, pl.Utf8):
                    return pl.String()
                case (pl.Float64, _) | (_, pl.Float64):
                    return pl.Float64()
                case (pl.Float32, pl.Int64) | (pl.Int64, pl.Float32):
                    return pl.Float64()
                case _:
                    return t1 if priority(t1) >= priority(t2) else t2

        result = {}
        for schema in schemas:
            for col, dtype in schema.items():
                result[col] = promote(result[col], dtype) if col in result else dtype

        LOG.debug(f"  Final schema: {len(result)} columns")
        return result

    # def _backup_source_files(self, task: "Task", ctx: "ExtractContext") -> None:
    #     """Backup source files to archive storage if configured (non-blocking)."""
    #     backup_config = ctx.params.get("backup", {})
    #     if not backup_config.get("enabled"):
    #         return

    #     source_path = ctx.resource  # or ctx.resource
    #     if not source_path:
    #         LOG.debug("No source path to backup")
    #         return

    #     # Check if source path exists
    #     if not Path(source_path).exists():
    #         LOG.debug(f"Source path does not exist, skipping backup: {source_path}")
    #         return

    #     backup_service_cfg = backup_config.get(SERVICE_REF_NEW_KEY)
    #     if not backup_service_cfg:
    #         LOG.warning("Backup enabled but no service config not found")
    #         return

    #     try:
    #         backup_type = backup_service_cfg.pop("type")
    #         backup_service = ServiceFactory.get_archive(
    #             **backup_service_cfg
    #         )
    #         dest_path = task.get_archive_path(suffix="/raw")
    #         backup_service.store(Path(source_path), dest_path)
    #         LOG.info(f"Backing up source: {source_path} -> {dest_path}")

    #     except Exception:
    #         # Don't fail extraction if backup fails
    #         LOG.exception("Source backup failed. Continuing...")

    def resolve_resource_identify(self, extractor: "Extractor") -> str:
        """
        Delegate to service for source-specific identity.

        The service knows best how to name the source.
        """
        source_files = getattr(extractor, "source_files", [])

        # Pass additional context for better naming
        return self.source.resolve_identity(
            target=str(self.config.resource), items=source_files, glob=self.config.glob
        )
