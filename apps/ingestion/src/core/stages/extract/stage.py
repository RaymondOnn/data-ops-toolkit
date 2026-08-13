import hashlib
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
from libs.clients.base import ClientCantConnect
from libs.resilience.circuit_breaker import CircuitOpen
from libs.utils.dates import current_timestamp, seconds_diff
from loguru import logger

from src.core.stages.contracts.stage import ExecutionStage, ExecutionStageRegistry
from src.core.stages.enums import Stage
from src.services.factory import ServiceFactory

from .config import ExtractConfig
from .enums import ExtractPayload, FileInfo
from .execution import (
    ExtractContext,
    Extractor,
    ExtractorFactory,
)

if TYPE_CHECKING:
    from src.core.models.task import Task

LOG = logger


@ExecutionStageRegistry.register(Stage.EXTRACT.value)
class ExtractStage(ExecutionStage[ExtractConfig]):
    """Extract raw data using a registered Extractor."""

    config_class = ExtractConfig

    def pre_flight(self, task: "Task") -> None:
        super().pre_flight(task)
        try:
            self.source = ServiceFactory.get_source(**self.config.connection)
            LOG.info(
                f"PRE-FLIGHT: Service {self.config.connection['type']} initialized successfully"
            )
        except Exception:
            LOG.exception("PRE-FLIGHT: Failed to initialize source")
            raise

    def _execute(self, task: "Task") -> str:
        LOG.debug(f"EXECUTE: ExtractStage starting for task {task.run_id}")
        start_ts = current_timestamp(naive=True)

        try:
            ctx = ExtractContext(
                # kind=self.config.type,
                resource=self.config.resource,
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
                sql_context=self.config.sql_context,
                columns=self.config.columns,
                batch_size=self.config.batch_size,
                null_if=self.config.null_if,
                glob=self.config.glob,
                flatten=self.config.flatten,
                # select=self.config.select,
                # where=self.config.where,
                # limit=self.config.limit,
                # compression=self.config.compression,
                # header=self.config.header,
                # skip_blank_lines=self.config.skip_blank_lines,
                # temp_folder=str(task.workspace.path),
                # tmp_cleanup=False,
            )

            # Get extractor
            extractor = ExtractorFactory.get(self.config.connection["type"])
            LOG.info(f"  Extractor type: {type(extractor).__name__}")

            # Get data folder
            data_store = task.workspace.reset_data_dir(self.step_id)
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

            final_schema = self._merge_schemas(schemas)
            audit_identity = self.resolve_resource_identify(extractor)
            LOG.info(f"  Audit identity: {audit_identity}")

            payload = ExtractPayload(
                step_id=self.step_id,
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
            return self._next_step(task)

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
        self, schemas: list[dict[str, pl.DataType] | pl.Schema]
    ) -> dict[str, pl.DataType]:
        LOG.debug("  Merging schemas using Polars diagonal relaxed concat...")
        if not schemas:
            return {}

        # Build empty DataFrames for each schema and concatenate diagonally
        empty_dfs = [pl.DataFrame(schema=s) for s in schemas]
        merged_schema = pl.concat(empty_dfs, how="diagonal_relaxed").schema

        LOG.debug(f"  Final schema: {len(merged_schema)} columns")
        return dict(merged_schema)

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
