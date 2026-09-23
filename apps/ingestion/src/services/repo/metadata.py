"""Metadata Repository for the Ingestion App.

Encapsulates all database interactions with the metadata database (state store,
timeout monitor, checkpoints, schema evolution) using ServiceFactory internally.
All queries are compiled dynamically using DatabaseConnector's SQLCompiler.
"""

from collections.abc import Generator, Sequence
from pathlib import Path
from typing import Any

import msgspec
import polars as pl
from libs.utils.dates import current_timestamp
from loguru import logger

from src.services.database import DatabaseService
from src.services.factory import ServiceFactory
from src.utils.constants import STRIP_TZ_FOR_DB

LOG = logger
EXECUTION_HISTORY_TBL = "META.EXECUTION_HISTORY"
SCHEMA_EVOLUTION_TBL = "META.SCHEMA_EVOLUTION_LOG"
CHECKPOINT_TBL = "META.CHECKPOINTS"
CURRENT_EXECUTION_TBL = "META.CURRENT_EXECUTION"


class SchemaChangeRecord(msgspec.Struct, frozen=True):
    dataset_id: str
    schema_json: str
    applied_at: str


class EmptyResultSetRecord(msgspec.Struct, frozen=True):
    job_id: str
    dataset_id: str
    partition_date: str
    run_id: str
    start_ts: str
    overrides: dict[str, str]


class CheckpointRecord(msgspec.Struct, frozen=True):
    job_id: str
    dataset_id: str
    checkpoint_type: str | None = None
    checkpoint_end_value: str | None = None
    state_payload: str | None = None
    created_at: str | None = None


class MetadataRepository:
    """Single repository encapsulating all metadata database operations.

    Accepts connection kwargs (or configuration dict) and internally leverages
    ServiceFactory to obtain and reuse the database service instance.
    """

    def __init__(self, **connection_kwargs: Any) -> None:
        """Initialize the repository with connection configuration."""
        self._connection_kwargs = connection_kwargs
        self._db_service: Any = None

    @property
    def db(self) -> "DatabaseService":
        """Lazy-loaded database service handle retrieved via ServiceFactory."""
        if self._db_service is None:
            LOG.debug("Initializing metadata DB service via ServiceFactory")
            service = ServiceFactory.get(**self._connection_kwargs)
            assert isinstance(
                service, DatabaseService
            ), f"Expected DatabaseService, got {type(service)}"
            self._db_service = service
        return self._db_service

    # Helper method to execute single scalar queries via Polars fetch_df
    def _fetch_scalar(self, query: str) -> Any | None:
        """Executes a query and returns the first column value of the first row."""
        try:
            df = next(self.db.fetch_df(query), None)
            if df is not None and not df.is_empty():
                return df.item(0, 0)
            return None
        except StopIteration:
            return None

    # Helper method to execute queries returning list of dict rows
    def _fetch_rows(self, query: str) -> list[dict[str, Any]]:
        """Executes a query and returns all rows as a list of dictionaries."""
        rows: list[dict[str, Any]] = []
        for df_batch in self.db.fetch_df(query):
            rows.extend(df_batch.to_dicts())
        return rows

    # =========================================================================
    # 1. State Store Operations
    # =========================================================================

    def get_table_schema(self, table_name: str = "META.EXECUTION_LOG") -> pl.DataFrame:
        """Retrieves column schema metadata for a table using SQLCompiler catalog rules."""
        return self.db.connector.get_schema(table_name)

    def bulk_load_parquet(self, table_name: str, stage_dir: Path) -> None:
        """Streams staged Parquet files into the metadata table via DatabaseConnector."""
        # Retrieve target table schema
        schema_df = self.get_table_schema(table_name)

        # Extract column names from the schema DataFrame
        columns = (
            schema_df["column_name"].to_list()
            if not schema_df.is_empty() and "column_name" in schema_df.columns
            else None
        )

        self.db.connector.copy_from_file(
            table=table_name,
            source_dir=str(stage_dir),
            file_format="parquet",
            columns=columns,
        )

    def fetch_daemon_task_records(self) -> Generator[pl.DataFrame, None, None]:
        """Yields DataFrame batches for active daemon task polling."""
        sql = self.db.connector.select(table="META.DAEMON_TASK_POLL")
        yield from self.db.fetch_df(sql)

    # =========================================================================
    # 2. Timeout Monitor Operations
    # =========================================================================

    def fetch_p95_stage_durations(
        self, job_id: str, dataset_id: str, stages: list[str]
    ) -> dict[str, dict[str, float]]:
        """Fetches p95 execution durations and sample counts for a job and dataset."""
        if not stages:
            return {}

        where_cond = self.db.connector.where(
            [
                ("JOB_STATUS", "=", "SUCCESS"),
                ("JOB_ID", "=", job_id),
                ("DATASET_ID", "=", dataset_id),
                ("JOB_ID", "IN", stages),
                "START_TIMESTAMP_LC > now() - INTERVAL 30 DAY",
            ]
        )

        subquery = self.db.connector.select(
            table=EXECUTION_HISTORY_TBL,
            fields=[
                "JOB_ID",
                "dateDiff('second', START_TIMESTAMP_LC, END_TIMESTAMP_LC) AS duration",
            ],
            where_cond=where_cond,
        )

        query = f"""
            SELECT
                JOB_ID AS stage,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration) AS p95_duration,
                COUNT(*) AS sample_count
            FROM ({subquery}) AS t
            GROUP BY JOB_ID
        """

        results = {}
        for row in self._fetch_rows(query):
            results[str(row["stage"])] = {
                "p95": float(row["p95_duration"]),
                "sample_count": int(row.get("sample_count", 0)),
            }
        return results

    # =========================================================================
    # 3. Checkpoint Operations
    # =========================================================================

    def get_latest_checkpoint(
        self, job_id: str, dataset_id: str
    ) -> CheckpointRecord | None:
        """Retrieves the latest checkpoint record for a job and dataset."""
        where_cond = self.db.connector.where(
            [("JOB_ID", "=", job_id), ("DATASET_ID", "=", dataset_id)]
        )

        query = f"""
            {self.db.connector.select(table=CHECKPOINT_TBL, where_cond=where_cond)}
            ORDER BY CREATED_AT_TS_LC DESC LIMIT 1
        """

        rows = self._fetch_rows(query)
        if not rows:
            return None

        r = rows[0]
        return CheckpointRecord(
            job_id=str(r.get("job_id", job_id)),
            dataset_id=str(r.get("dataset_id", dataset_id)),
            checkpoint_type=(
                str(r["checkpoint_type"]) if r.get("checkpoint_type") else None
            ),
            checkpoint_end_value=(
                str(r["checkpoint_end_value"])
                if r.get("checkpoint_end_value")
                else None
            ),
            state_payload=str(r["state_payload"]) if r.get("state_payload") else None,
            created_at=str(r["created_at"]) if r.get("created_at") else None,
        )

    # =========================================================================
    # 4. Schema Evolution Operations
    # =========================================================================

    def record_schema_change(
        self,
        *,
        target_table: str,
        action: str,  # "CREATE_TABLE", "ADD_COLUMN", etc.
        run_id: str | None = None,
        column_name: str | None = None,
        data_type: str | None = None,
        schema_json: dict[str, str] | None = None,
    ) -> None:
        """Inspects table schema using `DESCRIBE TABLE` and records the evolution event into META.SCHEMA_CHANGES."""

        data = [
            {
                "RUN_ID": f"'{run_id}'",
                "TARGET_TABLE": target_table,
                "ACTION": action,
                "COLUMN_NAME": column_name,
                "DATA_TYPE": data_type,
                "SCHEMA": schema_json,
                "EXECUTED_AT_LC": current_timestamp(naive=STRIP_TZ_FOR_DB).isoformat(
                    sep=" "
                ),
            }
        ]

        sql = self.db.connector.build_sql(
            operation="insert", table_name=SCHEMA_EVOLUTION_TBL, records=data
        )
        self.db.connector.db.command(sql)

    def get_schema_changes_today(self) -> Sequence[SchemaChangeRecord]:
        """Retrieves all schema change records that have not yet been notified."""
        where_cond = self.db.connector.where(
            [("toDate(EXECUTED_AT_LC)", "=", "toDate(now64(3) + INTERVAL 8 HOUR)")]
        )
        query = f"""
            {self.db.connector.select(
                table=SCHEMA_EVOLUTION_TBL,where_cond=where_cond
            )}
            ORDER BY executed_at ASC
        """

        rows = self._fetch_rows(query)
        if not rows:
            return []

        return [
            SchemaChangeRecord(
                dataset_id=str(r["dataset_id"]),
                schema_json=str(r.get("schema_json", "")),
                applied_at=str(r.get("executed_at", "")),
            )
            for r in rows
        ]

    # =========================================================================
    # 5. Empty Result Set Operations
    # =========================================================================

    def get_empty_result_sets_today(self) -> Sequence[EmptyResultSetRecord]:
        """Retrieves all SUCCESS execution history records where is_empty_result_set=True in FULL_MANIFEST."""
        where_cond = self.db.connector.where(
            [
                ("JOB_STATUS", "=", "SUCCESS"),
                ("FINAL_MANIFEST.IS_EMPTY_RESULT_SET", "=", "TRUE"),
                (
                    "toDate(LAST_UPDATED_AT_TS_LC)",
                    "=",
                    "toDate(now64(3) + INTERVAL 8 HOUR)",
                ),
            ]
        )
        query = self.db.connector.select(
            table=CURRENT_EXECUTION_TBL,
            fields=[
                "RUN_ID",
                "JOB_ID",
                "DATASET_ID",
                "PARTITION_DATE",
                "START_TIMESTAMP_LC",
                "RUNTIME_OVERRIDES",
            ],
            where_cond=where_cond,
        )

        rows = self._fetch_rows(query)
        if not rows:
            return []

        results: list[EmptyResultSetRecord] = []
        for r in rows:
            results.append(
                EmptyResultSetRecord(
                    run_id=str(r["RUN_ID"]),
                    job_id=str(r["JOB_ID"]),
                    dataset_id=str(r["DATASET_ID"]),
                    partition_date=str(r["PARTITION_DATE"]),
                    start_ts=str(r.get("START_TIMESTAMP_LC", "")),
                    overrides=r["RUNTIME_OVERRIDES"],
                )
            )
        return results
