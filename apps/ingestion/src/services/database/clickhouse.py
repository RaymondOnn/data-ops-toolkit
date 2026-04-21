from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from apps.ingestion.src.services.database.base import DatabaseSink, DatabaseSource
from apps.ingestion.src.services.factory import ServiceFactory
from libs.database.clients.clickhouse import ClickhouseClient
from loguru import logger

LOG = logger


@ServiceFactory.register("clickhouse_db")
class ClickHouseService(DatabaseSource, DatabaseSink):
    def _init_client(self, **config: Any) -> ClickhouseClient:
        # 1. Resolve Password safely
        # If 'secret_key' was used, 'password' is a Secret object.
        # If 'password' was a string in YAML, it stays a string.
        raw_password = config.get("password", "")
        resolved_password = (
            raw_password.resolve(sanitize=True)
            if hasattr(raw_password, "resolve")
            else str(raw_password)
        )
        print(f"{resolved_password=}")
        return ClickhouseClient(
            host=config.get("host", "localhost"),
            port=config.get("port", 8123),
            user=config.get("user", "default"),
            password=resolved_password,
        )

    def stage_data(
        self,
        source_dir: Path,
        target_table: str,
        expected_count: int,
        file_ext: str = "parquet",
        audit_values: dict[str, Any] | None = None,
    ) -> tuple[str, int] | None:
        name = target_table.split(".", 1)[
            -1
        ]  # Use table name as part of staging table for clarity
        staging_table = (
            f"stg_{name}_{int(datetime.now().astimezone().strftime('%Y%m%d%H%M%S'))}"
        )
        audit_values = audit_values or {}
        success = False
        try:
            # Different stages use separate sessions. Hence, TEMP Table approach not feasible.
            tmp_sql = f"""CREATE OR REPLACE TABLE {staging_table} 
                    ENGINE = MergeTree() 
                    ORDER BY tuple()
                    AS {target_table} 
                """
            LOG.debug(
                "Creating staging table from target",
                staging_table=staging_table,
                target_table=target_table,
            )
            self.client.sql(tmp_sql)

            self.client.copy_from_file(
                table=staging_table,
                source_dir=str(source_dir),
                file_ext=file_ext,
                audit_values=audit_values,
            )
            rows_staged = self.get_row_count(staging_table)

            LOG.info(
                "Staged data to ClickHouse",
                table=staging_table,
                rows=rows_staged,
            )

            if rows_staged != expected_count:
                raise ValueError(
                    f"Row count mismatch after staging. "
                    f"Expected {expected_count}, got {rows_staged}."
                )

            success = True
            return staging_table, rows_staged

        except Exception as exc:
            LOG.exception("Error during staging data to ClickHouse")
            raise exc
        finally:
            # CRITICAL: Only drop on failure.
            # On success, the table must persist for the PublishStage to find it.
            if not success:
                drop_sql = f"DROP TABLE IF EXISTS {staging_table}"
                self.client.sql(drop_sql)
                LOG.warning(
                    "Staging failed. Cleaned up table {table}", table=staging_table
                )

    def promote_data(
        self,
        staging_table: str,
        target_table: str,
        partition_col: str,
        partition_val: str,
        expected_count: int,
    ) -> None:
        """
        Atomic metadata swap.
        ClickHouse moves the actual data parts on disk
        Note: {partition_val} must match the internal ClickHouse partition ID format.
        """
        # 1. Schema & Partition Audit for Debugging
        target_schema = self.client.sql(f"DESCRIBE TABLE {target_table}")
        staging_schema = self.client.sql(f"DESCRIBE TABLE {staging_table}")

        LOG.info(
            "Auditing schemas before promotion",
            target=target_table,
            target_columns=[row[0] for row in target_schema],
            staging=staging_table,
            staging_columns=[row[0] for row in staging_schema],
        )

        success = False
        try:
            delete_sql = (
                f"DELETE FROM {target_table} WHERE {partition_col} = '{partition_val}'"
            )
            self.client.sql(delete_sql)
            LOG.debug(
                "Deleted existing partition from target table",
                table=target_table,
                partition_col=partition_col,
                partition_val=partition_val,
                sql=delete_sql,
            )

            insert_sql = f"INSERT INTO {target_table} SELECT * FROM {staging_table}"
            self.client.sql(insert_sql)
            LOG.info(
                "Promoted data to ClickHouse",
                table=target_table,
                partition=partition_val,
                sql=insert_sql,
            )

            rows_promoted = self.get_row_count(target_table)
            if rows_promoted != expected_count:
                raise ValueError(
                    f"Row count mismatch after promotion. Expected {expected_count}, got {rows_promoted}."
                )

            LOG.success(
                "Promoted {partition_col}={partition_val} to {table}",
                table=target_table,
                partition_col=partition_col,
                partition_val=partition_val,
            )
            success = True

        except Exception:
            LOG.exception("Error during promotion to ClickHouse")
            raise
        finally:
            # Always drop the staging table after the swap attempt
            if success:
                drop_sql = f"DROP TABLE IF EXISTS {staging_table}"
                self.client.sql(drop_sql)
                LOG.info(
                    "Promotion successful. Cleaning up staging table.",
                    staging_table=staging_table,
                    sql=drop_sql,
                )

    def is_equal(
        self,
        reference: Path,
        other: Path,
        exclude_columns: set[str] | None = None,
    ) -> bool:
        exclude_columns = exclude_columns or set()
        exclude_str = (
            f"EXCEPT ({', '.join(exclude_columns)})" if exclude_columns else ""
        )

        sql = f"""
            SELECT * {exclude_str}
            FROM {reference}
            EXCEPT
            SELECT * {exclude_str}
            FROM {other}
        """
        results = self.client.sql(sql)
        is_match = len(results) == 0
        LOG.info(
            "Comparing tables", reference=reference, other=other, is_match=is_match
        )
        if not is_match:
            LOG.warning("Table comparison failed", differences=len(results))
        return is_match

    def clone(self, reference: str, other: str) -> None:
        sql = f"""
            CREATE TEMPORARY TABLE {other} 
            ENGINE = MergeTree() AS 
                SELECT * FROM {reference}
                WHERE 1 = 0
        """
        LOG.info("Cloning table structure", source=reference, destination=other)
        self.client.sql(sql)

    def get_row_count(self, table_name: str) -> int:
        """Returns the total row count for a specified table."""
        res = self.client.sql(f"SELECT COUNT(*) FROM {table_name}")
        return int(res[0][0]) if res and res[0] else 0

    def fetch(self, query: str) -> list[Sequence[Any]]:
        """Proxy to the client's sql method for standard DB access."""
        return self.client.sql(query)
