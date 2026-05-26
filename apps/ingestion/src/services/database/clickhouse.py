import re
from collections.abc import Sequence
from functools import cached_property
from pathlib import Path
from typing import Any

from apps.ingestion.src.services.database.base import DatabaseSink, DatabaseSource
from apps.ingestion.src.services.factory import ServiceFactory
from libs.database.clients.clickhouse import ClickhouseClient
from libs.utils.dates import get_current_timestamp
from loguru import logger

LOG = logger


@ServiceFactory.register("clickhouse_db")
class ClickHouseService(DatabaseSource, DatabaseSink):
    @cached_property
    def client(self) -> ClickhouseClient:
        # 1. Resolve Password safely
        # If 'secret_key' was used, 'password' is a Secret object.
        # If 'password' was a string in YAML, it stays a string.
        raw_password = self._config.get("password", "")
        resolved_password = (
            raw_password.resolve(sanitize=True)
            if hasattr(raw_password, "resolve")
            else str(raw_password)
        )
        return ClickhouseClient(
            host=self._config.get("host", "localhost"),
            port=self._config.get("port", 8123),
            user=self._config.get("user", "default"),
            password=resolved_password,
            database=self._config.get("database", "default"),
        )

    def close(self) -> None:
        """Closes the underlying ClickhouseClient connection."""
        if hasattr(self, "_client") and self._client is not None:
            self._client.close()
            del self._client  # Clear the cached property

    def get_total_count(self, target: str, filter_condition: str | None = None) -> int:
        """Implementation required for resource-aware scaling in ExtractStage."""
        return self.get_row_count(target, filter_condition)

    def stage_data(
        self,
        source_dir: Path,
        target_table: str,
        expected_count: int,
        file_ext: str = "parquet",
        audit_values: dict[str, Any] | None = None,
    ) -> tuple[str, int]:

        # Extract database and table names to fully qualify the staging table
        parts = target_table.split(".", 1)
        db_name = parts[0] if len(parts) > 1 else None
        table_name = parts[-1]

        timestamp = get_current_timestamp(strip_tz=True).strftime("%Y%m%d%H%M%S")
        staging_table_name = f"stg_{table_name}_{timestamp}"
        staging_table = (
            f"{db_name}.stg_{table_name}_{timestamp}" if db_name else staging_table_name
        )

        audit_values = audit_values or {}
        success = False
        try:
            # Different stages use separate sessions.
            # Hence, TEMP Table approach not feasible.
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

        # Convert schema results to dictionaries: {column_name: data_type}
        target_cols = {row[0]: row[1] for row in target_schema}
        staging_cols = {row[0]: row[1] for row in staging_schema}

        if target_cols != staging_cols:
            missing_in_staging = set(target_cols.keys()) - set(staging_cols.keys())
            extra_in_staging = set(staging_cols.keys()) - set(target_cols.keys())
            type_mismatches = {
                col: {"target": target_cols[col], "staging": staging_cols[col]}
                for col in set(target_cols.keys()) & set(staging_cols.keys())
                if target_cols[col] != staging_cols[col]
            }

            LOG.error(
                "Schema mismatch detected during promotion",
                target_table=target_table,
                staging_table=staging_table,
                missing_in_staging=list(missing_in_staging),
                extra_in_staging=list(extra_in_staging),
                type_mismatches=type_mismatches,
            )
            raise ValueError(
                f"Cannot promote {staging_table} to {target_table}: Schema mismatch. "
                f"Missing: {missing_in_staging}, Extra: {extra_in_staging}, "
                "Mismatches: {type_mismatches}"
            )

        LOG.info("Schema audit successful", target=target_table, staging=staging_table)

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

            rows_promoted = self.get_row_count(
                target=target_table,
                filter_condition=f"{partition_col} = '{partition_val}'",
            )
            if rows_promoted != expected_count:
                raise ValueError(
                    "Row count mismatch after promotion. "
                    f"Expected {expected_count}, got {rows_promoted}."
                )

            LOG.success(
                "Promoted {partition_col}={partition_val} to {table}",
                table=target_table,
                partition_col=partition_col,
                partition_val=partition_val,
            )
            success = True

        except Exception:
            LOG.exception(
                "Error during promotion to ClickHouse table: {table}",
                table=target_table,
            )
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
        reference: str,
        other: str,
        exclude_columns: set[str] | None = None,
    ) -> bool:
        """
        Identity check using a tiered validation pyramid.
        """
        # Tier 1: Row Counts (Near-instant)
        if self.get_row_count(reference) != self.get_row_count(other):
            return False

        # Tier 2: Checksum (High-speed hash fingerprint)
        if self.get_checksum(reference) == self.get_checksum(other):
            return True

        # Tier 3: Set-Difference (Full deterministic check)
        return self.minus(reference, other, exclude_columns) == 0

    def minus(
        self, reference: str, other: str, exclude_columns: set[str] | None = None
    ) -> int:
        """Calculates the count of rows in reference that are missing from other."""
        exclude_columns = exclude_columns or set()

        # Schema discovery to handle evolution/alignment
        cols_ref = {
            row[0].decode("utf-8") if isinstance(row[0], bytes) else str(row[0])
            for row in self.fetch(f"DESCRIBE TABLE {reference}")
        }
        cols_other = {
            row[0].decode("utf-8") if isinstance(row[0], bytes) else str(row[0])
            for row in self.fetch(f"DESCRIBE TABLE {other}")
        }

        compare_cols = (cols_ref & cols_other) - exclude_columns
        if not compare_cols:
            LOG.error("No common columns found between tables for comparison.")
            return 999_999_999  # Sentinel for "Totally different"

        # Explicitly sort to ensure positional alignment in EXCEPT
        col_selection = ", ".join(sorted(compare_cols))

        sql = f"""
            SELECT count() FROM (
                SELECT {col_selection} FROM {reference}
                EXCEPT
                SELECT {col_selection} FROM {other}
            )
        """
        res = self.client.sql(sql)
        return int(res[0][0]) if res else 0

    def clone(self, reference: str, other: str) -> None:
        sql = f"""
            CREATE TABLE IF NOT EXISTS {other} 
            ENGINE = MergeTree() AS 
                SELECT * FROM {reference}
                WHERE 1 = 0
        """
        LOG.info("Cloning table structure", source=reference, destination=other)
        self.client.sql(sql)

    def get_checksum(self, identifier: str, columns: list[str] | None = None) -> str:
        """
        Generates a 64-bit table fingerprint.
        Uses cityHash64 for speed and groupBitXor for order-independence.
        """
        col_expr = ", ".join(columns) if columns else "*"
        # We wrap in hex() for a readable string representation
        query = f"SELECT hex(groupBitXor(cityHash64({col_expr}))) FROM {identifier}"
        try:
            res = self.client.sql(query)
            return str(res[0][0]) if res else "0"
        except Exception as e:
            LOG.error(f"Checksum calculation failed for {identifier}: {e}")
            return "ERROR"

    def drop(self, identifier: str) -> None:
        sql = f"DROP TABLE IF EXISTS {identifier}"
        LOG.warning("Dropping table from ClickHouse", table=identifier)
        self.client.sql(sql)

    def get_row_count(self, target: str, filter_condition: str | None = None) -> int:
        # 1. Clean the where clause (Case-Insensitive)
        clean_where = "1=1"
        if filter_condition and filter_condition.strip():
            # Removes "where " or "WHERE " from the start
            clean_where = re.sub(r"(?i)^where\s+", "", filter_condition.strip())

        # Protect table_name by wrapping in backticks and removing existing ones
        # safe_table = '"{}"'.format(table_name.replace('"', '""'))

        query = f"SELECT COUNT(*) FROM {target} WHERE {clean_where.rstrip('; ')}"

        try:
            res = self.client.sql(query)
            return int(res[0][0]) if res and len(res) > 0 else 0
        except Exception:
            # Log error using Loguru!
            # logger.error(f"Query failed: {e}")
            LOG.exception("Failed to get row count", table=target, query=query)
            return 0

    def fetch(self, query: str) -> list[Sequence[Any]]:
        """Proxy to the client's sql method for standard DB access."""
        return self.client.sql(query)
