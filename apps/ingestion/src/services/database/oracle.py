from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any

from apps.ingestion.src.services.database.base import DatabaseSink, DatabaseSource
from apps.ingestion.src.services.factory import ServiceFactory
from libs.database.clients.oracle import OracleClient
from loguru import logger

if TYPE_CHECKING:
    from libs.auth.models import Secret


LOG = logger


@ServiceFactory.register("oracle_db")
class OracleService(DatabaseSource, DatabaseSink):
    @cached_property
    def client(self) -> OracleClient:
        secret: Secret = self._config["password"]
        return OracleClient(
            user=self._config["user"],
            password=secret.resolve(sanitize=True),
            dsn=self._config["dsn"],
        )

    def stage_data(
        self,
        source_dir: Path,
        target_table: str,
        expected_count: int,
        file_ext: str = "parquet",
        audit_values: dict[str, Any] | None = None,
    ) -> tuple[str, int]:
        staging_table = f"STG_{target_table}"

        # Oracle 'ORACLE_BIGDATA' driver can read all files in a location
        # if the location is defined as a directory or a specific URI pattern
        sql = f"""
        CREATE TABLE {staging_table} (
            -- Schema columns
        )
        ORGANIZATION EXTERNAL (
            TYPE ORACLE_BIGDATA
            ACCESS PARAMETERS (
                com.oracle.bigdata.fileformat=parquet
            )
            -- Oracle allows wildcards in the location for BigData driver
            LOCATION ('{source_dir}/*.{file_ext}')
        )
        REJECT LIMIT UNLIMITED
        """
        self.client.sql(sql)
        res = self.client.sql(f"SELECT COUNT(*) FROM {staging_table}")
        rows_staged = int(res[0][0]) if res and res[0] else 0
        LOG.info(
            "Staged data to Oracle",
            table=staging_table,
            rows=rows_staged,
        )
        return staging_table, rows_staged

    def promote_data(
        self,
        staging_table: str,
        target_table: str,
        partition_col: str,
        partition_val: str,
        expected_count: int,
    ) -> None:
        # If partition_val is '2026-03-10', we wipe that day and replace it
        sql = f"""
        BEGIN
            -- Idempotency: Clear the target slice
            DELETE FROM {target_table}
            WHERE {partition_col} = '{partition_val};

            -- Performance: Use APPEND hint for direct-path insert
            -- (bypasses buffer cache)
            INSERT /*+ APPEND */ INTO {target_table}
            SELECT * FROM {staging_table};

            COMMIT;
            EXECUTE IMMEDIATE 'DROP TABLE {staging_table}';
        EXCEPTION WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
        END;
        """
        self.client.sql(sql)
        LOG.info(
            "Promoted partition",
            table=target_table,
            partition=partition_val,
        )

    def is_equal(
        self,
        reference: Any,
        other: Any,
        exclude_columns: set[str] | None = None,
    ) -> bool:
        exclude_columns = exclude_columns or set()
        ref_table, other_table = str(reference).upper(), str(other).upper()

        # 1. Performance Optimization: Check row counts
        count_ref = self.client.sql(f"SELECT count(*) FROM {ref_table}")[0][0]
        count_other = self.client.sql(f"SELECT count(*) FROM {other_table}")[0][0]

        if count_ref != count_other:
            LOG.warning(
                "Table comparison failed: Row count mismatch",
                ref=ref_table,
                other=other_table,
            )
            return False

        # 2. Fast-Path: Checksum Comparison
        if self.get_checksum(ref_table) == self.get_checksum(other_table):
            LOG.info("Fast-path: Table checksums match.", table=ref_table)
            return True

        # 3. Schema alignment
        col_sql = "SELECT column_name FROM all_tab_columns WHERE table_name = '{}'"
        cols_ref = {row[0] for row in self.client.sql(col_sql.format(ref_table))}
        cols_other = {row[0] for row in self.client.sql(col_sql.format(other_table))}

        compare_cols = (cols_ref & cols_other) - {c.upper() for c in exclude_columns}
        if not compare_cols:
            LOG.error(
                "No common columns found for comparison",
                ref=ref_table,
                other=other_table,
            )
            return False

        col_selection = ", ".join(sorted(compare_cols))

        sql = f"""
            SELECT {col_selection} FROM {ref_table}
            MINUS
            SELECT {col_selection} FROM {other_table}
        """
        results = self.client.sql(sql)
        return len(results) == 0

    def clone(self, reference: str, other: str) -> None:
        # Removed TEMPORARY as regression shadow tables must persist between sessions
        sql = f"""
            CREATE TABLE {other} AS
            SELECT * FROM {reference}
            WHERE 1 = 0
        """
        LOG.info("Cloning table structure", source=reference, destination=other)
        self.client.sql(sql)

    def get_checksum(self, identifier: str, columns: list[str] | None = None) -> str:
        """Generates an order-independent checksum using ORA_HASH and SUM."""
        col_expr = (
            " || '|' || ".join(columns) if columns else "RAWTOHEX(SYS_GUID())"
        )  # Fallback if no cols
        # Oracle ORA_HASH is very fast for fingerprinting
        query = f"SELECT SUM(ORA_HASH({col_expr})) FROM {identifier}"
        try:
            res = self.client.sql(query)
            return str(res[0][0]) if res else "0"
        except Exception as e:
            LOG.error(f"Checksum failed for {identifier}: {e}")
            return "ERROR"

    def drop(self, identifier: str) -> None:
        sql = f"DROP TABLE {identifier} PURGE"
        LOG.warning("Dropping table from Oracle", table=identifier)
        self.client.sql(sql)
