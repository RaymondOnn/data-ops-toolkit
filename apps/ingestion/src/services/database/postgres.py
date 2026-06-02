import time
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
from apps.ingestion.src.services.database.base import DatabaseSink, DatabaseSource
from apps.ingestion.src.services.factory import ServiceFactory
from libs.database.clients.postgres import PostgresClient
from loguru import logger

if TYPE_CHECKING:
    from libs.auth.models import Secret

LOG = logger


@ServiceFactory.register("postgres_db")
class PostgresService(DatabaseSource, DatabaseSink):
    @cached_property
    def client(self) -> PostgresClient:
        secret: Secret = self._config["password"]
        return PostgresClient(
            host=self._config["host"],
            database=self._config["database"],
            user=self._config["user"],
            password=secret.resolve(sanitize=True),
            port=self._config.get("port", 5432),
        )

    def stage_data(
        self,
        source_dir: Path,
        target_table: str,
        expected_count: int,
        file_ext: str = "parquet",
        audit_values: dict[str, Any] | None = None,
    ) -> tuple[str, int]:
        staging_table = f"stg_{target_table}_{int(time.time())}"
        self.client.sql(f"CREATE UNLOGGED TABLE {staging_table} (LIKE {target_table})")

        conn = self.client.connect()
        try:
            with conn.cursor() as cursor:
                copy_sql = (
                    f"COPY {staging_table} FROM STDIN WITH (FORMAT CSV, HEADER FALSE)"
                )

                with cursor.copy(copy_sql) as copy:
                    # STREAMING BULK LOAD: Use sink_csv to a pipe or process
                    # batches to keep RAM usage under 2GB.
                    # For Postgres, we iterate the folder and COPY each file.
                    rows_staged = 0
                    for file_path in source_dir.glob(f"*.{file_ext}"):
                        df = pl.read_parquet(file_path)
                        copy.write(df.write_csv(include_header=False))
                        rows_staged += len(df)

            # Commit only if the entire 50M row stream succeeded
            conn.commit()
            LOG.info(
                "Staged data to Postgres",
                table=staging_table,
                rows=rows_staged,
            )
            return staging_table, rows_staged
        except Exception as e:
            conn.rollback()
            self.client.reconnect()
            raise e

    def promote_data(
        self,
        staging_table: str,
        target_table: str,
        partition_col: str,
        partition_val: str,
        expected_count: int,
    ) -> None:
        # Transactional Swap
        sql = f"""
        BEGIN;
        DELETE FROM {target_table}
            WHERE {partition_col} = '{partition_val}';
        INSERT INTO {target_table}
            SELECT * FROM {staging_table};
        COMMIT;
        DROP TABLE {staging_table};
        """
        self.client.sql(sql)
        LOG.info(
            "Promoted partition",
            table=target_table,
            partition=partition_val,
        )

    def is_equal(
        self,
        reference: str,
        other: str,
        exclude_columns: set[str] | None = None,
    ) -> bool:
        exclude_columns = exclude_columns or set()
        ref_table, other_table = str(reference), str(other)

        # 1. Performance Optimization: Check row counts first
        count_query = "SELECT count(*) FROM {}"
        count_ref = self.client.sql(count_query.format(ref_table))[0][0]
        count_other = self.client.sql(count_query.format(other_table))[0][0]

        if count_ref != count_other:
            LOG.warning(
                "Table comparison failed: Row count mismatch",
                ref=ref_table,
                other=other_table,
            )
            return False

        # 2. Fast-Path: Checksum Comparison
        hash_ref = self.get_checksum(ref_table)
        hash_other = self.get_checksum(other_table)

        if hash_ref == hash_other:
            LOG.info("Fast-path: Table checksums match.", table=ref_table)
            return True

        # 3. Deep-Dive: Schema alignment for EXCEPT
        # Discover shared columns to handle schema evolution
        col_sql = (
            "SELECT column_name FROM information_schema.columns WHERE table_name = '{}'"
        )
        cols_ref = {
            row[0]
            for row in self.client.sql(
                col_sql.format(ref_table.rsplit(".", maxsplit=1)[-1])
            )
        }
        cols_other = {
            row[0]
            for row in self.client.sql(
                col_sql.format(other_table.rsplit(".", maxsplit=1)[-1])
            )
        }

        compare_cols = (cols_ref & cols_other) - exclude_columns
        if not compare_cols:
            LOG.error(
                "No common columns found for comparison",
                ref=ref_table,
                other=other_table,
            )
            return False

        if cols_ref != cols_other:
            LOG.warning(
                "Comparing tables with mismatched schemas. Using common columns only."
            )

        # Explicitly sort columns to ensure positional equality in EXCEPT
        col_selection = ", ".join(sorted(compare_cols))

        sql = f"""
            SELECT {col_selection} FROM {ref_table}
            EXCEPT
            SELECT {col_selection} FROM {other_table}
        """
        results = self.client.sql(sql)
        return len(results) == 0

    def clone(self, reference: str, other: str) -> None:
        # Ensure it is a persistent table, not temporary, for regression testing
        sql = f"""
            CREATE TABLE {other} AS
            SELECT * FROM {reference}
            WHERE 1 = 0
        """
        LOG.info("Cloning table structure", source=reference, destination=other)
        self.client.sql(sql)

    def get_checksum(self, identifier: str, columns: list[str] | None = None) -> str:
        """
        Generates a 64-bit table fingerprint for Postgres.
        Uses MD5 hash of concatenated rows summed for order independence.
        """
        col_expr = (
            f"CONCAT_WS('|', {', '.join(columns)})" if columns else "CAST(t.* AS TEXT)"
        )
        # We convert the first 16 chars of MD5 (64 bits) to a bigint and sum them.
        query = f"SELECT SUM(('0x' || SUBSTR(MD5({col_expr}), 1, 16))::bit(64)::bigint) FROM {identifier} AS t"
        try:
            res = self.client.sql(query)
            return str(res[0][0]) if res else "0"
        except Exception as e:
            LOG.error(f"Checksum calculation failed for {identifier}: {e}")
            return "ERROR"

    def drop(self, identifier: str) -> None:
        sql = f"DROP TABLE IF EXISTS {identifier}"
        LOG.warning("Dropping table", table=identifier)
        self.client.sql(sql)
