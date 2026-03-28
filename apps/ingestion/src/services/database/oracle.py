from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog
from apps.ingestion.src.services.database.base import DatabaseSink, DatabaseSource
from apps.ingestion.src.services.factory import ServiceFactory
from libs.database.clients.oracle import OracleClient

if TYPE_CHECKING:
    from libs.auth.models import Secret


LOG = structlog.get_logger(__name__)


@ServiceFactory.register("oracle_db")
class OracleService(DatabaseSource, DatabaseSink):
    def _init_client(self, **config: Any) -> Any:
        secret: Secret = config["password"]
        return OracleClient(
            user=config["user"],
            password=secret.resolve(sanitize=True),
            dsn=config["dsn"],
        )

    def stage_data(
        self,
        source_dir: Path,
        target_table: str,
        file_ext: str = "parquet",
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
        reference: Path,
        other: Path,
        exclude_columns: set[str] | None = None,
    ) -> bool:
        pass
        exclude_columns = exclude_columns or set()
        exclude_str = (
            f"EXCEPT ({', '.join(exclude_columns)})" if exclude_columns else ""
        )

        sql = f"""
            SELECT * {exclude_str}
            FROM {reference}
            MINUS
            SELECT * {exclude_str}
            FROM {other}
        """
        results = self.sql(sql)
        is_match = len(results) == 0
        LOG.info(
            "Comparing tables", reference=reference, other=other, is_match=is_match
        )
        if not is_match:
            LOG.warning("Table comparison failed", differences=len(results))
        return is_match

    def clone(self, reference: str, other: str) -> None:
        sql = f"""
            CREATE TEMPORARY TABLE {other} AS 
            SELECT * FROM {reference} 
            WHERE 1 = 0
        """
        LOG.info("Cloning table structure", source=reference, destination=other)
        self.client.sql(sql)
