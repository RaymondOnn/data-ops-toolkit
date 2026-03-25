import time
from pathlib import Path
from typing import Any

import structlog
from apps.ingestion.src.services.database.base import DatabaseSink, DatabaseSource
from apps.ingestion.src.services.factory import ServiceFactory
from libs.database.clients.chdb import ChDBClient

LOG = structlog.get_logger(__name__)


@ServiceFactory.register("chdb")
class ChDBService(DatabaseSource, DatabaseSink):
    def _init_client(self, **config: Any) -> ChDBClient:
        # chDB only needs a path for persistence
        return ChDBClient(
            path=config.get("path", "./.chdb_data")
        )

    def stage_data(
        self, source_dir: Path, target_table: str, file_ext: str = "parquet"
    ) -> tuple[str, int]:
        staging_table = f"stg_{target_table}_{int(time.time())}"

        # 1. Create Staging Table (Matches Target Structure)
        # chDB supports 'CREATE TABLE AS'
        self.client.sql( f"""
                CREATE TABLE IF NOT EXISTS {staging_table} 
                ENGINE = Log AS 
                    SELECT * FROM {target_table} 
                    LIMIT 0
            """
        )

        try:
            # 2. Insert directly from filesystem
            # chDB can read local files easily since it runs in the same process
            path_pattern = source_dir / f"*.{file_ext}"
            sql = f"""
                INSERT INTO {staging_table} 
                SELECT * FROM file('{path_pattern}', '{file_ext}')
            """

            self.client.sql(sql)
            res = self.client.sql(f"SELECT COUNT(*) FROM {staging_table}")
            rows_staged = int(res[0][0]) if res and res[0] else 0

            LOG.info(
                "Staged data to chDB",
                table=staging_table,
                rows=rows_staged,
            )
            return staging_table, rows_staged

        except Exception:
            self.client.sql(f"DROP TABLE IF EXISTS {staging_table}")
            raise

    def promote_data(
        self,
        staging_table: str,
        target_table: str,
        partition_col: str,
        partition_val: str,
    ) -> None:
        """
        chDB (MergeTree) supports REPLACE PARTITION just like ClickHouse server.
        """
        sql = f"""
            ALTER TABLE {target_table} 
            REPLACE PARTITION '{partition_val}' 
            FROM {staging_table}
        """
        try:
            self.client.sql(sql)
        finally:
            LOG.info(
                "Promoted partition (Local chDB)",
                table=target_table,
                partition=partition_val,
            )
            self.client.sql(f"DROP TABLE IF EXISTS {staging_table}")

    def is_equal(
        self,
        reference: Path,
        other: Path,
        exclude_columns: list[str] | None = None,
    ) -> bool:
        exclude_columns = exclude_columns or []
        exclude_str = (
            f"EXCEPT ({', '.join(exclude_columns)})" if exclude_columns else ""
        )

        sql = f"""
            SELECT * {exclude_str} FROM {reference}
            EXCEPT
            SELECT * {exclude_str} FROM {other}
        """
        results = self.sql(sql)
        is_match = len(results) == 0
        if not is_match:
            LOG.warning("Table comparison failed", differences=len(results))
        return is_match

    def clone(self, reference: str, other: str) -> None:
        sql = f"""
            CREATE TABLE {other} ENGINE = MergeTree() AS 
            SELECT * FROM {reference}
            WHERE 1 = 0
        """
        self.client.sql(sql)
