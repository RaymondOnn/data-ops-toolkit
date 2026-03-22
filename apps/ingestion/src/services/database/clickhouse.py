import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog
from src.services.database.base import DatabaseSink, DatabaseSource
from src.services.factory import ServiceFactory

from libs.clients.database.clickhouse import ClickhouseClient

if TYPE_CHECKING:
    from libs.auth.models import Secret

LOG = structlog.get_logger(__name__)


@ServiceFactory.register("clickhouse_db")
class ClickHouseService(DatabaseSource, DatabaseSink):
    def _init_client(self, **config: Any) -> ClickhouseClient:
        secret: Secret = config["password"]
        return ClickhouseClient(
            host=config.get("host", "localhost"),
            port=config.get("port", 8123),
            user=config.get("user", "default"),
            password=secret.resolve(sanitize=True) if secret else "",
        )

    def stage_data(self, source_dir: Path, target_table: str) -> tuple[str, int]:
        staging_table = f"stg_{target_table}_{int(time.time())}"
        try:
            self.client.sql(f"CREATE TEMPORARY TABLE {staging_table} AS {target_table}")

            # ClickHouse pulls the folder directly - no Python RAM used
            path_pattern = source_dir / "*.parquet"
            sql = f"""
                INSERT INTO {staging_table} 
                SELECT * FROM file('{path_pattern}', 'Parquet')
            """

            self.client.sql(sql)
            res = self.client.sql(f"SELECT COUNT(*) FROM {staging_table}")
            rows_staged = int(res[0][0]) if res and res[0] else 0
            return staging_table, rows_staged

        except Exception:
            # Cleanup staging on failure to prevent orphan temp tables
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
        Atomic metadata swap.
        ClickHouse moves the actual data parts on disk
        Note: {partition_val} must match the internal ClickHouse partition ID format.
        """
        sql = f"""
            ALTER TABLE {target_table} 
            REPLACE PARTITION '{partition_val}' 
            FROM {staging_table}
        """
        try:
            self.client.sql(sql)
        finally:
            # Always drop the staging table after the swap attempt
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
            SELECT * {exclude_str}
            FROM {reference}
            EXCEPT
            SELECT * {exclude_str}
            FROM {other}
        """
        results = self.sql(sql)

        return len(results) == 0

    def clone(self, reference: str, other: str) -> None:
        sql = f"""
            CREATE TEMPORARY TABLE {other} ENGINE = MergeTree() AS 
            SELECT * FROM {reference}
            WHERE 1 = 0
        """
        self.client.sql(sql)
