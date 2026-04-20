import time
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
    def _init_client(self, **config: Any) -> PostgresClient:
        secret: Secret = config["password"]
        return PostgresClient(
            host=config["host"],
            database=config["database"],
            user=config["user"],
            password=secret.resolve(sanitize=True),
            port=config.get("port", 5432),
        )

    def stage_data(
        self,
        source_dir: Path,
        target_table: str,
        file_ext: str = "parquet",
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
            CREATE TEMPORARY TABLE {other} AS 
            SELECT * FROM {reference} 
            WHERE 1 = 0
        """
        LOG.info("Cloning table structure", source=reference, destination=other)
        self.client.sql(sql)
