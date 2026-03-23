import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import structlog
from src.services.database.base import DatabaseSink, DatabaseSource
from src.services.factory import ServiceFactory

from libs.database.clients.postgres import PostgresClient

if TYPE_CHECKING:
    from libs.auth.models import Secret

LOG = structlog.get_logger(__name__)


@ServiceFactory.register("postgres_db")
class PostgresService(DatabaseSource, DatabaseSink):
    def __init__(self, name: str, **config: Any) -> None:
        super().__init__(name, **config)
        self.client = self._init_client(**config)

    def _init_client(self, **config: Any) -> PostgresClient:
        secret: Secret = config["password"]
        return PostgresClient(
            host=config["host"],
            database=config["database"],
            user=config["user"],
            password=secret.resolve(sanitize=True),
            port=config.get("port", 5432),
        )

    def stage_data(self, source_dir: Path, target_table: str, file_ext: str = "parquet") -> tuple[str, int]:
        staging_table = f"stg_{target_table}_{int(time.time())}"
        self.client.sql(f"CREATE UNLOGGED TABLE {staging_table} (LIKE {target_table})")

        # Polars scan_parquet handles a directory path natively.
        # It will treat all parquet files in the folder as a single dataset.
        lf = pl.scan_parquet(f"{source_dir}/*.{file_ext}")

        # 1. Get the connection from the DBAPI
        conn = self.client.connect()

        try:
            with conn.cursor() as cursor:
                # 2. Open the COPY pipe
                copy_sql = (
                    f"COPY {staging_table} FROM STDIN WITH (FORMAT CSV, HEADER FALSE)"
                )

                with cursor.copy(copy_sql) as copy:
                    # Stream in 100k chunks to keep RAM flat
                    df = lf.collect()
                    if not isinstance(df, pl.DataFrame):
                        raise TypeError(f"Expected polars.DataFrame, got {type(df)}")

                    rows_staged = df.height

                    for batch_df in df.iter_slices(n_rows=100_000):
                        # write_csv returns bytes, which we feed into the copy pipe
                        copy.write(batch_df.write_csv(include_header=False))

            # Commit only if the entire 50M row stream succeeded
            conn.commit()
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
            CREATE TEMPORARY TABLE {other} AS 
            SELECT * FROM {reference} 
            WHERE 1 = 0
        """
        self.client.sql(sql)
