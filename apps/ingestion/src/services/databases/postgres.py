"""PostgreSQL service implementation."""

import time
from functools import cached_property
from pathlib import Path
from typing import Any

import polars as pl
from libs.database.clients.postgres import PostgresClient
from loguru import logger

from src.services.database.base import DatabaseSink, DatabaseSource
from src.services.factory import ServiceFactory

LOG = logger


@ServiceFactory.register("postgres_db")
class PostgresService(DatabaseSource, DatabaseSink):
    @cached_property
    def client(self) -> PostgresClient:
        secret = self._config["password"]
        return PostgresClient(
            host=self._config["host"],
            database=self._config["database"],
            user=self._config["user"],
            password=secret.resolve(url_encode=True),
            port=self._config.get("port", 5432),
        )

    def stage(
        self,
        source_dir: Path,
        target: str,
        expected_count: int,
        file_ext: str = "parquet",
        audit_values: dict[str, Any] | None = None,
    ) -> tuple[str, int]:
        staging = f"stg_{target}_{int(time.time())}"
        self.client.sql(f"CREATE UNLOGGED TABLE {staging} (LIKE {target})")

        conn = self.client.connect()
        try:
            with conn.cursor() as cur:
                copy_sql = f"COPY {staging} FROM STDIN WITH (FORMAT CSV, HEADER FALSE)"
                with cur.copy(copy_sql) as copy:
                    rows = 0
                    for f in source_dir.glob(f"*.{file_ext}"):
                        df = pl.read_parquet(f)
                        copy.write(df.write_csv(include_header=False))
                        rows += len(df)
            conn.commit()
            LOG.info(f"Staged {rows:_} rows to {staging}")
            return staging, rows
        except Exception:
            conn.rollback()
            self.client.reconnect()
            raise

    def promote(
        self,
        staging: str,
        target: str,
        partition_on: str,
        partition_value: str,
        expected_count: int,
    ) -> None:
        sql = f"""
        BEGIN;
        DELETE FROM {target} WHERE {partition_on} = '{partition_value}';
        INSERT INTO {target} SELECT * FROM {staging};
        COMMIT;
        DROP TABLE {staging};
        """
        self.client.sql(sql)
        LOG.info(f"Promoted to {target} partition {partition_value}")

    def clone(self, source: str, dest: str) -> None:
        self.client.sql(f"CREATE TABLE {dest} AS SELECT * FROM {source} WHERE 1=0")
        LOG.info(f"Cloned {source} -> {dest}")

    def is_equal(
        self,
        ref: str,
        other: str,
        exclude_columns: set[str] | None = None,
    ) -> bool:
        exclude = exclude_columns or set()
        count_ref = self.client.sql(f"SELECT COUNT(*) FROM {ref}")[0][0]
        count_other = self.client.sql(f"SELECT COUNT(*) FROM {other}")[0][0]
        if count_ref != count_other:
            return False

        if self.get_checksum(ref) == self.get_checksum(other):
            return True

        def get_cols(table: str) -> set[str]:
            return {
                row[0]
                for row in self.client.sql(f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = '{table.rsplit('.', maxsplit=1)[-1]}'
                """)
            }

        common = (get_cols(ref) & get_cols(other)) - exclude
        if not common:
            return False

        cols = ", ".join(sorted(common))
        result = self.client.sql(
            f"SELECT {cols} FROM {ref} EXCEPT SELECT {cols} FROM {other}"
        )
        return len(result) == 0

    def get_checksum(self, name: str, columns: list[str] | None = None) -> str:
        col_expr = (
            f"CONCAT_WS('|', {', '.join(columns)})" if columns else "CAST(t.* AS TEXT)"
        )
        try:
            result = self.client.sql(f"""
                SELECT SUM(
                    ('x' || SUBSTR(MD5({col_expr}), 1, 16))::bit(64)::bigint
                )
                FROM {name} AS t
            """)
            return str(result[0][0]) if result else "0"
        except Exception:
            LOG.exception(f"Checksum failed for {name}")
            return "ERROR"

    def delete(self, target: str) -> None:
        self.client.sql(f"DROP TABLE IF EXISTS {target}")
        LOG.warning(f"Dropped table: {target}")
