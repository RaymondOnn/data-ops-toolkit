"""Oracle service implementation."""

from functools import cached_property
from pathlib import Path
from typing import Any

from libs.database.clients.oracle import OracleClient
from loguru import logger

from src.services.database.base import DatabaseSink, DatabaseSource
from src.services.factory import ServiceFactory

LOG = logger


@ServiceFactory.register("oracle_db")
class OracleService(DatabaseSource, DatabaseSink):
    @cached_property
    def client(self) -> OracleClient:
        secret = self._config["password"]
        return OracleClient(
            user=self._config["user"],
            password=secret.resolve(url_encode=True),
            dsn=self._config["dsn"],
        )

    def stage(
        self,
        source_dir: Path,
        target: str,
        expected_count: int,
        file_ext: str = "parquet",
        audit_values: dict[str, Any] | None = None,
    ) -> tuple[str, int]:
        staging = f"STG_{target.upper()}"
        sql = f"""
        CREATE TABLE {staging} ORGANIZATION EXTERNAL (
            TYPE ORACLE_BIGDATA
            ACCESS PARAMETERS (com.oracle.bigdata.fileformat={file_ext})
            LOCATION ('{source_dir}/*.{file_ext}')
        ) REJECT LIMIT UNLIMITED
        """
        self.client.sql(sql)
        result = self.client.sql(f"SELECT COUNT(*) FROM {staging}")
        rows = int(result[0][0]) if result else 0
        LOG.info(f"Staged {rows:_} rows to {staging}")
        return staging, rows

    def promote(
        self,
        staging: str,
        target: str,
        partition_on: str,
        partition_value: str,
        expected_count: int,
    ) -> None:
        sql = f"""
        BEGIN
            DELETE FROM {target} WHERE {partition_on} = '{partition_value}';
            INSERT /*+ APPEND */ INTO {target} SELECT * FROM {staging};
            COMMIT;
            EXECUTE IMMEDIATE 'DROP TABLE {staging}';
        EXCEPTION WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
        END;
        """
        self.client.sql(sql)
        LOG.info(f"Promoted to {target} partition {partition_value}")

    def is_equal(
        self,
        ref: str,
        other: str,
        exclude_columns: set[str] | None = None,
    ) -> bool:
        exclude = exclude_columns or set()
        ref, other = ref.upper(), other.upper()

        count_ref = self.client.sql(f"SELECT COUNT(*) FROM {ref}")[0][0]
        count_other = self.client.sql(f"SELECT COUNT(*) FROM {other}")[0][0]
        if count_ref != count_other:
            return False

        if self.get_checksum(ref) == self.get_checksum(other):
            return True

        cols_sql = "SELECT column_name FROM all_tab_columns WHERE table_name = '{}'"
        cols_ref = {row[0] for row in self.client.sql(cols_sql.format(ref))}
        cols_other = {row[0] for row in self.client.sql(cols_sql.format(other))}
        common = (cols_ref & cols_other) - {c.upper() for c in exclude}

        if not common:
            return False

        cols = ", ".join(sorted(common))
        result = self.client.sql(
            f"SELECT {cols} FROM {ref} MINUS SELECT {cols} FROM {other}"
        )
        return len(result) == 0

    def clone(self, source: str, dest: str) -> None:
        self.client.sql(f"CREATE TABLE {dest} AS SELECT * FROM {source} WHERE 1=0")
        LOG.info(f"Cloned {source} -> {dest}")

    def get_checksum(self, name: str, columns: list[str] | None = None) -> str:
        col_expr = " || '|' || ".join(columns) if columns else "RAWTOHEX(SYS_GUID())"
        try:
            result = self.client.sql(f"SELECT SUM(ORA_HASH({col_expr})) FROM {name}")
            return str(result[0][0]) if result else "0"
        except Exception:
            LOG.exception(f"Checksum failed for {name}")
            return "ERROR"

    def delete(self, target: str) -> None:
        self.client.sql(f"DROP TABLE {target} PURGE")
        LOG.warning(f"Dropped table: {target}")
