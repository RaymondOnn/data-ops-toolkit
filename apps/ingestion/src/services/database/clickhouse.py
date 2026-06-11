"""ClickHouse service implementation."""

import re
from collections.abc import Sequence
from functools import cached_property
from pathlib import Path
from typing import Any

from apps.ingestion.src.services.database.base import DatabaseSink, DatabaseSource
from apps.ingestion.src.services.factory import ServiceFactory
from libs.database.clients.clickhouse import ClickhouseClient
from libs.utils.dates import current_timestamp
from loguru import logger

LOG = logger


@ServiceFactory.register("clickhouse_db")
class ClickHouseService(DatabaseSource, DatabaseSink):
    @cached_property
    def client(self) -> ClickhouseClient:
        raw_pwd = self._config.get("password", "")
        password = (
            raw_pwd.resolve(url_encode=True)
            if hasattr(raw_pwd, "resolve")
            else str(raw_pwd)
        )
        return ClickhouseClient(
            host=self._config.get("host"),
            port=self._config.get("port", 8123),
            user=self._config.get("user"),
            password=password,
            database=self._config.get("database"),
        )

    def close(self) -> None:
        if "client" in self.__dict__:
            if client := self.__dict__["client"]:
                try:
                    client.close()
                    LOG.info("Closed ClickHouse client", service=self.name)
                except Exception as e:
                    LOG.warning(f"Error closing ClickHouse: {e}")
            del self.__dict__["client"]

    def stage(
        self,
        source_dir: Path,
        target: str,
        expected_count: int,
        file_ext: str = "parquet",
        audit_values: dict[str, Any] | None = None,
    ) -> tuple[str, int]:
        parts = target.split(".", 1)
        db = parts[0] if len(parts) > 1 else None
        table = parts[-1]

        timestamp = current_timestamp(naive=True).strftime("%Y%m%d%H%M%S")
        staging = f"stg_{table}_{timestamp}"
        full_staging = f"{db}.{staging}" if db else staging

        success = False
        try:
            self.client.sql(
                f"""
                    CREATE OR REPLACE TABLE {full_staging}
                    ENGINE = MergeTree()
                    ORDER BY tuple()
                    AS {target}
                """
            )
            self.client.copy_from_file(
                table=full_staging,
                source_dir=str(source_dir),
                file_ext=file_ext,
                audit_values=audit_values or {},
            )
            rows = self.count_rows(full_staging)

            if rows != expected_count:
                raise ValueError(
                    f"Row count mismatch: expected {expected_count}, got {rows}"
                )

            success = True
            return full_staging, rows

        except Exception:
            LOG.exception("Staging failed")
            raise
        finally:
            if not success:
                self.client.sql(f"DROP TABLE IF EXISTS {full_staging}")
                LOG.warning(f"Cleaned up failed staging: {full_staging}")

    def promote(
        self,
        staging: str,
        target: str,
        partition_by: str,
        partition_value: str,
        expected_count: int,
    ) -> None:
        # Validate schema
        target_cols = {row[0]: row[1] for row in self.fetch(f"DESCRIBE TABLE {target}")}
        staging_cols = {
            row[0]: row[1] for row in self.fetch(f"DESCRIBE TABLE {staging}")
        }

        if target_cols != staging_cols:
            missing = set(target_cols) - set(staging_cols)
            extra = set(staging_cols) - set(target_cols)
            raise ValueError(f"Schema mismatch - missing: {missing}, extra: {extra}")

        success = False
        try:
            self.client.sql(
                f"DELETE FROM {target} WHERE {partition_by} = '{partition_value}'"
            )
            self.client.sql(f"INSERT INTO {target} SELECT * FROM {staging}")
            promoted = self.count_rows(target, f"{partition_by} = '{partition_value}'")
            if promoted != expected_count:
                raise ValueError(
                    f"Row count mismatch after promotion: "
                    f"expected {expected_count}, got {promoted}"
                )
            success = True
            LOG.success(f"Promoted {partition_by}={partition_value} to {target}")

        finally:
            if success:
                self.client.sql(f"DROP TABLE IF EXISTS {staging}")
                LOG.info(f"Cleaned up staging: {staging}")

    def is_equal(
        self,
        ref: str,
        other: str,
        exclude_columns: set[str] | None = None,
    ) -> bool:
        if self.count_rows(ref) != self.count_rows(other):
            return False
        if self._get_checksum(ref) == self._get_checksum(other):
            return True
        return self._minus(ref, other, exclude_columns) == 0

    def clone(self, source: Any, dest: Any) -> None:
        self.client.sql(f"""
                CREATE TABLE OR REPLACE {dest}
                ENGINE = MergeTree() AS
                    SELECT * FROM {source}
                    WHERE 1=0
            """)
        LOG.info(f"Cloned {source} -> {dest}")

    def _minus(
        self,
        ref: str,
        other: str,
        exclude_columns: set[str] | None = None,
    ) -> int:
        exclude = exclude_columns or set()
        cols_ref = {
            row[0].decode() if isinstance(row[0], bytes) else row[0]
            for row in self.fetch(f"DESCRIBE TABLE {ref}")
        }
        cols_other = {
            row[0].decode() if isinstance(row[0], bytes) else row[0]
            for row in self.fetch(f"DESCRIBE TABLE {other}")
        }

        common = (cols_ref & cols_other) - exclude
        if not common:
            raise ValueError(
                f"No common columns found. " f"ref: {cols_ref}, other: {cols_other}"
            )

        cols = ", ".join(sorted(common))
        result = self.client.sql(f"""
            SELECT count() FROM (
                SELECT {cols} FROM {ref}
                EXCEPT
                SELECT {cols} FROM {other}
            )""")
        return int(result[0][0]) if result else 0

    def _get_checksum(self, name: str, columns: list[str] | None = None) -> str:
        cols = ", ".join(columns) if columns else "*"
        try:
            result = self.client.sql(
                f"SELECT hex(groupBitXor(cityHash64({cols}))) FROM {name}"
            )
            return str(result[0][0]) if result else "0"
        except Exception:
            LOG.exception(f"Checksum failed for {name}")
            return "ERROR"

    def delete(self, target: str) -> None:
        self.client.sql(f"DROP TABLE IF EXISTS {target}")
        LOG.warning(f"Dropped table: {target}")

    def count_rows(
        self, target: str, filter_condition: str | None = None, **kwargs
    ) -> int:
        where = (
            re.sub(r"(?i)^where\s+", "", filter_condition.strip())
            if filter_condition
            else "1=1"
        )
        query = f"SELECT COUNT(*) FROM {target} WHERE {where.rstrip('; ')}"
        try:
            result = self.client.sql(query)
            return int(result[0][0]) if result else 0
        except Exception:
            LOG.exception(f"Count failed for {target}")
            return 0

    def fetch(self, query: str) -> list[Sequence[Any]]:
        return self.client.sql(query)
