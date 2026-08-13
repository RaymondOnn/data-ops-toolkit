import logging
from pathlib import Path
from typing import Any

import pyarrow as pa
from duckdb import DuckDBPyConnection

from .base import DBClient
from .factory import DatabaseFactory

LOG = logging.getLogger(__name__)


@DatabaseFactory.register
class DuckDBClient(DBClient):
    type: str = "duckdb"

    def __init__(self, **config):
        super().__init__(**config)
        self.conn = self.connect()

        # Ensure base remote extensions are available
        self._ensure_extension("httpfs")

        # Performance Tuning
        threads = self.config.get("threads", 4)
        max_memory = self.config.get("max_memory", "8GB")
        self.command(f"SET threads = {threads};")
        self.command(f"SET max_memory = '{max_memory}';")
        self.command("SET preserve_insertion_order = false;")
        self.command("SET enable_object_cache = true;")

    def connect(self) -> DuckDBPyConnection:
        import duckdb

        from libs.clients.base import ClientCantConnect

        try:
            conn = duckdb.connect(
                database=self.config.get("database", ":memory:"), read_only=False
            )
            self._ping(conn)

            return conn
        except Exception as e:
            raise ClientCantConnect("Failed to connect to DuckDB") from e

    def _ping(self, conn: DuckDBPyConnection) -> None:
        conn.execute("SELECT 1")

    def command(self, sql: str, params: Any = None) -> None:
        self._execute(sql, params)

    def query(
        self,
        sql: str,
        params: Any = None,
        chunk_size: int | None = None,
    ) -> pa.Table | pa.RecordBatchReader:
        """Executes a SQL query and returns PyArrow Table or RecordBatchReader stream."""
        cursor = self._execute(sql, params)
        if chunk_size is not None:
            return cursor.fetch_record_batch(chunk_size)
        return cursor.fetch_arrow_table()

    def configure_temp_dir(
        self, temp_dir: str | Path, max_size: str | None = None
    ) -> None:
        """Configures temp directory and optional memory limits for out-of-core operations."""
        path = Path(temp_dir)
        path.mkdir(parents=True, exist_ok=True)
        self.command(f"SET temp_directory = '{path}'")
        if max_size:
            self.command(f"SET max_temp_directory_size = '{max_size}'")

    def _ensure_extension(self, ext_name: str, repository: str | None = None) -> None:
        ext = ext_name.lower()
        query = "SELECT extension_name, installed, loaded FROM duckdb_extensions() WHERE lower(extension_name) = ?"
        res = self.conn.execute(query, (ext,)).fetchone()

        is_installed = res[1] if res else False
        is_loaded = res[2] if res else False

        if not is_installed:
            LOG.debug(f"Extension '{ext}' not installed. Installing...")
            install_cmd = (
                f"INSTALL {ext} FROM {repository};" if repository else f"INSTALL {ext};"
            )
            self.command(install_cmd)

        if not is_loaded:
            LOG.debug(f"Loading extension '{ext}'...")
            self.command(f"LOAD {ext};")

    def resolve_source_expr(
        self,
        file_paths: str | Path | list[str] | list[Path],
        ext: str | None = None,
    ) -> str:
        """Resolves file path(s) into a generic DuckDB reader expression."""
        if isinstance(file_paths, str | Path):
            file_paths = [str(file_paths)]
        else:
            file_paths = [str(f) for f in file_paths]

        # Auto-detect extension if not explicitly supplied
        if not ext and file_paths:
            ext = Path(file_paths[0]).suffix.lstrip(".").lower()
        elif not ext:
            ext = "parquet"

        escaped_files = [f.replace("'", "''") for f in file_paths]
        file_list_sql = "[" + ", ".join(f"'{f}'" for f in escaped_files) + "]"

        match ext.lower():
            case "csv":
                source_expr = (
                    f"read_csv({file_list_sql}, union_by_name=True, ignore_errors=True)"
                )
            case "json" | "jsonl":
                source_expr = f"read_json_auto({file_list_sql})"
            case "parquet":
                source_expr = f"read_parquet({file_list_sql})"
            case "xml":
                try:
                    self._ensure_extension("webbed", "community")
                    source_expr = f"read_xml({file_list_sql})"
                except Exception as e:
                    LOG.warning(
                        f"Could not load webbed XML extension ({e}). Reading raw text."
                    )
                    source_expr = f"read_text({file_list_sql})"
            case "delta":
                self._ensure_extension("delta")
                source_expr = (
                    f"delta_scan({file_list_sql})"
                    if len(file_paths) > 1
                    else f"delta_scan('{escaped_files[0]}')"
                )
            case "iceberg":
                self._ensure_extension("iceberg")
                source_expr = (
                    f"iceberg_scan({file_list_sql})"
                    if len(file_paths) > 1
                    else f"iceberg_scan('{escaped_files[0]}')"
                )
            case _:
                LOG.info(
                    "Unsupported extension. Attempting to select directly from file/s."
                )
                source_expr = (
                    file_list_sql if len(file_paths) > 1 else f"'{escaped_files[0]}'"
                )
        return source_expr

    def register_view(
        self,
        view_name: str,
        source: str,
        is_expression: bool = True,
    ) -> None:
        """Registers a SQL expression/query or raw path as a named view."""
        sql_src = source if is_expression else f"'{source}'"
        self.command(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {sql_src}")
        LOG.debug(f"Registered view '{view_name}' pointing to {sql_src}")

    def register_source_view(
        self,
        view_name: str,
        paths: str | Path | list[str] | list[Path],
        ext: str | None = None,
    ) -> None:
        """Generic view registration handling single files, directories, globs, or lists."""
        if isinstance(paths, Path):
            paths = str(paths)

        # If a directory is provided, append glob matching pattern
        if isinstance(paths, str) and Path(paths).is_dir():
            file_ext = ext or "parquet"
            paths = str(Path(paths) / f"*.{file_ext}")
            if not ext:
                ext = file_ext

        source_expr = self.resolve_source_expr(file_paths=paths, ext=ext)
        self.register_view(view_name, source_expr, is_expression=True)

    def copy_to_parquet(
        self,
        query: str,
        output_path: str | Path,
        per_thread: bool = True,
        **copy_options: Any,
    ) -> None:
        """Exports query results directly into Parquet files."""
        out_path = Path(output_path)
        out_path.mkdir(parents=True, exist_ok=True)

        opts = ["FORMAT PARQUET", f"PER_THREAD_OUTPUT {str(per_thread).upper()}"]
        for k, v in copy_options.items():
            opts.append(f"{k.upper()} {v}")

        options_str = ", ".join(opts)
        self.command(f"COPY ({query}) TO '{out_path}' ({options_str})")

    def copy(
        self,
        table: str,
        source_dir: str,
        file_ext: str = "parquet",
        audit_values: dict[str, Any] | None = None,
    ) -> None:
        pattern = f"{source_dir}/*.{file_ext}"
        self.command(f"INSERT INTO {table} SELECT * FROM read_parquet('{pattern}')")

    def _execute(self, query: str, params: tuple | dict | None = None) -> Any:
        if params:
            return self.conn.execute(query, params)
        return self.conn.execute(query)

    def close(self) -> None:
        self.conn.close()
