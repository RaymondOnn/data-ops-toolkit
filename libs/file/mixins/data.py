import logging
from pathlib import Path
from typing import Any

import msgspec
import polars as pl
import pyarrow as pa
from libs.database.clients.duckdb import DuckDBClient
from libs.database.sql import SelectQueryContext, SQLCompiler, SQLContext
from libs.file import FileSystemClient
from libs.file.archive import ArchiveContext
from libs.file.formats.factory import FormatFactory
from libs.file.utils import get_file_ext

LOG = logging.getLogger(__name__)


class FileReader:
    """Mixin providing high-volume file I/O and explicit archive virtualization.

    Notes:
    - By leveraging fsspec, this mixin allows the same ingestion logic to
      operate seamlessly across S3, Azure Blob, and Local filesystems.
    - The mixin accepts explicit archive_path parameters instead of '::'
      string delimiters, providing a cleaner API for structured configurations.
    """

    def __init__(
        self,
        fs: FileSystemClient,
        **conn_kwargs,
    ):
        self.fs = fs
        # Wrap DuckDB inside DatabaseConnector for standardized SQL execution
        self.db = DuckDBClient(**conn_kwargs)
        self.sql = SQLCompiler(dialect="duckdb")

        # Pass fsspec handle so DuckDB can stream s3:// or az:// paths directly
        self._register_fsspec()

    def _register_fsspec(self) -> None:
        try:
            protocol = self.fs.fs.protocol
            if isinstance(protocol, tuple):
                protocol = protocol[0]
            if protocol not in ("file", "local"):
                self.db.conn.register_filesystem(self.fs.fs)
                LOG.debug(f"Registered fsspec filesystem protocol: {protocol}")
        except Exception as e:
            LOG.warning(f"Could not register fsspec filesystem: {e}")

    def _convert_format(
        self,
        source_path: str,
        target_path: str,
        source_format: str = "csv",
        target_format: str = "parquet",
        partition_cols: list[str] | None = None,
    ) -> None:
        """
        Converts files directly on disk/cloud using zero-copy DuckDB execution.
        Supports Hive-style automatic directory partitioning.
        """
        src = self.fs.resolve(source_path)
        dst = self.fs.resolve(target_path)

        # Build Partition Clause if specified
        partition_clause = ""
        if partition_cols:
            cols_str = ", ".join(f"'{c}'" for c in partition_cols)
            partition_clause = f", PARTITION_BY ({cols_str})"

        # Handle formatting options
        if source_format.lower() in ("json", "jsonl"):
            read_stmt = f"read_json_auto('{src}')"
        elif source_format.lower() == "csv":
            read_stmt = f"read_csv('{src}')"
        else:
            read_stmt = f"SELECT * FROM '{src}'"

        sql = f"""
            COPY (SELECT * FROM {read_stmt})
            TO '{dst}' (FORMAT '{target_format.upper()}'{partition_clause})
        """
        LOG.info(f"Converting {src} -> {dst} (Format: {target_format})")
        self.db.command(sql)

    @staticmethod
    def _hydrate_sql_context(ctx: SQLContext, source_expr: str) -> SQLContext:
        """Injects the target file source expression into SQLContext if from_table and sql are missing."""
        # 1. Top-level raw SQL override: Do not modify
        if ctx.sql:
            return ctx

        new_ctes = list(ctx.ctes)
        new_main = ctx.main

        # 2. Case A: Context has CTEs -> Inject source_expr into the first CTE if unassigned
        if new_ctes:
            first_cte = new_ctes[0]
            if not first_cte.from_table and not first_cte.sql:
                updated_first = msgspec.structs.replace(
                    first_cte, from_table=source_expr
                )
                new_ctes[0] = updated_first

        # 3. Case B: Context has a main query -> Inject source_expr if unassigned
        elif new_main:
            if not new_main.from_table and not new_main.sql:
                new_main = msgspec.structs.replace(new_main, from_table=source_expr)

        # 4. Case C: Neither ctes nor main defined -> Create a default main block
        else:
            new_main = SelectQueryContext(from_table=source_expr)

        # Return updated SQLContext instance (handles frozen Structs safely)
        return msgspec.structs.replace(ctx, ctes=new_ctes, main=new_main)

    def _read(
        self,
        files: list[str],
        # sql_query: str | None = None,
        # where_clause: str | None = None,
        # select: list[str] | None = None,
        limit: int | None = None,
        sql_context: SQLContext | None = None,
        table_alias: str = "df",
        chunk_size: int = 50_000,
        format_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> pl.LazyFrame:
        """
        Streams a list of assigned files directly into a Polars LazyFrame.
        Handles LIMIT gracefully via early-stopping PyArrow stream iteration.
        """
        if not files:
            return pl.LazyFrame()

        resolved_files = [self.fs.resolve(f) for f in files]
        ext = get_file_ext(resolved_files[0])

        LOG.info(
            f"Streaming {len(resolved_files)} file(s) (type: '{ext}') "
            f"via DuckDB with chunk_size={chunk_size}."
        )

        # 1. Register view dynamically
        self.db.register_source_view(table_alias, resolved_files, ext=ext)

        # 2. SQL Context Hydration & Query Compilation
        if sql_context is None:
            sql_context = SQLContext(main=SelectQueryContext(from_table=table_alias))
        else:
            sql_context = self._hydrate_sql_context(sql_context, table_alias)

        final_query = self.sql.compile_context(sql_context)
        LOG.info(f"Streaming via DuckDB: {final_query}")

        # 3. Stream Arrow batches using unified query method
        arrow_reader = self.db.query(final_query, chunk_size=chunk_size)

        if not limit:
            res = pl.from_arrow(arrow_reader)
            df = res.to_frame() if isinstance(res, pl.Series) else res
            return df.lazy()

        # 4. Stream Batches with Early Stopping for LIMIT
        collected_batches: list[pa.Table] = []
        rows_accumulated = 0

        for batch in arrow_reader:
            if rows_accumulated + batch.num_rows > limit:
                remaining_needed = limit - rows_accumulated
                collected_batches.append(batch.slice(0, remaining_needed))
                LOG.debug(f"Reached LIMIT ({limit} rows). Early-stopping file stream.")
                arrow_reader.close()
                break

            collected_batches.append(batch)
            rows_accumulated += batch.num_rows

        if not collected_batches:
            return pl.LazyFrame()

        table = pa.Table.from_batches(collected_batches)
        res = pl.from_arrow(table)
        df = res.to_frame() if isinstance(res, pl.Series) else res
        return df.lazy()

    def extract(
        self,
        source: str | list[str],
        file_pattern: str | None = None,
        sql_context: SQLContext | None = None,
        table_alias: str = "df",
        chunk_size: int = 50_000,
        temp_folder: str | Path | None = None,
        **kwargs: Any,
    ) -> pl.LazyFrame:
        sources = [source] if isinstance(source, str) else source
        formatted_paths = [
            self.format_archive_path(f, file_pattern=file_pattern) for f in sources
        ]

        if not formatted_paths:
            raise FileNotFoundError(f"No files found at {source}")

        archive_files = [p for p in formatted_paths if "!!" in p]

        # Case 1: Archives present - extract members via context manager and stream
        if archive_files:
            base_archive, _ = archive_files[0].split("!!", 1)

            with ArchiveContext(self.fs, base_archive, temp_dir=temp_folder) as archive:
                ready_files: list[str] = []
                for path in formatted_paths:
                    if "!!" in path:
                        _, inner_file = path.split("!!", 1)
                        ready_files.append(archive.extract_member(inner_file))
                    else:
                        ready_files.append(self.fs.resolve(path))

                return self._read(
                    files=ready_files,
                    sql_context=sql_context,
                    table_alias=table_alias,
                    chunk_size=chunk_size,
                    **kwargs,
                )

        # Case 2: Standard non-archive files
        ready_files = [self.fs.resolve(p) for p in formatted_paths]
        return self._read(
            files=ready_files,
            # sql_query=sql_query,
            # where_clause=where_clause,
            # select=select,
            # limit=limit,
            sql_context=sql_context,
            table_alias=table_alias,
            chunk_size=chunk_size,
            **kwargs,
        )

    def load(
        self,
        df: pl.LazyFrame | pl.DataFrame,
        target_path: str,
        file_format: str = "parquet",
        **kwargs: Any,
    ) -> None:
        """
        Sinks a Polars LazyFrame or DataFrame out to storage by delegating
        directly to the appropriate FormatHandler from FormatFactory.
        """
        resolved_dst = self.fs.resolve(target_path)
        fmt = file_format.lower()

        LOG.info(f"Writing DataFrame to {resolved_dst} (Format: {fmt})")

        # 1. Instantiate format handler via factory
        handler = FormatFactory.get(fmt, self.fs, self.fs.opts)

        # 2. Delegate write logic to the format handler's from_df
        handler.from_df(df, resolved_dst, **kwargs)

    def format_archive_path(
        self,
        path: str,
        file_pattern: str | None = None,
    ) -> str:
        """Formats an archive or standard path into normalized virtual syntax.

        Args:
            path: Path to file or archive (e.g., 's3://bucket/data.zip', '/local/data.csv').
            file_pattern: Inner file path or glob pattern (e.g., 'sales.csv', '*.parquet').
                        Defaults to '*' to match all inner files if path is an archive.

        Examples:
            - ('s3://bucket/data.zip', None)            -> 'zip://s3://bucket/data.zip/*'
            - ('data.zip', 'folder/january.csv')        -> 'zip://data.zip/folder/january.csv'
            - ('data.tar.gz', '*.csv')                  -> 'archive://data.tar.gz!!*.csv'
            - ('local_file.csv', None)                  -> '/resolved/path/local_file.csv'
        """
        resolved = self.fs.resolve(path)

        if not ArchiveContext.is_supported(resolved):
            return path

        pattern = (file_pattern or "*").lstrip("/")
        clean_path = path.replace("zip://", "").replace("archive://", "")
        return f"{clean_path}!!{pattern}"

    # def get_files(
    #     self,
    #     source: str | list[str],
    #     file_pattern: str | None = None,
    #     archive: str | None = None,
    #     repair: bool = False,
    #     **kwargs,
    # ) -> pl.LazyFrame:
    #     """Read files into Polars LazyFrame.

    #     Args:
    #         source: The source path or list of paths.
    #         file_pattern: The pattern to match.
    #         archive: The archive path.
    #         repair: Whether to repair the files.
    #         **kwargs: Additional keyword arguments.

    #     Returns:
    #         pl.LazyFrame: The LazyFrame containing the files.
    #     """

    #     temp_dir = None

    #     try:
    #         # Resolve targets
    #         if isinstance(source, list):
    #             targets = source
    #         # Handle archive files
    #         elif archive or self.fs.is_archive(source):
    #             archive = archive or source
    #             temp_dir = self.fs.extract_archive(archive)
    #             LOG.debug(f"Extracted archive: {temp_dir}")

    #             # Find files in extracted directory
    #             full_path = self.fs.resolve(temp_dir)
    #             if file_pattern:
    #                 all_files = self.fs.fs.find(full_path)
    #                 targets = filter_files(all_files, file_pattern, temp_dir)
    #             else:
    #                 targets = [str(p) for p in Path(temp_dir).rglob("*") if p.is_file()]
    #         else:
    #             mount_path = source
    #             full_path = self.fs.resolve(mount_path)
    #             all_files = (
    #                 self.fs.fs.find(full_path)
    #                 if self.fs.fs.isdir(full_path)
    #                 else [full_path]
    #             )
    #             targets = filter_files(all_files, file_pattern, source)

    #         frames = []
    #         for target in targets:
    #             if not self.fs.is_readable(target):
    #                 continue

    #             ext = Path(target).suffix.lstrip(".").lower()
    #             handler = FormatFactory.get(ext, self.fs, self.fs.opts)

    #             encoding = "utf-8"
    #             if ext != "parquet":
    #                 _, encoding = self.fs._detect_encoding(target)

    #             frames.append(
    #                 handler.to_df(
    #                     target, encoding=encoding, force_repair=repair, **kwargs
    #                 )
    #             )

    #         return pl.concat(frames) if frames else pl.LazyFrame()

    #     finally:
    #         if temp_dir:
    #             shutil.rmtree(temp_dir, ignore_errors=True)
