from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

import polars as pl
import structlog
from apps.validation.src.services.database.base import DatabaseService
from apps.validation.src.utils.decorators import parquet_cache

LOG = structlog.get_logger(__name__)


class Dataset(ABC):
    def __init__(self, name: str, strategy=None):
        self.name = name
        self.strategy = strategy

    @abstractmethod
    def get_raw_stream(self) -> pl.LazyFrame:
        """Fetch the data handle from the source."""
        pass

    @parquet_cache(cache_dir="./audit_cache")
    def get_normalized_stream(self, schema) -> pl.LazyFrame:
        """
        Returns a 'Proven' Normalized Stream.
        Crashes if strict=True fails, or if a side-effect is detected.
        """
        raw_lf = self.get_raw_stream()

        if not self.strategy:
            return raw_lf

        # 1. Apply Strategy with STRICT=True
        # This will raise a pl.ComputeError if types are fundamentally incompatible.
        try:
            norm_exprs = self.strategy.get_cast_map(raw_lf.schema)
            norm_lf = raw_lf.select(norm_exprs)
        except pl.exceptions.ComputeError as e:
            LOG.error(f"Strict Type Casting Failed: {e}")
            raise

        # 2. Trigger the Safety Audit (The Side-Effect Detector)
        # This ensures we didn't lose data during the 'successful' cast.
        self._run_internal_safety_audit(raw_lf, norm_lf)

        return norm_lf

    def _run_internal_safety_audit(self, raw_lf: pl.LazyFrame, norm_lf: pl.LazyFrame):
        """
        A high-speed, 512MB-safe check for truncation and nullification.
        """
        # Define the metrics we want to compare
        audit_exprs = [
            pl.all().null_count().sum().alias("total_nulls"),
            *[
                pl.col(c).str.len_chars().sum().alias(f"len_{c}")
                for c, t in raw_lf.schema.items()
                if t == pl.String
            ],
        ]

        # Execute both in parallel via the streaming engine
        # Since this is a 'select', it only returns a single row.
        raw_res = raw_lf.select(audit_exprs).collect(streaming=True)
        norm_res = norm_lf.select(audit_exprs).collect(streaming=True)

        # Comparison Logic
        if not raw_res.frame_equal(norm_res):
            # Find the culprit
            diffs = []
            for col in raw_res.columns:
                if raw_res[col][0] != norm_res[col][0]:
                    diffs.append(
                        f"{col}: Raw({raw_res[col][0]}) vs Norm({norm_res[col][0]})"
                    )

            error_msg = f"Data Integrity Violation in {self.name}! Side effects detected: {', '.join(diffs)}"
            LOG.error(error_msg)
            raise ValueError(error_msg)

        LOG.info(f"Safety Audit Passed for {self.name}. No side-effects detected.")

    def get_schema(self) -> pl.DataFrame:
        raise NotImplementedError("get_schema must be implemented by subclasses if schema introspection is supported.")


class SQLDataset(Dataset):
    def __init__(
        self,
        service: DatabaseService,
        table_name: str,
        schema: str = "public",
        config: dict[str, Any] | None = None,
    ):
        from libs.database.utils import get_fully_qualified_table
        
        super().__init__(name=table_name)
        self.config = config or {}
        self.service = service
        self.fq_table = get_fully_qualified_table(
            database=self.config.get("database"), 
            schema=schema, 
            table=table_name
        )

    def get_raw_stream(self) -> pl.LazyFrame:
        """
        Orchestrates the scan by passing the dynamic filter to the service.
        """
        # Generate the WHERE clause (Snapshot vs Incremental)
        sql_filter = self._generate_sql_filter() 
        
        # Pass the table name and filter to the service scan
        return self.service.scan(
            table_name=self.fq_table, 
            filter_sql=sql_filter
        )

    def _generate_sql_filter(self) -> str:
        """
        Determines the WHERE clause based on workload type.
        Supports Snapshot (1=1) and Incremental logic.
        """
        filters = []
        
        # 1. Incremental Logic
        if self.config.get("mode") == "incremental":
            col = self.config.get("incremental_column", "updated_at")
            val = self.config.get("watermark")
            if val:
                filters.append(f"{col} >= '{val}'")
        
        # 2. Static/Snapshot filters
        if "filter_sql" in self.config:
            # Clean up potential WHERE prefix to prevent double keywords
            clean_filter = self.config["filter_sql"].replace("WHERE", "").strip()
            if clean_filter:
                filters.append(clean_filter)

        return " AND ".join(filters) if filters else "1=1"
    
    def get_schema(self) -> pl.DataFrame:
        # For SQL datasets, we can fetch the schema directly from the service
        return self.service.get_actual_schema(self.fq_table)

class FileDataset(Dataset):
    def __init__(self, path: str, format: str = "parquet", strategy=None):
        super().__init__(name=path.split("/")[-1], strategy=strategy)
        self.path = path
        self.format = format

    def get_raw_stream(self) -> pl.LazyFrame:
        if self.format == "parquet":
            return pl.scan_parquet(self.path)
        if self.format == "csv":
            return pl.scan_csv(self.path)
        raise ValueError(f"Unsupported format: {self.format}")
    
    


class APIDataset(Dataset):
    def __init__(self, endpoint_url: str, headers: dict, strategy=None):
        super().__init__(name=endpoint_url, strategy=strategy)
        self.url = endpoint_url
        self.headers = headers

    def get_raw_stream(self) -> pl.LazyFrame:
        # We fetch the data into a buffer, but treat it lazily
        # for the rest of the pipeline's logic.
        import requests

        response = requests.get(self.url, headers=self.headers)
        # Convert JSON response to a DataFrame, then to Lazy
        return pl.DataFrame(response.json()).lazy()
