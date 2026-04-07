import logging
import polars as pl
from abc import ABC, abstractmethod
from typing import Optional, Any

LOG = logging.getLogger(__name__)

# --- 1. Base Interface ---
class ValidationTier(ABC):
    def __init__(self):
        self.next_tier: Optional['ValidationTier'] = None

    def set_next(self, tier: 'ValidationTier') -> 'ValidationTier':
        self.next_tier = tier
        return tier

    @abstractmethod
    def name(self) -> str: pass
    
    @abstractmethod
    def get_expressions(self, schema: pl.Schema) -> list[pl.Expr]:
        return []

    @abstractmethod
    def evaluate(self, source_metrics: pl.DataFrame, sink_metrics: pl.DataFrame) -> bool:
        """Logic to determine if this tier passes or fails."""
        pass

# --- 2. Concrete Tier Implementations ---

class VolumeTier(ValidationTier):
    """
    Totals
    - SUM() for Numeric, Boolean
    - SUM(LENGTH) for String
    """
    def name(self): return "VOLUME"
    def get_expressions(self, schema):
        # We target all numeric columns for SUM
        num_cols = [name for name, dtype in schema.items() if dtype.is_numeric()]
        return [
            pl.count().alias("row_count"),
            *[pl.col(c).sum().alias(f"sum_{c}") for c in num_cols]
        ]

    def evaluate(self, source_metrics, sink_metrics):
        # Simple equality check for all calculated aggregates
        passed = source_metrics.equals(sink_metrics)
        if not passed:
            LOG.error("Volume mismatch detected between source and sink")
        return passed

class IntegrityTier(ValidationTier):
    """Bit-Level Integrity using XOR aggregation of row hashes."""
    def name(self): return "INTEGRITY"
    def get_expressions(self, schema):
        return [
            pl.struct(pl.all())
            .hash()
            # Note: Polars native XOR aggregation is extremely memory efficient
            .list.eval(pl.element().xor())
            .alias("integrity_signature")
        ]

    def evaluate(self, source_metrics, sink_metrics):
        s_sig = source_metrics.get_column("integrity_signature")[0]
        t_sig = sink_metrics.get_column("integrity_signature")[0]
        return s_sig == t_sig

class HealthTier(ValidationTier):
    def name(self): return "HEALTH"
    def get_expressions(self, schema):
        # Null checks and Range checks for all columns
        return [
            *[pl.col(c).null_count().alias(f"nulls_{c}") for c in schema.names()],
            *[pl.col(c).max().alias(f"max_{c}") for c in schema.names() if schema[c].is_numeric()]
        ]

    def evaluate(self, source_metrics, sink_metrics):
        # Health failures are often warnings unless null counts differ significantly
        return source_metrics.equals(sink_metrics)

# --- 3. The Orchestrator (The Runner) ---

class ValidationRunner:
    def __init__(self, source: pl.LazyFrame, sink: pl.LazyFrame):
        self.source = source
        self.sink = sink
        self.first_tier: Optional[ValidationTier] = None
        
    def set_pipeline(self, first_tier: ValidationTier):
        self.first_tier = first_tier

    def execute(self, source_cache: str, sink_cache: str):
        """
        Executes all tiers using the 'Gate' strategy.
        Satisfies the 2-query constraint: 
        1. Schema check (Lazy metadata)
        2. Data + Stats + XOR (Streaming execution)
        """
        # Query 1: Schema Contract (Metadata only)
        if not self.source.schema == self.sink.schema:
            LOG.error("Tier 0 Failure: Schema Mismatch")
            return {"status": "FAILED", "tier": "SCHEMA"}

        # Prepare expressions for Tiers 1-3
        all_exprs = []
        curr = self.first_tier
        while curr:
            all_exprs.extend(curr.get_expressions(self.source.schema))
            curr = curr.next_tier

        LOG.info("🚀 Starting Unified Stream for multi-tier validation...")

        # Query 2: The "Tee" Strategy (Streaming pass)
        # We stream data from source/sink, write to Parquet, and calculate metrics
        source_metrics = (
            self.source
            .sink_parquet(source_cache)
            .select(all_exprs)
            .collect(streaming=True)
        )
        
        sink_metrics = (
            self.sink
            .sink_parquet(sink_cache)
            .select(all_exprs)
            .collect(streaming=True)
        )

        # Evaluation Loop (Chain of Responsibility)
        curr = self.first_tier
        while curr:
            LOG.info(f"Evaluating Tier: {curr.name()}")
            if not curr.evaluate(source_metrics, sink_metrics):
                LOG.error(f"Validation failed at Tier: {curr.name()}")
                return {
                    "status": "FAILED", 
                    "tier": curr.name(),
                    "diagnostics": self.run_diagnostics(source_cache, sink_cache) if curr.name() == "INTEGRITY" else None
                }
            curr = curr.next_tier

        return {"status": "SUCCESS"}

    def run_diagnostics(self, cache_path_s, cache_path_t):
        from .core.analyzer import find_mismatched_keys, triangulate_culprit
        print("🔍 Integrity failure detected. Starting diagnostics...")
        
        # 1. Find which rows failed
        mismatches = find_mismatched_keys(cache_path_s, cache_path_t, pk_col)
        
        # 2. For 'VALUE_MISMATCH' types, find the column
        detailed_report = []
        for pk, error_type in mismatches:
            if error_type == "VALUE_MISMATCH":
                analysis = triangulate_culprit(cache_path_s, cache_path_t, pk_col, pk)
                detailed_report.append(analysis)
            else:
                detailed_report.append({"pk": pk, "error": error_type})
        
        return detailed_report
# --- 4. Usage ---
# lazy_df = pl.scan_ipc("data.arrow") # Example source
# runner = ValidationRunner(lazy_df)
# runner.add_tier(VolumeTier())
# runner.add_tier(IntegrityTier())
# runner.add_tier(HealthTier())

# results = runner.execute("local_cache.parquet")