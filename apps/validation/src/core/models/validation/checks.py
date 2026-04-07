from abc import ABC, abstractmethod

import polars as pl
import structlog
from apps.validation.src.core.models.dataset import Dataset
from libs.database.dtypes import TypeResolver

LOG = structlog.get_logger(__name__)


class Check(ABC):
    """The standard contract for all validation logic."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Returns the unique identifier for the check (e.g., 'volume_check')."""
        pass

    @abstractmethod
    def get_expressions(
        self, schema: pl.Schema, mapping_df: pl.DataFrame, is_source: bool
    ) -> list[pl.Expr]:
        """
        Returns a list of Polars expressions to be included
        in the single-pass 'Mega-Scan'.
        """
        return []


class SchemaCheck:
    def __init__(self):
        self.name = "schema_integrity"

    def apply(self, source: Dataset, sink: Dataset, mapping: pl.DataFrame) -> None:
        # Note: These return eager DataFrames as we discussed earlier
        src_actual: pl.DataFrame = source.get_schema()
        snk_actual: pl.DataFrame = sink.get_schema()

        # Convert DataFrames to dicts for O(1) lookup: {col_name: pl.DataType}
        src_lookup = {
            row["column_name"].lower(): row["data_type"]
            for row in src_actual.to_dicts()
        }
        snk_lookup = {
            row["column_name"].lower(): row["data_type"]
            for row in snk_actual.to_dicts()
        }

        for row in mapping.to_dicts():
            # 1. Source Check (Skip audit columns)
            if not row["internal_flag"] and row["source_col"]:
                self._validate_contract(
                    "Source", row["source_col"], row["source_dtype"], src_lookup
                )

            # 2. Sink Check (Includes audit columns)
            self._validate_contract(
                "Sink", row["target_col"], row["target_dtype"], snk_lookup
            )

            # 3. Cross-System Capacity Check (The "DB-to-DB" specific check)
            if not row["internal_flag"]:
                self._check_physical_compatibility(row, src_lookup, snk_lookup)

    def _validate_contract(
        self, label, col_name, expected_group_str, actual_schema_dict
    ):
        col_name = col_name.lower()
        if col_name not in actual_schema_dict:
            raise ValueError(
                f"[{label}] Contract Violation: Column '{col_name}' missing from database."
            )

        # Logic: Resolve the actual DB type to its Logical Group
        # e.g., 'varchar2' -> TypeGroup.TEXT
        actual_type = actual_schema_dict[col_name]
        actual_group = TypeResolver.resolve_to_group(label.lower(), str(actual_type))

        if actual_group.value != expected_group_str:
            LOG.error(
                f"[{label}] Group Mismatch: '{col_name}' is {actual_group.value}, "
                f"contract expects {expected_group_str}"
            )

    def _check_physical_compatibility(self, row, src_lookup, snk_lookup):
        """Specifically handles DB-to-DB risks like truncation and precision."""
        # Precision Loss Check (from your requirement)
        if (row.get("source_scale") or 0) > (row.get("target_scale") or 0):
            LOG.warning(
                f"Precision Risk: '{row['target_col']}' scales down from "
                f"{row['source_scale']} to {row['target_scale']}. Expect XOR mismatches."
            )

        # Length/Capacity Check
        if (row.get("source_length") or 0) > (row.get("target_length") or 0):
            LOG.warning(
                f"Truncation Risk: '{row['target_col']}' target length ({row['target_length']}) "
                f"is smaller than source ({row['source_length']})."
            )


class TotalsCheck(Check):
    @property
    def name(self):
        return "totals"

    def get_expressions(
        self, schema: pl.Schema, mapping_df: pl.DataFrame, is_source: bool
    ) -> list[Expr]:
        # 1. Row Count
        exprs = [pl.len().alias("row_count")]

        for row in mapping_df.to_dicts():
            col_name = row["source_col"] if is_source else row["target_col"]
            if not col_name or col_name.startswith("_"):
                continue

            # Check if column exists in the actual physical schema
            if col_name not in schema:
                continue

            # 2. Numeric with Rounding (Addressing your Floating Point concern)
            if schema[col_name].is_numeric():
                scale = row.get("target_scale", 2)
                exprs.append(
                    pl.col(col_name).round(scale).sum().alias(f"sum_{col_name}")
                )

            # 3. Temporal (Dates/Timestamps)
            elif schema[col_name].is_temporal():
                exprs.append(
                    pl.col(col_name).dt.timestamp("ms").sum().alias(f"sum_{col_name}")
                )

            # 4. Strings (Length sum)
            elif schema[col_name] == pl.Utf8:
                exprs.append(
                    pl.col(col_name)
                    .str.len_chars()
                    .sum()
                    .fill_null(0)
                    .alias(f"sum_{col_name}")
                )

            # 5. Boolean (True count)
            elif schema[col_name] == pl.Boolean:
                exprs.append(
                    pl.col(col_name)
                    .cast(pl.Int32)
                    .sum()
                    .fill_null(0)
                    .alias(f"sum_{col_name}")
                )

        return exprs


def compare_totals(self, source_totals: pl.DataFrame, sink_totals: pl.DataFrame):
    # Unpivot (melt) the data so we have a list of metrics
    src_melted = source_totals.unpivot(variable_name="metric", value_name="source_val")
    snk_melted = sink_totals.unpivot(variable_name="metric", value_name="sink_val")

    # Join the two sets of metrics
    report = src_melted.join(snk_melted, on="metric", how="outer")

    # Add a difference and percentage column
    report = report.with_columns(
        [
            (pl.col("source_val") - pl.col("sink_val")).alias("diff"),
            (
                (pl.col("source_val") - pl.col("sink_val")) / pl.col("source_val") * 100
            ).alias("diff_pct"),
        ]
    )

    # Filter for only the failures
    discrepancies = report.filter(pl.col("diff") != 0)

    if not discrepancies.is_empty():
        LOG.error("Totals Check Failed!", extra={"failures": discrepancies.to_dicts()})
        return discrepancies

    LOG.info("Totals Check Passed: All sums align.")
    return None


class ProfilingCheck(Check):
    @property
    def name(self):
        return "profiling"

    def get_expressions(
        self, schema: pl.Schema, mapping_df: pl.DataFrame, is_source: bool
    ) -> list[pl.Expr]:
        exprs = []

        # We only profile non-internal columns to ensure source/sink parity
        profile_cols = mapping_df.filter(pl.col("internal_flag") == False)

        for row in profile_cols.to_dicts():
            col_name = row["source_col"] if is_source else row["target_col"]
            if col_name not in schema:
                continue

            # 2. Skip internal audit columns for profiling parity
            if row.get("internal_flag"):
                continue

            dtype = schema[col_name]
            level = row.get("profile_level", "none").lower()
            prefix = f"profile_{col_name}_"

            # 3. Universal Stats (Cheap)
            exprs.append(pl.col(col_name).null_count().alias(f"{prefix}null_count"))

            # 4. Conditional Cardinality (The 512MB RAM Safety Gate)
            if level in ("low", "high"):
                exprs.append(pl.col(col_name).n_unique().alias(f"{prefix}cardinality"))

            # 5. Data-Type Specific Stats (More Expensive, so gated by profile_level)
            # 2. Numeric Profiling (Distributional Integrity)
            if dtype.is_numeric():
                exprs.extend(
                    [
                        pl.col(col_name).min().alias(f"{prefix}min"),
                        pl.col(col_name).max().alias(f"{prefix}max"),
                        pl.col(col_name).mean().alias(f"{prefix}mean"),
                        # Median is a great way to detect skew without being
                        # affected by a few outliers
                        pl.col(col_name).median().alias(f"{prefix}median"),
                    ]
                )

            # 3. String Profiling
            elif dtype == pl.Utf8:
                exprs.extend(
                    [
                        pl.col(col_name)
                        .str.len_chars()
                        .min()
                        .alias(f"{prefix}min_len"),
                        pl.col(col_name)
                        .str.len_chars()
                        .max()
                        .alias(f"{prefix}max_len"),
                    ]
                )

            # 4. Temporal Profiling
            elif dtype.is_temporal():
                exprs.extend(
                    [
                        pl.col(col_name).min().alias(f"{prefix}min_date"),
                        pl.col(col_name).max().alias(f"{prefix}max_date"),
                    ]
                )

        return exprs

    def compare_profiles(
        self,
        source_profile: pl.DataFrame,
        sink_profile: pl.DataFrame,
        threshold: float = 0.01,
    ):
        # Melt both to vertical format: [Metric, Value]
        src = source_profile.unpivot(variable_name="metric", value_name="src_val")
        snk = sink_profile.unpivot(variable_name="metric", value_name="snk_val")

        report = src.join(snk, on="metric", how="inner")

        # Calculate Relative Drift
        # For metrics like 'min' or 'max', we expect exact matches.
        # For 'mean' or 'std_dev', we allow a small threshold.
        report = report.with_columns(
            is_match=pl.when(pl.col("metric").str.contains("min|max|count|cardinality"))
            .then(pl.col("src_val") == pl.col("snk_val"))
            .otherwise((pl.col("src_val") - pl.col("snk_val")).abs() < threshold)
        )

        failures = report.filter(pl.col("is_match") == False)
        return failures


class ValuesCheck(Check):
    @property
    def name(self):
        return "values"

    def apply(self, source_ds: Dataset, sink_ds: Dataset, mapping: pl.DataFrame):
        # 1. Fetch schemas once to avoid repetitive metadata calls
        src_schema = source_ds.get_raw_stream().schema
        snk_schema = sink_ds.get_raw_stream().schema

        # 2. Fast Global XOR Check
        src_hash = self._get_global_xor(source_ds, src_schema, mapping, is_source=True)
        snk_hash = self._get_global_xor(sink_ds, snk_schema, mapping, is_source=False)

        if src_hash == snk_hash:
            LOG.info("Integrity Check Passed: Global bit-hashes match.")
            return

        # 3. Trigger Forensic Diagnostic
        LOG.error("Hash Mismatch Detected! Investigating row-level differences...")
        self._run_orphan_diagnostic(source_ds, sink_ds, src_schema, snk_schema, mapping)

    def _get_row_hash_expr(
        self, schema: pl.Schema, mapping_df: pl.DataFrame, is_source: bool
    ) -> pl.Expr:
        col_key = "source_col" if is_source else "target_col"
        scale_key = "source_scale" if is_source else "target_scale"
        len_key = "source_length" if is_source else "target_length"

        standardized = []
        for row in mapping_df.to_dicts():
            c = row[col_key]
            if not c or c not in schema or row.get("internal_flag"):
                continue

            dtype = schema[c]

            # 1. Numerics: Use the scale from mapping_df
            if dtype.is_numeric():
                scale = row.get(scale_key) or 0
                expr = pl.col(c).round(scale).cast(pl.Utf8)

            # 2. Dates: Truncate time portion to 00:00:00
            elif dtype.is_temporal():
                raw_type = row.get("source_dtype", "").upper()

                # If the mapping explicitly says it's a DATE (not TIMESTAMP/DATETIME)
                if "DATE" in raw_type and "TIME" not in raw_type:
                    expr = pl.col(c).dt.to_string("%Y-%m-%d 00:00:00")
                else:
                    expr = pl.col(c).dt.to_string("%Y-%m-%d %H:%M:%S")

            # 3. Strings: Truncate to the length defined in mapping_df
            elif dtype == pl.Utf8:
                length = row.get(len_key)
                expr = pl.col(c).cast(pl.Utf8).str.strip_chars()
                if length:
                    expr = expr.str.slice(0, length)

            else:
                expr = pl.col(c).cast(pl.Utf8).str.strip_chars()

            standardized.append(expr.fill_null("_NULL_").replace("", "_NULL_"))

        return pl.concat_str(standardized, separator="|").hash().alias("row_hash")

    def _get_global_xor(
        self, ds: Dataset, schema: pl.Schema, mapping: pl.DataFrame, is_source: bool
    ) -> int:
        lf = ds.get_raw_stream()
        hash_expr = self._get_row_hash_expr(schema, mapping, is_source)
        # Use xor_agg for the global 'fingerprint'
        return lf.select(hash_expr.xor_agg()).collect(streaming=True).item()

    def _run_orphan_diagnostic(
        self, source_ds: Dataset, sink_ds: Dataset, src_schema, snk_schema, mapping
    ):
        # 1. Get hash-only streams
        src_hashes = source_ds.get_raw_stream().select(
            self._get_row_hash_expr(src_schema, mapping, True)
        )
        snk_hashes = sink_ds.get_raw_stream().select(
            self._get_row_hash_expr(snk_schema, mapping, False)
        )

        # 2. Identify the asymmetric difference
        # We collect() ONLY the hashes (Int64). 100k mismatches would only be ~8MB.
        missing_hashes = src_hashes.join(
            snk_hashes, on="row_hash", how="anti"
        ).collect()
        ghost_hashes = snk_hashes.join(src_hashes, on="row_hash", how="anti").collect()

        # 3. Targeted extraction for the Analyzer
        # We KEEP the row_hash here so the Analyzer can use it for pairing
        self.evidence_missing = (
            source_ds.get_raw_stream()
            .with_columns(self._get_row_hash_expr(src_schema, mapping, True))
            .filter(pl.col("row_hash").is_in(missing_hashes["row_hash"]))
            .collect()
        )

        self.evidence_ghosts = (
            sink_ds.get_raw_stream()
            .with_columns(self._get_row_hash_expr(snk_schema, mapping, False))
            .filter(pl.col("row_hash").is_in(ghost_hashes["row_hash"]))
            .collect()
        )

    @parquet_cache(step="values_evidence")
    def get_evidence(self, direction: str) -> pl.DataFrame:
        """
        direction: 'missing' or 'ghosts'
        This ensures the Analyzer reads from a local Parquet file
        instead of triggering the 10M row hash logic again.
        """
        return self.evidence_missing if direction == "missing" else self.evidence_ghosts


# 5. FreshnessCheck
class FreshnessCheck(Check):
    """Mainly for snapshot jobs to detect decommission or staleness."""

    def __init__(self, col):
        self.col = col

    @property
    def name(self):
        return "freshness"

    def get_expressions(self, schema):
        return [pl.col(self.col).max().alias("max_ts")] if self.col in schema else []
