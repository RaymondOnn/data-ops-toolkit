from diskcache import Cache
import polars as pl

# def find_mismatched_keys(source_path: str, sink_path: str, key_col: str):
#     """Identifies keys that exist in one side or have different hashes."""
#     # Use a local SQLite-backed cache
#     with Cache("./diff_cache") as ds_cache:
#         # Step 1: Record Source State
#         # We stream the PK and the Hash only
#         for batch in pl.scan_parquet(source_path).select([key_col, "row_hash"]).collect(streaming=True).to_dicts():
#             ds_cache[batch[key_col]] = batch["row_hash"]

#         mismatched_keys = []

#         # Step 2: Compare with Sink
#         for batch in pl.scan_parquet(sink_path).select([key_col, "row_hash"]).collect(streaming=True).to_dicts():
#             pk = batch[key_col]
#             sink_hash = batch["row_hash"]

#             if pk not in ds_cache:
#                 mismatched_keys.append((pk, "ORPHAN_IN_SINK"))
#             elif ds_cache[pk] != sink_hash:
#                 mismatched_keys.append((pk, "VALUE_MISMATCH"))
#                 del ds_cache[pk] # Remove matched to find missing later
#             else:
#                 del ds_cache[pk] # Perfect match

#         # Step 3: Anything left in cache is missing in Sink
#         for pk in ds_cache:
#             mismatched_keys.append((pk, "MISSING_IN_SINK"))

#     return mismatched_keys

# def triangulate_culprit(source_path, sink_path, pk_col, target_pk):
#     # 1. Extract the two specific rows from local Parquet
#     s_row = pl.scan_parquet(source_path).filter(pl.col(pk_col) == target_pk).collect()
#     t_row = pl.scan_parquet(sink_path).filter(pl.col(pk_col) == target_pk).collect()

#     all_cols = [c for c in s_row.columns if c != "row_hash"]
#     culprits = []

#     # 2. Systematic Exclusion
#     for i in range(len(all_cols)):
#         # Create a subset of columns excluding the current index (N-1)
#         test_cols = all_cols[:i] + all_cols[i+1:]

#         # Calculate N-1 Hash for both
#         s_hash = s_row.select(pl.struct(test_cols).hash()).item()
#         t_hash = t_row.select(pl.struct(test_cols).hash()).item()

#         # If the hashes match now, the excluded column was the culprit
#         if s_hash == t_hash:
#             culprits.append(all_cols[i])

#     return {
#         "pk": target_pk,
#         "culprit_columns": culprits,
#         "source_values": s_row.select(culprits).to_dicts()[0],
#         "sink_values": t_row.select(culprits).to_dicts()[0]
#     }


class Analyzer:
    def __init__(
        self,
        source_ds,
        sink_ds,
        audit_metrics=None,
        cache_path="./audit_cache",
        epsilon=1e-9,
    ):
        self.source = source_ds
        self.sink = sink_ds
        self.metrics = audit_metrics or {}
        self.cache_path = cache_path
        self.epsilon = epsilon

    def analyze(self, id_column: str = None, sample_limit: int = 10):
        """
        The Diagnostic Engine.
        Returns a summary of orphans/mismatches and a sample of detailed pairs.
        """
        # 1. Column Selection (Zero-Entropy Filtered)
        anchors = self._get_anchor_columns(id_column, sample_size=sample_limit)

        # 2. INTEGRITY CHECK: Only critical if we are inventing a PK
        if not id_column:
            is_stable, msg = self._validate_anchor_integrity(anchors)
            if not is_stable:
                print(
                    f"Forensic Warning: {msg}"
                )  # Or log to your validation_runs table

        suspects = self._get_suspect_columns()
        search_cols = list(set(anchors + suspects))

        # 2. Run Balance Sheet (The Full Scan)
        findings, stats = self._run_disk_balance_sheet(id_column, search_cols)

        # 3. The Unhappy Path: If T4 Hash failed but Forensics found nothing
        if (
            stats["total_mismatches"] == 0
            and stats["missing_in_sink"] == 0
            and stats["missing_in_source"] == 0
        ):
            return {
                "status": "INCONCLUSIVE",
                "message": "Aggregate hash mismatch detected, but row-level scan found no deltas. Check for encoding/invisible character differences.",
                "metrics": stats,
            }

        # 4. Detailed Triangulation for 'Value Mismatches' up to the limit
        detailed_samples = []
        mismatch_items = [f for f in findings if f["type"] == "VALUE_MISMATCH"]

        for item in mismatch_items[:sample_limit]:
            report = self._triangulate_culprit_columns(item, id_column, search_cols)
            detailed_samples.append(report)

        # 5. Add Orphans to the samples (if space remains under sample_limit)
        orphan_limit = max(0, sample_limit - len(detailed_samples))
        orphans = [f for f in findings if "MISSING" in f["type"]]
        for item in orphans[:orphan_limit]:
            detailed_samples.append(
                {
                    "id": item["ref"],
                    "type": item["type"],
                    "culprit_columns": [],
                    "values": {},  # No pair to compare
                }
            )

        return {
            "status": "DIAGNOSTIC_COMPLETE",
            "metrics": stats,
            "samples": detailed_samples,
            "anchors_used": anchors,
        }

    def _run_disk_balance_sheet(self, id_column, cols):
        """Calculates counts for missing records and identifies row-pairs."""
        findings = []
        stats = {"missing_in_sink": 0, "missing_in_source": 0, "total_mismatches": 0}

        with Cache(self.cache_path) as ds_cache:
            ds_cache.clear()

            # Source: Record full row states to disk
            src_stream = self.source.get_normalized_stream().select(cols)
            for idx, batch in enumerate(src_stream.collect(streaming=True).to_dicts()):
                key = batch[id_column] if id_column else idx
                ds_cache[key] = batch

            # Sink: Compare and identify orphans/mismatches
            snk_stream = self.sink.get_normalized_stream().select(cols)
            for idx, batch in enumerate(snk_stream.collect(streaming=True).to_dicts()):
                key = batch[id_column] if id_column else idx

                if key not in ds_cache:
                    stats["missing_in_source"] += 1
                    findings.append({"ref": key, "type": "MISSING_IN_SOURCE"})
                else:
                    if not self._is_row_equal(ds_cache[key], batch):
                        stats["total_mismatches"] += 1
                        findings.append({"ref": key, "type": "VALUE_MISMATCH"})
                    del ds_cache[key]

            # Remainder in cache: Records that never made it to Sink
            stats["missing_in_sink"] = len(ds_cache)
            for key in ds_cache:
                findings.append({"ref": key, "type": "MISSING_IN_SINK"})

        return findings, stats

    def _is_row_equal(self, row_a, row_b):
        """
        Enhanced comparison that identifies 'Soft Mismatches'.
        Returns (is_equal, sub_status)
        """
        for col in row_a:
            val_a, val_b = row_a[col], row_b[col]

            # 1. Null Check
            if val_a is None or val_b is None:
                if val_a != val_b:
                    return False, "NULL_MISMATCH"
                continue

            # 2. Numeric Epsilon Check
            if isinstance(val_a, (float, int)) and isinstance(val_b, (float, int)):
                if abs(val_a - val_b) > self.epsilon:
                    return False, "VALUE_MISMATCH"

            # 3. String 'Soft' Checks
            elif isinstance(val_a, str) and isinstance(val_b, str):
                if val_a == val_b:
                    continue
                if val_a.strip() == val_b.strip():
                    return False, "WHITESPACE_DIFF"
                if val_a.lower() == val_b.lower():
                    return False, "CASING_DIFF"
                return False, "VALUE_MISMATCH"

            # 4. Standard Equality
            elif val_a != val_b:
                return False, "VALUE_MISMATCH"

        return True, "MATCH"

    def _triangulate_culprit_columns(self, mismatch_info, id_column, cols):
        """Systematic exclusion to identify the specific field that failed."""
        ref = mismatch_info["ref"]
        s_row, t_row = self._get_row_pair(ref, id_column)

        if s_row.is_empty() or t_row.is_empty():
            return {"id": ref, "type": "DATA_RETRIEVAL_ERROR"}

        culprits = {}
        for col in cols:
            val_s, val_t = s_row[col][0], t_row[col][0]

            # Use same equality logic as the balance sheet
            if not self._is_row_equal({col: val_s}, {col: val_t}):
                culprits[col] = {"src": val_s, "snk": val_t}

        return {
            "id": ref,
            "type": "VALUE_MISMATCH",
            "culprit_columns": list(culprits.keys()),
            "values": culprits,
        }

    def _get_row_pair(self, ref, id_column):
        """Fetches a specific pair for high-resolution comparison."""
        if id_column:
            # Cast ref to string for filter safety
            s = (
                self.source.get_normalized_stream()
                .filter(pl.col(id_column) == ref)
                .collect()
            )
            t = (
                self.sink.get_normalized_stream()
                .filter(pl.col(id_column) == ref)
                .collect()
            )
        else:
            # Fallback to absolute offset
            s = self.source.get_normalized_stream().slice(ref, 1).collect()
            t = self.sink.get_normalized_stream().slice(ref, 1).collect()
        return s, t

    def _get_anchor_columns(self, id_column, sample_size=5000):
        """Identifies high-entropy columns to act as a Synthetic PK."""
        if id_column:
            return [id_column]
        sample = self.source.get_raw_stream().limit(sample_size).collect()
        scores = []
        for col in sample.columns:
            u_count = sample[col].n_unique()

            # --- ZERO-ENTROPY FILTER ---
            # If a column has only one value, it provides zero information for triangulation.
            if u_count <= 1:
                continue  # Remove Zero-Entropy columns
            scores.append((col, u_count / sample_size))
        return [c for c, s in sorted(scores, key=lambda x: x[1], reverse=True)][:3]

    def _get_suspect_columns(self):
        """Pulls from audit metrics to prioritize columns that already showed deltas."""
        return self.metrics.get("suspect_columns", [])

    def _validate_anchor_integrity(self, anchors):
        """Checks if the chosen anchors are actually unique enough to use as a key."""
        # We check a sample to save memory
        sample = self.source.get_raw_stream().select(anchors).limit(10000).collect()
        unique_count = sample.n_unique()

        if unique_count < len(sample):
            # This is a warning, not a failure.
            # It tells the engineer: 'Results might be slightly inaccurate due to collisions'
            return (
                False,
                f"Non-Unique Anchors: {unique_count}/{len(sample)} uniqueness.",
            )
        return True, "Stable"


class Analyzer:
    def __init__(self, cache_path="./audit_cache", epsilon=1e-9):
        self.cache_path = cache_path
        self.epsilon = epsilon

    def analyze_from_cache(self, mapping_df: pl.DataFrame, audit_metrics: dict = None):
        """
        The Diagnostic Engine. 
        Reads the evidence Parquet files created by ValuesCheck.
        """
        # 1. Load Evidence from Parquet (Small, safe for 512MB)
        try:
            missing_df = pl.read_parquet(f"{self.cache_path}/values_evidence_missing.parquet")
            ghosts_df = pl.read_parquet(f"{self.cache_path}/values_evidence_ghosts.parquet")
        except FileNotFoundError:
            return {"status": "SKIPPED", "message": "No evidence files found in cache."}

        # 2. Pairing (Fuzzy Matching)
        # We use the row_hash if available, or anchor columns
        pairs = self._pair_rows(missing_df, ghosts_df, mapping_df)

        # 3. Deep Diff with Metrics Awareness
        detailed_samples = []
        for src_row, snk_row in pairs:
            # We pass audit_metrics so the diff knows if, say, 
            # a 'NULL_MISMATCH' is expected for certain columns.
            report = self._triangulate_diffs(src_row, snk_row, mapping_df, audit_metrics)
            detailed_samples.append(report)

        return {
            "status": "DIAGNOSTIC_COMPLETE",
            "total_mismatches": len(detailed_samples),
            "samples": detailed_samples
        }

    def _triangulate_diffs(self, row_a, row_b, mapping_df, metrics):
        culprits = {}
        for row in mapping_df.to_dicts():
            col = row["source_col"]
            val_a, val_b = row_a.get(col), row_b.get(row["target_col"])

            is_equal, reason = self._is_val_equal(val_a, val_b)
            
            # Use metrics to 'de-noise' the report
            # e.g., if metrics show this column is 100% Null in Source, 
            # don't freak out if it's Null in Sink.
            if not is_equal:
                culprits[col] = {
                    "src": val_a, 
                    "snk": val_b, 
                    "reason": reason,
                    "col_cardinality": metrics.get(f"profile_{col}_cardinality") if metrics else None
                }

        return {"row_hash": row_a.get("row_hash"), "culprits": culprits}

    def _is_val_equal(self, val_a, val_b):
        # ... (Same logic as before: Null normalization, Epsilon, Date vs Datetime)
        # 1. Oracle NULL normalization
        if (val_a is None or val_a == "") and (val_b is None or val_b == ""):
            return True, "MATCH"
        
        # 2. Date vs Datetime Logic (Your hardcode '00:00:00' requirement)
        if isinstance(val_a, (date, datetime)) and isinstance(val_b, (date, datetime)):
            if val_a.strftime("%Y-%m-%d %H:%M:%S") == val_b.strftime("%Y-%m-%d %H:%M:%S"):
                return True, "MATCH"
            if val_a.strftime("%Y-%m-%d") == val_b.strftime("%Y-%m-%d"):
                return False, "TIME_COMPONENT_MISMATCH"
        
        # 3. Numeric / String logic...
        return val_a == val_b, "VALUE_MISMATCH"