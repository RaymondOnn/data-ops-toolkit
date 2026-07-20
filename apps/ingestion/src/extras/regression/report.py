from typing import Any

from loguru import logger
from msgspec import Struct, field

LOG = logger


class ColumnComparison(Struct):
    """Comparison results for a single column.

    Notes:
        Decision: Metric Granularity.
        We track null counts and unique counts per column to help identify not just if
        data values changed, but if the distribution or density of the column
        has drifted significantly (e.g., a previously mandatory field
        becoming nullable).
    """

    name: str
    dtype_baseline: str
    dtype_candidate: str
    match: bool
    nulls_baseline: int
    nulls_candidate: int
    uniques_baseline: int
    uniques_candidate: int
    sample_diffs: list[dict[str, Any]] = field(default_factory=list)


class ComparisonReport(Struct):
    """Datacompy-style comparison report between two datasets.

    Notes:
        Decision: Hierarchical Summary.
        By pre-calculating the match percentage and drift flags, we allow automated
        systems (like CI/CD pipelines) to fail the build immediately based on a
        single boolean property (`has_drift`), without parsing the full JSON report.
    """

    dataset_id: str
    partition_date: str

    # Row metrics
    baseline_rows: int
    candidate_rows: int
    matched_rows: int
    mismatched_rows: int
    missing_in_candidate: int
    missing_in_baseline: int
    match_percent: float = 0.0

    # Schema metrics
    common_columns: list[str] = field(default_factory=list)
    missing_in_baseline_cols: list[str] = field(default_factory=list)
    missing_in_candidate_cols: list[str] = field(default_factory=list)

    # Column-level details
    column_comparisons: list[ColumnComparison] = field(default_factory=list)

    # Row samples
    sample_mismatches: list[dict[str, Any]] = field(default_factory=list)
    sample_missing_in_candidate: list[dict[str, Any]] = field(default_factory=list)
    sample_missing_in_baseline: list[dict[str, Any]] = field(default_factory=list)

    # Join keys
    join_keys: list[str] = field(default_factory=list)
    error: str | None = None

    def __post_init__(self):
        """Calculates match percentage after initialization.

        Notes:
            Decision: Derived Metrics.
            By calculating the percentage post-init, we ensure data consumers get
            a consistent summary without needing to perform calculations manually.
        """
        if self.matched_rows + self.mismatched_rows > 0:
            total = self.matched_rows + self.mismatched_rows
            self.match_percent = round((self.matched_rows / total) * 100, 2)

    @property
    def has_drift(self) -> bool:
        """Checks if any schema or data differences were detected.

        Returns:
            bool: True if differences exist, False otherwise.
        """
        return (
            self.mismatched_rows > 0
            or self.missing_in_candidate > 0
            or self.missing_in_baseline > 0
            or bool(self.missing_in_baseline_cols)
            or bool(self.missing_in_candidate_cols)
            or any(not c.match for c in self.column_comparisons)
        )

    @property
    def total_rows_compared(self) -> int:
        """Calculates the total number of rows analyzed during the audit.

        Returns:
            int: Sum of matched and mismatched rows.
        """
        return self.matched_rows + self.mismatched_rows

    def to_dict(self) -> dict[str, Any]:
        """Serializes the report into a dictionary for JSON logging or API responses.

        Returns:
            dict[str, Any]: A structured summary of the comparison results.
        """
        if self.error:
            return {"dataset_id": self.dataset_id, "error": self.error}

        return {
            "dataset_id": self.dataset_id,
            "partition_date": self.partition_date,
            "rows": {
                "baseline": self.baseline_rows,
                "candidate": self.candidate_rows,
                "matched": self.matched_rows,
                "mismatched": self.mismatched_rows,
                "missing_in_candidate": self.missing_in_candidate,
                "missing_in_baseline": self.missing_in_baseline,
                "match_percent": self.match_percent,
            },
            "schema": {
                "common_columns": self.common_columns,
                "only_in_baseline": self.missing_in_candidate_cols,
                "only_in_candidate": self.missing_in_baseline_cols,
            },
            "columns": [
                {
                    "name": c.name,
                    "dtype_baseline": c.dtype_baseline,
                    "dtype_candidate": c.dtype_candidate,
                    "match": c.match,
                    "nulls_baseline": c.nulls_baseline,
                    "nulls_candidate": c.nulls_candidate,
                    "uniques_baseline": c.uniques_baseline,
                    "uniques_candidate": c.uniques_candidate,
                }
                for c in self.column_comparisons
            ],
            "samples": {
                "mismatches": self.sample_mismatches[:10],
                "missing_in_candidate": self.sample_missing_in_candidate[:5],
                "missing_in_baseline": self.sample_missing_in_baseline[:5],
            },
            "join_keys": self.join_keys,
            "has_drift": self.has_drift,
        }

    def print_summary(self) -> None:
        """Prints a color-coded human-readable summary to the console.

        Notes:
            Decision: Console Observability.
            By using `LOG.warning` for drifts and `LOG.info` for success, we allow
            automated log parsers to distinguish between valid data and regressions.
        """
        LOG.info("=" * 70)
        LOG.info(f"📊 COMPARISON: {self.dataset_id} @ {self.partition_date}")
        LOG.info("=" * 70)
        LOG.info(
            f"Rows:     Baseline={self.baseline_rows:_} | "
            f"Candidate={self.candidate_rows:_}"
        )
        LOG.info(f"Match:    {self.match_percent}% ({self.matched_rows:_} rows)")

        if self.mismatched_rows > 0:
            LOG.warning(f"Mismatch: {self.mismatched_rows:_} rows")
        if self.missing_in_candidate > 0:
            LOG.warning(f"Missing in candidate: {self.missing_in_candidate:_} rows")
        if self.missing_in_baseline > 0:
            LOG.warning(f"Extra in candidate: {self.missing_in_baseline:_} rows")

        if self.missing_in_candidate_cols:
            LOG.warning(f"Columns only in baseline: {self.missing_in_candidate_cols}")
        if self.missing_in_baseline_cols:
            LOG.warning(f"Columns only in candidate: {self.missing_in_baseline_cols}")

        drift_cols = [c.name for c in self.column_comparisons if not c.match]
        if drift_cols:
            LOG.warning(f"Column type drift: {drift_cols}")


class RegressionSummary(Struct):
    """Summary of entire regression suite run.

    Notes:
        Decision: Batch Observability.
        Aggregating duration and success counts across the entire job provides the
        necessary high-level telemetry for ingestion performance monitoring in
        shared environments.
    """

    job_id: str
    partition_date: str
    total_datasets: int
    duration_sec: float
    reports: list[ComparisonReport]
    failed_datasets: list[str]

    @property
    def success_count(self) -> int:
        """Calculates the number of datasets that completed testing successfully.

        Returns:
            int: Total successful reports.
        """
        return len(self.reports)

    @property
    def datasets_with_drift(self) -> list[str]:
        """Identifies dataset IDs where data drift was detected.

        Returns:
            list[str]: Dataset IDs with has_drift=True.
        """
        return [r.dataset_id for r in self.reports if r.has_drift]

    def to_dict(self) -> dict[str, Any]:
        """Serializes the entire batch summary to a dictionary.

        Returns:
            dict: Aggregated results for the entire regression run.

        Notes:
            Decision: JSON Telemetry.
            Returning a structured dict allows this summary to be sent to a
            Slack webhook or stored in an ELK stack for historical tracking.
        """
        return {
            "job_id": self.job_id,
            "partition_date": self.partition_date,
            "duration_sec": round(self.duration_sec, 2),
            "total_datasets": self.total_datasets,
            "successful": self.success_count,
            "failed": len(self.failed_datasets),
            "datasets_with_drift": self.datasets_with_drift,
            "failed_datasets": self.failed_datasets,
            "details": {r.dataset_id: r.to_dict() for r in self.reports},
        }

    def print_summary(self) -> None:
        """Prints the final summary footer for the regression suite.

        Notes:
            Decision: Visual Hierarchy.
            The footer is designed to be the "Bottom Line" for developers running
            manual tests, ensuring they don't miss failure counts in high-volume logs.
        """
        LOG.info("=" * 70)
        LOG.info("🏁 REGRESSION SUITE COMPLETE")
        LOG.info(f"Job: {self.job_id} | Date: {self.partition_date}")
        LOG.info(f"Duration: {self.duration_sec:.2f}s")
        LOG.info(f"Success: {self.success_count}/{self.total_datasets}")

        if self.datasets_with_drift:
            LOG.warning(f"⚠️ Drift detected in: {', '.join(self.datasets_with_drift)}")
        if self.failed_datasets:
            LOG.error(f"❌ Failed: {', '.join(self.failed_datasets)}")
        LOG.info("=" * 70)
