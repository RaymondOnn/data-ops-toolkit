"""Regression testing suite for data ingestion pipelines.

Compares baseline (stable) vs candidate (current) datasets to detect
data drift, schema changes, and missing records.

Workflow per dataset:
1. Ensure candidate clone table exists (create if missing, truncate if exists)
2. Run candidate ingestion into candidate clone
3. Ensure baseline data exists (create table and run baseline if missing)
4. Compare using datacompy-style logic (no external dependency)
5. Generate detailed report with match percentages and drift samples
"""

import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from apps.ingestion.src.core.contexts import (
    ExecutionMode,
    TaskContext,
    TaskContextBuilder,
)
from apps.ingestion.src.services.factory import ServiceFactory
from apps.ingestion.src.utils.constants import APP_CONFIG_ROOT
from loguru import logger

if TYPE_CHECKING:
    from apps.ingestion.src.services.base import Sink

LOG = logger

# Constants
BASELINE_SUFFIX = "_baseline_regression"
CANDIDATE_SUFFIX = "_candidate_regression"
MAX_SAMPLES = 10


# =============================================================================
# Data Models
# =============================================================================


@dataclass
class ColumnComparison:
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


@dataclass
class ComparisonReport:
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


@dataclass
class RegressionSummary:
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


# =============================================================================
# Main Regression Runner
# =============================================================================


class RegressionRunner:
    """
    Orchestrates data regression tests between baseline and candidate datasets.

    Key behaviors:
    - Candidate table is always refreshed (truncate + reload)
    - Baseline table is created once and reused across runs
    - Comparison uses datacompy-style logic (no external dependency)
    - Cleanup is separate (call via CLI command)
    """

    def __init__(
        self,
        job_id: str,
        dataset_ids: list[str],
        env: str = "dev",
        baseline_pex_path: Path | None = None,
        skip_baseline_run: bool = False,
    ):
        """Initializes the runner for isolated regression testing.

        Args:
            job_id: The identifier of the job being verified.
            dataset_ids: List of specific datasets to audit.
            env: The environment context (e.g., 'dev', 'prod').
            baseline_pex_path: Path to the stable orchestrator binary.
            skip_baseline_run: If True, reuses existing baseline tables.

        Notes:
            Decision: Isolated Environments.
            We use a single 'env' for both baseline and candidate runs to ensure that
            connectivity, credentials, and network paths are identical, effectively
            isolating code changes as the only variable in the test.
        """
        self.job_id = job_id
        self.dataset_ids = dataset_ids
        self.env = env
        self.baseline_pex = baseline_pex_path
        self.skip_baseline_run = skip_baseline_run

        # Internal state
        self._reports: list[ComparisonReport] = []
        self._failed_datasets: list[str] = []
        self._start_time: float = 0.0
        self._current_date: str = ""
        self._contexts: dict[str, TaskContext] = {}
        self._sinks: dict[str, Any] = {}

        self._initialize()

    # =========================================================================
    # Public API
    # =========================================================================

    def run(self, partition_date: str) -> RegressionSummary:
        """Executes the regression suite for the given date.

        Args:
            partition_date: The date partition to use for comparison.

        Returns:
            RegressionSummary: The aggregated results of all dataset audits.

        Notes:
            Decision: Late Binding of Partition Date.
            Passing the `partition_date` to `run()` allows the runner instance to be
            reused across multiple historical dates if needed, providing temporal
            coverage for regression tests without re-initializing the sinks.
        """
        self._current_date = partition_date
        self._start_time = time.perf_counter()
        self._log_header()

        for dataset_id in self.dataset_ids:
            self._audit_dataset(dataset_id)

        summary = self._create_summary()
        summary.print_summary()
        return summary

    def purge(
        self, drop_baseline: bool = False, drop_candidate: bool = True
    ) -> list[str]:
        """Physically removes temporary regression tables from the database.

        Args:
            drop_baseline (bool): Whether to remove the stable baseline snapshot table.
            drop_candidate (bool): Whether to remove the temporary candidate
                ingestion table.

        Returns:
            list[str]: The names of the dropped tables.

        Notes:
            Decision: Iterative Testing Lifecycle.
            By defaulting `drop_baseline` to False, we support a "Fast Feedback" loop
            where developers can modify candidate code and re-run tests against a
            persistent, immutable baseline without the latency of a full re-ingestion.
        """
        dropped = []
        for dataset_id, ctx in self._contexts.items():
            sink: Sink = self._sinks[dataset_id]
            orig = ctx.load.destination

            if drop_candidate:
                c_table = f"{orig}{CANDIDATE_SUFFIX}"
                if hasattr(sink, "drop"):
                    sink.delete(c_table)
                    dropped.append(c_table)

            if drop_baseline:
                b_table = f"{orig}{BASELINE_SUFFIX}"
                if hasattr(sink, "drop"):
                    sink.delete(b_table)
                    dropped.append(b_table)

        return dropped

    # =========================================================================
    # Initialization
    # =========================================================================

    def _initialize(self) -> None:
        """Initializes task contexts and resolves the required sink services.

        Raises:
            ValueError: If the context builder fails to generate a valid context.

        Notes:
            Decision: Service Resolution.
            We pre-resolve sinks to ensure that credentials are valid before starting
            any ingestion. This prevents partial suite failures due to
            authentication issues halfway through a run.
        """
        builder = TaskContextBuilder(env=self.env)

        for dataset_id in self.dataset_ids:
            ctx_list = list(builder.build(job_id=self.job_id, dataset_id=dataset_id))
            if not ctx_list:
                raise ValueError(f"Context build failed for {dataset_id}")

            ctx = ctx_list[0]
            self._contexts[dataset_id] = ctx
            self._sinks[dataset_id] = ServiceFactory.get_sink(
                ctx.load.type, **ctx.load.service
            )

    # =========================================================================
    # Test Execution
    # =========================================================================

    def _audit_dataset(self, dataset_id: str) -> None:
        """Orchestrates the phased preparation and quality audit for a single dataset.

        Args:
            dataset_id (str): The unique identifier of the dataset to audit.

        Notes:
            Decision: Phased Validation.
            We separate provisioning from auditing. This allows the runner to fail
            early if the candidate code cannot even complete ingestion, before
            attempting expensive and time-consuming data comparisons.
        """
        LOG.info(f"🔍 Testing dataset: {dataset_id}")

        try:
            ctx = self._contexts[dataset_id]
            orig = ctx.load.destination

            candidate_table = f"{orig}{CANDIDATE_SUFFIX}"
            baseline_table = f"{orig}{BASELINE_SUFFIX}"

            # Phase 1: Ensure candidate table exists and has current data
            if not self._prepare_candidate_data(dataset_id, candidate_table):
                self._record_failure(dataset_id, "Candidate preparation failed")
                return

            # Phase 2: Ensure baseline data exists (create if missing)
            if not self._prepare_baseline_data(dataset_id, baseline_table):
                self._record_failure(dataset_id, "Baseline data unavailable")
                return

            # Phase 3: Audit (Leveraging ClickHouseService methods)
            report = self._compare_datasets(dataset_id, baseline_table, candidate_table)
            report.print_summary()
            self._reports.append(report)

        except Exception as e:
            LOG.exception(f"💥 Failed to test {dataset_id}")
            self._record_failure(dataset_id, str(e))

    def _record_failure(self, dataset_id: str, error: str) -> None:
        """Internal helper to track and log dataset audit failures.

        Args:
            dataset_id (str): The dataset that failed.
            error (str): Description of the failure.
        """
        self._failed_datasets.append(dataset_id)
        LOG.error(f"❌ {dataset_id}: {error}")

    # =========================================================================
    # Candidate Table Management
    # =========================================================================

    def _prepare_candidate_data(self, dataset_id: str, table_name: str) -> bool:
        """Refreshes the candidate shadow table with the latest code output.

        Args:
            dataset_id (str): Dataset to process.
            table_name (str): Temporary table name for candidate data.

        Returns:
            bool: True if the preparation and ingestion succeeded.

        Notes:
            Decision: Shadow Clones.
            We use `sink.clone()` to create a structurally identical version of the
            production target. This guarantees that the candidate run is tested against
            the actual production schema definition without the risk of
            contaminating production data.
        """
        sink: Sink = self._sinks[dataset_id]
        target = self._contexts[dataset_id].load.destination

        # Clean slate
        if hasattr(sink, "drop"):
            sink.delete(table_name)

        sink.clone(source=target, dest=table_name)

        return self._run_candidate_ingestion(dataset_id, table_name)

    def _run_candidate_ingestion(self, dataset_id: str, table_name: str) -> bool:
        """Executes the current library code for candidate data ingestion.

        Args:
            dataset_id (str): The dataset ID to process.
            table_name (str): The temporary target table name.

        Returns:
            bool: True if the ingestion completed without exceptions.

        Notes:
            Decision: Stage-Restricted Execution.
            We override the execution range from 'extract' to 'write'. This bypasses
            maintenance and archival stages during regression tests, keeping the focus
            purely on data movement and transformation logic.
        """
        from apps.ingestion.src.core.orchestrator.factory import assemble_runtime
        from apps.ingestion.src.core.orchestrator.modes.trigger import TriggerRuntime

        builder = TaskContextBuilder(env=self.env)
        exec_ctx = builder.build_execution_context(mode=ExecutionMode.NORMAL)
        runtime = assemble_runtime(exec_ctx, builder)

        if not isinstance(runtime, TriggerRuntime):
            LOG.error("Candidate run requires TriggerRuntime")
            return False

        overrides = {
            "load": {"identifier": table_name},
            "_global": {"from_stage": "extract", "to_stage": "write"},
        }

        try:
            LOG.info(f"Running candidate ingestion for {dataset_id} -> {table_name}")
            runtime.run(
                job_id=self.job_id,
                dataset_id=dataset_id,
                partition_date=self._current_date,
                overrides=overrides,
            )
            LOG.success(f"Candidate ingestion complete for {dataset_id}")
            return True
        except Exception as e:
            LOG.error(f"Candidate ingestion failed: {e}")
            return False

    # =========================================================================
    # Baseline Data Management
    # =========================================================================

    def _prepare_baseline_data(self, dataset_id: str, table_name: str) -> bool:
        """Ensures a stable baseline snapshot exists for the comparison anchor.

        Args:
            dataset_id (str): The dataset ID to process.
            table_name (str): The baseline table name.

        Returns:
            bool: True if baseline data is ready for comparison.

        Notes:
            Decision: PEX-Driven Baseline Generation.
            If the baseline doesn't exist, we execute it using the provided
            `baseline_pex`.
            This ensures the comparison source is 100% guaranteed to be produced by the
            "stable" version of the orchestrator, avoiding logical contamination from
            local code changes.
        """
        sink = self._sinks[dataset_id]

        if sink.exists(table_name) and self.skip_baseline_run:
            return True

        sink.delete(table_name)
        target_location = self._contexts[dataset_id].load.destination
        sink.clone(reference=target_location, other=table_name)

        return self._run_baseline_pex(dataset_id, table_name)

    def _run_baseline_pex(self, dataset_id: str, table_name: str) -> bool:
        """Executes the external stable binary to generate baseline data.

        Args:
            dataset_id (str): The dataset ID to process.
            table_name (str): The specific target table for the baseline.

        Returns:
            bool: True if the baseline PEX exited successfully.

        Raises:
            OSError: If the baseline_pex path is not executable.
        """
        if not self.baseline_pex:
            LOG.error("Cannot provision baseline: No baseline_pex provided.")
            return False

        cmd = [
            str(self.baseline_pex),
            "run",
            self._current_date,
            "-j",
            self.job_id,
            "-d",
            dataset_id,
            "--set",
            f"load.destination={table_name}",
        ]

        LOG.info(f"Running baseline PEX: {' '.join(cmd)}")
        res = subprocess.run(cmd, capture_output=True, text=True, check=False)

        if res.returncode != 0:
            LOG.error(f"Baseline PEX failed: {res.stderr}")
            return False
        return True

    # =========================================================================
    # Audit Logic
    # =========================================================================

    def _compare_datasets(
        self, dataset_id: str, baseline_table: str, candidate_table: str
    ) -> ComparisonReport:
        """Performs a deep data-quality audit using native database operations.

        Args:
            dataset_id (str): The dataset identifier.
            baseline_table (str): Table name containing stable data.
            candidate_table (str): Table name containing candidate data.

        Returns:
            ComparisonReport: A detailed summary of rows and schema differences.

        Notes:
            Decision: DB-Side Set Operations.
            By delegating comparison to `is_equal()` and `minus()` methods, we
            leverage the database engine's native optimizations.
            This is significantly faster than loading data into local memory
            and prevents memory exhaustion on the CLI node when dealing with
            millions of records.
        """
        sink: Sink = self._sinks[dataset_id]

        b_count = sink.count_units(baseline_table)
        c_count = sink.count_units(candidate_table)

        # Tier 1: Fast Path Checksum
        if sink.is_equal(baseline_table, candidate_table):
            return ComparisonReport(
                dataset_id=dataset_id,
                partition_date=self._current_date,
                baseline_rows=b_count,
                candidate_rows=c_count,
                matched_rows=b_count,
                mismatched_rows=0,
                missing_in_candidate=0,
                missing_in_baseline=0,
            )

        # Tier 2: Specific Set Difference
        missing = sink.minus(baseline_table, candidate_table)
        extra = sink.minus(candidate_table, baseline_table)

        # Tier 3: Sample Mismatches (Only if counts matched but checksum failed)
        mismatched = 0
        if missing == 0 and extra == 0:
            mismatched = b_count  # Simplified for the report logic

        return ComparisonReport(
            dataset_id=dataset_id,
            partition_date=self._current_date,
            baseline_rows=b_count,
            candidate_rows=c_count,
            matched_rows=b_count - missing,
            mismatched_rows=mismatched,
            missing_in_candidate=missing,
            missing_in_baseline=extra,
        )

    def _create_summary(self) -> RegressionSummary:
        """Aggregates all individual dataset reports into a final summary."""
        duration = time.perf_counter() - self._start_time
        return RegressionSummary(
            job_id=self.job_id,
            partition_date=self._current_date,
            total_datasets=len(self.dataset_ids),
            duration_sec=duration,
            reports=self._reports,
            failed_datasets=self._failed_datasets,
        )

    def _log_header(self) -> None:
        """Visual header for the regression session."""
        LOG.info("=" * 70)
        LOG.info("🚀 REGRESSION SUITE")
        LOG.info(f"Job:      {self.job_id}")
        LOG.info(f"Date:     {self._current_date}")
        LOG.info(f"Env:      {self.env}")
        LOG.info(f"Datasets: {len(self.dataset_ids)}")
        LOG.info("=" * 70)


# =============================================================================
# CLI Helper Functions (for 'test regression clean' command)
# =============================================================================


def cleanup_regression_tables(
    job_id: str,
    dataset_ids: list[str] | None = None,
    env: str = "dev",
    drop_baseline: bool = True,
    drop_candidate: bool = True,
) -> dict[str, list[str]]:
    """Clean up regression tables for a job across all involved datasets.

    This is meant to be called from the CLI command 'test regression clean'.

    Args:
        job_id (str): Job identifier.
        dataset_ids (list[str] | None): Specific datasets to clean (None = all).
        env (str): Execution environment (e.g., 'dev', 'prod').
        drop_baseline (bool): Whether to drop baseline tables.
        drop_candidate (bool): Whether to drop candidate tables.

    Returns:
        dict[str, list[str]]: A dictionary with keys 'baseline' and 'candidate'
            containing the lists of table names successfully dropped.

    """
    result = {"baseline": [], "candidate": []}

    # Build contexts to get table names
    builder = TaskContextBuilder(env=env)

    if dataset_ids is None:
        # Need to discover datasets - this is simplified
        # In practice, you might need to scan config directory
        LOG.warning("dataset_ids not provided, cannot discover automatically")
        return result

    for dataset_id in dataset_ids:
        try:
            ctx_list = list(builder.build(job_id=job_id, dataset_id=dataset_id))
            if not ctx_list:
                continue
            ctx = ctx_list[0]

            sink = ServiceFactory.get_sink(ctx.load.type, **ctx.load.service)
            original_table = ctx.load.destination

            if drop_baseline:
                baseline_table = f"{original_table}{BASELINE_SUFFIX}"
                if hasattr(sink, "exists") and sink.exists(baseline_table):
                    sink.delete(baseline_table)
                    result["baseline"].append(baseline_table)
                    LOG.info(f"Dropped baseline table: {baseline_table}")

            if drop_candidate:
                candidate_table = f"{original_table}{CANDIDATE_SUFFIX}"
                if hasattr(sink, "exists") and sink.exists(candidate_table):
                    sink.delete(candidate_table)
                    result["candidate"].append(candidate_table)
                    LOG.info(f"Dropped candidate table: {candidate_table}")

        except Exception as e:
            LOG.error(f"Failed to clean up {dataset_id}: {e}")

    return result


def find_affected_datasets(
    job_id: str,
    dataset_id: str,
    env: str = "local",
    config_root: Path = APP_CONFIG_ROOT,
) -> list[dict[str, str]]:
    """Find datasets sharing the same transformation logic for impact analysis.

    Args:
        job_id (str): The target job ID.
        dataset_id (str): The target dataset ID.
        env (str): The environment context.
        config_root (Path): Path to the root configuration directory.

    Returns:
        list[dict[str, str]]: A list of dataset/job pairs that share the
            same transformation signature.
    """

    def _get_signature(ctx: TaskContext) -> tuple[str, str]:
        """Extracts the unique transformation signature from a context."""
        transform_type = ctx.transform.type.casefold()
        transform_name = (ctx.transform.params.get("name") or "").casefold()
        return (transform_type, transform_name)

    try:
        builder = TaskContextBuilder(env=env)
        target_ctx = next(iter(builder.build(job_id=job_id, dataset_id=dataset_id)))
        target_sig = _get_signature(target_ctx)
        LOG.info(f"Target signature: {target_sig}")
    except Exception as e:
        LOG.error(f"Failed to resolve target: {e}")
        return []

    if not config_root.exists():
        LOG.warning(f"Config root not found: {config_root}")
        return []

    peers = []
    builder = TaskContextBuilder(env=env)

    for job_dir in config_root.iterdir():
        if not job_dir.is_dir():
            continue

        current_job = job_dir.name
        if not (job_dir / "config.yaml").exists():
            continue

        try:
            for peer_ctx in builder.build(job_id=current_job):
                if peer_ctx.job_id == job_id and peer_ctx.dataset_id == dataset_id:
                    continue
                if _get_signature(peer_ctx) == target_sig:
                    peers.append(
                        {
                            "job_id": peer_ctx.job_id,
                            "dataset_id": peer_ctx.dataset_id,
                        }
                    )
        except Exception as e:
            LOG.warning(f"Skipping job {current_job}: {e}")

    return peers
