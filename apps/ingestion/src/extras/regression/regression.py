import subprocess
import time
from pathlib import Path
from typing import Any

from apps.ingestion.src.core.contexts import (
    ExecutionMode,
    TaskContext,
    TaskContextBuilder,
)
from apps.ingestion.src.core.models.stages.enums import StageName
from apps.ingestion.src.services.factory import ServiceFactory
from apps.ingestion.src.utils.constants import APP_CONFIG_ROOT
from loguru import logger  # Import logger


class DatasetRegressionContext:  # No changes needed here, keeping for context
    """Context manager for dataset-level setup and teardown."""

    def __init__(self, runner: "RegressionRunner", dataset_id: str):
        """Initializes the dataset-level context.

        Args:
            runner: The parent RegressionRunner instance.
            dataset_id: The specific dataset to manage.
        """
        self.runner = runner
        self.dataset_id = dataset_id

    def __enter__(self):
        """Creates the skeleton clone before the regression run."""
        self.runner._run_skeleton_clone(self.dataset_id)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Removes the shadow sink after the regression run."""
        self.runner._run_cleanup(self.dataset_id)


CLONE_SUFFIX = "_clone"


LOG = logger


class RegressionRunner:
    """
    Orchestrates data regression suites by comparing baseline and candidate datasets.

    Leverages class state to maintain service handles, accumulate drift metrics,
    and generate a unified execution report. It uses a context manager pattern
    to ensure proper setup and teardown of testing artifacts.
    """

    def __init__(
        self,
        job_id: str,
        dataset_ids: list[str],
        env_baseline: str = "prod",
        env_candidate: str = "dev",
        workspace_dir: Path | None = None,
        baseline_pex_path: Path | None = None,
        skip_baseline_run: bool = False,
    ):
        """
        Initializes the regression context.

        Args:
            job_id: The identifier for the job being tested.
            dataset_ids: List of datasets to audit within the job.
            env_baseline: The environment acting as the 'source of truth'.
            env_candidate: The environment being validated.
            workspace_dir: Path to the local config and logs directory.
            baseline_pex_path: Path to the stable/main PEX used to generate
                the comparison baseline.
            skip_baseline_run: If True, assumes the clone tables are already
                populated and skips the baseline ingestion phase.
        """
        self.job_id = job_id
        self.dataset_ids = dataset_ids
        self.env_baseline = env_baseline
        self.env_candidate = env_candidate
        self.workspace_dir = workspace_dir or Path.cwd()
        self.baseline_pex = baseline_pex_path
        self.skip_baseline = skip_baseline_run

        # State Storage
        self.results: dict[str, Any] = {}
        self.start_time: float = 0
        self.total_drift_count: int = 0
        self.task_contexts: dict[str, TaskContext] = {}

        # Service instances (resolved dynamically per dataset)
        self.baseline_services: dict[str, Any] = {}
        self.candidate_services: dict[str, Any] = {}

        # Initialize contexts and services immediately
        self._prepare()

    def _prepare(self) -> None:
        """Initializes TaskContexts and resolves Sink services for all datasets."""
        builder_baseline = TaskContextBuilder(env=self.env_baseline)
        builder_candidate = TaskContextBuilder(env=self.env_candidate)

        for ds_id in self.dataset_ids:
            # Build Baseline Context
            results_baseline = builder_baseline.build(
                job_id=self.job_id, dataset_id=ds_id
            )
            if not results_baseline:
                raise ValueError(f"Could not build baseline context for {ds_id}")
            ctx_baseline = next(iter(results_baseline))

            # Build Candidate Context
            results_candidate = builder_candidate.build(
                job_id=self.job_id, dataset_id=ds_id
            )
            if not results_candidate:
                raise ValueError(f"Could not build candidate context for {ds_id}")
            ctx_candidate = next(iter(results_candidate))

            self.task_contexts[ds_id] = ctx_candidate

            self.baseline_services[ds_id] = ServiceFactory.get_sink(
                ctx_baseline.load.sink_type, **ctx_baseline.load.sink_config
            )
            self.candidate_services[ds_id] = ServiceFactory.get_sink(
                ctx_candidate.load.sink_type, **ctx_candidate.load.sink_config
            )

    def _run_baseline_job(self, dataset_id: str, suffix: str = CLONE_SUFFIX) -> bool:
        """Executes the baseline (stable) version of a dataset ingestion.

        Uses a subprocess call to an external PEX to ensure the 'Source of Truth'
        is generated using the production-stable code.

        Args:
            dataset_id: The dataset to run.
            suffix: The clone suffix to append to the sink identifier.

        Returns:
            bool: True if the baseline job execution succeeded.
        """
        if not self.baseline_pex or not self.baseline_pex.exists():
            LOG.error("Baseline PEX not found. Cannot generate comparison data.")
            return False

        ctx = self.task_contexts[dataset_id]
        target_ident = f"{ctx.load.sink_identifier}{suffix}"

        # Command: stable.pex add [DATE] -j [JOB] -d [DS]
        # --set load.sink_identifier=[CLONE]
        cmd = [
            str(self.baseline_pex.resolve()),
            "add",
            "2024-01-01",  # Static date for stable regressions
            "-j",
            self.job_id,
            "-d",
            dataset_id,
            "--env",
            self.env_baseline,
            "--set",
            f"load.sink_identifier={target_ident}",
        ]

        LOG.info(f"Running Baseline logic via PEX: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            LOG.error(f"Baseline run failed: {result.stderr}")
        return result.returncode == 0

    def _run_candidate_job(self, dataset_id: str, suffix: str = CLONE_SUFFIX) -> bool:
        """Executes the candidate (current) version of a dataset ingestion.

        Uses the internal TriggerRuntime to execute the current code against
        the skeleton clone table.

        Args:
            dataset_id: The dataset to run.
            suffix: The clone suffix to append to the sink identifier.

        Returns:
            bool: True if the candidate job execution succeeded.
        """
        from apps.ingestion.src.core.orchestrator.factory import assemble_runtime
        from apps.ingestion.src.core.orchestrator.modes.trigger import TriggerRuntime

        ctx = self.task_contexts[dataset_id]
        target_ident = f"{ctx.load.sink_identifier}{suffix}"

        # 1. Setup candidate runtime
        builder = TaskContextBuilder(env=self.env_candidate)
        exec_ctx = builder.get_execution_context(mode=ExecutionMode.NORMAL)
        runtime = assemble_runtime(exec_ctx, builder)

        if not isinstance(runtime, TriggerRuntime):
            LOG.error("Regression candidate run requires TriggerRuntime")
            return False

        # 2. Execute via internal TriggerRuntime
        try:
            # Override sink identifier to point to the shadow clone
            overrides: dict[str, Any] = {
                "load": {"sink_identifier": target_ident},
                "_global": {
                    "from_stage": StageName.first().label,
                    "to_stage": StageName.last().label,
                },
            }

            LOG.info(f"Running Candidate logic via TriggerRuntime: {dataset_id}")
            runtime.run(
                job_id=self.job_id,
                dataset_id=dataset_id,
                partition_date_str="2024-01-01",
                overrides=overrides,
            )
            return True
        except Exception as e:
            LOG.error(f"Candidate run failed: {e}")
            return False

    def _run_skeleton_clone(self, dataset_id: str, suffix: str = CLONE_SUFFIX):
        """Creates a skeleton clone of the baseline table.

        Args:
            dataset_id: Dataset identifier.
            suffix: Suffix for the clone table.
        """
        ctx = self.task_contexts[dataset_id]
        service = self.candidate_services[dataset_id]

        source_ident = ctx.load.sink_identifier
        target_ident = f"{source_ident}{suffix}"

        if hasattr(service, "clone"):
            service.clone(reference=source_ident, other=target_ident)
        else:
            LOG.warning(f"Service {service.name} does not support cloning.")

    def _run_cleanup(self, dataset_id: str, suffix: str = CLONE_SUFFIX):
        """Removes the shadow sink created for regression testing.

        Args:
            dataset_id: Dataset identifier.
            suffix: Suffix for the clone table.
        """
        ctx = self.task_contexts[dataset_id]
        service = self.candidate_services[dataset_id]
        target_ident = f"{ctx.load.sink_identifier}{suffix}"

        if hasattr(service, "drop"):
            service.drop(target_ident)
        else:
            LOG.warning(f"Service {service.name} does not support dropping tables.")

    def run_comparison(
        self,
        dataset_id: str,
        suffix: str = CLONE_SUFFIX,
        ignore_cols: set[str] | None = None,
    ) -> dict[str, Any]:
        """Executes the comparison logic for a single dataset.

        Uses generic Sink methods to calculate row mismatches and schema drift.

        Args:
            dataset_id: The ID of the dataset to compare.
            suffix: The suffix used for the cloned table.
            ignore_cols: Columns to exclude from the comparison.

        Returns:
            dict: A detailed comparison report for the dataset.

        Raises:
            ValueError: If no primary keys are defined in the schema.
        """
        ctx = self.task_contexts[dataset_id]
        candidate_service = self.candidate_services[dataset_id]

        ref_table = ctx.load.sink_identifier
        other_table = f"{ref_table}{suffix}"
        ignore_cols = ignore_cols or set()

        # 1. Schema Comparison
        schema_ref_df = candidate_service.get_schema(ref_table)
        schema_target_df = candidate_service.get_schema(other_table)

        schema_ref = set(schema_ref_df["column_name"].to_list())
        schema_target = set(schema_target_df["column_name"].to_list())

        extra_cols = schema_target - schema_ref
        missing_cols = schema_ref - schema_target

        # 2. Row Count Comparison
        ref_total = candidate_service.get_row_count(ref_table)
        target_total = candidate_service.get_row_count(other_table)

        # 3. Data Mismatch (Generic minus logic)
        # Rows in ref not in target
        missing_rows = candidate_service.minus(ref_table, other_table, ignore_cols)
        # Rows in target not in ref
        extra_rows = candidate_service.minus(other_table, ref_table, ignore_cols)

        # 4. Identity Drift (Only if Primary Keys exist)
        primary_keys = [
            col.target_col for col in ctx.extract.schema_items if col.primary_key
        ]

        if not primary_keys:
            raise ValueError(
                f"Regression failed for {dataset_id}: No primary keys defined. "
                "At least one column must be marked as 'primary_key' for deep auditing."
            )

        drift_samples: list[Any] = []
        # Future: Implement PK-based hash drift detection here

        return {
            "counts": {
                "ref_total": ref_total,
                "target_total": target_total,
                "missing_in_target": missing_rows,
                "extra_in_target": extra_rows,
            },
            "schema": {
                "extra_columns": list(extra_cols),
                "missing_columns": list(missing_cols),
            },
            "drift": {
                "mismatched_pk_samples": drift_samples,
            },
        }

    def run(self) -> dict[str, Any]:
        """Executes a batch of regressions for the specified datasets.

        Returns:
            dict: A comprehensive regression report.
        """
        self.start_time = time.perf_counter()
        LOG.info(f"🚀 Starting Regression Suite [Job: {self.job_id}]")
        LOG.info(f"Baseline: {self.env_baseline} ➔ Candidate: {self.env_candidate}")

        for ds_id in self.dataset_ids:
            with DatasetRegressionContext(self, ds_id):
                try:
                    LOG.info(f"🔍 Auditing dataset: {ds_id}")

                    # 1. Populate Baseline (Source of Truth)
                    baseline_ok = (
                        True if self.skip_baseline else self._run_baseline_job(ds_id)
                    )

                    if not baseline_ok:
                        self.results[ds_id] = {"error": "Baseline PEX failed"}
                        continue

                    # 2. Populate Candidate (The Current Code)
                    candidate_ok = self._run_candidate_job(ds_id)

                    if candidate_ok:
                        # 3. Generic Comparison
                        report = self.run_comparison(ds_id)
                        self.results[ds_id] = report
                        LOG.success(f"✅ Audit complete for {ds_id}")
                    else:
                        LOG.error(f"❌ {ds_id}: Candidate execution failed.")
                        self.results[ds_id] = {"error": "Candidate execution failed"}

                except Exception as e:
                    LOG.exception(f"💥 Failed to regress dataset {ds_id}")
                    self.results[ds_id] = {"error": str(e)}

        return self.generate_summary()

    def generate_summary(self) -> dict[str, Any]:
        """Generates a high-level summary of the entire batch run."""
        duration = time.perf_counter() - self.start_time

        return {
            "job_id": self.job_id,
            "status": "COMPLETED",
            "duration_sec": round(duration, 2),
            "total_datasets": len(self.results),
            "details": self.results,
        }


def get_task_ctx(job_id: str, dataset_id: str, env: str = "local") -> TaskContext:
    """Helper to resolve a single task context."""
    builder = TaskContextBuilder(env=env)
    return next(iter(builder.build(job_id=job_id, dataset_id=dataset_id)))


def suggest_affected_datasets(
    target_job_id: str,
    target_dataset_id: str,
    env: str = "local",
    config_root: Path = APP_CONFIG_ROOT,
) -> list[dict[str, str]]:
    """
    Heuristic utility to find datasets sharing the same transformation logic.

    Warning: This is a best-effort scan. Developers should always verify
    affected peers manually as cross-module Python dependencies may not
    be captured by signature matching.

    Args:
        target_job_id: The job ID currently being modified.
        target_dataset_id: The specific dataset ID modified.
        env: The environment context for config resolution.
        config_root: The root directory of job configurations.

    Returns:
        list[dict]: A list of suggested {job_id, dataset_id} pairs.
    """
    builder = TaskContextBuilder(env=env)
    peers = []

    try:
        target_ctx = get_task_ctx(target_job_id, target_dataset_id, env=env)
        target_transform_type = target_ctx.transform.transform_type.casefold()
        target_transform_name = (
            target_ctx.transform.transform_params.get("name") or ""
        ).casefold()
        LOG.info(
            f"Target transformation signature: Type='{target_transform_type}', "
            f"Name='{target_transform_name}'"
        )
    except Exception as e:
        LOG.error(f"Failed to resolve target job/dataset context: {e}")
        return []

    # 2. Glob and Scan
    if not config_root.exists():
        LOG.warning(f"Configuration root not found: {config_root}")
        return []

    for job_dir in config_root.iterdir():
        if not job_dir.is_dir():
            continue

        job_id = job_dir.name
        config_path = job_dir / "config.yaml"
        if not config_path.exists():
            continue

        try:
            # Build all contexts for this job_id to get all datasets
            all_contexts_for_job = builder.build(job_id=job_id)

            for peer_ctx in all_contexts_for_job:
                # Skip the target itself
                if (
                    peer_ctx.job_id == target_job_id
                    and peer_ctx.dataset_id == target_dataset_id
                ):
                    continue

                peer_transform_type = peer_ctx.transform.transform_type.casefold()
                peer_transform_name = (
                    peer_ctx.transform.transform_params.get("name") or ""
                ).casefold()

                # Compare transformation signatures
                if (
                    peer_transform_type == target_transform_type
                    and peer_transform_name == target_transform_name
                ):
                    peers.append(
                        {"job_id": peer_ctx.job_id, "dataset_id": peer_ctx.dataset_id}
                    )
        except Exception as e:
            LOG.warning(f"Skipping job '{job_id}' due to configuration error: {e}")
            continue

    return peers
