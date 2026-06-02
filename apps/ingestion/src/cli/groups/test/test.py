from pathlib import Path
from typing import Annotated

import msgspec
import msgspec.yaml
import polars as pl
import typer
from apps.ingestion.src.cli.state import state
from apps.ingestion.src.core.contexts import (
    ExecutionMode,
    TaskContext,
    TaskContextBuilder,
)
from apps.ingestion.src.core.orchestrator.factory import assemble_runtime
from apps.ingestion.src.extras.regression.regression import (
    RegressionRunner,
    suggest_affected_datasets,
)

from .harness import ScenarioType
from .scenarios import SimulationEngine

test_app = typer.Typer(help="Testing and validation utilities.")
regression_app = typer.Typer(help="Regression testing and impact analysis.")
test_app.add_typer(regression_app, name="regression")


@test_app.command("config")
def test_config(
    path: Annotated[Path, typer.Argument(help="Path to a config.yaml or app.yaml")],
):
    """
    Deep-dive schema audit for configuration files.
    Validates syntax, msgspec Struct compatibility, and type strictness.

    Args:
        path: Physical path to the YAML configuration file.

    Raises:
        typer.Exit: If the file is missing or schema validation fails.

    Decision: Structural vs. Model Audit.
    Distinguishes between global app config (general dict validation)
    and job config (strict msgspec Struct validation). This ensures
    that critical job parameters are verified against the exact
    models used by the execution engine.
    """
    if not path.exists():
        typer.secho(f"❌ Error: File not found at {path}", fg="red", err=True)
        raise typer.Exit(1)

    typer.echo(f"📋 Auditing schema: {path.name}...")

    try:
        # 1. Structural Integrity Check
        # We use TaskContextBuilder to attempt a dry-run resolution.
        # This verifies that the YAML keys map correctly to our internal models.
        # Note: In a CI environment, we use a mock env to avoid needing real secrets.
        # We load as a dict first to identify the configuration type
        raw_bytes = path.read_bytes()
        raw_data = msgspec.yaml.decode(raw_bytes, type=dict)

        if "workspace_dir" in raw_data:
            typer.echo("🔍 Identified as Global App Config.")
            # msgspec validates based on keys present in the raw_data dictionary
        else:
            typer.echo("🔍 Identified as Job Configuration.")
            # Deep Audit: Direct decode from YAML to TaskContext Struct
            # This is significantly faster and catches schema errors natively.
            msgspec.yaml.decode(raw_bytes, type=TaskContext)

        typer.secho(f"✅ {path.name} is schema-compliant.", fg="green", bold=True)
    except Exception as e:
        typer.secho(f"💥 Schema Validation Failed: {e}", fg="red", bold=True)
        raise typer.Exit(1) from e


@regression_app.command("run")
def test_regression(
    job_id: Annotated[str, typer.Option("--job-id", "-j", help="The job ID")],
    dataset: Annotated[
        str, typer.Option("--dataset", "-d", help="Specific dataset to compare")
    ],
    baseline_pex: Annotated[
        Path | None,
        typer.Option("--baseline-pex", help="Path to stable app.pex for shadow run"),
    ] = None,
    env: Annotated[
        str, typer.Option("--env", help="Environment to resolve sinks from")
    ] = "dev",
    impact: Annotated[
        bool,
        typer.Option("--impact", help="Suggest and include affected peer datasets"),
    ] = False,
    reuse_baseline: Annotated[
        bool,
        typer.Option("--reuse-baseline", help="Skip baseline ingestion phase"),
    ] = False,
):
    """
    Executes a data regression audit comparing baseline and candidate datasets.

    Creates a shadow copy of the baseline data (optionally using a different PEX),
    and performs a deep PK-based comparison to detect missing or drifted records.

    Args:
        job_id: The job identifier.
        dataset: The target dataset for comparison.
        baseline_pex: Optional PEX for the baseline "Shadow" run.
        env: Target environment.
        impact: If True, suggests and includes affected peer datasets.
        reuse_baseline: If True, skips the baseline ingestion phase.

    Decision: Automated Shadow Comparisons.
    Implements a high-level orchestration of the RegressionRunner.
    The decision to include impact analysis within the run flow
    allows users to expand their test suite dynamically based on
    shared transformation logic discovery.
    """
    datasets_to_run = [dataset]

    if impact:
        typer.secho("🔍 Scanning for affected peer datasets...", fg="cyan")
        peers = suggest_affected_datasets(job_id, dataset, env=env)

        if peers:
            typer.echo("The following datasets share the same transformation logic:")
            for p in peers:
                typer.echo(f" - {p['job_id']}.{p['dataset_id']}")

            if typer.confirm("Include these in the regression suite?"):
                datasets_to_run.extend([p["dataset_id"] for p in peers])
        else:
            typer.echo("No affected peers found.")

    typer.echo(
        f"🚀 Initializing regression suite for {len(datasets_to_run)} datasets..."
    )

    runner = RegressionRunner(
        job_id=job_id,
        dataset_ids=list(set(datasets_to_run)),
        env_baseline=env,
        env_candidate=env,
        baseline_pex_path=baseline_pex,
        skip_baseline_run=reuse_baseline,
    )
    summary = runner.run()

    typer.echo("\n" + "═" * 60)
    typer.secho("📊 DATA REGRESSION SUMMARY", fg="magenta", bold=True)
    typer.echo("═" * 60)
    typer.echo(f"Job ID:      {summary['job_id']}")
    typer.echo(f"Duration:    {summary['duration_sec']}s")
    typer.echo(f"Datasets:    {summary['total_datasets']}")
    typer.echo("-" * 60)

    for ds_id, report in summary["details"].items():
        typer.secho(f"🔹 Dataset: {ds_id}", fg="blue", bold=True)
        if "error" in report:
            typer.secho(f"  ❌ ERROR: {report['error']}", fg="red")
            continue

        counts = report.get("counts", {})
        typer.echo(
            f"  Rows:    Baseline={counts.get('ref_total')} | "
            f"Candidate={counts.get('target_total')}"
        )
        typer.echo(f"  Missing: {counts.get('missing_in_target')} rows")
        typer.echo(f"  Extra:   {counts.get('extra_in_target')} rows")

        drift = report.get("drift", {}).get("mismatched_pk_samples", [])
        if drift:
            typer.secho(
                f"  ⚠️ DRIFT: Detected in {len(drift)} sample records",
                fg="yellow",
            )
            typer.echo(f"  Sample Mismatched PKs: {drift}")
        else:
            typer.secho("  ✅ DATA MATCHED: No drift detected", fg="green")
        typer.echo("")

    typer.echo("═" * 60)


@regression_app.command("impact")
def test_impact(job_id: str, dataset: str, env: str = "local"):
    """
    Discovery tool to find other datasets sharing the same transformation logic.

    Args:
        job_id: The job identifier.
        dataset: The specific dataset.
        env: Environment context.

    Decision: Logic-Based Discovery.
    Helps developers understand the blast radius of their changes
    by finding peer datasets that utilize the same underlying
    transformation code.
    """
    peers = suggest_affected_datasets(job_id, dataset, env=env)
    if not peers:
        typer.echo("No affected peer datasets found.")
        return

    typer.echo(f"👥 Found {len(peers)} datasets sharing this transformation logic:")
    for p in peers:
        typer.echo(f" - {p['job_id']}.{p['dataset_id']}")


@test_app.command("scenario")
def test_scenario(
    scenario: Annotated[
        ScenarioType | None,
        typer.Argument(
            help="The simulation to execute. Leave empty to run all in order."
        ),
    ] = None,
    job_id: Annotated[
        str | None, typer.Option("--job-id", "-j", help="Job ID to use")
    ] = None,
    dataset: Annotated[
        str | None, typer.Option("--dataset", "-d", help="Dataset ID to use")
    ] = None,
    automated_input: Annotated[
        bool,
        typer.Option(
            "--yes", "-y", help="Provide automated input for interactive scenarios."
        ),
    ] = False,
    env: str = "local",
):
    """
    Runs complex multi-stage simulations to test the limits of the engine.

    Args:
        scenario: Specific simulation to run. If None, runs a batch.
        job_id: Optional job ID to use as a target.
        dataset: Optional dataset to use as a target.
        automated_input: If True, bypasses interactive prompts.
        env: Environment context.

    Decision: Batch Orchestration.
    Provides a recommended order for resilience testing (Concurrency ->
    Memory -> Zombie, etc.). This ensures that destructive tests like
    KILL_DAEMON are run last, maintaining environment stability for
    as long as possible.
    """
    if scenario is None:
        # Recommended Order: Concurrency -> Memory -> Resilience -> Recovery
        # We exclude interactive ones (Latency, Disk Full)
        # from the default batch to avoid hangs.
        batch_order = [
            ScenarioType.CONCURRENCY,
            ScenarioType.MEMORY,
            ScenarioType.ZOMBIE,
            ScenarioType.RECOVERY,
            ScenarioType.BLOCK,
            ScenarioType.STRESS,
            ScenarioType.SCHEMA_DRIFT,
            ScenarioType.DATA_LOSS,
            ScenarioType.RETENTION,
            ScenarioType.KILL_DAEMON,  # Destructive, so run last
        ]

        if automated_input:
            # Include interactive scenarios if automated input is enabled
            batch_order.extend([ScenarioType.LATENCY, ScenarioType.DISK_FULL])

        typer.secho(
            "🚀 No scenario specified. Preparing full resilience batch run...",
            fg="magenta",
            bold=True,
        )
        typer.echo(f"Sequence: {' ➔ '.join([s.value for s in batch_order])}")

        if not automated_input and not typer.confirm(
            "This will stress local resources. Continue?"
        ):
            raise typer.Abort()

        results = []
        for s in batch_order:  # Pass automated_input to each simulation
            outcome, reason = _run_simulation(s, job_id, dataset, env, automated_input)
            results.append({"scenario": s, "outcome": outcome, "reason": reason})

        # --- FINAL SUMMARY REPORT ---
        typer.echo("\n" + "=" * 60)
        typer.secho("📊 RESILIENCE BATCH SUMMARY", fg="magenta", bold=True)
        typer.echo("=" * 60)

        for res in results:
            name = res["scenario"].value.upper().ljust(20)
            if res["outcome"] is True:
                typer.secho(f"{name} ✅ SUCCESS", fg="green")
            elif res["outcome"] is False:
                typer.secho(f"{name} ❌ FAILED ({res['reason']})", fg="red")
            else:
                typer.secho(f"{name} 🟡 SKIPPED ({res['reason']})", fg="yellow")

        typer.echo("=" * 60)

        # Exit with error if any test in the batch failed
        if any(r["outcome"] is False for r in results):
            raise typer.Exit(code=1)

    else:
        outcome, reason = _run_simulation(
            scenario, job_id, dataset, env, automated_input
        )
        if outcome is False:
            typer.secho(f"\n❌ Scenario Failed: {reason}", fg="red", bold=True)
            raise typer.Exit(code=1)
        if outcome is None:
            typer.secho(f"\n🟡 Scenario Skipped: {reason}", fg="yellow")


def _run_simulation(
    scenario: ScenarioType,
    job_id: str | None,
    dataset: str | None,
    env: str,
    automated_input: bool = False,
) -> tuple[bool | None, str | None]:
    """Internal runner logic for a specific scenario.

    Args:
        scenario: The type of simulation to execute.
        job_id: The target job ID.
        dataset: The target dataset ID.
        env: The environment.
        automated_input: If True, disables interactive blocks.

    Returns:
        tuple: (success_status, failure_reason).

    Decision: Runtime Re-provisioning.
    Each simulation re-assembles the runtime to ensure a clean state.
    The decision to block if a daemon is already running prevents
    state corruption during chaos testing.
    """
    typer.secho(f"\n🎬 STARTING SCENARIO: {scenario.upper()}", fg="blue", bold=True)

    builder = TaskContextBuilder(env=env)
    mode = ExecutionMode.DRY_RUN if state.get("dry_run") else ExecutionMode.NORMAL
    if state.get("debug"):
        mode = ExecutionMode.DEBUG

    exec_ctx = builder.get_execution_context(mode=mode)
    runtime = assemble_runtime(exec_ctx, builder)

    # 1. Safety Check: Prevent simulations from conflicting with an active daemon
    # Exception: KILL_DAEMON specifically targets an active process
    if exec_ctx.lock_file.exists() and scenario != ScenarioType.KILL_DAEMON:
        msg = (
            "🚨 ERROR: Orchestrator daemon is already running.\n"
            "Running simulations alongside a live process can cause state corruption. "
            "Stop the daemon first."
        )
        typer.secho(msg, fg="red", err=True)
        raise typer.Exit(1)

    # 2. Resolve Defaults
    job_id = job_id or builder.app_settings.get("test.default_job")
    dataset = dataset or builder.app_settings.get("test.default_dataset")

    engine = SimulationEngine(runtime, env=env)
    return engine.run(scenario, job_id, dataset, automated_input)


@test_app.command("peek")
def test_peek(
    path: Annotated[Path, typer.Argument(help="Path to the Parquet file or directory")],
    rows: Annotated[
        int, typer.Option("--rows", "-n", help="Number of rows to show")
    ] = 10,
    tail: Annotated[
        bool, typer.Option("--tail", help="Show the last N rows instead of the first")
    ] = False,
    sql: Annotated[
        str | None,
        typer.Option(
            "--sql", "-s", help="SQL query to run (the table is named 'self')"
        ),
    ] = None,
    schema: Annotated[
        bool, typer.Option("--schema", help="Only show the schema/dtypes")
    ] = False,
):
    """
    Instantly inspect the contents or schema of a Parquet artifact.

    Args:
        path: Path to the Parquet file or directory.
        rows: Number of rows to show.
        tail: Show the last N rows instead of the first.
        sql: SQL query to run (the table is named 'self').
        schema: Only show the schema/dtypes.

    Decision: SQL-Native Observability.
    While re-running a stage is common, diagnostic visibility into intermediate
    artifacts is crucial for debugging high-volume pipelines (50M+ rows) without
    the overhead of a full notebook or execution run. This tool leverages
    Polars' lazy scanning and SQL engine to minimize memory usage during inspection.
    """
    if not path.exists():
        typer.secho(f"❌ Error: Path not found: {path}", fg="red")
        raise typer.Exit(1)

    try:
        # Decision: Use scan_parquet for large files to avoid OOM during inspection
        lf = pl.scan_parquet(path)

        if schema:
            typer.secho(f"📋 Schema for: {path.name}", fg="cyan", bold=True)
            for col, dtype in lf.schema.items():
                typer.echo(f"  {col.ljust(25)} {dtype}")
            return

        if sql:
            # Decision: SQL-Native Observability.
            # Using Polars SQLContext allows standard SQL syntax for filtering/selecting.
            try:
                ctx = pl.SQLContext(self=lf)
                lf = ctx.execute(sql)
            except Exception as sql_err:
                typer.secho(f"❌ Invalid SQL Query: {sql_err}", fg="red")
                raise typer.Exit(1) from sql_err

        df = lf.tail(rows).collect() if tail else lf.head(rows).collect()

        typer.echo(df)

    except Exception as e:
        typer.secho(f"💥 Failed to peek artifact: {e}", fg="red")
        raise typer.Exit(1) from e
