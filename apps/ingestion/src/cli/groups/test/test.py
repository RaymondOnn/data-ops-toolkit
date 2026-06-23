"""Testing and validation CLI commands."""

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
    find_affected_datasets,
)

from .harness import ScenarioType
from .scenarios import SimulationRunner

test_app = typer.Typer(help="Testing and validation utilities")
regression_app = typer.Typer(help="Regression testing and impact analysis")
test_app.add_typer(regression_app, name="regression")


# =============================================================================
# Configuration Validation
# =============================================================================


@test_app.command("config")
def validate_config(
    path: Annotated[Path, typer.Argument(help="Path to config.yaml or app.yaml")],
) -> None:
    """Audits the syntax and structural integrity of YAML configuration files.

    Args:
        path (Path): The filesystem path to the YAML configuration file.

    Raises:
        typer.Exit: If the file is missing or schema validation fails.

    Notes:
        Decision: Model-Based Auditing.
        We distinguish between 'app.yaml' (global infrastructure) and 'config.yaml'
        (job-specific logic) to apply the correct msgspec validation model.
    """
    if not path.exists():
        typer.secho(f"❌ File not found: {path}", fg="red")
        raise typer.Exit(1)

    typer.echo(f"📋 Auditing: {path.name}")

    try:
        raw = msgspec.yaml.decode(path.read_bytes(), type=dict)

        if "workspace_dir" in raw:
            typer.echo("🔍 Global app config detected")
        else:
            typer.echo("🔍 Job configuration detected")
            msgspec.yaml.decode(path.read_bytes(), type=TaskContext)

        typer.secho(f"✅ {path.name} is valid", fg="green", bold=True)
    except Exception as e:
        typer.secho(f"💥 Validation failed: {e}", fg="red", bold=True)
        raise typer.Exit(1) from e


# =============================================================================
# Regression Testing
# =============================================================================


@regression_app.command("run")
def run_regression(
    partition_date: Annotated[str, typer.Argument(help="Partition date (YYYY-MM-DD)")],
    job_id: Annotated[str, typer.Option("--job-id", "-j")],
    dataset: Annotated[str, typer.Option("--dataset", "-d")],
    baseline_pex: Annotated[Path | None, typer.Option("--baseline-pex")] = None,
    env: Annotated[str, typer.Option("--env")] = "dev",
    reuse_baseline: Annotated[bool, typer.Option("--reuse-baseline")] = False,
) -> None:
    """Triggers a regression audit comparing stable baseline data with local code.

    Args:
        partition_date (str): The date partition (YYYY-MM-DD) used to slice data.
        job_id (str): The job identifier to test.
        dataset (str): The specific dataset for comparison.
        baseline_pex (Path | None): Optional path to a stable PEX binary for
            baseline generation.
        env (str): The environment context for the run.
        reuse_baseline (bool): If True, skips the baseline generation phase and
            uses existing tables.

    Notes:
        Decision: Positional Argument Consistency.
        Making `partition_date` a positional argument aligns with the core `run`
        command UX, providing a familiar interface for developers targeting
        specific temporal snapshots for debugging.
    """
    datasets = [dataset]
    typer.echo(f"🚀 Running regression on {len(datasets)} datasets...")

    runner = RegressionRunner(
        job_id=job_id,
        dataset_ids=list(set(datasets)),
        env=env,
        baseline_pex_path=baseline_pex,
        skip_baseline_run=reuse_baseline,
    )
    summary = runner.run(partition_date)
    summary.print_summary()


@regression_app.command("impact")
def show_impact(job_id: str, dataset: str, env: str = "local") -> None:
    """Identifies peer datasets that share the same transformation logic.

    Args:
        job_id (str): The identifier of the job containing the target dataset.
        dataset (str): The specific dataset ID to analyze.
        env (str): The environment context to scan for peers. Defaults to "local".

    Raises:
        typer.Exit: If the configuration cannot be parsed.

    Notes:
        Decision: Impact Analysis.
        By analyzing shared transformation signatures (logic name + type), we
        enable developers to perform "Static Impact Analysis." This prevents
        localized fixes in one job from inadvertently breaking peer datasets
        that rely on the same shared transformation strategy.
    """
    peers = find_affected_datasets(job_id, dataset, env=env)

    if not peers:
        typer.echo("No affected datasets found")
        return

    typer.echo(f"👥 {len(peers)} affected datasets:")
    for p in peers:
        typer.echo(f"  - {p['job_id']}.{p['dataset_id']}")


# =============================================================================
# Simulation Scenarios
# =============================================================================


@test_app.command("scenario")
def run_scenario(
    scenario: Annotated[
        ScenarioType | None, typer.Argument(help="Simulation to run (omit for batch)")
    ] = None,
    job_id: Annotated[str | None, typer.Option("--job-id", "-j")] = None,
    dataset: Annotated[str | None, typer.Option("--dataset", "-d")] = None,
    auto: Annotated[
        bool, typer.Option("--yes", "-y", help="Auto-confirm prompts")
    ] = False,
    env: str = "local",
) -> None:
    """Entry point for executing chaos engineering and resilience scenarios.

    Args:
        scenario (ScenarioType | None): Simulation to run (omit for batch).
        job_id (str | None): Target job ID for the simulation.
        dataset (str | None): Target dataset ID for the simulation.
        auto (bool): If True, bypasses interactive confirmation.
        env (str): Target environment.

    Notes:
        Decision: Hybrid Dispatch.
        Supports both automated batch runs for CI and targeted single runs
        for local debugging.
    """
    if scenario is None:
        _run_batch(auto, job_id, dataset, env)
    else:
        _run_single(scenario, job_id, dataset, auto, env)


def _run_batch(auto: bool, job_id: str | None, dataset: str | None, env: str) -> None:
    """Executes a predefined sequence of resilience and chaos engineering tests.

    Args:
        auto (bool): If True, skips manual confirmations and runs extended scenarios
            (e.g., Disk Full).
        job_id (str | None): The job identifier to use for the simulation.
        dataset (str | None): The dataset identifier to use for the simulation.
        env (str): The environment context (e.g., 'local').

    Notes:
        Decision: Curated Test Sequence.
        The batch runs tests in an order that maximizes early discovery of
        critical system failures.
    """
    order = [
        ScenarioType.CONCURRENCY,
        ScenarioType.MEMORY,
        ScenarioType.ZOMBIE,
        ScenarioType.RECOVERY,
        ScenarioType.BLOCK,
        ScenarioType.STRESS,
        ScenarioType.SCHEMA_DRIFT,
        ScenarioType.DATA_LOSS,
        ScenarioType.RETENTION,
        ScenarioType.KILL_DAEMON,
    ]

    if auto:
        order.extend([ScenarioType.LATENCY, ScenarioType.DISK_FULL])

    typer.secho("🚀 Running full resilience batch...", fg="magenta", bold=True)
    typer.echo(f"Sequence: {' ➔ '.join([s.value for s in order])}")

    if not auto and not typer.confirm("This will stress local resources. Continue?"):
        raise typer.Abort()

    results = []
    for s in order:
        outcome, reason = _execute_scenario(s, job_id, dataset, env, auto)
        results.append({"scenario": s, "outcome": outcome, "reason": reason})

    # Summary
    typer.echo("\n" + "=" * 60)
    typer.secho("📊 RESILIENCE BATCH SUMMARY", fg="magenta", bold=True)
    typer.echo("=" * 60)

    for r in results:
        name = r["scenario"].value.upper().ljust(20)
        if r["outcome"] is True:
            typer.secho(f"{name} ✅ PASS", fg="green")
        elif r["outcome"] is False:
            typer.secho(f"{name} ❌ FAIL ({r['reason']})", fg="red")
        else:
            typer.secho(f"{name} 🟡 SKIP ({r['reason']})", fg="yellow")

    if any(r["outcome"] is False for r in results):
        raise typer.Exit(1)


def _run_single(
    scenario: ScenarioType,
    job_id: str | None,
    dataset: str | None,
    auto: bool,
    env: str,
) -> None:
    """Executes a specific simulation and handles terminal failures.

    Args:
        scenario (ScenarioType): The type of test to run.
        job_id (str | None): The target job ID.
        dataset (str | None): The target dataset ID.
        auto (bool): Auto-input flag.
        env (str): Target environment.

    Notes:
        Decision: Error Reporting.
        Single runs provide detailed failure reasons to the console for
        rapid iteration.
    """
    outcome, reason = _execute_scenario(scenario, job_id, dataset, env, auto)

    if outcome is False:
        typer.secho(f"\n❌ Scenario failed: {reason}", fg="red", bold=True)
        raise typer.Exit(1)
    if outcome is None:
        typer.secho(f"\n🟡 Scenario skipped: {reason}", fg="yellow")


def _execute_scenario(
    scenario: ScenarioType,
    job_id: str | None,
    dataset: str | None,
    env: str,
    auto: bool,
) -> tuple[bool | None, str | None]:
    """Initializes the runtime and dispatches a scenario to the SimulationRunner.

    Args:
        scenario (ScenarioType): The chaos scenario type.
        job_id (str | None): The job ID.
        dataset (str | None): The dataset ID.
        env (str): The environment.
        auto (bool): Auto-input flag.

    Returns:
        tuple[bool | None, str | None]: A pair of (success_status, failure_reason).

    Raises:
        typer.Exit: If the daemon is running and preventing the simulation.

    Notes:
        Decision: Runtime Isolation for Simulations.
        Each scenario re-provisions the entire runtime to ensure chaos effects
        (like disk pressure or ray worker cancellations) do not leak across
        different tests in a batch.
    """
    typer.secho(f"\n🎬 STARTING: {scenario.value.upper()}", fg="blue", bold=True)

    builder = TaskContextBuilder(env=env)
    mode = ExecutionMode.DRY_RUN if state.get("dry_run") else ExecutionMode.NORMAL
    exec_ctx = builder.build_execution_context(mode=mode)
    runtime = assemble_runtime(exec_ctx, builder)

    # Don't allow simulations with active daemon (except kill test)
    if exec_ctx.lock_file.exists() and scenario != ScenarioType.KILL_DAEMON:
        typer.secho(
            "🚨 Daemon is running. Stop it before running simulations.", fg="red"
        )
        raise typer.Exit(1)

    # Resolve defaults
    job_id = job_id or builder.app_settings.get("test.default_job")
    dataset = dataset or builder.app_settings.get("test.default_dataset")

    runner = SimulationRunner(runtime, env=env)
    return runner.run(scenario, job_id, dataset, auto)


# =============================================================================
# Data Inspection
# =============================================================================


@test_app.command("peek")
def peek_parquet(
    path: Annotated[Path, typer.Argument(help="Path to Parquet file or directory")],
    rows: Annotated[int, typer.Option("--rows", "-n")] = 10,
    tail: Annotated[bool, typer.Option("--tail")] = False,
    sql: Annotated[str | None, typer.Option("--sql", "-s")] = None,
    show_schema: Annotated[bool, typer.Option("--schema")] = False,
) -> None:
    """Low-overhead utility to inspect the contents and schema of Parquet files.

    Args:
        path (Path): Path to the parquet file or directory.
        rows (int): Number of rows to display. Defaults to 10.
        tail (bool): If True, shows the last N rows. Defaults to False.
        sql (str | None): Optional SQL query to run against the file (via Polars).
        show_schema (bool): If True, only prints the column types and returns.

    Raises:
        typer.Exit: If the path does not exist or file parsing fails.

    Notes:
        Decision: Zero-Copy Inspection.
        By using `pl.scan_parquet`, we can inspect multi-GB files without
        loading them into memory, making the CLI safe to use on shared nodes.
    """
    if not path.exists():
        typer.secho(f"❌ Path not found: {path}", fg="red")
        raise typer.Exit(1)

    try:
        lf = pl.scan_parquet(path)

        if show_schema:
            typer.secho(f"📋 Schema: {path.name}", fg="cyan", bold=True)
            for col, dtype in lf.schema.items():
                typer.echo(f"  {col.ljust(25)} {dtype}")
            return

        if sql:
            ctx = pl.SQLContext(self=lf)
            lf = ctx.execute(sql)

        df = lf.tail(rows).collect() if tail else lf.head(rows).collect()
        typer.echo(df)

    except Exception as e:
        typer.secho(f"💥 Failed to peek: {e}", fg="red")
        raise typer.Exit(1) from e
