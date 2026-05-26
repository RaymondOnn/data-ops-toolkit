from pathlib import Path
from typing import Annotated

import msgspec
import msgspec.yaml
import typer
from apps.ingestion.src.cli import actions
from apps.ingestion.src.core.contexts import (
    ExecutionMode,
    TaskContext,
    TaskContextBuilder,
)
from apps.ingestion.src.core.orchestrator.factory import assemble_runtime
from apps.ingestion.src.extras.regression.regression import (
    find_affected_peers,
    run_comparison,
)

from .harness import ScenarioType
from .scenarios import SimulationEngine

test_app = typer.Typer(help="Testing and validation utilities.")


@test_app.command("config")
def test_config(
    path: Annotated[Path, typer.Argument(help="Path to a config.yaml or app.yaml")],
):
    """
    Deep-dive schema audit for configuration files.
    Validates syntax, msgspec Struct compatibility, and type strictness.
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
        raise typer.Exit(1)


@test_app.command("compare")
def test_compare(
    job_id: Annotated[str, typer.Argument(help="The job ID containing the dataset")],
    dataset: Annotated[
        str, typer.Option("--dataset", "-d", help="Specific dataset to compare")
    ],
    env: Annotated[
        str, typer.Option("--env", help="Environment to resolve sinks from")
    ] = "local",
    suffix: Annotated[
        str, typer.Option("--suffix", help="Suffix used for the shadow table")
    ] = "_shadow",
    ignore_cols: Annotated[
        str, typer.Option("--ignore", help="Comma-separated columns to skip")
    ] = "",
    detailed: Annotated[
        bool, typer.Option("--detailed", help="Use analytical reporting")
    ] = False,
):
    """
    Performs a side-by-side equality check between production and shadow sinks.
    """
    ignore_set = {c.strip() for c in ignore_cols.split(",") if c.strip()}

    typer.echo(f"🔍 Comparing {dataset} against shadow copy...")

    success = run_comparison(
        job_id=job_id,
        dataset_id=dataset,
        env=env,
        suffix=suffix,
        ignore_cols=ignore_set,
        detailed=detailed,
    )

    if not success:
        typer.secho("❌ Regression detected! Data mismatch found.", fg="red", bold=True)
        raise typer.Exit(1)

    typer.secho("✅ Regression test passed. Data is identical.", fg="green")


@test_app.command("impact")
def test_impact(job_id: str, dataset: str, env: str = "local"):
    """
    Discovery tool to find other datasets sharing the same transformation logic.
    """
    peers = find_affected_peers(job_id, dataset, env=env)
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
    """
    if scenario is None:
        # Recommended Order: Concurrency -> Memory -> Resilience -> Recovery
        # We exclude interactive ones (Latency, Disk Full) from the default batch to avoid hangs.
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
        outcome, reason = _run_simulation(scenario, job_id, dataset, env, automated_input)
        if outcome is False:
            typer.secho(f"\n❌ Scenario Failed: {reason}", fg="red", bold=True)
            raise typer.Exit(code=1)
        elif outcome is None:
            typer.secho(f"\n🟡 Scenario Skipped: {reason}", fg="yellow")


def _run_simulation(
    scenario: ScenarioType,
    job_id: str | None,
    dataset: str | None,
    env: str,
    automated_input: bool = False,
) -> tuple[bool | None, str | None]:
    """Internal runner logic for a specific scenario."""
    typer.secho(f"\n🎬 STARTING SCENARIO: {scenario.upper()}", fg="blue", bold=True)

    builder = TaskContextBuilder(env=env)
    mode = (
        ExecutionMode.DRY_RUN if actions.state.get("dry_run") else ExecutionMode.NORMAL
    )
    if actions.state.get("debug"):
        mode = ExecutionMode.DEBUG

    exec_ctx = builder.get_execution_context(mode=mode)
    runtime = assemble_runtime(exec_ctx, builder)

    # 1. Safety Check: Prevent simulations from conflicting with an active daemon
    # Exception: KILL_DAEMON specifically targets an active process
    if exec_ctx.lock_file.exists() and scenario != ScenarioType.KILL_DAEMON:
        typer.secho(
            "🚨 ERROR: Orchestrator daemon is already running.", fg="red", err=True
        )
        typer.echo(
            "Running simulations alongside a live process can cause state corruption. Stop the daemon first."
        )
        raise typer.Exit(1)

    # 2. Resolve Defaults
    job_id = job_id or builder.app_settings.get("test.default_job")
    dataset = dataset or builder.app_settings.get("test.default_dataset")

    engine = SimulationEngine(runtime, env=env)
    return engine.run(scenario, job_id, dataset, automated_input)
