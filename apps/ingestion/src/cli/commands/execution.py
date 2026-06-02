"""
CLI Execution Module.

Provides the Typer-based entry points for manual execution, ad-hoc run
triggering, and surgical task recovery, bridging user input with the
engine's runtime modes.
"""

import traceback
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any

import typer
from apps.ingestion.src.cli.state import app, configure_runtime, state
from apps.ingestion.src.cli.utils import _write_signal_file
from apps.ingestion.src.core.contexts import (
    ExecutionMode,
    TaskContextBuilder,
    parse_set_options,
)
from apps.ingestion.src.core.models.stages.enums import StageName
from apps.ingestion.src.core.models.task import Task
from apps.ingestion.src.core.orchestrator.factory import assemble_runtime
from apps.ingestion.src.core.orchestrator.modes.trigger import TriggerRuntime
from apps.ingestion.src.utils.common import make_short_hash, setup_logger
from libs.utils.dates import get_current_timestamp
from loguru import logger


def _log_startup_msg(
    dataset: str, partition_date: datetime, from_stage: str | None, to_stage: str | None
) -> None:
    """Constructs and prints a descriptive startup message to the console.

    Args:
        dataset: The identifier of the dataset being processed.
        partition_date: The logical date for the ingestion run.
        from_stage: The starting stage label, if restricted.
        to_stage: The ending stage label, if restricted.

    Decision: Visual Confirmation.
    Providing immediate visual feedback of the target dataset and date
    partition helps users catch configuration errors before heavy
    computational resources are provisioned.
    """
    msg = f"🚀 Initializing {dataset} for {partition_date.date()}"
    if from_stage or to_stage:
        start_label = from_stage or StageName.first().label
        end_label = to_stage or StageName.last().label
        msg += f" (Range: {start_label} ➔ {end_label})"
    typer.echo(msg)


def _report_run_failures(eg: ExceptionGroup) -> None:
    """Parses an ExceptionGroup to provide human-readable recovery instructions.

    Args:
        eg: The group of exceptions collected during parallel execution.

    Raises:
        IndexError: If the exception message format does not match the
            expected 'Run: [ID]' pattern.

    Decision: Instructions Parsing.
    In high-concurrency environments, identifying exactly which run failed
    can be difficult. We parse the specific run_ids from the failure messages
    to give the user the exact command needed to resume the task.
    """
    typer.secho("\n❌ Some tasks failed during execution.", fg="red", bold=True)
    typer.echo("To resume failed tasks, use:")

    # Deduplicate run_ids from exception messages
    run_ids = set()
    for e in eg.exceptions:
        msg = str(e)
        if "Run: " in msg:
            # Extract run_id: "[Run: 2024... | Stage: ...]" -> "2024..."
            try:
                rid = msg.split("Run: ")[1].split(" |", maxsplit=1)[0]
                run_ids.add(rid)
            except IndexError:
                continue

    for run_id in sorted(run_ids):
        typer.secho(f"  python -m ingestion run resume {run_id}", fg="cyan")


def _get_trigger_runtime(
    debug: bool, ray_mode: Any
) -> tuple[TriggerRuntime, TaskContextBuilder]:
    """Bootstraps the execution context and TriggerRuntime for a synchronous run.

    Args:
        debug: Whether to enable verbose logging and debug mode.
        ray_mode: The distribution mode (LOCAL, CLUSTER, etc.) to use.

    Returns:
        tuple: (TriggerRuntime instance, TaskContextBuilder instance).

    Decision: Centralized Assembly.
    By wrapping the builder and runtime assembly, we ensure that the
    ExecutionMode is correctly mapped from the CLI flags before the
    Orchestrator begins discovery, preventing environment mismatch.
    """
    exec_mode = ExecutionMode.DEBUG if debug else ExecutionMode.NORMAL
    builder = TaskContextBuilder()
    exec_ctx = builder.get_execution_context(mode=exec_mode)
    exec_ctx.ray_mode = ray_mode

    runtime = assemble_runtime(exec_ctx, builder)
    if not isinstance(runtime, TriggerRuntime):
        typer.secho("❌ Error: 'run' command requires TriggerRuntime.", fg="red")
        raise typer.Exit(code=1)

    return runtime, builder


def _apply_run_overrides(
    overrides: dict, from_stage: str | None, to_stage: str | None
) -> dict:
    """Injects execution range constraints into the global override dictionary.

    Args:
        overrides: Existing custom user settings.
        from_stage: The label of the stage to start from.
        to_stage: The label of the stage to stop after.

    Returns:
        dict: The updated overrides dictionary.

    Decision: Global Range Policy.
    We fold 'from' and 'to' stages into the '_global' key. This allows
    individual stage strategies to remain unaware of the total range while
    the Orchestrator enforces the execution boundaries.
    """
    if from_stage:
        overrides.setdefault("_global", {})["from_stage"] = from_stage
    if to_stage:
        overrides.setdefault("_global", {})["to_stage"] = to_stage
    return overrides


def _validate_stage_label(label: str | None) -> str | None:
    """Verifies that a provided stage label exists in the StageName registry.

    Args:
        label: The string label to validate.

    Returns:
        str | None: The validated label.
    """
    if not label:
        return None
    try:
        StageName.from_label(label)
        return label
    except (ValueError, KeyError):
        valid_stages = [s.label for s in StageName]
        raise typer.BadParameter(
            f"Invalid stage '{label}'. Valid stages: {', '.join(valid_stages)}"
        ) from None


def _resume_locally(
    exec_ctx: Any, builder: Any, run_id: str, from_stage: str | None, overrides: dict
) -> None:
    """Initializes a local runtime to surgically resume a failed task.

    Args:
        exec_ctx: The execution context.
        builder: The context builder for re-provisioning.
        run_id: The ID of the task to recover.
        from_stage: Optional stage label to rewind to.
        overrides: Custom settings to apply during recovery.

    Decision: Local Recovery Ownership.
    In Trigger mode, the CLI assumes full ownership of the execution.
    It manually triggers the Janitor and drives the engine loop
    synchronously, providing immediate console feedback to the operator
    during surgical repairs.
    """
    runtime = assemble_runtime(exec_ctx, builder)
    folder = runtime.orchestrator.state_store.resolve_task_path(run_id)

    if not folder:
        typer.secho(f"❌ Error: Run ID {run_id} not found in workspace.", fg="red")
        raise typer.Exit(code=1)

    # Apply Surgical Overrides to the quarantined config before recovery
    if from_stage or overrides:
        task = Task.from_folder(folder, exec_ctx)
        updates = {}
        if from_stage:
            updates["current_stage"] = from_stage
        if overrides:
            updates["custom_overrides"] = overrides

        if updates:
            task.update_manifest(updates)

    typer.echo(f"🔧 Recovering task {run_id}...")
    runtime.orchestrator.janitor.recover_task_by_path(folder)

    # In Trigger mode, we must manually drive the engine to process the recovered task
    typer.echo("🚀 Starting execution engine...")
    runtime.orchestrator.process_task_events()
    runtime.orchestrator._drive_engine()

    logger.success(f"Manual resume of {run_id} completed.")


@app.command(name="run")
def execute_pipeline(
    partition_date: Annotated[
        datetime | None,
        typer.Argument(
            formats=["%Y-%m-%d"], help="The target date for processing (YYYY-MM-DD)"
        ),
    ],
    job_id: Annotated[
        str | None,
        typer.Option("--job-id", "-j", help="The unique UUID for this run"),
    ] = None,
    dataset: Annotated[
        str | None,
        typer.Option("--dataset", "-d", help="Dataset identifier (e.g., 'sales_data')"),
    ] = None,
    from_stage: Annotated[
        str | None, typer.Option("--from", help="Start execution from this stage")
    ] = None,
    to_stage: Annotated[
        str | None, typer.Option("--to", help="Stop execution after this stage")
    ] = None,
    debug: Annotated[
        bool, typer.Option("--debug", help="Enable verbose logging")
    ] = False,
    settings: Annotated[
        list[str] | None,
        typer.Option(
            "--set", "-s", help="Override config: key=value (e.g. -s batch_size=5000)"
        ),
    ] = None,
) -> None:
    """Execute the ingestion pipeline for a specific date and dataset.

    Args:
        partition_date: The target date for processing (YYYY-MM-DD).
        job_id: The unique identifier for this specific pipeline run.
        dataset: Dataset identifier (e.g., 'sales_data').
        from_stage: Start execution from this specific stage.
        to_stage: Stop execution after this specific stage.
        debug: Enable verbose logging and diagnostics.
        settings: Override config parameters using key=value pairs.

    Raises:
        typer.BadParameter: If required run arguments are missing.
        typer.Exit: On execution failure.

    Decision: CLI Command Implementation.
    This is the primary synchronous entry point. It blocks until the
    Ray cluster completes, providing a post-mortem summary on failure
    to assist in CI/CD pipeline observability.
    """
    if partition_date is None or job_id is None or dataset is None:
        # Since we removed the callback, Typer handles the help auto-display
        # for missing arguments in commands automatically.
        raise typer.BadParameter("Missing required arguments: date, job-id, dataset")

    configure_runtime(
        debug=debug,
        dry_run=state["dry_run"],
        ray_mode=state["ray_mode"].value,
    )

    try:
        # 1. Setup Environment
        overrides = _apply_run_overrides(
            parse_set_options(settings), from_stage, to_stage
        )
        setup_logger(
            log_dir=Path("./.workspace/logs"),
            is_prod=not state["debug"],
            is_debug=state["debug"],
            filename=f"{job_id}.jsonl",
            enqueue=True,
        )

        # 2. Assemble and Run
        runtime, _ = _get_trigger_runtime(state["debug"], state["ray_mode"])
        _log_startup_msg(dataset, partition_date, from_stage, to_stage)

        runtime.run(
            job_id=job_id,
            dataset_id=dataset,
            partition_date_str=partition_date.strftime("%Y-%m-%d"),
            overrides=overrides,
        )

        runtime.orchestrator.process_task_events()

    except ExceptionGroup as eg:
        _report_run_failures(eg)
        raise typer.Exit(code=1) from eg

    except Exception as e:
        logger.critical(f"Execution failed: {e}")
        if state["debug"]:
            traceback.print_exc()
        raise typer.Exit(code=1) from e
    finally:
        logger.complete()


@app.command(name="add")
def add_adhoc_run(
    partition_date: Annotated[
        str, typer.Argument(help="The partition date (YYYY-MM-DD) for the run")
    ],
    job_id: Annotated[
        str, typer.Option("--job-id", "-j", help="The job ID to execute")
    ],
    dataset_id: Annotated[
        str | None,
        typer.Option(
            "--dataset",
            "-d",
            help="Optional: Specific dataset ID within the job. "
            "If omitted, all datasets for the job will run.",
        ),
    ] = None,
    env: Annotated[
        str, typer.Option("--env", help="Environment to resolve configs from")
    ] = "local",
) -> None:
    """Triggers an ad-hoc run command for the Always-On Daemon.

    Args:
        partition_date: The date partition for the task.
        job_id: The identifier of the job to trigger.
        dataset_id: Optional specific dataset filter.
        env: The environment (local/dev/prod) for config resolution.

    Decision: Decoupled Signaling.
    This command does not execute the job. It persists a JSON payload as a
    .cmd file in the signals directory. The Daemon's CommandProcessor
    detects this file and performs the trigger within the managed reactive
    loop.
    """
    builder = TaskContextBuilder(env=env)
    exec_ctx = builder.get_execution_context()

    payload = {
        "job_id": job_id,
        "partition_date": partition_date,
        "dataset_id": dataset_id,
    }

    timestamp = get_current_timestamp().strftime("%Y%m%d%H%M%S")
    unique_id = make_short_hash(6)
    _write_signal_file(exec_ctx, f"ADD_{timestamp}_{unique_id}.cmd", payload)


@app.command(name="resume")
def resume_failed_run(
    run_id: Annotated[str, typer.Argument(help="The specific Run ID to resume")],
    from_stage: Annotated[
        str | None,
        typer.Option("--from", help="Force restart from this stage (Rewind)"),
    ] = None,
    settings: Annotated[
        list[str] | None,
        typer.Option("--set", "-s", help="Override config parameters: key=value"),
    ] = None,
    env: Annotated[
        str, typer.Option("--env", help="Environment to resolve configs from")
    ] = "local",
) -> None:
    """Surgically recovers a failed or held task.

    Args:
        run_id: The unique identifier of the failed task.
        from_stage: Force a rewind to this specific stage.
        settings: Key=value overrides for the recovery run.
        env: Environment to resolve configurations from.

    Raises:
        typer.BadParameter: If the provided 'from_stage' label is invalid.

    Decision: Dual-Mode Recovery.
    If the Daemon is active (detected via lock file), this drops a RESUME
    signal. If running in standalone mode, it re-provisions the task and
    immediately drives the engine loop to process the recovery.

    Decision: Human Override Policy.
    Manual resumes reset the retry_count in the manifest, assuming that
    human intervention has resolved the root cause of the previous failure.
    """
    builder = TaskContextBuilder(env=env)
    exec_ctx = builder.get_execution_context()

    _validate_stage_label(from_stage)

    # Process overrides if provided
    overrides = parse_set_options(settings) if settings else {}
    payload = {"run_id": run_id, "from_stage": from_stage, "overrides": overrides}

    if exec_ctx.always_on:
        # --- DAEMON MODE: Drop Signal ---
        _write_signal_file(exec_ctx, f"RESUME_{run_id}.cmd", payload)
    else:
        # --- TRIGGER MODE: Execute Immediately ---
        _resume_locally(exec_ctx, builder, run_id, from_stage, overrides)
