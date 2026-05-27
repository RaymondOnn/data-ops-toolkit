import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Annotated

import msgspec
import typer
from apps.ingestion.src.cli import actions
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

run_app = typer.Typer(
    help="Execution and ad-hoc job commands.", invoke_without_command=True
)


@run_app.callback()
def run_pipeline(
    ctx: typer.Context,
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
    """
    Execute the ingestion pipeline for a specific date and dataset.
    """
    if ctx.invoked_subcommand is not None:
        return

    if partition_date is None or job_id is None or dataset is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()

    if debug:
        actions.configure_runtime(
            debug=True,
            dry_run=actions.state["dry_run"],
            ray_mode=actions.state["ray_mode"].value,
        )

    try:
        overrides = parse_set_options(settings)

        setup_logger(
            log_dir=Path("./.workspace/logs"),
            is_prod=not actions.state["debug"],
            is_debug=actions.state["debug"],
            filename=f"{job_id}.jsonl",
            enqueue=True,
        )

        exec_mode = (
            ExecutionMode.DEBUG if actions.state["debug"] else ExecutionMode.NORMAL
        )
        builder = TaskContextBuilder()
        exec_ctx = builder.get_execution_context(mode=exec_mode)
        exec_ctx.ray_mode = actions.state["ray_mode"]
        runtime = assemble_runtime(exec_ctx, builder)

        if not isinstance(runtime, TriggerRuntime):
            typer.secho("❌ Error: 'run' command requires TriggerRuntime.", fg="red")
            raise typer.Exit(code=1)

        msg = f"🚀 Initializing {dataset} for {partition_date.date()}"
        if from_stage or to_stage:
            start_label = from_stage or StageName.first().label
            end_label = to_stage or StageName.last().label
            msg += f" (Range: {start_label} ➔ {end_label})"
        typer.echo(msg)

        if from_stage:
            overrides["_global"]["from_stage"] = from_stage
        if to_stage:
            overrides["_global"]["to_stage"] = to_stage

        runtime.run(
            job_id=job_id,
            dataset_id=dataset,
            partition_date_str=partition_date.strftime("%Y-%m-%d"),
            overrides=overrides,
        )

        # After runtime.run completes, we should check for any failures
        # and explicitly mention the resume command for the user.
        runtime.orchestrator.process_task_events()

    except ExceptionGroup as eg:
        typer.secho("\n❌ Some tasks failed during execution.", fg="red", bold=True)
        typer.echo("To resume failed tasks, use:")
        for run_id in [
            str(e).split("Run: ")[1].split(" |")[0]
            for e in eg.exceptions
            if "Run: " in str(e)
        ]:
            typer.secho(f"  python -m ingestion run resume {run_id}", fg="cyan")
        raise typer.Exit(code=1) from eg
    except Exception as e:
        logger.critical(f"Execution failed: {e}")
        if actions.state["debug"]:
            traceback.print_exc()
        raise typer.Exit(code=1) from e
    finally:
        logger.complete()


@run_app.command("adhoc")
def run_adhoc(
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
):
    """
    Triggers an ad-hoc run for a specific job and partition date.
    This creates a .cmd file in the signals/ directory for the daemon to pick up.
    """
    builder = TaskContextBuilder(env=env)
    exec_ctx = builder.get_execution_context()

    payload = {
        "job_id": job_id,
        "partition_date": partition_date,
        "dataset_id": dataset_id,
    }

    # Generate a unique filename to avoid collisions and 
    # allow multiple adhoc runs to be queued
    timestamp = get_current_timestamp().strftime("%Y%m%d%H%M%S")
    unique_id = make_short_hash(6)
    cmd_filename = f"ADHOC_RUN_{timestamp}_{unique_id}.cmd"
    cmd_file_path = exec_ctx.signal_path / cmd_filename

    try:
        with cmd_file_path.open("wb") as f:
            f.write(msgspec.json.encode(payload))
        logger.success(
            f"Ad-hoc run command '{cmd_filename}' created successfully "
            f"in {exec_ctx.signal_path}."
        )
        logger.debug(f"Payload: {payload}")
    except Exception as e:
        logger.error(f"Failed to create ad-hoc run command: {e}")
        sys.exit(1)


@run_app.command("resume")
def run_resume(
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
):
    """
    Resumes a failed task in the Always-On Orchestrator (Daemon).
    By default, resumes from the point of failure.
    """
    builder = TaskContextBuilder(env=env)
    exec_ctx = builder.get_execution_context()

    # Validate the from_stage if provided
    if from_stage:
        try:
            StageName.from_label(from_stage)
        except (ValueError, KeyError):
            valid_stages = [s.label for s in StageName]
            raise typer.BadParameter(
                f"Invalid stage '{from_stage}'. "
                f"Valid stages are: {', '.join(valid_stages)}"
            ) from None

    # Process overrides if provided
    from apps.ingestion.src.core.contexts.builder import parse_set_options

    overrides = parse_set_options(settings) if settings else {}

    payload = {"run_id": run_id, "from_stage": from_stage, "overrides": overrides}

    if exec_ctx.always_on:
        # --- DAEMON MODE: Drop Signal ---
        cmd_filename = f"RESUME_{run_id}.cmd"
        cmd_file_path = exec_ctx.signal_path / cmd_filename

        if cmd_file_path.exists():
            typer.secho(
                f"⚠️ A resume command for {run_id} is already pending.", fg="yellow"
            )
            raise typer.Exit()

        try:
            with cmd_file_path.open("wb") as f:
                f.write(msgspec.json.encode(payload))
            logger.success(f"Resume command created: {cmd_filename}")
        except Exception as e:
            logger.error(f"Failed to create resume command: {e}")
            sys.exit(1)
    else:
        # --- TRIGGER MODE: Execute Immediately ---
        runtime = assemble_runtime(exec_ctx, builder)
        folder = runtime.orchestrator.state_store.resolve_task_path(run_id)

        if not folder:
            typer.secho(f"❌ Error: Run ID {run_id} not found in workspace.", fg="red")
            raise typer.Exit(1)

        # Apply Surgical Overrides to the quarantined config before recovery
        if from_stage or overrides:
            # Explicitly cast to Path as resolve_task_path returns Path | None
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
