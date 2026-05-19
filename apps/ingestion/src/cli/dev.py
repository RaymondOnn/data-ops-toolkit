from typing import Annotated

import typer
from apps.ingestion.src.extras.regression.regression import (
    run_cleanup,
    run_skeleton_clone,
)

dev_app = typer.Typer(help="🛠️ Local development and prototyping sandbox.")


@dev_app.command("clone")
def dev_clone(
    job_id: Annotated[str, typer.Argument(help="The job ID containing the dataset")],
    dataset: Annotated[
        str, typer.Option("--dataset", "-d", help="Specific dataset to clone")
    ],
    env: Annotated[
        str, typer.Option("--env", help="Environment to resolve the source schema from")
    ] = "local",
    suffix: Annotated[
        str, typer.Option("--suffix", help="Suffix for the shadow table")
    ] = "_shadow",
):
    """
    Creates an empty 'shadow' copy of the destination container (e.g. Table)
    for testing.
    """
    typer.echo(f"🌀 Preparing skeleton clone for {dataset}...")

    try:
        run_skeleton_clone(job_id=job_id, dataset_id=dataset, env=env, suffix=suffix)
        typer.secho(
            f"✅ Success! Shadow sink created with suffix '{suffix}'",
            fg="green",
            bold=True,
        )
    except Exception as e:
        typer.secho(f"❌ Clone failed: {e}", fg="red")
        raise typer.Exit(1) from e


@dev_app.command("cleanup")
def dev_cleanup(
    job_id: Annotated[str, typer.Argument(help="The job ID containing the dataset")],
    dataset: Annotated[
        str, typer.Option("--dataset", "-d", help="Specific dataset to clean")
    ],
    env: Annotated[
        str, typer.Option("--env", help="Environment where the shadow exists")
    ] = "local",
    suffix: Annotated[
        str, typer.Option("--suffix", help="Suffix used for the shadow table")
    ] = "_shadow",
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation prompt"),
):
    """
    Physically removes the 'shadow' table created for testing.
    """
    if not force and not typer.confirm(
        f"Are you sure you want to drop the shadow sink for {dataset}?"
    ):
        raise typer.Abort()

    try:
        run_cleanup(job_id=job_id, dataset_id=dataset, env=env, suffix=suffix)
        typer.secho("✅ Shadow sink purged.", fg="green")
    except Exception as e:
        typer.secho(f"❌ Cleanup failed: {e}", fg="red")
