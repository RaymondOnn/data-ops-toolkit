"""Notification CLI Group.

Provides CLI commands for triggering operational notification digests (e.g. via Airflow or Cron).
"""

from typing import Annotated

import typer
from libs.alerts.email import EmailClient
from loguru import logger

from src.services.alerts.digest import OperationalDigestService
from src.services.repo.metadata import MetadataRepository

notify_app = typer.Typer(
    help="Notification utilities for operational digests.",
    no_args_is_help=True,
)


@notify_app.command("digest")
def send_digest(
    recipients: Annotated[
        list[str],
        typer.Option(
            "--recipient",
            "-r",
            help="Recipient email address(es) for the digest.",
        ),
    ],
    db_key: Annotated[
        str,
        typer.Option(
            "--db-key",
            help="Metadata database service type configuration key.",
        ),
    ] = "postgres_db",
) -> None:
    """Send pending operational digest emails (schema changes, task errors, etc.)."""
    if not recipients:
        typer.secho(
            "Error: At least one --recipient must be provided.", fg=typer.colors.RED
        )
        raise typer.Exit(code=1)

    logger.info(
        f"Triggering operational digest notification for {len(recipients)} recipients..."
    )

    meta_repo = MetadataRepository(type=db_key)
    email_client = EmailClient()

    service = OperationalDigestService(
        repo=meta_repo,
        email_client=email_client,
        recipients=recipients,
    )

    sent = service.send_digest()
    if sent:
        typer.secho(
            "Operational digest dispatched successfully.", fg=typer.colors.GREEN
        )
    else:
        typer.secho("No pending events for digest.", fg=typer.colors.YELLOW)
