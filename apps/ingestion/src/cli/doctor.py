from typing import Annotated

import typer

doctor_app = typer.Typer(help="🩺 Diagnose environment health and configuration.")
from apps.ingestion.src.core.contexts import TaskContextBuilder
from apps.ingestion.src.core.orchestrator.common import StateStore
from apps.ingestion.src.core.orchestrator.doctor import Doctor

network_app = typer.Typer(help="🌐 Network path and connectivity diagnostics.")
doctor_app.add_typer(network_app, name="network")


def _get_doctor(env: str = "local") -> Doctor:
    """Helper to instantiate the Doctor utility with its dependencies."""
    builder = TaskContextBuilder(env=env)
    exec_ctx = builder.get_execution_context()
    db_config = builder.app_settings.get("services.clickhouse", {}).to_dict()
    state_store = StateStore(exec_ctx=exec_ctx, db_config=db_config)
    return Doctor(exec_ctx, lambda: state_store.db, builder)


@doctor_app.callback(invoke_without_command=True)
def doctor_main(
    ctx: typer.Context,
    debug: bool = typer.Option(
        False, "--debug", help="Show detailed diagnostic output."
    ),
):
    """Runs all diagnostic checks by default."""
    doctor = _get_doctor()

    if ctx.invoked_subcommand is None:
        doctor.check_all(debug)


@doctor_app.command("fs")
def doctor_fs(
    debug: bool = typer.Option(False, "--debug", help="Show detailed output.")
):
    """Checks filesystem health (disk space, permissions)."""
    doctor = _get_doctor()

    doctor.check_filesystem(debug)


@network_app.command("check")
def network_check(
    target_host: Annotated[
        str, typer.Argument(help="The host to test (e.g. 's3.amazonaws.com')")
    ],
    target_port: Annotated[int, typer.Argument(help="The port to test (e.g. 443)")],
    proxy_url: Annotated[
        str, typer.Option("--proxy", help="Proxy URL to use for the check")
    ] = "http://127.0.0.1:3128",
    debug: bool = typer.Option(False, "--debug", help="Show detailed output."),
):
    """
    Deep-dive diagnostic of the network path to a specific host and port.
    Checks VPN, DNS, Proxies (CNTLM), and Firewalls.
    """
    doctor = _get_doctor()

    success = doctor.run_network_diagnostics(
        target_host, target_port, proxy_url=proxy_url
    )
    if not success:
        raise typer.Exit(code=1)


@network_app.command("trace")
def network_trace(
    target_host: Annotated[
        str, typer.Argument(help="The host to visualize (e.g. 'clickhouse.prod')")
    ],
    debug: bool = typer.Option(False, "--debug", help="Show detailed output."),
):
    """
    Visualizes every network hop between this machine and the target host.
    """
    doctor = _get_doctor()
    success = doctor.run_network_trace(target_host)
    if not success:
        raise typer.Exit(code=1)


@doctor_app.command("connect")
def doctor_connect(
    service_name: Annotated[
        str,
        typer.Argument(
            help="The registered service name (type) to test (e.g., 'oracle_db')"
        ),
    ],
    env: Annotated[
        str, typer.Option("--env", help="Target environment to load credentials from")
    ] = "local",
    debug: bool = typer.Option(False, "--debug", help="Show detailed output."),
):
    """
    Tests connectivity for a specific service type across all its configured instances.
    """
    doctor = _get_doctor(env=env)
    doctor.check_service_connectivity(service_name)


@doctor_app.command("config")
def doctor_config(
    job_id: Annotated[
        str | None,
        typer.Argument(help="Optional job ID to validate specific config.yaml"),
    ] = None,
    all_jobs: Annotated[
        bool, typer.Option("--all", help="Validate all jobs in the configs directory")
    ] = False,
    env: Annotated[
        str, typer.Option("--env", help="Environment context for validation")
    ] = "local",
    debug: bool = typer.Option(False, "--debug", help="Show detailed output."),
):
    """
    Validates YAML syntax and schema models for configurations.
    """
    doctor = _get_doctor(env=env)
    doctor.check_config(job_id, all_jobs=all_jobs, debug=debug)


@doctor_app.command("inspect")
def doctor_inspect(
    job_id: Annotated[str, typer.Argument(help="The job ID to inspect")],
    dataset: Annotated[
        str | None,
        typer.Option("--dataset", "-d", help="Specific dataset identifier"),
    ] = None,
    env: Annotated[
        str, typer.Option("--env", help="Environment context for resolution")
    ] = "local",
):
    """
    Displays the fully merged configuration (App + Job + Dataset) for a specific run.
    """
    doctor = _get_doctor(env=env)
    doctor.inspect_config(job_id, dataset_id=dataset)
