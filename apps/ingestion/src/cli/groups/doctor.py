from typing import Annotated

import typer

from src.cli.state import state
from src.core.contexts import TaskContextBuilder
from src.core.orchestrator.common import StateHub
from src.core.orchestrator.doctor import Doctor

doctor_app = typer.Typer(help="🩺 Diagnose environment health and configuration.")
network_app = typer.Typer(help="🌐 Network path and connectivity diagnostics.")
doctor_app.add_typer(network_app, name="network")


def _get_doctor(env: str = "local") -> Doctor:
    """Helper to instantiate the Doctor utility with its dependencies.

    Args:
        env: The environment context to load configurations from.
            Defaults to "local".

    Returns:
        Doctor: An initialized diagnostic engine.

    Decision: Dependency Injection.
    Encapsulates the complex setup of the Doctor component (StateStore,
    DB config, and ContextBuilder) into a single factory method. This
    ensures that CLI commands remain focused on user interaction
    rather than object graph construction.
    """
    builder = TaskContextBuilder(env=env)
    exec_ctx = builder.build_execution_context()
    db_config = builder.app_settings.get("services.clickhouse", {}).to_dict()
    state = StateHub(exec_ctx=exec_ctx, db_config=db_config)
    return Doctor(exec_ctx, lambda: state.sink.db, builder)


def _exit_on_failure(success: bool) -> None:
    """Exits the CLI process with code 1 if the operation failed.

    Args:
        success: The boolean result of a diagnostic check.

    Raises:
        typer.Exit: If success is False.

    Decision: Consistent CLI Exit.
    Standardizes failure reporting across all diagnostic commands.
    Using a central helper ensures that failed checks always return
    a non-zero exit code, which is critical for CI/CD pre-flight
    validation.
    """
    if not success:
        raise typer.Exit(code=1)


@doctor_app.callback(invoke_without_command=True)
def doctor_main(
    ctx: typer.Context,
):
    """Runs all diagnostic checks by default.

    Args:
        ctx: The Typer context.
        debug: Enables verbose output for checks.

    Decision: Multi-level Dispatch.
    Uses the Typer callback mechanism to provide a "check everything"
    experience by default when no subcommand is provided, while still
    allowing specialized tools (like network or config) to be
    invoked individually.
    """
    doctor = _get_doctor()

    if ctx.invoked_subcommand is None:
        doctor.run_all(state["verbose_level"] >= 2)


@doctor_app.command("fs")
def doctor_fs():
    """Checks filesystem health including disk space and permissions.

    Args:
        debug: Enables verbose output.

    Decision: Separation of Concerns.
    Delegates storage-specific diagnostics to the Doctor class to
    ensure that CLI logic doesn't become brittle if the underlying
    disk-checking logic needs to evolve.
    """
    doctor = _get_doctor()

    doctor.check_filesystem(state["verbose_level"] >= 2)


@network_app.command("check")
def network_check(
    target_host: Annotated[
        str, typer.Argument(help="The host to test (e.g. 's3.amazonaws.com')")
    ],
    target_port: Annotated[int, typer.Argument(help="The port to test (e.g. 443)")],
    proxy_url: Annotated[
        str, typer.Option("--proxy", help="Proxy URL to use for the check")
    ] = "http://127.0.0.1:3128",
):
    """
    Deep-dive diagnostic of the network path to a specific host and port.
    Checks VPN, DNS, Proxies (CNTLM), and Firewalls.

    Args:
        target_host: The host to test.
        target_port: The port to test.
        proxy_url: Optional proxy settings.
        debug: Enables verbose output.

    Decision: Diagnostic Verbosity.
    Provides a specific entry point for deep-path analysis. The inclusion
    of proxy settings is a specific decision to support enterprise
    environments where corporate proxies (like CNTLM) often interfere
    with cloud service connectivity.
    """
    doctor = _get_doctor()

    success = doctor.check_network_path(target_host, target_port, proxy_url=proxy_url)
    _exit_on_failure(success)


@network_app.command("trace")
def network_trace(
    target_host: Annotated[
        str, typer.Argument(help="The host to visualize (e.g. 'clickhouse.prod')")
    ],
):
    """
    Visualizes every network hop between this machine and the target host.

    Args:
        target_host: The host to visualize.
        debug: Enables verbose output.

    Decision: Visual Troubleshooting.
    Tracing hops is often a manual terminal task; bringing it into the
    doctor suite ensures that operators can troubleshoot latency or
    firewall drops without leaving the application's toolset.
    """
    doctor = _get_doctor()
    success = doctor.trace_route(target_host)
    _exit_on_failure(success)


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
):
    """
    Tests connectivity for a specific service type across all its configured instances.

    Args:
        service_name: The service identifier (e.g., 'oracle_db').
        env: Target environment for credential resolution.
        debug: Enables verbose output.

    Decision: Config-Driven Validation.
    Instead of hardcoding hosts or ports, this command resolves details
    from the environment's configuration, ensuring that the health check
    matches the actual parameters used by the execution engine.
    """
    doctor = _get_doctor(env=env)
    doctor.check_service(service_name)


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
):
    """
    Validates YAML syntax and schema models for configurations.

    Args:
        job_id: Optional ID of a specific job to validate.
        all_jobs: If True, validates the entire configs directory.
        env: Environment context for validation.
        debug: Enables verbose output.

    Decision: Proactive Validation.
    Allows operators to check syntax and schema constraints before
    triggering a run, significantly reducing the "fail-at-runtime" loop.
    """
    doctor = _get_doctor(env=env)
    doctor.check_config(job_id, all_jobs=all_jobs)


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

    Args:
        job_id: The identifier for the job.
        dataset: Optional specific dataset identifier.
        env: Environment context for resolution.

    Decision: Observability.
    Merging YAML configurations across levels can create surprising
    results. This command acts as a "What You See Is What You Get"
    viewer for the internal task configuration.
    """
    doctor = _get_doctor(env=env)
    doctor.check_config(job_id, dataset_id=dataset)
