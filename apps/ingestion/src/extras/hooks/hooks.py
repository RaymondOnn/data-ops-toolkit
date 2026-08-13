import shutil
from typing import TYPE_CHECKING, Any

from loguru import logger
from upath import UPath

from src.services.factory import ServiceFactory

from .enums import HookAction, HookType

if TYPE_CHECKING:
    from .runner import HookRunner

LOG = logger
MAX_ROUTINE_DEPTH = 5

# Define strategy registry map
HOOK_STRATEGIES = {}


def resolve_src_paths(src_raw: str, active_src: Any | None) -> list[str]:
    """Discover source files safely handling wildcards and literal matches."""
    from upath import UPath

    if active_src:
        if "*" in src_raw:
            return active_src.glob(src_raw, recursive=True)

        resolved_src = active_src.resolve(src_raw)
        if active_src.is_file(resolved_src):
            return [src_raw]
        if active_src.fs.isdir(resolved_src):
            return active_src.glob(f"{src_raw.rstrip('/')}/**/*", recursive=True)
        return []

    # Local filesystem fallback
    src_upath = UPath(src_raw)
    if "*" in src_raw:
        return [str(p) for p in src_upath.parent.glob(src_upath.name)]
    return [src_raw] if src_upath.exists() else []


def register_strategy(hook_type):
    def decorator(func):
        HOOK_STRATEGIES[hook_type] = func
        return func

    return decorator


@register_strategy(HookType.STORE)
def _run_store(runner: "HookRunner", action: "HookAction", **kwargs):
    if not action.key:
        raise ValueError("STORE hook requires a 'key' field.")
    rendered_value = (
        action.value.format(**runner._template_vars) if action.value else ""
    )
    runner.store_map[action.key] = rendered_value
    LOG.debug(f"Stored mapping: store.{action.key} = '{rendered_value}'")


@register_strategy(HookType.QUERY)
def _run_query(runner: "HookRunner", action: "HookAction", **kwargs):
    """Execute a SQL query hook."""
    if not action.query or not action.connection:
        raise ValueError("Query hook requires 'query' and 'connection'")
    # Template substitution
    query = action.query.format(**runner._template_vars)
    # Support file:// references
    if query.startswith("file://"):
        from upath import UPath

        query = UPath(query.removeprefix("file://")).read_text()
    sink = ServiceFactory.get_sink(action.connection)
    sink.command(query)


@register_strategy(HookType.HTTP)
def _run_http(runner: "HookRunner", action: "HookAction", **kwargs):
    """Execute an HTTP webhook hook."""
    import niquests

    if not action.url:
        raise ValueError("HTTP hook requires 'url'")

    payload = {
        "job_id": runner.task.job_id,
        "run_id": runner.task.run_id,
        "step": runner.task.target_step_id,
        "partition_date": runner.task.partition_date,
    }
    resp = niquests.post(action.url, json=payload, timeout=action.timeout_seconds)

    # Try to safe-extract JSON if applicable
    try:
        resp_json = resp.json()
    except Exception:
        resp_json = None

    return {"response": {"status_code": resp.status_code, "json": resp_json}}


@register_strategy(HookType.SCRIPT)
def _run_script(runner: "HookRunner", action: "HookAction", **kwargs):
    """Execute a shell script hook."""
    import subprocess

    if not action.command:
        raise ValueError("Script hook requires 'command'")
    subprocess.run(
        action.command,
        shell=True,
        check=True,
        timeout=action.timeout_seconds,
        env={
            "TASK_RUN_ID": runner.task.run_id,
            "TASK_JOB_ID": runner.task.job_id,
            "TASK_PARTITION_DATE": runner.task.partition_date,
        },
    )


@register_strategy(HookType.COPY)
def _run_copy(runner: "HookRunner", action: "HookAction", **kwargs):
    """Transfer files between storage locations.

    Automatically discovers files using centralized globbing, detects directory
    targets to prevent filename truncation, and handles both cloud (S3) and
    local file system storage drivers dynamically.
    """
    if not action.from_path or not action.to_path:
        raise ValueError("Copy hook requires 'from_path' and 'to_path'")

    src_raw = action.from_path.format(**runner._template_vars)
    dst_raw = action.to_path.format(**runner._template_vars)

    src_client = (
        ServiceFactory.get(**action.from_connection).connector
        if action.from_connection
        else None
    )
    dst_client = (
        ServiceFactory.get(**action.to_connection).connector
        if action.to_connection
        else None
    )
    active_src = src_client or dst_client
    active_dst = dst_client or src_client

    src_paths = resolve_src_paths(src_raw, active_src)
    if not src_paths:
        raise FileNotFoundError(
            f"No source objects matched the path criteria: {src_raw}"
        )

    dst_upath = UPath(dst_raw)
    is_dst_dir = dst_raw.endswith("/") or not dst_upath.suffix

    try:
        for src_path in src_paths:
            current_dst = dst_raw
            src_item_upath = UPath(src_path)

            is_file = (
                active_src.is_file(active_src.resolve(src_path))
                if active_src
                else src_item_upath.is_file()
            )
            if is_file and is_dst_dir:
                current_dst = str(UPath(dst_raw) / src_item_upath.name)

            if (
                (action.from_connection or action.to_connection)
                and active_src
                and active_dst
            ):
                active_dst.cp(
                    active_src.resolve(src_path),
                    active_dst.resolve(current_dst),
                    recursive=not is_file,
                )
            else:
                local_dst = UPath(current_dst)
                if is_file:
                    local_dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(str(src_item_upath), str(local_dst))
                else:
                    shutil.copytree(
                        str(src_item_upath), str(local_dst), dirs_exist_ok=True
                    )
        if len(src_paths) == 1:
            LOG.success(f"Copied '{src_paths}' → '{dst_raw}'")
        else:
            LOG.success(f"Copied {len(src_paths)} item(s) → '{dst_raw}'")
    except Exception:
        LOG.exception(f"Failed to copy files from: {src_raw} -> {dst_raw}")
        raise


def _delete_directory(path_raw: str, recursive: bool, client: Any | None) -> None:
    """Handles removing a full directory container or prefix."""
    if client:
        resolved = client.resolve(path_raw)
        if client.exists(resolved):
            client.rm(resolved, recursive=recursive)
    else:
        path = UPath(path_raw)
        if path.exists() and path.is_dir():
            if recursive:
                shutil.rmtree(str(path))
            else:
                path.rmdir()  # Safe fallback: raises OSError if not empty


def _delete_files_by_pattern(path_raw: str, client: Any | None) -> None:
    """Handles resolving and unlinking individual files or matched patterns."""
    targets = resolve_src_paths(path_raw, client)
    if not targets:
        LOG.warning(f"Delete targets not found (skipping): {path_raw}")
        return

    for target in targets:
        if client:
            resolved = client.resolve(target)
            if client.exists(resolved):
                client.rm(resolved, recursive=False)
        else:
            path = UPath(target)
            if path.is_file():
                path.unlink()


@register_strategy(HookType.DELETE)
def _run_delete(runner: "HookRunner", action: "HookAction", **kwargs):
    """Remove files or directories at a storage location.

    If location points to a directory, it deletes the directory container.
    If location points to a file or wildcard pattern, it targets individual files.
    """
    if not action.location:
        raise ValueError("Delete hook requires 'location'")

    loc_raw = action.location.format(**runner._template_vars)
    client = (
        ServiceFactory.get(**action.connection).connector if action.connection else None
    )

    is_directory_target = loc_raw.endswith("/") or (
        "*" not in loc_raw and not UPath(loc_raw).suffix
    )
    if is_directory_target:
        _delete_directory(loc_raw, action.recursive, client)
    else:
        _delete_files_by_pattern(loc_raw, client)


@register_strategy(HookType.ROUTINE)
def _run_routine(runner: "HookRunner", action: "HookAction", _depth: int = 0, **kwargs):
    """Execute a reusable sequence of hook actions from a YAML file.

    Routine files are YAML lists of HookAction definitions. Supports
    nesting up to _MAX_ROUTINE_DEPTH levels to prevent infinite loops.
    """
    import msgspec
    import yaml

    if not action.routine_path:
        raise ValueError("Routine hook requires 'routine_path'")

    if _depth >= MAX_ROUTINE_DEPTH:
        raise RecursionError(
            f"Routine nesting exceeded max depth ({MAX_ROUTINE_DEPTH}): "
            f"{action.routine_path}"
        )

    path = UPath(action.routine_path.format(**runner._template_vars))
    if not path.exists():
        raise FileNotFoundError(f"Routine file not found: {path}")

    raw = yaml.safe_load(path.read_text())
    if not isinstance(raw, list):
        raise ValueError(f"Routine file must contain a YAML list of actions: {path}")

    from .enums import HookAction as HookActionStruct

    # Convert raw dicts into validated HookAction structs
    routine_actions: list[HookActionStruct] = msgspec.convert(
        raw, type=list[HookActionStruct]
    )

    LOG.info(
        f"Running routine '{path.name}' with {len(routine_actions)} actions "
        f"(depth={_depth + 1})"
    )
    for i, sub_action in enumerate(routine_actions):
        LOG.debug(f"  Routine action {i + 1}/{len(routine_actions)}: {sub_action.type}")
        runner._execute_action(sub_action, _depth=_depth + 1)
