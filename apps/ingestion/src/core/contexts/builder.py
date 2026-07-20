"""Configuration builder for task contexts with hierarchical resolution."""

import os
import re
from collections import ChainMap
from datetime import datetime
from pathlib import Path
from typing import Any

import msgspec
import pendulum
from apps.ingestion.src.core.contexts.execution import (
    ExecutionContext,
    ExecutionMode,
    RayMode,
)
from apps.ingestion.src.core.contexts.task import TaskContext
from apps.ingestion.src.core.models.stages.enums import ALL_STAGES, Stage
from apps.ingestion.src.extras.hooks import HookAction, StageHooks
from apps.ingestion.src.services.factory import SECRET_PROTOCOL
from apps.ingestion.src.utils.constants import (
    APP_CONFIG_ROOT,
    APP_CURRENT_ENV,
    DEFAULT_PARTITION_COL,
)
from dynaconf import Dynaconf, LazySettings
from dynaconf.utils.boxing import DynaBox
from libs.utils.dates import current_timestamp
from libs.utils.dict import find_keys_by_pattern, flatten_dict, set_nested_key
from libs.utils.file import is_path_like
from loguru import logger

LOG = logger
DEFAULT_CONFIG_PATH = APP_CONFIG_ROOT / "defaults.yaml"
APP_CONFIG_PATH = APP_CONFIG_ROOT / "app.yaml"
SERVICES_CONFIG_PATH = APP_CONFIG_ROOT / "services.yaml"
JOB_CONFIG_DIR = APP_CONFIG_ROOT / "jobs"
SERVICE_REF_OLD_KEY = "service_ref"
SERVICE_REF_NEW_KEY = "connection"
PATH_PREFIX_BLACKLIST = (SECRET_PROTOCOL, "http://", "https://", "s3://")


def interpolate_env_vars(value: Any) -> Any:
    """Recursively resolve ${VAR:-DEFAULT}, ${VAR}, or $VAR syntax in a structure.

    Args:
        value: The configuration value (string, dict, or list) to interpolate.

    Returns:
        Any: The structural copy with environment variables expanded.
    """
    if isinstance(value, dict):
        return {k: interpolate_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [interpolate_env_vars(v) for v in value]

    if not isinstance(value, str) or "$" not in value:
        return value

    pattern = re.compile(r"\$?\$\{([^:-]+)(?::-([^}]*))?\}|\$([a-zA-Z_][a-zA-Z0-9_]*)")

    def replacer(match):
        var_name = match.group(1) or match.group(3)
        default = match.group(2)
        return os.getenv(var_name, default if default is not None else "")

    return pattern.sub(replacer, value)


def parse_cli_overrides(settings: list[str] | None) -> dict[str, Any]:
    """
    Parse CLI '--set' options into scoped overrides.

    Format:
        - Global: key=value
        - Scoped: dataset:key=value

    Returns:
        {"_global": {...}, "dataset_name": {...}}
    """
    result: dict[str, Any] = {"_global": {}}
    if not settings:
        return result

    for item in settings:
        if "=" not in item:
            continue

        key, raw_value = item.split("=", 1)
        key = key.strip()
        value = raw_value.strip()

        # Type inference
        if value.lower() in {"true", "false", "none"}:
            value = {"true": True, "false": False, "none": None}[value.lower()]
        elif value.isdigit():
            value = int(value)

        if ":" in key:
            scope, actual_key = key.split(":", 1)
            scope = scope.strip()
            if scope not in result:
                result[scope] = {}
            result[scope][actual_key.strip()] = value
        else:
            result["_global"][key] = value

    return result


class TaskContextBuilder:
    """Builds task contexts from hierarchical configuration sources."""

    def __init__(self, config_path: str | None = None, env: str = APP_CURRENT_ENV):
        """Initializes the builder with application and service settings.

        Args:
            config_path: Path to the primary app.yaml.
            env: The current execution environment (e.g., 'prod', 'dev').
        """
        self.config_path = config_path or APP_CONFIG_PATH
        self.env = env

        self._service_configs = None
        self._defaults_settings = None

        LOG.info(f"Loading config from: {self.config_path}")

        self.app_settings = self.build_app_context(self.config_path)

        self._log_config_summary()

    @property
    def service_configs(self) -> LazySettings:
        if self._service_configs is None:
            if not SERVICES_CONFIG_PATH.exists():
                raise FileNotFoundError(
                    f"Services config not found: {SERVICES_CONFIG_PATH}"
                )

            self._service_configs = Dynaconf(
                envvar_prefix="SVC",
                argv_prefix="--SVC",
                settings_files=[str(SERVICES_CONFIG_PATH)],
                environments=True,
                env=self.env,
                load_dotenv=True,
            )
        return self._service_configs

    @property
    def defaults_settings(self) -> LazySettings:
        if self._defaults_settings is None:
            if not DEFAULT_CONFIG_PATH.exists():
                raise FileNotFoundError(
                    f"Defaults config not found: {DEFAULT_CONFIG_PATH}"
                )

            self._defaults_settings = Dynaconf(
                settings_files=[str(DEFAULT_CONFIG_PATH)],
                environments=True,
                env=self.env,
                load_dotenv=True,
            )
        return self._defaults_settings

    def _log_config_summary(self) -> None:
        """Log loaded configuration keys for debugging."""
        try:
            keys = [k for k in self.app_settings if "DYNACONF" not in k]
            LOG.debug(f"Loaded config keys: {', '.join(keys)}")
        except Exception:
            LOG.debug("Could not log config summary")

    def build_execution_context(
        self, mode: ExecutionMode = ExecutionMode.NORMAL
    ) -> ExecutionContext:
        """Builds the global execution context from application settings.

        Args:
            mode: The execution mode (NORMAL, TEST, etc.).

        Returns:
            ExecutionContext: The initialized execution context.
        """
        # 1. Gather all raw settings bound for the execution context into a raw dict
        raw_exec_data = {
            "workspace_dir": self.app_settings.get("workspace_dir"),
            "code_pex_path": self.app_settings.get("code_pex_path"),
            "deps_pex_path": self.app_settings.get("deps_pex_path"),
            "cache_config": (
                self.app_settings.get("cache", {}).to_dict()
                if hasattr(self.app_settings.get("cache", {}), "to_dict")
                else self.app_settings.get("cache", {})
            ),
            "task_queue_config": (
                self.app_settings.get("task_queue", {}).to_dict()
                if hasattr(self.app_settings.get("task_queue", {}), "to_dict")
                else self.app_settings.get("task_queue", {})
            ),
            "provider_config": (
                self.app_settings.get("secret_provider", {}).to_dict()
                if hasattr(self.app_settings.get("secret_provider", {}), "to_dict")
                else self.app_settings.get("secret_provider", {})
            ),
        }

        # 2. Provide base temporal/context variables to handle any {YYYY} or paths
        now = datetime.now()
        context_variables = {
            "workspace_dir": str(self.app_settings.get("workspace_dir", "")),
            "YYYY": now.strftime("%Y"),
            "MM": now.strftime("%m"),
            "DD": now.strftime("%d"),
        }

        # 3. Safe mutation step on the pure primitives first
        resolved_exec_data = self.resolve_strings(raw_exec_data, context_variables)

        # 4. Handle directory generation using fully evaluated string values
        workspace = Path(resolved_exec_data["workspace_dir"]).expanduser().resolve()
        workspace.mkdir(parents=True, exist_ok=True)

        code_pex = resolved_exec_data["code_pex_path"]
        deps_pex = resolved_exec_data["deps_pex_path"]

        # 5. Instantiate your strongly typed dataclass/msgspec container safely
        return ExecutionContext(
            workspace_dir=workspace,
            timezone=self.app_settings.get("timezone"),
            execution_mode=mode,
            ray_mode=RayMode(self.app_settings.get("ray_mode", "cluster").lower()),
            env=self.env,
            code_pex_path=Path(code_pex) if code_pex else None,
            deps_pex_path=Path(deps_pex) if deps_pex else None,
            cache_config=resolved_exec_data["cache_config"],
            task_queue_config=resolved_exec_data["task_queue_config"],
            provider_config=resolved_exec_data["provider_config"],
            disable_self_healing=self.app_settings.get("disable_self_healing", False),
            drain_timeout_secs=self.app_settings.get("drain_timeout_secs", 600),
        )

    def build_app_context(self, app_config_file: str | Path = APP_CONFIG_PATH):
        app_config_file = str(app_config_file)
        if not Path(app_config_file).exists():
            raise FileNotFoundError(f"App config not found: {app_config_file}")
        settings = Dynaconf(
            envvar_prefix="APP",
            argv_prefix="--APP",
            settings_files=[app_config_file],
            environments=True,
            env=self.env,
            load_dotenv=True,
        )

        # 1. Isolate the early infrastructure blocks that require path resolution
        infra_keys = ["secret_provider", "task_queue", "cache", "workspace_dir"]

        # We only need static variables like workspace_dir or basic time units here
        now = datetime.now()
        base_variables = {
            "workspace_dir": str(settings.get("workspace_dir", "")),
            "YYYY": now.strftime("%Y"),
            "MM": now.strftime("%m"),
            "DD": now.strftime("%d"),
        }

        for key in infra_keys:
            infra_data = settings.get(key)
            # Ensure it is a non-empty dictionary/box block before attempting to convert
            if infra_data and hasattr(infra_data, "items"):
                # Convert the DynaBox to a clean primitive dict for resolve_strings
                raw_dict = (
                    infra_data.to_dict()
                    if hasattr(infra_data, "to_dict")
                    else dict(infra_data)
                )

                # Resolve paths and environment variables early
                resolved_dict = self.resolve_strings(raw_dict, base_variables)

                # Write the clean, resolved dictionary back into Dynaconf
                settings.set(key, resolved_dict)

        # 2. Proceed with service reference mapping
        # print(f"LOADED SETTINGS: {settings.to_dict()}")
        return self._resolve_service_refs(settings)

    def _resolve_partition_date(
        self,
        spec: dict[str, Any],
        override: str | None = None,
    ) -> str | None:
        """Resolves the partition date based on configuration or manual override.

        Args:
            spec: The partition_date_spec dictionary.
            override: A manually provided date string.

        Returns:
            str | None: The formatted date string, or None if no spec provided.
        """
        if override:
            return pendulum.from_format(override, "YYYY-MM-DD").strftime("%Y-%m-%d")

        if not spec:
            return None

        tz = self.app_settings.get("timezone")
        base = current_timestamp(timezone=tz, naive=True)
        offset = spec.get("offset", {})

        date = pendulum.instance(base).add(
            years=offset.get("years", 0),
            months=offset.get("months", 0),
            days=offset.get("days", spec.get("offset_days", 0)),
        )

        fmt = spec.get("format", "%Y-%m-%d")
        result = date.strftime(fmt)

        if spec.get("wrap_quotes", False):
            return f"'{result}'"
        return result

    def _get_nested(
        self, settings: Dynaconf, dataset_id: str, path: str, default: Any = None
    ) -> Any:
        """Get configuration value with hierarchical fallback."""
        # Priority: dataset.<id>.<path> > job.<path> > app.<path>
        search_paths = [f"datasets.{dataset_id}.{path}", f"job.{path}"]

        for sp in search_paths:
            val = settings.get(sp)
            if val is not None:
                return interpolate_env_vars(val)

        val = self.defaults_settings.get(path)
        return interpolate_env_vars(val) if val is not None else default

    # def _resolve_service_ref(self, service_ref: str) -> dict:
    #     """Resolves service configuration by reference or inline definition.

    #     We attempt to find a 'service_ref' string. If present, we look up the
    #     full definition in services.yaml

    #     Args:
    #         service_ref: The service reference.

    #     Returns:
    #         dict: The resolved service parameters.
    #     """

    #     # Look for definition in job local services or global services.yaml
    #     svc = self.service_configs.get(service_ref.upper())
    #     if not svc:
    #         LOG.warning(f"Service not found: {service_ref}")
    #     return svc if isinstance(svc, dict) else svc.to_dict()

    def _resolve_service_refs(self, settings: Dynaconf):
        """Resolves service references in a service spec.

        Args:
            settings: The Dynaconf settings object.

        Returns:
            dict: The resolved service parameters.
        """
        # print(f"{settings.to_dict()=}")
        for key in list(settings.keys()):
            value = settings.get(key)
            if not isinstance(value, DynaBox):
                continue

            # Get the current state of this service as a dict
            current_dict = value.to_dict()

            # Find all service refs in this service
            items = list(find_keys_by_pattern(current_dict, SERVICE_REF_OLD_KEY))
            if not items:
                continue

            # Process each ref sequentially, updating current_dict each time
            for item in items:
                path, service_ref = item

                svc = self.service_configs.get(service_ref.upper())
                if not svc:
                    LOG.warning(f"Service not found: {service_ref} at {path}")
                    resolved_ref = None  # or some default
                else:
                    resolved_ref = svc.to_dict() if isinstance(svc, DynaBox) else svc

                # print(f"RESOLVED SERVICE REF: {service_ref} -> {resolved_ref} at {path}")
                # Update the current_dict with this resolution
                current_dict = set_nested_key(
                    data=current_dict,  # Pass the updated dict from previous iterations
                    path=path,
                    new_key=SERVICE_REF_NEW_KEY,
                    new_value=resolved_ref,
                )

                LOG.trace(f"Resolved service_ref: {service_ref} at {path}")

            # After all refs are resolved, set the final dict once
            settings.set(key, current_dict)
            # print(settings.to_dict())
        # print(f"RESOLVED: {settings.to_dict()=}")
        return settings

    def parse_stagehooks(
        self, hooks: dict[str, Any], context_variables: dict[str, str]
    ) -> dict[str, StageHooks]:
        """Parses raw stage hook configurations into typed StageHooks objects."""
        parsed = {}
        for stage, hook_config in hooks.items():
            if not hook_config:
                continue

            # Process both pre and post actions dynamically using a unified tracking map
            actions_map = {"pre": [], "post": []}
            for phase, _ in actions_map.items():
                for action in hook_config.get(phase) or []:
                    actions_map[phase].append(
                        self._parse_single_hook_action(action, context_variables)
                    )

            parsed[stage] = StageHooks(pre=actions_map["pre"], post=actions_map["post"])
        return parsed

    def _parse_single_hook_action(
        self, action: dict[str, Any], context_variables: dict[str, str]
    ) -> HookAction:
        """Helper to transform and serialize a raw hook action configuration dict."""
        resolved = self.resolve_strings(dict(action), context_variables)
        mapped = dict(resolved)

        # Flatten 'to' and 'from' prefix blocks
        for prefix in ("to", "from"):
            if prefix in mapped:
                for k, v in mapped.pop(prefix).items():
                    mapped[f"{prefix}_{k}"] = v

        # Standardize conditional syntax
        if "if" in mapped:
            mapped["condition"] = mapped.pop("if")

        # Clean empty parameters
        for k in list(mapped.keys()):
            if mapped[k] is None:
                mapped.pop(k)

        return msgspec.convert(mapped, type=HookAction)

    @staticmethod
    def resolve_strings(
        ctx_data: dict[str, Any],
        context_variables: dict[str, Any],
        exclude_prefixes: tuple[str, ...] = PATH_PREFIX_BLACKLIST,
    ) -> dict[str, Any]:
        """Flattens config, resolves templates/paths using dict utils, and reconstructs the dict."""

        # 1. Flatten the dictionary to get clear dot-notation tracks
        # e.g., {"extract": {"object": "./file.csv"}} -> {"extract.object": "./file.csv"}[cite: 8]
        flat_configs = flatten_dict(ctx_data)

        # Work on a copy of the base dictionary structure
        resolved_data = ctx_data.copy()

        env_pattern = re.compile(
            r"\$?\$\{([^:-]+)(?::-([^}]*))?\}|\$([a-zA-Z_][a-zA-Z0-9_]*)"
        )

        # Matches valid variable names inside braces, e.g., {job_id}, {YYYY}
        # This intentionally ignores empty JSON structures {} or numeric indices like {0}
        named_template_pattern = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")

        for path, value in flat_configs.items():
            if not value or not isinstance(value, str):
                continue

            mutated_val = value

            # STEP 1: Resolve Named String Templates FIRST (Safe from Path intervention)
            if named_template_pattern.search(mutated_val):
                try:
                    clean_template = mutated_val.replace("@format ", "")
                    # This will now successfully insert job_id, partition_date, and run_id
                    mutated_val = clean_template.format(**context_variables)
                except (KeyError, IndexError, ValueError):
                    pass

            # STEP 2: Resolve Environment Variables (${VAR:-DEFAULT})
            if "$" in mutated_val:

                def _replacer(match):
                    var_name = match.group(1) or match.group(3)
                    default = match.group(2)
                    return os.getenv(var_name, default if default is not None else "")

                mutated_val = env_pattern.sub(_replacer, mutated_val)

            # STEP 3: Resolve Path-Like Strings LAST (Once fully populated)
            if is_path_like(mutated_val, exclude_prefixes=exclude_prefixes):
                mutated_val = str(Path(mutated_val).expanduser().resolve().absolute())

            # STEP 4: Save changes back if structural value mutated
            if mutated_val != value:
                resolved_data = set_nested_key(
                    resolved_data, path=path, new_value=mutated_val
                )

        return resolved_data

    def build(
        self,
        job_id: str,
        dataset_ids: set[str] | str | None = None,
        partition_date: str | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> list[TaskContext]:
        """Builds all task contexts for a given job and its datasets.

        Args:
            job_id: The identifier for the job.
            dataset_id: Optional filter for a specific dataset.
            partition_date: Optional override for the partition date.
            overrides: Optional key-value overrides from the CLI.

        Returns:
            list[TaskContext]: A list of populated context objects.
        """
        dataset_ids = dataset_ids or set()
        if isinstance(dataset_ids, str):
            dataset_ids = {dataset_ids}

        LOG.debug(f"Building contexts for job={job_id}, dataset={dataset_ids}")

        config_file = JOB_CONFIG_DIR / job_id / "config.yaml"

        settings = Dynaconf(
            envvar_prefix="JOB",
            settings_files=[config_file],
            environments=True,
            env=self.env,
            load_dotenv=True,
        )
        settings = self._resolve_service_refs(settings)

        # Get datasets
        all_datasets = settings.get("DATASETS", {})
        target_ids = set(all_datasets.keys()).intersection(dataset_ids)

        contexts = []
        for ds_id in target_ids:
            if ds_id not in all_datasets:
                LOG.warning(f"Dataset '{ds_id}' not found in job '{job_id}'")
                continue

            ctx = self._build_dataset_context(
                job_id=job_id,
                dataset_id=ds_id,
                partition_date=partition_date or "",
                settings=settings,
                overrides=overrides,
            )
            contexts.append(ctx)

        LOG.debug(f"Built {len(contexts)} contexts")
        return contexts

    def _build_dataset_context(
        self,
        job_id: str,
        dataset_id: str,
        partition_date: str,
        settings: Dynaconf,
        overrides: dict[str, Any] | None = None,
    ) -> TaskContext:
        """Builds a single task context for a specific dataset.

        Args:
            job_id: The job identifier.
            dataset_id: The dataset identifier.
            partition_date: The resolved partition date.
            settings: The job-specific configuration.
            overrides: Optional CLI overrides.

        Returns:
            TaskContext: The fully rehydrated context.
        """

        def get(p: str, default: Any = None) -> Any:
            return self._get_nested(settings, dataset_id, p, default)

        # Load schema
        # schema = get("schema", [])
        # if schema_file := get("extract.schema_file"):
        #     schema = self._load_schema_file(job_id, schema_file)

        # Resolve partition date
        date_spec = get("partition_date", {})
        final_date = self._resolve_partition_date(date_spec, partition_date)
        if not final_date:
            tz = self.app_settings.get("timezone", "Asia/Singapore")
            final_date = current_timestamp(timezone=tz, naive=True).strftime("%Y-%m-%d")

        # Resolve services and stage hooks dynamically using Stage enum
        connections = {}
        for stage in (Stage.EXTRACT, Stage.WRITE, Stage.ARCHIVE):
            ref = self._get_nested(
                settings, dataset_id, f"{stage.value}.{SERVICE_REF_NEW_KEY}"
            )
            connections[stage.value] = ref

        hooks_raw = {}
        for stage in ALL_STAGES:
            stage_hooks = get(f"{stage.value}.hooks")
            if stage_hooks:
                hooks_raw[stage.value] = stage_hooks

        # Get cache details to determine current run contexts if needed
        now = datetime.now()
        context_variables = {
            "job_id": job_id,
            "dataset_id": dataset_id,
            "partition_date": final_date,
            "workspace_dir": str(self.app_settings.get("workspace_dir", "")),
            # "run_id": "default_run",  # Ensure this tracks or falls back safely
            "YYYY": now.strftime("%Y"),
            "MM": now.strftime("%m"),
            "DD": now.strftime("%d"),
        }

        # STEP 1: Resolve strings and path locations while hooks are still raw dict objects

        # --- STEP 1: Build raw dictionary components ---
        raw_extract = {
            "source_params": get("extract.params", {}),
            SERVICE_REF_NEW_KEY: connections["extract"],
            "num_workers": get("num_workers", 10),
            "partition_on": get("partition_on"),
            "mode": get("mode"),
            "select": get("extract.select", []),
            "columns": get("extract.columns", {}),
            "batch_size": get("extract.batch_size"),
            "null_if": get("extract.null_if"),
            "flatten": get("extract.flatten"),
            "sql": get("extract.sql"),
            "where": get("extract.where"),
            "limit": get("extract.limit"),
            "object": get("extract.object"),
            "compression": get("extract.compression"),
            "format": get("extract.format"),
            "header": get("extract.header"),
            "skip_blank_lines": get("extract.skip_blank_lines"),
            "encoding": get("extract.encoding"),
        }
        raw_transform = {
            "transform_type": get("transform.type", "default"),
            "transform_params": get("transform.options", {}),
        }
        raw_write = {
            "sink_params": get("write.params", {}),
            SERVICE_REF_NEW_KEY: connections["write"],
            "partition_on": get("write.partition_on", DEFAULT_PARTITION_COL),
            "partition_value": partition_date,
        }
        raw_archive = {
            "archive_enabled": get("archive.enable_archival", False),
            "archive_params": get("archive.archive_params", {}),
            SERVICE_REF_NEW_KEY: connections["archive"],
            "retention_days": get("archive.retention_days"),
            "type": get("archive.archive_type"),
        }

        # --- STEP 2: Safe string template & path resolution on pure dicts ---
        resolved_extract = self.resolve_strings(raw_extract, context_variables)
        resolved_transform = self.resolve_strings(raw_transform, context_variables)
        resolved_write = self.resolve_strings(raw_write, context_variables)
        resolved_archive = self.resolve_strings(raw_archive, context_variables)

        # Assemble finalized dict config without hooks yet
        ctx_data = {
            "job_id": job_id,
            "dataset_id": dataset_id,
            "partition_date": final_date,
            "primary_keys": get("primary_keys"),
            "output_path": f"storage/active/{job_id}/{dataset_id}",
            "extract": resolved_extract,
            "transform": resolved_transform,
            "write": resolved_write,
            "archive": resolved_archive,
            "flags": get("feature_flags", {}),
        }

        # Apply CLI overrides
        if overrides:
            active = ChainMap(
                overrides.get(dataset_id, {}), overrides.get("_global", {})
            )
            custom = {}
            for key, value in active.items():
                if key in ctx_data:
                    ctx_data[key] = value
                else:
                    custom[key] = value
            if custom:
                ctx_data["custom_overrides"] = custom

        # --- STEP 3: Convert hooks to typed msgspec objects LAST ---
        # This guarantees resolve_strings never steps on msgspec converted models
        ctx_data["hooks"] = self.parse_stagehooks(hooks_raw, context_variables)

        return TaskContext.from_params(ctx_data)
