"""Configuration builder for task contexts with hierarchical resolution."""

from collections import ChainMap, defaultdict
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from dynaconf import DataDict, Dynaconf, LazySettings
from libs.utils.dates import current_timestamp
from libs.utils.dict import deep_merge, find_keys_by_pattern, set_nested_key
from loguru import logger

from src.core.contexts.execution import (
    ExecutionContext,
    ExecutionMode,
    RayMode,
)
from src.core.contexts.step import StepContext
from src.core.contexts.task import TaskContext
from src.core.stages import parse_stage_config
from src.extras.flags import FeatureFlags
from src.utils.constants import (
    APP_CONFIG_ROOT,
    APP_CURRENT_ENV,
    STRIP_TZ_FOR_DB,
)

from .hooks import parse_hooks
from .template import TemplateContext, build_template_scope, resolve_strings
from .utils import resolve_partition_date

LOG = logger
DEFAULT_CONFIG_PATH = APP_CONFIG_ROOT / "defaults.yaml"
APP_CONFIG_PATH = APP_CONFIG_ROOT / "app.yaml"
SERVICES_CONFIG_PATH = APP_CONFIG_ROOT / "services.yaml"
JOB_CONFIG_DIR = APP_CONFIG_ROOT / "jobs"
SERVICE_REF_OLD_KEY = "service_ref"
SERVICE_REF_NEW_KEY = "connection"


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


def load_dynaconf_settings(
    *,
    settings_files: Sequence[str | Path],
    env: str = APP_CURRENT_ENV,
    envvar_prefix: str | None = None,
    argv_prefix: str | None = None,
    lower_keys: bool = True,
    settings_dict: dict[str, Any] | None = None,
    **kwargs: Any,
) -> Dynaconf:
    """Factory function to build standardized Dynaconf instances."""
    missing_files: list[FileNotFoundError] = []
    valid_files: list[str] = []

    for file_path in settings_files:
        path = Path(file_path)
        if not path.exists():
            missing_files.append(
                FileNotFoundError(
                    f"Dynaconf configuration file not found: '{path.resolve()}'"
                )
            )
        else:
            valid_files.append(str(path))

    if missing_files:
        raise ExceptionGroup(
            f"Failed to load settings: {len(missing_files)} file(s) do not exist",
            missing_files,
        )

    dynaconf_kwargs: dict[str, Any] = {
        "settings_files": valid_files,
        "environments": True,
        "env": env,
        "load_dotenv": True,
        "lower_keys": lower_keys,
        **kwargs,
    }

    if settings_dict:
        dynaconf_kwargs.update(settings_dict)
    if envvar_prefix:
        dynaconf_kwargs["envvar_prefix"] = envvar_prefix
    if argv_prefix:
        dynaconf_kwargs["argv_prefix"] = argv_prefix

    return Dynaconf(**dynaconf_kwargs)


def generate_run_id() -> str:
    """Generate a unique run ID."""
    from src.utils.common import short_hash

    timestamp = current_timestamp(naive=STRIP_TZ_FOR_DB).strftime("%Y%m%d-%H%M%S")
    return f"{timestamp}-{short_hash(8)}"


def resolve_config_val(
    key: str,
    settings: Dynaconf,
    context_vars: dict[str, Any],
    step_data: dict[str, Any],
) -> Any:
    """Helper to check step overrides first, then Dynaconf settings, resolving template strings."""
    val = step_data.get(key) or settings.get(key)
    if val is None:
        return None
    return resolve_strings(val, context_vars)


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

    @property
    def service_configs(self) -> "LazySettings":
        if self._service_configs is None:
            self._service_configs = load_dynaconf_settings(
                settings_files=[SERVICES_CONFIG_PATH],
                env=self.env,
                envvar_prefix="SVC",
                argv_prefix="--SVC",
            )
        return self._service_configs

    @property
    def defaults_settings(self) -> "LazySettings":
        if self._defaults_settings is None:
            self._defaults_settings = load_dynaconf_settings(
                settings_files=[DEFAULT_CONFIG_PATH],
                env=self.env,
            )
        return self._defaults_settings

    def build_execution_context(
        self, mode: ExecutionMode = ExecutionMode.NORMAL
    ) -> "ExecutionContext":
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
                self.app_settings.get("cache", {}).as_dict()
                if hasattr(self.app_settings.get("cache", {}), "as_dict")
                else self.app_settings.get("cache", {})
            ),
            "task_queue_config": (
                self.app_settings.get("task_queue", {}).as_dict()
                if hasattr(self.app_settings.get("task_queue", {}), "as_dict")
                else self.app_settings.get("task_queue", {})
            ),
            "provider_config": (
                self.app_settings.get("secret_provider", {}).as_dict()
                if hasattr(self.app_settings.get("secret_provider", {}), "as_dict")
                else self.app_settings.get("secret_provider", {})
            ),
            "metadata_db_config": (
                self.app_settings.get("meta_db.connection", {}).as_dict()
                if hasattr(self.app_settings.get("meta_db.connection", {}), "as_dict")
                else self.app_settings.get("meta_db.connection", {})
            ),
        }

        # 2. Provide base temporal/context variables to handle any {YYYY} or paths
        context_vars = TemplateContext(
            workspace_dir=str(self.app_settings.get("workspace_dir", ""))
        )

        # 3. Safe mutation step on the pure primitives first
        resolved_exec_data = resolve_strings(raw_exec_data, context_vars)

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
            metadata_db_config=resolved_exec_data["metadata_db_config"],
            provider_config=resolved_exec_data["provider_config"],
            disable_self_healing=self.app_settings.get("disable_self_healing", False),
            drain_timeout_secs=self.app_settings.get("drain_timeout_secs", 600),
        )

    def build_app_context(self, app_config_file: str | Path = APP_CONFIG_PATH):
        app_config_file = str(app_config_file)
        if not Path(app_config_file).exists():
            raise FileNotFoundError(f"App config not found: {app_config_file}")
        settings = load_dynaconf_settings(
            settings_files=[app_config_file],
            env=self.env,
            envvar_prefix="APP",
            argv_prefix="--APP",
        )

        # 1. Isolate the early infrastructure blocks that require path resolution
        infra_keys = [
            "secret_provider",
            "task_queue",
            "cache",
            "meta_db",
            "workspace_dir",
        ]
        context_vars = TemplateContext(workspace_dir=settings.get("workspace_dir", ""))

        for key in infra_keys:
            infra_data = settings.get(key)
            # Ensure it is a non-empty dictionary/box block before attempting to convert
            if infra_data and hasattr(infra_data, "items"):
                raw_dict = (
                    infra_data.as_dict()
                    if hasattr(infra_data, "as_dict")
                    else dict(infra_data)
                )

                # Resolve paths and environment variables early
                resolved_dict = resolve_strings(raw_dict, context_vars)

                # Write the clean, resolved dictionary back into Dynaconf
                settings.set(key, resolved_dict)

        # 2. Proceed with service reference mapping
        # Convert settings to dict, resolve refs, and apply back
        resolved_app_data = self._resolve_service_refs(
            settings.as_dict(), self.service_configs
        )
        return load_dynaconf_settings(
            settings_files=[],
            env=self.env,
            lower_keys=True,
            settings_dict=resolved_app_data,
        )

    @staticmethod
    def _resolve_service_refs(
        config_dict: dict[str, Any], service_configs: "LazySettings"
    ) -> dict[str, Any]:
        """Resolves service references in a configuration dictionary.

        Args:
            config_dict: Configuration dictionary to search and update.
            service_configs: LazySettings instance for service lookups.

        Returns:
            dict: Updated configuration dictionary with resolved connections.
        """
        # 1. Find all 'service_ref' occurrences across the entire nested dict/list structure
        items = list(find_keys_by_pattern(config_dict, SERVICE_REF_OLD_KEY))
        if not items:
            return config_dict

        resolved_dict = config_dict
        for path, service_ref in items:
            if not isinstance(service_ref, str):
                LOG.warning(
                    f"Invalid service_ref type at {path}: {type(service_ref)}. "
                    f"Please ensure to add quotes around the service_ref string in your config."
                )
                continue

            svc = service_configs.get(service_ref.upper())
            if not svc:
                LOG.warning(f"Service not found: {service_ref} at {path}")
                resolved_ref = None
            else:
                resolved_ref = svc.as_dict() if hasattr(svc, "as_dict") else svc

            # 2. Replace service_ref with connection dictionary at dot-path
            resolved_dict = set_nested_key(
                data=resolved_dict,
                path=path,
                new_key=SERVICE_REF_NEW_KEY,
                new_value=resolved_ref,
            )

            LOG.trace(f"Resolved service_ref: {service_ref} at {path}")

        return resolved_dict

    @classmethod
    def _resolve_merged_config(
        cls, defaults: dict[str, Any], raw_job: dict[str, Any], raw_ds: dict[str, Any]
    ) -> dict[str, Any]:
        """Deep-merges job and dataset configurations and expands stage defaults into steps."""
        # Step 1: Merge app defaults with job level defaults
        defaults = {k.casefold(): v for k, v in defaults.items()}
        job_with_defaults = deep_merge(defaults, raw_job)

        # Step 2: Merge combined job configuration with dataset overrides
        merged = deep_merge(job_with_defaults, raw_ds)

        calls = merged.pop("calls", {})
        steps = merged.get("steps", [])

        if not calls or not steps:
            return merged

        # 2. Pass 2: Inject stage defaults from 'calls' into each step's 'with' config
        resolved_steps = []
        for step in steps:
            call_name = step.get("call")

            if call_name and call_name in calls:
                stage_defaults = calls[call_name]
                if hasattr(stage_defaults, "as_dict"):
                    stage_defaults = stage_defaults.as_dict()

                step_params = step.get("with", {})

                # Deep-merge stage defaults with step parameters ('with' overrides 'calls')
                step["with"] = deep_merge(stage_defaults, step_params)

            resolved_steps.append(step)

        merged["steps"] = resolved_steps
        return merged

    def build(
        self,
        job_id: str,
        dataset_ids: set[str] | str | None = None,
        partition_date: str | None = None,
        overrides: dict[str, Any] | None = None,
        run_id: str | None = None,
    ) -> list[TaskContext]:
        """Builds all task contexts for a given job and its datasets.

        Args:
            job_id: The identifier for the job.
            dataset_id: Optional filter for a specific dataset.
            partition_date: Optional override for the partition date.
            overrides: Optional key-value overrides from the CLI.

        Returns:
            list[TaskContext]: A list of populated context objects.

        Notes:
        - For daemon runtime, we build one dataset at a time
        - For trigger runtime, we generate the run_id as we build the dataset task context.
        """

        dataset_ids = (
            {dataset_ids} if isinstance(dataset_ids, str) else (dataset_ids or set())
        )

        LOG.debug(f"Building contexts for job={job_id}, dataset={dataset_ids}")
        config_file = JOB_CONFIG_DIR / job_id / "config.yaml"

        if not config_file.exists():
            raise FileNotFoundError(f"Job configuration not found: {config_file}")

        # 1. Load job configuration for active environment (e.g. self.env="prod")
        job_settings = load_dynaconf_settings(
            settings_files=[config_file],
            env=self.env,
        )

        # 2. Extract datasets dict after env overrides are applied
        all_datasets = job_settings.get("datasets", {})
        all_dataset_keys = set(all_datasets.keys())

        # Validate requested datasets
        if dataset_ids:
            missing_ids = dataset_ids - all_dataset_keys
            for missing_id in missing_ids:
                LOG.error(
                    f"Dataset '{missing_id}' not found in job configuration '{job_id}'"
                )

            target_ids = dataset_ids.intersection(all_dataset_keys)
        else:
            target_ids = all_dataset_keys

        contexts = []
        defaults_dict = self.defaults_settings.as_dict()
        for ds_id in target_ids:
            job_defaults: DataDict = job_settings.get("job", {})
            ds_overrides: DataDict = job_settings.get(f"datasets.{ds_id}", {})
            LOG.trace(
                f"{ds_id=}, {job_defaults=}, {ds_overrides=}, {type(job_defaults)=}, {type(ds_overrides)=}"
            )

            # Convert Box objects to native dicts
            raw_job: dict[str, Any] = job_defaults.to_dict()
            raw_ds: dict[str, Any] = ds_overrides.to_dict()

            # Deep-merge job defaults with dataset overrides using libs.utils.dict
            merged_dict = self._resolve_merged_config(defaults_dict, raw_job, raw_ds)
            LOG.trace(f"Merging job and dataset configs for '{ds_id}': {merged_dict=}")

            ctx = self._build_dataset_context(
                job_id=job_id,
                dataset_id=ds_id,
                partition_date=partition_date or "",
                merged_config=merged_dict,
                raw_job=raw_job,
                overrides=overrides,
                run_id=run_id,
            )
            contexts.append(ctx)

        LOG.debug(f"Built {len(contexts)} contexts")
        return contexts

    def _build_dataset_context(
        self,
        *,
        job_id: str,
        dataset_id: str,
        partition_date: str,
        merged_config: dict[str, Any],
        raw_job: dict[str, Any],
        run_id: str | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> TaskContext:
        """Builds a task context utilizing native Dynaconf lookups."""

        resolved_run_id = run_id or generate_run_id()

        resolved_partition_date = resolve_partition_date(
            spec=merged_config.get("partition_date", {}),
            override=partition_date,
            timezone=self.app_settings.get("timezone"),
        )

        # Build template context scope with initial merged dataset & job config
        context_vars = build_template_scope(
            job_id=job_id,
            dataset_id=dataset_id,
            run_id=resolved_run_id,
            partition_date=resolved_partition_date,
            workspace_dir=self.app_settings.get("workspace_dir", ""),
            dataset_cfg=merged_config,
            job_cfg=raw_job,
        )

        # Phase 1: Pre-service-resolution string interpolation
        # Resolves placeholders like {job.source.service_ref} -> "mock_data_folder"
        interpolated_dict = resolve_strings(merged_config, context_vars)
        LOG.trace(f"Interpolated config for dataset {dataset_id}: {interpolated_dict}")

        # Phase 2: Service reference resolution
        # Replaces service_ref names with connection dictionaries from services.yaml
        service_resolved_dict = self._resolve_service_refs(
            interpolated_dict, self.service_configs
        )
        LOG.trace(
            f"Service-resolved config for dataset {dataset_id}: {service_resolved_dict}"
        )

        # Phase 3: Post-service-resolution string interpolation
        # Resolves dynamic templates that exist inside service connection blocks or remaining configs
        final_dict = resolve_strings(service_resolved_dict, context_vars)
        LOG.trace(f"Final resolved config for dataset {dataset_id}: {final_dict}")

        raw_steps: list[dict[str, Any]] = final_dict.get("steps", [])
        built_steps: list[StepContext] = []
        # hooks_raw: dict[str, Any] = {}
        stage_counts = defaultdict(int)

        for raw_step in raw_steps:
            call_name = raw_step.get("call")
            stage = raw_step.get("stage", call_name or "").casefold()

            # Interpolate step configuration with current context scope
            resolved_step = resolve_strings(raw_step, context_vars)

            stage_counts[stage] += 1
            step_id = resolved_step.get("id") or (
                f"{stage}_{dataset_id}_data"
                if stage_counts[stage] == 1
                else f"{stage}_{stage_counts[stage]}"
            )
            step_hooks = resolved_step.get("with", {}).get("hooks")

            # Bind step context into scope engine
            context_vars["step"] = {"id": step_id, "stage": stage, **resolved_step}

            # Helper for stage parsers to get setting values from step overrides or root config
            def get_val(key: str, step_data: dict[str, Any] = resolved_step) -> Any:
                val = step_data.get(key)
                if val is None:
                    val = final_dict.get(key)
                return resolve_strings(val, context_vars) if val is not None else None

            built_steps.append(
                StepContext(
                    id=step_id,
                    stage=stage,
                    config=parse_stage_config(
                        stage=stage,
                        step=resolved_step,
                        context=final_dict,
                    ),
                    hooks=parse_hooks(step_hooks, context_vars),
                )
            )

            # if step_hooks := resolved_step.get("with", {}).get("hooks"):
            #     hooks_raw[step_id] = step_hooks

        # TaskContext Overrides
        ctx_overrides = {}
        if overrides:
            active = ChainMap(
                overrides.get(dataset_id, {}), overrides.get("_global", {})
            )
            ctx_overrides = dict(active)

        return TaskContext(
            steps=built_steps,
            # hooks=parse_stagehooks(hooks_raw, context_vars),
            run_id=resolved_run_id,
            job_id=job_id,
            dataset_id=dataset_id,
            partition_date=resolved_partition_date,
            from_step=final_dict.get("from_step", "start"),
            to_step=final_dict.get("to_step", ""),
            mode=final_dict.get("mode"),
            update_key=final_dict.get("update_key"),
            primary_keys=final_dict.get("primary_keys", []),
            expires_at=final_dict.get("expires_at"),
            overrides=ctx_overrides,
            extras=final_dict.get("extras", {}),
            flags=final_dict.get("feature_flags", FeatureFlags()),
        )
