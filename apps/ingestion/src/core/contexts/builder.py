"""Configuration builder for task contexts with hierarchical resolution."""

import os
import re
from collections import ChainMap, defaultdict
from pathlib import Path
from typing import Any

import msgspec
import pendulum
from dynaconf import Dynaconf, LazySettings
from libs.utils.dates import current_timestamp
from libs.utils.dict import find_keys_by_pattern, flatten_dict, set_nested_key
from libs.utils.file import is_path_like
from loguru import logger

from src.core.contexts.execution import (
    ExecutionContext,
    ExecutionMode,
    RayMode,
)
from src.core.contexts.step import StepConfig
from src.core.contexts.task import TaskContext
from src.core.contexts.template import TemplateContext
from src.core.stages import parse_stage_config
from src.extras.flags import FeatureFlags
from src.extras.hooks import HookAction, StageHooks
from src.services.factory import SECRET_PROTOCOL
from src.utils.constants import (
    APP_CONFIG_ROOT,
    APP_CURRENT_ENV,
    STRIP_TZ_FOR_DB,
)

LOG = logger
DEFAULT_CONFIG_PATH = APP_CONFIG_ROOT / "defaults.yaml"
APP_CONFIG_PATH = APP_CONFIG_ROOT / "app.yaml"
SERVICES_CONFIG_PATH = APP_CONFIG_ROOT / "services.yaml"
JOB_CONFIG_DIR = APP_CONFIG_ROOT / "jobs"
SERVICE_REF_OLD_KEY = "service_ref"
SERVICE_REF_NEW_KEY = "connection"
PATH_PREFIX_BLACKLIST = (SECRET_PROTOCOL, "http://", "https://", "s3://", "arn:")


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


def generate_run_id() -> str:
    """Generate a unique run ID."""
    from src.utils.common import short_hash

    timestamp = current_timestamp(naive=STRIP_TZ_FOR_DB).strftime("%Y%m%d-%H%M%S")
    return f"{timestamp}-{short_hash(8)}"


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
    def service_configs(self) -> "LazySettings":
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
    def defaults_settings(self) -> "LazySettings":
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
        context_vars = TemplateContext(
            workspace_dir=str(self.app_settings.get("workspace_dir", ""))
        )

        # 3. Safe mutation step on the pure primitives first
        resolved_exec_data = self.resolve_strings(raw_exec_data, context_vars)

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
        context_vars = TemplateContext(workspace_dir=settings.get("workspace_dir", ""))

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
                resolved_dict = self.resolve_strings(raw_dict, context_vars)

                # Write the clean, resolved dictionary back into Dynaconf
                settings.set(key, resolved_dict)

        # 2. Proceed with service reference mapping
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

    # def _get_nested(
    #     self, settings: Dynaconf, dataset_id: str, path: str, default: Any = None
    # ) -> Any:
    #     """Get configuration value with hierarchical fallback."""
    #     # Priority: dataset.<id>.<path> > job.<path> > defaults.<path>
    #     search_paths = [f"datasets.{dataset_id}.{path}", f"job.{path}"]

    #     for sp in search_paths:
    #         val = settings.get(sp)
    #         if val is not None:
    #             return interpolate_env_vars(val)

    #     val = self.defaults_settings.get(path)
    #     return interpolate_env_vars(val) if val is not None else default

    def _get_nested(
        self,
        settings: Dynaconf,
        dataset_id: str,
        path: str,
        stage: str | None = None,
        default: Any = None,
    ) -> Any:
        """Get configuration value with hierarchical fallback."""
        clean_path = path.lower()

        # 1. Check dataset/job explicit paths first
        search_paths = [
            f"datasets.{dataset_id}.{clean_path}",
            f"job.{clean_path}",
        ]

        for sp in search_paths:
            val = settings.get(sp)
            if val is not None:
                if hasattr(val, "to_dict") and not val.to_dict():
                    continue
                return interpolate_env_vars(val)

        # 2. Check defaults.yaml (trying stage-namespaced path e.g. extract.select)
        default_search_paths = []
        if stage:
            default_search_paths.append(f"{stage.lower()}.{clean_path}")
        default_search_paths.append(clean_path)

        for dp in default_search_paths:
            default_val = self.defaults_settings.get(dp)
            if default_val is not None:
                if hasattr(default_val, "to_dict") and not default_val.to_dict():
                    continue
                return interpolate_env_vars(default_val)

        return default

    def _resolve_service_refs(self, settings: "Dynaconf"):
        """Resolves service references in a service spec.

        Args:
            settings: The Dynaconf settings object.

        Returns:
            dict: The resolved service parameters.
        """
        for key, value in settings.to_dict().items():
            if not isinstance(value, dict):
                continue

            # Find all service refs in this service
            items = list(find_keys_by_pattern(value, SERVICE_REF_OLD_KEY))
            if not items:
                continue

            # Maintain an updated copy separate from the loop target
            updated_value = value
            for item in items:
                path, service_ref = item

                svc = self.service_configs.get(service_ref.upper())
                if not svc:
                    LOG.warning(f"Service not found: {service_ref} at {path}")
                    resolved_ref = None
                else:
                    resolved_ref = svc.to_dict() if not isinstance(svc, dict) else svc

                # Pass updated_value through iterations sequentially
                updated_value = set_nested_key(
                    data=updated_value,
                    path=path,
                    new_key=SERVICE_REF_NEW_KEY,
                    new_value=resolved_ref,
                )

                LOG.trace(f"Resolved service_ref: {service_ref} at {path}")

            # Update settings with the final resolved dictionary
            settings.set(key, updated_value)
        return settings

    def parse_stagehooks(
        self, hooks: dict[str, Any], context_variables: dict[str, str]
    ) -> dict[str, "StageHooks"]:
        """Parses raw stage hook configurations into typed StageHooks objects."""
        parsed = {}
        for stage, hook_config in hooks.items():
            if not hook_config:
                continue

            # Process both pre and post actions dynamically using a unified tracking map
            actions_map: dict[str, list[HookAction]] = {"pre": [], "post": []}
            for phase, _ in actions_map.items():
                for action in hook_config.get(phase) or []:
                    actions_map[phase].append(
                        self._parse_single_hook_action(action, context_variables)
                    )

            parsed[stage] = StageHooks(pre=actions_map["pre"], post=actions_map["post"])
        return parsed

    def _parse_single_hook_action(
        self, action: dict[str, Any], context_variables: dict[str, str]
    ) -> "HookAction":
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
        # e.g., {"extract": {"object": "./file.csv"}} -> {"extract.object": "./file.csv"}
        flat_configs = flatten_dict(ctx_data)

        # Work on a copy of the base dictionary structure
        resolved_data = ctx_data.copy()

        env_pattern = re.compile(
            r"\$?\$\{([^:-]+)(?::-([^}]*))?\}|\$([a-zA-Z_][a-zA-Z0-9_]*)"
        )

        # Matches valid variable names inside braces, e.g., {job_id}, {YYYY}
        # This intentionally ignores empty JSON structures {} or numeric indices like {0}
        named_template_pattern = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")

        # Also matches dot-notation step refs like { extract.output_data } (with optional spaces)
        step_ref_pattern = re.compile(
            r"\{\s*([a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*)\s*\}"
        )

        for path, value in flat_configs.items():
            if not value or not isinstance(value, str):
                continue

            mutated_val = value

            # STEP 0: Normalise template expressions — strip internal whitespace
            # Converts "{ extract.output_data }" -> "{extract.output_data}"
            mutated_val = step_ref_pattern.sub(
                lambda m: "{" + m.group(1) + "}", mutated_val
            )

            # STEP 1: Resolve Named String Templates FIRST (Safe from Path intervention)
            if named_template_pattern.search(mutated_val) or step_ref_pattern.search(
                mutated_val
            ):
                try:
                    clean_template = mutated_val.replace("@format ", "")
                    # This will now successfully insert job_id, partition_date, and run_id
                    mutated_val = clean_template.format_map(context_variables)
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
        run_id: str | None = None,
    ) -> list["TaskContext"]:
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
                run_id=run_id,
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
        run_id: str | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> "TaskContext":
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

        def get(p: str, stage: str | None = None, default: Any = None) -> Any:
            return self._get_nested(settings, dataset_id, p, stage, default)

        # Handle adhoc edge case: generate a run_id if none was provided in advance
        resolved_run_id = run_id or generate_run_id()

        # Resolve partition date
        date_spec = get("partition_date", default={})
        resolved_partition_date = self._resolve_partition_date(
            date_spec, partition_date
        )
        if not resolved_partition_date:
            tz = self.app_settings.get("timezone", "Asia/Singapore")
            resolved_partition_date = current_timestamp(
                timezone=tz, naive=True
            ).strftime("%Y-%m-%d")

        # Base template variables available to ALL steps
        context_vars = TemplateContext(
            job_id=job_id,
            dataset_id=dataset_id,
            run_id=resolved_run_id,
            partition_date=resolved_partition_date,
            workspace_dir=self.app_settings.get("workspace_dir", ""),
        )

        raw_steps = get("steps", default=[])
        built_steps: list[StepConfig] = []
        hooks_raw: dict[str, Any] = {}

        stage_counts = defaultdict(int)

        data_root = (
            Path(self.app_settings.get("workspace_dir", "")).expanduser().resolve()
        )

        for raw_step in raw_steps or []:
            # raw_step is a DynaBox; convert to plain dict
            step_dict = (
                raw_step.to_dict() if hasattr(raw_step, "to_dict") else dict(raw_step)
            )

            # Resolve service_refs within this step before string templating
            step_id = raw_step.get("id")
            stage = step_dict.get("stage", "").casefold()
            stage_counts[stage] = +1
            if not step_id:
                step_id = f"{stage}_{stage_counts[stage]}"

            # String template resolution (with cross-step vars already in context)
            resolved_step = self.resolve_strings(step_dict, context_vars)

            # Parse step dict directly into typed stage config
            parsed_config = parse_stage_config(
                stage=stage,
                step=resolved_step,
                get_val=lambda key, default=None, s=stage: get(
                    key, stage=s, default=default
                ),
            )

            built_steps.append(
                StepConfig(
                    id=step_id,
                    stage=stage,
                    config=parsed_config,
                )
            )

            # Resolve hooks from all steps
            if step_hooks := step_dict.get("hooks"):
                hooks_raw[step_id] = step_hooks

            # After building this step, inject its output_data path into context_variables
            # so subsequent steps can reference it as {step_id.output_data}
            step_data_path = (
                data_root
                / "data"
                / job_id
                / dataset_id
                / resolved_partition_date
                / step_id
            )
            context_vars[f"{step_id}.output_data"] = str(step_data_path)
            context_vars[f"{step_id}.step_id"] = step_id
            context_vars[f"{step_id}.stage"] = stage

        # Assemble final context dict
        ctx_data: dict[str, Any] = {
            "mode": get("mode"),
            "output_path": f"storage/active/{job_id}/{dataset_id}",
            "flags": get("feature_flags", default={}),
        }

        # Apply CLI overrides
        if overrides:
            active = ChainMap(
                overrides.get(dataset_id, {}), overrides.get("_global", {})
            )
            ctx_data["overrides"] = dict(active)

        return TaskContext(
            # Steps (new canonical)
            steps=built_steps,
            # Identity
            run_id=resolved_run_id,
            job_id=job_id,
            dataset_id=dataset_id,
            partition_date=resolved_partition_date,
            # Execution boundaries
            from_step=ctx_data.get("from_step", "start"),
            to_step=ctx_data.get("to_step", ""),
            # from_step=ctx_data.get("from_step", Stage.first().value),
            # to_step=ctx_data.get("to_step", Stage.last().value),
            mode=get("mode"),
            partition_on=ctx_data.get("partition_on", []),
            primary_keys=get("primary_keys", default=[]),
            # Metadata
            audit_columns=ctx_data.get(
                "audit_columns", ["_partition", "_run_id", "_source"]
            ),
            expires_at=ctx_data.get("expires_at"),
            overrides=ctx_data.get("overrides", {}),
            extras=ctx_data.get("extras", {}),
            flags=get("feature_flags", default=FeatureFlags()),
            # Parse hooks LAST (after resolve_strings, to avoid stepping on msgspec models)
            hooks=self.parse_stagehooks(hooks_raw, context_vars),
        )
