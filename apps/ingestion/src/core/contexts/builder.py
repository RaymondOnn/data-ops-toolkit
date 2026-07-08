"""Configuration builder for task contexts with hierarchical resolution."""

import os
import re
from collections import ChainMap
from pathlib import Path
from typing import Any

import msgspec
import pendulum
from apps.ingestion.src.core.contexts.execution import ExecutionContext, ExecutionMode
from apps.ingestion.src.core.contexts.task import (
    ArchiveConfig,
    ColumnMapping,
    ExtractConfig,
    LoadConfig,
    TaskContext,
    TransformConfig,
)
from apps.ingestion.src.utils.constants import (
    APP_CONFIG_ROOT,
    APP_CURRENT_ENV,
    DEFAULT_PARTITION_COL,
)
from dynaconf import Dynaconf, LazySettings
from dynaconf.utils.boxing import DynaBox
from libs.utils.dates import current_timestamp
from libs.utils.dict import find_keys_by_pattern, set_nested_key
from loguru import logger

from .execution import RayMode

LOG = logger
DEFAULT_CONFIG_PATH = APP_CONFIG_ROOT / "defaults.yaml"
APP_CONFIG_PATH = APP_CONFIG_ROOT / "app.yaml"
SERVICES_CONFIG_PATH = APP_CONFIG_ROOT / "services.yaml"
JOB_CONFIG_DIR = APP_CONFIG_ROOT / "jobs"
SERVICE_REF_OLD_KEY = "service_ref"
SERVICE_REF_NEW_KEY = "service"


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
        workspace = Path(self.app_settings.get("workspace_dir")).expanduser().resolve()
        workspace.mkdir(parents=True, exist_ok=True)

        # Resolve PEX paths if configured
        code_pex = self.app_settings.get("code_pex_path")
        deps_pex = self.app_settings.get("deps_pex_path")

        return ExecutionContext(
            workspace_dir=workspace,
            timezone=self.app_settings.get("timezone"),
            execution_mode=mode,
            ray_mode=RayMode(self.app_settings.get("ray_mode", "cluster").lower()),
            env=self.env,
            code_pex_path=Path(code_pex) if code_pex else None,
            deps_pex_path=Path(deps_pex) if deps_pex else None,
            cache_config=self.app_settings.get("cache", {}).to_dict(),
            task_queue_config=self.app_settings.get("task_queue", {}).to_dict(),
            provider_config=self.app_settings.get("secret_provider", {}).to_dict(),
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

        return self._resolve_service_refs(settings)

        # for key in list(settings.keys()):
        #     value = settings.get(key)
        #     if not isinstance(value, DynaBox):
        #         continue

        #     if "service_ref" not in value:
        #         continue

        #     resolved_value = self._resolve_service_ref(value.get("service_ref"))
        #     settings.set(key, resolved_value)

        # return settings

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

        return settings

    def _load_schema_file(self, job_id: str, schema_file: str) -> list[ColumnMapping]:
        """Loads column mappings from a CSV schema file.

        Args:
            job_id: The identifier for the job folder.
            schema_file: The name of the CSV file.

        Returns:
            list[ColumnMapping]: A list of mapping objects.
        """
        import csv

        schema_path = JOB_CONFIG_DIR / job_id / schema_file
        if not schema_path.exists():
            LOG.warning(f"Schema file not found: {schema_path}")
            return []

        LOG.debug(f"Loading schema from: {schema_path}")

        mappings = []
        with schema_path.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    mapping = ColumnMapping.from_csv_row(row)
                    mappings.append(mapping)
                except Exception as e:
                    LOG.error(f"Failed to parse row {row}: {e}")
                    raise

        LOG.debug(f"Loaded {len(mappings)} schema mappings")
        if mappings:
            LOG.debug(
                f"First mapping: source_col={mappings[0].source_col}, "
                f"target_col={mappings[0].target_col}"
            )

        return mappings

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

        # Resolve partition date
        date_spec = settings.get("partition_date_spec", {})
        final_date = self._resolve_partition_date(date_spec, partition_date)
        if not final_date:
            tz = self.app_settings.get("timezone", "Asia/Singapore")
            final_date = current_timestamp(timezone=tz, naive=True).strftime("%Y-%m-%d")

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
                partition_date=final_date,
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
        schema = get("schema", [])
        if schema_file := get("extract.schema_file"):
            schema = self._load_schema_file(job_id, schema_file)

        # Resolve services
        services = {}
        for section in ("extract", "load", "archive"):
            # print(section, self._get_nested(settings, dataset_id, f"{section}.service"))
            ref = self._get_nested(
                settings, dataset_id, f"{section}.{SERVICE_REF_NEW_KEY}"
            )
            services[section] = ref

        ctx_data = {
            "job_id": job_id,
            "dataset_id": dataset_id,
            "partition_date": partition_date,
            "output_path": f"storage/active/{job_id}/{dataset_id}",
            "extract": ExtractConfig.from_params(
                source_params=get("extract.params", {}),
                service=services["extract"],
                schema=schema,
                num_workers=get("num_workers", 10),
                load_mode=get("extract.load_mode", "snapshot"),
            ),
            "transform": TransformConfig.from_params(
                transform_type=get("transform.type", "default"),
                transform_params=get("transform.options", {}),
                # source_dir=get("transform.source_dir", None),
            ),
            "load": LoadConfig.from_params(
                sink_params=get("load.params", {}),
                service=services["load"],
                partition_by=get("load.partition_by", DEFAULT_PARTITION_COL),
                partition_value=get("load.partition_value", partition_date),
            ),
            "archive": ArchiveConfig.from_params(
                archive_enabled=get("archive.enable_archival", False),
                archive_params=get("archive.archive_params", {}),
                service=services["archive"],
                retention_days=get("archive.retention_days"),
                type=get("archive.archive_type"),
            ),
            "flags": get("feature_flags", {}),
        }

        # Apply overrides
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

        return msgspec.convert(ctx_data, type=TaskContext)
