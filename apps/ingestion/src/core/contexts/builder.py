import os
import re
from collections import ChainMap
from datetime import datetime
from pathlib import Path
from typing import Any

import msgspec
from apps.ingestion.src.core.contexts.execution import ExecutionContext, ExecutionMode
from apps.ingestion.src.core.contexts.task import TaskContext
from apps.ingestion.src.utils.constants import (
    APP_CONFIG_ROOT,
    APP_CURRENT_ENV,
    DEFAULT_PARTITION_COL,
)
from dateutil.relativedelta import relativedelta
from dynaconf import Dynaconf
from libs.utils.dates import get_current_timestamp
from loguru import logger

LOG = logger
APP_DEFAULT_CONFIG = APP_CONFIG_ROOT / "app.yaml"


def expand_env_vars(value: Any) -> Any:
    """
    Recursively resolves ${VAR:-DEFAULT}, ${VAR}, or $VAR syntax in strings.
    Leverages os.getenv for reliable cross-platform resolution.
    """
    if isinstance(value, dict):
        return {k: expand_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [expand_env_vars(v) for v in value]

    if not isinstance(value, str) or "$" not in value:
        return value

    # Regex to capture ${VAR:-DEFAULT}, ${VAR}, or $VAR
    pattern = re.compile(r"\$?\$\{([^:-]+)(?::-([^}]*))?\}|\$([a-zA-Z_][a-zA-Z0-9_]*)")

    def replacer(match):
        # group(1) & (2) are for ${VAR:-DEFAULT}, group(3) is for $VAR
        var_name = match.group(1) or match.group(3)
        default_val = match.group(2)

        # If variable is missing and no default was provided in the YAML,
        # return an empty string to allow downstream validators to catch it.
        return os.getenv(var_name, default_val if default_val is not None else "")

    return pattern.sub(replacer, value)


def parse_set_options(settings: list[str] | None) -> dict[str, Any]:
    """
    Parses CLI '--set' options into a scoped dictionary for configuration overrides.

    The parser supports two levels of specificity:
    1. Global Overrides: Applied to all datasets (e.g., '--set batch_size=5000')
    2. Scoped Overrides: Applied ONLY to a specific dataset
        (e.g., '--set sales:batch_size=1000')

    Syntax:
        - Global: [key]=[value]
        - Scoped: [dataset_name]:[key]=[value]

    Args:
        settings: A set of strings provided via the '--set' or '-s' CLI flag.

    Returns:
        A dictionary with a mandatory '_global' key and optional dataset keys.
        Example:
        {
            "_global": {"timeout": 60},
            "sales": {"batch_size": 5000}
        }
    """
    result: dict[str, Any] = {"_global": {}}
    if not settings:
        return result

    for item in settings:
        if "=" not in item:
            continue  # Or raise an error for invalid format

        key_val = item.split("=", 1)
        raw_key, value = key_val[0].strip(), key_val[1].strip()

        # Simple Type Inference
        if value.isdigit():
            value = int(value)
        elif value.lower() == "true":
            value = True
        elif value.lower() == "false":
            value = False

        # Check for scoping (dataset:key)
        if ":" in raw_key:
            dataset_scope, clean_key = raw_key.split(":", 1)
            dataset_scope = dataset_scope.strip()
            if dataset_scope not in result:
                result[dataset_scope] = {}
            result[dataset_scope][clean_key.strip()] = value
        else:
            result["_global"][raw_key] = value

    return result


class TaskContextBuilder:
    def __init__(self, app_cfg_path: str | None = None, env: str = APP_CURRENT_ENV):
        # 1. Initialize Dynaconf with the global app config and environment overrides
        self.app_cfg_path = app_cfg_path or APP_DEFAULT_CONFIG
        self.env = env
        print(f"Using App Config: {self.app_cfg_path}")
        self.app_settings = Dynaconf(
            envvar_prefix="APP",
            argv_prefix="--APP",
            settings_files=[self.app_cfg_path],
            environments=True,
            env=self.env,
            load_dotenv=True,
        )
        self._settings_cache: dict[str, Dynaconf] = {}

        # --- DEBUG INSTRUMENTATION ---
        LOG.debug(f"DEBUG: Config Path Absolute: {Path(self.app_cfg_path).resolve()}")
        LOG.debug(f"DEBUG: File Exists: {Path(self.app_cfg_path).exists()}")
        LOG.debug(
            f"DEBUG: Loaded Keys: {', '.join(filter(lambda x: 'DYNACONF' not in x, set(self.app_settings.keys())))}"
        )

        try:
            LOG.info("Resolved App Config", config=self.app_settings.to_dict())
        except Exception:
            LOG.exception("Could not serialize app config for logging")
            raise

    def _resolve_partition_date(
        self,
        spec: dict[str, Any],
        partition_date_str: str | None = None,
    ) -> str | None:
        """Resolves T-x logic into a formatted string."""
        if partition_date_str:
            date_val = datetime.strptime(partition_date_str, "%Y-%m-%d")
        elif spec:
            # Use app-level timezone or default to Asia/Singapore
            tz_name = self.app_settings.get("timezone", "Asia/Singapore")
            base_date = get_current_timestamp(timezone=tz_name, strip_tz=True)

            # Support multi-unit offsets (years, months, days)
            offset = spec.get("offset", {})

            # We merge with the legacy 'offset_days' for backward compatibility
            date_val = base_date + relativedelta(
                years=offset.get("years", 0),
                months=offset.get("months", 0),
                days=offset.get("days", spec.get("offset_days", 0)),
            )
        else:
            return None

        fmt = date_val.strftime(spec.get("format", "%Y-%m-%d"))
        formatted_date = date_val.strftime(fmt)
        return (
            f"'{formatted_date}'" if spec.get("wrap_quotes", False) else formatted_date
        )

    def get_execution_context(
        self, mode: ExecutionMode = ExecutionMode.NORMAL
    ) -> ExecutionContext:
        """Resolves the global app settings into a typed context."""
        workspace = Path(self.app_settings.get("workspace_dir")).expanduser().resolve()

        # Ensure the base workspace directory exists so lock files
        # and subdirectories can be created safely.
        workspace.mkdir(parents=True, exist_ok=True)

        cache_cfg = self.app_settings.get("cache").to_dict()

        return ExecutionContext(
            workspace_dir=workspace,
            execution_mode=mode,
            env=self.env,
            cache_config=cache_cfg,
            drain_timeout_secs=self.app_settings.get("drain_timeout_secs", 600),
        )

    def _get_val(
        self, settings: Dynaconf, dataset_id: str, path: str, default: Any = None
    ) -> Any:
        """Hierarchical lookup: Dataset (Env > Default) > Job (Env > Default) > App (Env > Default)."""
        search_paths = [f"datasets.{dataset_id}.{path}", f"job.{path}"]
        for p in search_paths:
            for env in [None, "default"]:
                s = settings if env is None else settings.from_env("default")
                val = s.get(p)
                if val is not None:
                    return expand_env_vars(val)

        # Fallback to global app settings
        val = self.app_settings.get(path) or self.app_settings.from_env("default").get(
            path
        )
        return expand_env_vars(val) if val is not None else default

    def _resolve_service(
        self, settings: Dynaconf, ref_key: str, dataset_id: str
    ) -> dict:
        """Resolves a service definition by reference or inline config."""
        ref = self._get_val(settings, dataset_id, f"{ref_key}.service_ref")
        if ref:
            # Search hierarchy for the 'services.{ref}' definition
            # Priority: Job Config (Env > Default) > Global Config (Env > Default)
            def_paths = [f"services.{ref}"]
            for p in def_paths:
                # We use None as dataset_id because service definitions are top-level
                svc_def = self._get_val(settings, "GLOBAL", p)
                if svc_def:
                    return svc_def if isinstance(svc_def, dict) else svc_def.to_dict()

            LOG.warning(
                f"Service reference '{ref}' not found in any definition blocks.",
                ref_key=ref_key,
            )

            # Fallback to inline config block
            svc_dict = self._get_val(
                settings, dataset_id, f"{ref_key}.config", default={}
            )
            if svc_dict and "type" not in svc_dict:
                LOG.warning(
                    f"Service dictionary for '{ref}' is missing the required 'type' key. "
                    "Factory initialization will likely fail.",
                    dataset_id=dataset_id,
                    config=svc_dict,
                )
            return svc_dict

        LOG.critical(
            f"No service reference found for '{ref_key}' in dataset '{dataset_id}'. "
            "Falling back to inline configuration if available."
        )
        return {}

    def _load_schema_file(self, job_id: str, schema_file: str) -> list[dict[str, Any]]:
        """
        Reads a CSV schema file from the job's config directory.
        Expected format: CSV with headers matching the required schema contract.
        """
        import csv

        schema_path = APP_CONFIG_ROOT / job_id / schema_file
        if not schema_path.exists():
            LOG.warning(
                "Schema file defined but not found on disk", path=str(schema_path)
            )
            return []

        LOG.debug("Loading schema from file", path=str(schema_path))
        with schema_path.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return list(reader)

    def build(
        self,
        job_id: str,
        dataset_id: str | None = None,
        partition_date_str: str | None = None,
        overrides_json: Path | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> list[TaskContext]:
        """Maps merged config into a set of msgspec TaskContext objects."""
        LOG.debug(
            "Building job contexts", job_id=job_id, partition_date=partition_date_str
        )

        task_cfg_path = APP_CONFIG_ROOT / job_id / "config.yaml"

        # 1. Initialize Dynaconf with job-specific overrides (cached)
        cache_key = f"{job_id}:{overrides_json}"
        if cache_key not in self._settings_cache:
            settings_files = [task_cfg_path]
            if overrides_json:
                settings_files.append(overrides_json)

            self._settings_cache[cache_key] = Dynaconf(
                envvar_prefix="APP",
                argv_prefix="--APP",
                settings_files=settings_files,
                environments=True,
                env=self.env,
                load_dotenv=True,
            )

        settings = self._settings_cache[cache_key]

        # 2. Establish partition_date
        # Priority: partition_date_str > Job Config > App Config > today
        p_date_raw = settings.get("partition_date") or self.app_settings.get(
            "partition_date"
        )
        p_spec = (
            p_date_raw
            if isinstance(p_date_raw, dict)
            else settings.get("partition_date_spec", {})
        )

        tz_name = self.app_settings.get("timezone", "Asia/Singapore")
        partition_date = (
            self._resolve_partition_date(p_spec, partition_date_str)
            or (p_date_raw if isinstance(p_date_raw, str) else None)
            or get_current_timestamp(timezone=tz_name, strip_tz=True).strftime(
                "%Y-%m-%d"
            )
        )

        # 4. Get the Task-level defaults and the Dataset-level specifics
        job_defaults = settings.get("job", {})
        all_datasets = settings.get("datasets", {})

        # Filter if a specific dataset was requested via CLI
        target_dataset_ids = [dataset_id] if dataset_id else list(all_datasets.keys())

        contexts = []
        for ds_id in target_dataset_ids:
            if ds_id not in all_datasets:
                LOG.warning(f"Dataset '{ds_id}' not found in job '{job_id}'. Skipping.")
                continue

            ds_cfg = all_datasets[ds_id]

            # 5. Build the context using the resolution: Dataset Spec > Task Default
            ctx = self._create_task_context(
                job_id=job_id,
                dataset_id=ds_id,
                partition_date=partition_date,
                # Pass the full settings object to resolve global service refs
                settings=settings,
                overrides=overrides,
            )

            contexts.append(ctx)

        LOG.debug("Resolved contexts", count=len(contexts), job_id=job_id)
        return contexts

    def _create_task_context(
        self,
        job_id: str,
        dataset_id: str,
        partition_date: str,
        settings: Dynaconf,
        overrides: dict[str, Any] | None = None,
    ) -> TaskContext:
        # Short-cut for cleaner calls
        def get_val(p, d=None):
            return self._get_val(settings, dataset_id, p, d)

        # 0. Handle Schema File Loading
        schema_file = get_val("extract.schema_file")
        schema_items = get_val("schema_items", [])
        if schema_file:
            schema_items = self._load_schema_file(job_id, schema_file)

        def resolve_full_config(key: str, fallback_path: str):
            svc = self._resolve_service(settings, key, dataset_id)
            stype = svc.pop("type", get_val(fallback_path))
            return stype, svc

        source_type, source_svc = resolve_full_config("extract", "extract.source_type")
        sink_type, sink_svc = resolve_full_config("load", "load.sink_type")

        enable_archival = get_val("archive.enable_archival")
        archive_type, archive_svc = (
            resolve_full_config("archive", "archive.archive_type")
            if enable_archival
            else (None, {})
        )

        ctx_data = {
            "job_id": job_id,
            "dataset_id": dataset_id,
            "partition_date": partition_date,
            "output_path": f"storage/active/{job_id}/{dataset_id}",
            "extract": {
                "source_type": source_type,
                "source_identifier": get_val("extract.source_identifier")
                or get_val("source_identifier"),
                "num_workers": get_val("extract.num_workers"),
                "load_mode": get_val("extract.load_mode"),
                "source_config": source_svc,
                "source_params": get_val("extract.source_params", {}),
                "schema_items": schema_items,
            },
            "transform": {
                "transform_type": get_val("transform.type", "default"),
                "transform_params": get_val("transform.options", {}),
            },
            "load": {
                "sink_type": sink_type,
                "sink_identifier": get_val("load.sink_identifier")
                or get_val("load.identifier")
                or get_val("target_destination"),
                "sink_config": sink_svc,
                "partition_col": get_val("load.partition_col", DEFAULT_PARTITION_COL),
                "partition_value": get_val("load.partition_value", partition_date),
                "load_params": get_val("load.load_params", {}),
            },
            "archive": {
                "enabled": enable_archival,
                "retention_days": (
                    get_val("archive.retention_days") if enable_archival else None
                ),
                "base_path": get_val("archive.base_path") if enable_archival else None,
                "type": archive_type,
                "config": archive_svc,
            },
            "feature_flags": get_val("feature_flags", {}),
        }

        # 3. Final Layer: Apply Runtime CLI Overrides (--set) BEFORE freezing
        if overrides:
            active_overrides = ChainMap(
                overrides.get(dataset_id, {}), overrides.get("_global", {})
            )
            for key, value in active_overrides.items():
                if key in ctx_data:
                    ctx_data[key] = value
                else:
                    ctx_data.setdefault("custom_overrides", {})[key] = value

        # Perform type-safe conversion and validation from dict to Struct
        return msgspec.convert(ctx_data, type=TaskContext)
