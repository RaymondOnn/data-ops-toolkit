import os
import re
from typing import Any

from dynaconf import Dynaconf, LazySettings


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


def get_nested(
    settings: Dynaconf,
    defaults_settings: LazySettings,
    dataset_id: str,
    path: str,
    stage: str | None = None,
    default: Any = None,
) -> Any:
    """Get configuration value with hierarchical fallback:
    dataset -> job -> defaults.yaml (stage/call-aware)
    """
    clean_path = path.lower() if path else ""

    # 1. Look in dataset or job configurations first
    search_paths = []
    if clean_path:
        search_paths.extend(
            [
                f"datasets.{dataset_id}.{clean_path}",
                f"job.{clean_path}",
            ]
        )
    else:
        search_paths.extend(
            [
                f"datasets.{dataset_id}",
                "job",
            ]
        )

    for sp in search_paths:
        val = settings.get(sp)
        if val is not None:
            if hasattr(val, "to_dict") and not val.to_dict():
                continue
            return interpolate_env_vars(val)

    # 2. Look in defaults.yaml (checking stage/call namespaces if supplied)
    default_search_paths = []
    if stage:
        default_search_paths.append(
            f"calls.{stage}.{clean_path}" if clean_path else f"calls.{stage}"
        )
        default_search_paths.append(f"{stage}.{clean_path}" if clean_path else stage)

    if clean_path:
        default_search_paths.append(clean_path)

    for dp in default_search_paths:
        default_val = defaults_settings.get(dp)
        if default_val is not None:
            if hasattr(default_val, "to_dict") and not default_val.to_dict():
                continue
            return interpolate_env_vars(default_val)

    return default


def resolve_partition_date(
    spec: dict[str, Any],
    override: str | None = None,
    timezone: str | None = None,
) -> str:
    """Resolves the partition date based on configuration or manual override.

    Args:
        spec: The partition_date_spec dictionary.
        override: A manually provided date string.

    Returns:
        str | None: The formatted date string, or None if no spec provided.
    """
    import pendulum
    from libs.utils.dates import current_timestamp

    if override:
        return pendulum.from_format(override, "YYYY-MM-DD").strftime("%Y-%m-%d")

    if not spec:
        return current_timestamp(
            timezone=timezone or "Asia/Singapore", naive=True
        ).strftime("%Y-%m-%d")

    base = current_timestamp(timezone=timezone, naive=True)
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
