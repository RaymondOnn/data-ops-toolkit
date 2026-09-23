from typing import Any

import msgspec

from src.extras.hooks import HookAction, StageHooks

from .template import resolve_strings


def _parse_single_hook_action(
    action: dict[str, Any], context_variables: dict[str, str]
) -> "HookAction":
    """Helper to transform and serialize a raw hook action configuration dict."""
    resolved = resolve_strings(dict(action), context_variables)
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


def parse_hooks(
    hook_config: dict[str, Any] | None, context_variables: dict[str, Any]
) -> StageHooks:
    """Parses a single raw stage hook configuration into a typed StageHooks object."""
    if not hook_config:
        return StageHooks()

    # Process both pre and post actions dynamically using a unified tracking map
    actions_map: dict[str, list[HookAction]] = {"pre": [], "post": []}
    for phase in actions_map:
        for action in hook_config.get(phase) or []:
            actions_map[phase].append(
                _parse_single_hook_action(action, context_variables)
            )

    return StageHooks(pre=actions_map["pre"], post=actions_map["post"])
