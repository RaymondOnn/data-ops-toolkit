"""Dictionary manipulation utilities for nested structures."""

import re
from collections.abc import Iterator
from typing import Any


def find_keys_by_pattern(
    data: Any, pattern: str, ignore_case: bool = False
) -> Iterator[tuple[str, Any]]:
    """
    Recursively find keys matching regex pattern in nested dict/list.

    Yields:
        (dot_notation_path, value) for each matching key.
    """
    flags = re.IGNORECASE if ignore_case else 0
    regex = re.compile(pattern, flags=flags)

    def _recurse(node: Any, path: str) -> Iterator[tuple[str, Any]]:
        if isinstance(node, dict):
            for key, value in node.items():
                key_str = str(key)
                new_path = f"{path}.{key_str}" if path else key_str
                if isinstance(key, str) and regex.search(key):
                    yield new_path, value
                yield from _recurse(value, new_path)
        elif isinstance(node, list | tuple):
            for idx, item in enumerate(node):
                yield from _recurse(item, f"{path}[{idx}]")

    return _recurse(data, "")


def set_nested_key(data: dict, path: str, new_key: str, new_value: Any = None) -> None:
    """Replace a key at dot-notation path with a new key/value."""
    parts = path.split(".")
    target = data

    # Navigate to parent
    for key in parts[:-1]:
        target = target[key]

    old_key = parts[-1]
    if old_key in target:
        value = new_value if new_value is not None else target[old_key]
        del target[old_key]
        target[new_key] = value


def deep_merge(base: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
    """Deep merge two dictionaries (non-destructive)."""
    result = base.copy()
    for key, value in updates.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result
