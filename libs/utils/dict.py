"""Dictionary manipulation utilities for nested structures."""

import re
from collections.abc import Iterator
from copy import deepcopy
from typing import Any


def find_keys_by_pattern(
    data: Any, pattern: str, ignore_case: bool = False
) -> Iterator[tuple[str, Any]]:
    """Recursively find keys matching regex pattern in nested dict/list.

    Args:
        data: The dictionary or list to search.
        pattern: The regex pattern to match.
        ignore_case: Whether to ignore case.

    Returns:
        Iterator[tuple[str, Any]]: An iterator of (dot_notation_path, value) tuples.
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


def set_nested_key(
    data: dict, path: str, new_key: str | None = None, new_value: Any = None
) -> dict[str, Any]:
    """Replace a key at dot-notation path with a new key/value.

    Args:
        data: The dictionary to modify.
        path: The dot-notation path to the key to replace.
        new_key: The new key to use.
        new_value: The new value to use.

    Returns:
        dict[str, Any]: The modified dictionary.
    """
    parts = path.split(".")
    target = deepcopy(data)
    current = target  # Keep a reference to navigate

    # Navigate to parent
    for key in parts[:-1]:
        current = current[key]  # Navigate through the copy

    old_key = parts[-1]
    if old_key in current:
        value = new_value if new_value is not None else current[old_key]
        new_key = new_key or old_key
        del current[old_key]
        current[new_key] = value

    return target  # Return the full root dictionary


def deep_merge(base: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
    """Deep merge two dictionaries (non-destructive).

    Args:
        base: The base dictionary.
        updates: The updates to apply to the base dictionary.

    Returns:
        dict[str, Any]: The merged dictionary.
    """
    result = base.copy()
    for key, value in updates.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result
