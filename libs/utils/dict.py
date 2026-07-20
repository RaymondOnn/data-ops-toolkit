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
    target = deepcopy(data)
    current = target

    def get_node(obj: Any, segment: str) -> tuple[Any, Any]:
        """Returns the parent container and the active key/index."""
        if "]" in segment:
            key, idx = segment.replace("]", "").split("[")
            return obj[key], int(idx)
        return obj, segment

    # Navigate to the final parent segment
    parts = path.split(".")
    for part in parts[:-1]:
        container, k = get_node(current, part)
        current = container[k]

    # Modify the terminal key or index
    container, k = get_node(current, parts[-1])

    if isinstance(k, int):  # It's a list index: update the value in place
        container[k] = new_value if new_value is not None else container[k]
    elif k in container:  # It's a dict key: swap the key/value
        val = new_value if new_value is not None else container[k]
        del container[k]
        container[new_key or k] = val

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


def flatten_dict(data: dict[str, Any], parent_key: str = "") -> dict[str, str]:
    """Recursively flattens a nested dictionary into dot-notation string mappings."""
    items: list[tuple[str, str]] = []
    for k, v in data.items():
        new_key = f"{parent_key}.{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key).items())
        else:
            items.append((new_key, str(v) if v is not None else ""))
    return dict(items)
