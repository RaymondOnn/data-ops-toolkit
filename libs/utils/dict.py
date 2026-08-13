"""Dictionary manipulation utilities for nested structures."""

import re
from collections.abc import Iterator
from copy import deepcopy
from typing import Any

# =========================================================================
# Private Helpers
# =========================================================================


def _parse_path(path: str) -> list[str | int]:
    """Parses a dot-notation key into dict keys and list indices.

    Examples:
        'a.b.c' -> ['a', 'b', 'c']
        'items[0][1].name' -> ['items', 0, 1, 'name']
    """
    parsed: list[str | int] = []
    for segment in path.split("."):
        if "[" in segment:
            parts = segment.replace("]", "").split("[")
            if parts[0]:  # Non-empty dict key before '['
                parsed.append(parts[0])
            parsed.extend(int(p) for p in parts[1:])
        else:
            parsed.append(segment)
    return parsed


def _build_path(parent_path: str, key_or_idx: str | int) -> str:
    """Constructs a dot-notation or bracketed path segment."""
    if isinstance(key_or_idx, int):
        return f"{parent_path}[{key_or_idx}]"
    return f"{parent_path}.{key_or_idx}" if parent_path else str(key_or_idx)


def _navigate_path(data: Any, parsed_path: list[str | int]) -> Any:
    """Traverses an existing nested container down to a specific path node."""
    current = data
    for key in parsed_path:
        current = current[key]
    return current


def _get_or_create_child(current: Any, key: str | int, next_is_list: bool) -> Any:
    """Retrieves or initializes the next nested container (dict or list)."""
    if isinstance(current, list):
        assert isinstance(key, int)
        while len(current) <= key:
            current.append(None)
        if current[key] is None:
            current[key] = [] if next_is_list else {}
        return current[key]

    assert isinstance(key, str)
    if key not in current:
        current[key] = [] if next_is_list else {}
    return current[key]


# =========================================================================
# Public API
# =========================================================================


def find_keys_by_pattern(
    data: Any, pattern: str, ignore_case: bool = False
) -> Iterator[tuple[str, Any]]:
    """Recursively find keys matching regex pattern in nested dict/list."""
    flags = re.IGNORECASE if ignore_case else 0
    regex = re.compile(pattern, flags=flags)

    def _recurse(node: Any, path: str) -> Iterator[tuple[str, Any]]:
        if isinstance(node, dict):
            for key, value in node.items():
                new_path = _build_path(path, str(key))
                if isinstance(key, str) and regex.search(key):
                    yield new_path, value
                yield from _recurse(value, new_path)
        elif isinstance(node, list | tuple):
            for idx, item in enumerate(node):
                new_path = _build_path(path, idx)
                yield from _recurse(item, new_path)

    return _recurse(data, "")


def set_nested_key(
    data: dict[str, Any],
    path: str,
    new_key: str | None = None,
    new_value: Any = None,
) -> dict[str, Any]:
    """Replace a key or update a value at dot-notation path."""
    target = deepcopy(data)
    parsed_path = _parse_path(path)

    # Annotate container as Any so type checkers don't enforce dict[str, Any]
    container: Any = (
        _navigate_path(target, parsed_path[:-1]) if len(parsed_path) > 1 else target
    )
    last_key = parsed_path[-1]

    # Type narrowing guarantees container is a list when last_key is an int
    if isinstance(container, list) and isinstance(last_key, int):
        if new_value is not None:
            container[last_key] = new_value
    elif (
        isinstance(container, dict)
        and isinstance(last_key, str)
        and last_key in container
    ):
        val = new_value if new_value is not None else container[last_key]
        del container[last_key]
        container[new_key or last_key] = val

    return target


def deep_merge(base: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
    """Deep merge two dictionaries (non-destructive)."""
    result = base.copy()
    for key, value in updates.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def flatten_dict(
    data: Any,
    parent_key: str = "",
    flatten_lists: bool = True,
) -> dict[str, Any]:
    """Recursively flattens nested dicts/lists into dot-notation key mappings."""
    result: dict[str, Any] = {}

    def _flatten(node: Any, path: str) -> None:
        if isinstance(node, dict):
            if not node and path:
                result[path] = {}
                return
            for k, v in node.items():
                _flatten(v, _build_path(path, str(k)))

        elif isinstance(node, list | tuple) and flatten_lists:
            if not node and path:
                result[path] = []
                return
            for idx, item in enumerate(node):
                _flatten(item, _build_path(path, idx))

        else:
            result[path] = node

    _flatten(data, parent_key)
    return result


def unflatten_dict(flat_dict: dict[str, Any]) -> dict[str, Any]:
    """Unflattens a dot-notation flattened dict back into nested dicts and lists."""
    result: dict[str, Any] = {}

    for flat_key, value in flat_dict.items():
        path = _parse_path(flat_key)
        current: Any = result

        for i, key in enumerate(path[:-1]):
            next_is_list = isinstance(path[i + 1], int)
            current = _get_or_create_child(current, key, next_is_list)

        last_key = path[-1]
        if isinstance(current, list):
            assert isinstance(last_key, int)
            while len(current) <= last_key:
                current.append(None)
            current[last_key] = value
        else:
            assert isinstance(last_key, str)
            current[last_key] = value

    return result
