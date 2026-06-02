import re
from collections.abc import Iterator
from typing import Any


def find_keys_by_pattern(
    data: Any, pattern: str, ignore_case: bool = False
) -> Iterator[tuple[str, Any]]:
    """
    Recursively scans a nested dictionary or list for keys that match a regex pattern.

    Args:
        data: The nested dictionary or list to scan.
        pattern: A regex pattern string to match against keys.
        ignore_case: If True, performs a case-insensitive regex match.

    Yields:
        tuple[str, Any]: A tuple containing the dot-notation path and the value.
    """
    flags = re.IGNORECASE if ignore_case else 0
    regex = re.compile(pattern, flags=flags)

    def _recurse(node: Any, current_path: str) -> Iterator[tuple[str, Any]]:
        if isinstance(node, dict):
            # Create a list snapshot to prevent RuntimeError
            # if the dict is modified during iteration
            for key, value in list(node.items()):
                # Construct the dot-notation path, ensuring the component is a string
                key_str = str(key)
                new_path = f"{current_path}.{key_str}" if current_path else key_str

                # Check if the key is a string and matches the pattern
                if isinstance(key, str) and regex.search(key):
                    yield new_path, value

                # Continue recursion into nested dicts or lists
                yield from _recurse(value, new_path)

        elif isinstance(node, list | tuple):
            for index, item in enumerate(node):
                # Construct the index-based path
                new_path = f"{current_path}[{index}]"
                yield from _recurse(item, new_path)

    return _recurse(data, "")


def update_nested_key(data, path, new_key, new_value=None):
    """
    Replaces a key at a specific dot-notation path with a new key and value.

    Args:
        data: The nested dictionary to modify in-place.
        path: The dot-notation path to the key (e.g., 'a.b.c').
        new_key: The name of the new key to insert.
        new_value: The value for the new key. If None, the old value is kept.
    """
    keys = path.split(".")
    target = data

    # 1. Navigate to the nested dictionary
    for key in keys[:-1]:
        target = target[key]

    old_key = keys[-1]

    if old_key in target:
        # 2. Get the value (use new_value if provided, else keep original)
        value_to_keep = new_value if new_value is not None else target[old_key]

        # 3. Remove old and insert new
        del target[old_key]
        target[new_key] = value_to_keep
