"""
========================================================================================
STANDARDIZED ARCHITECTURAL PATTERN (NO FACTORY CLASSES)
========================================================================================
To keep codebases clean and maintainable, avoid creating separate `*Factory` classes.
Instead, expose `.create()`, `.get_class()`, and `@BaseClass.register()` directly on
the domain Base Class.

Example Usage:
--------------
1. Internal Registry Definition (`libs/database/clients/registry.py`):
   ```python
   from typing import TYPE_CHECKING
   from libs.metaclasses.registry import ClassRegistry

   if TYPE_CHECKING:
       from .base import DBClient

   # Internal registry instance (do not expose directly to business logic)
   DBRegistry: ClassRegistry["DBClient"] = ClassRegistry(name="DBRegistry")"""

import importlib
import logging
import pkgutil
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar

LOG = logging.getLogger(__name__)

R = TypeVar("R")


def load_package_modules(package: Any) -> int:
    """Recursively imports all submodules under a given package or package dot-path.

    This function triggers class registration decorators (e.g., `@registry.register()`)
    across local execution and remote worker processes (such as Ray worker nodes).

    Args:
        package: The root package module object or dot-notation package string
            (e.g., `"src.core.stages"`).

    Returns:
        The total count of successfully imported modules.

    Raises:
        ValueError: If the resolved target is not a valid Python package.

    Example:
        Importing all stage modules when initializing a Ray worker process:

        >>> from libs.metaclasses.registry import load_package_modules
        >>> count = load_package_modules("src.core.stages")
        >>> print(f"Successfully discovered and loaded {count} stage modules.")
        Successfully discovered and loaded 8 stage modules.
    """
    if isinstance(package, str):
        package = importlib.import_module(package)

    if not hasattr(package, "__path__"):
        raise ValueError(f"Module '{package.__name__}' is not a package.")

    count = 0
    for _, module_name, _ in pkgutil.walk_packages(
        package.__path__,
        prefix=f"{package.__name__}.",
    ):
        try:
            importlib.import_module(module_name)
            count += 1
            LOG.debug(f"Loaded module: {module_name}")
        except Exception as e:
            LOG.warning(f"Failed to load module {module_name}: {e}")

    return count


def load_from_directory(
    directory: Path | str,
    base_package: str,
    pattern: str = "*.py",
    exclude: set[str] | None = None,
) -> int:
    """Loads and imports Python modules from a file system path using a base package path.

    Useful for loading external plugins or dynamic implementations stored in specific
    project subdirectories.

    Args:
        directory: Physical directory path on disk to scan.
        base_package: Base Python package dot-path matching the directory structure
            (e.g., `"src.core.plugins"`).
        pattern: Glob pattern for filtering file names. Defaults to `"*.py"`.
        exclude: File names to skip during import. Defaults to `{"__pycache__", "__init__.py"}`.

    Returns:
        The total count of successfully imported modules.

    Raises:
        ValueError: If `directory` does not exist or is not a directory.

    Example:
        Loading third-party or custom plugins dynamically from a directory:

        >>> from pathlib import Path
        >>> from libs.metaclasses.registry import load_from_directory
        >>> count = load_from_directory(
        ...     directory=Path("src/plugins/custom"),
        ...     base_package="src.plugins.custom",
        ... )
        >>> print(f"Loaded {count} plugin modules.")
    """
    if exclude is None:
        exclude = {"__pycache__", "__init__.py"}

    directory = Path(directory)
    if not directory.is_dir():
        raise ValueError(f"'{directory}' is not a valid directory.")

    count = 0
    for py_file in sorted(directory.rglob(pattern)):
        if py_file.name in exclude:
            continue

        rel_path = py_file.relative_to(directory).with_suffix("")
        module_name = f"{base_package}." + ".".join(rel_path.parts)

        try:
            importlib.import_module(module_name)
            count += 1
            LOG.debug(f"Loaded plugin module: {module_name}")
        except Exception as e:
            LOG.warning(f"Failed to load plugin module {module_name}: {e}")

    return count


# --- Base Registry (Abstract Container) ---
# --- Base Registry Mixin ---
# --- Base Non-Generic Registry ---


class Registry:
    """Shared non-generic execution logic for package discovery and storage."""

    registry_name: str | None = None
    package_paths: list[str] | None = None
    module_paths: list[str] | None = None
    auto_key: bool = True

    _registry_items: dict[str, Any]
    _registry_loaded: bool

    def __init_subclass__(
        cls,
        registry_name: str | None = None,
        package_paths: str | list[str] | None = None,
        module_paths: str | list[str] | None = None,
        auto_key: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init_subclass__(**kwargs)

        cls._registry_items = {}
        cls._registry_loaded = False
        cls.registry_name = registry_name or cls.__name__
        cls.auto_key = auto_key

        cls.package_paths = (
            [package_paths.strip()]
            if isinstance(package_paths, str)
            else (package_paths or [])
        )
        cls.module_paths = (
            [module_paths.strip()]
            if isinstance(module_paths, str)
            else (module_paths or [])
        )

    @classmethod
    def register_item(cls, key: str, item: Any) -> None:
        clean_key = cls._normalize_key(key)
        if clean_key in cls._registry_items:
            raise KeyError(
                f"Key '{clean_key}' already registered in {cls.registry_name}."
            )
        cls._registry_items[clean_key] = item

    @classmethod
    def _ensure_loaded(cls) -> None:
        if cls._registry_loaded:
            return
        cls._registry_loaded = True

        for pkg in cls.package_paths or []:
            load_package_modules(pkg)

        for mod in cls.module_paths or []:
            try:
                importlib.import_module(mod)
            except Exception as e:
                LOG.warning(f"[{cls.registry_name}] Failed to import '{mod}': {e}")

    @classmethod
    def keys(cls) -> list[str]:
        cls._ensure_loaded()
        return list(cls._registry_items.keys())

    @classmethod
    def clear_registry(cls) -> None:
        cls._registry_items.clear()
        cls._registry_loaded = False

    @classmethod
    def _normalize_key(cls, key: str) -> str:
        return key.strip().lower()

    @classmethod
    def _generate_key(cls, name: str) -> str:
        if not cls.auto_key:
            return name
        return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


# --- Concrete Registries ---

ClassT = TypeVar("ClassT", bound=type)


class ClassRegistry(Registry):
    """Registry specialized for class definitions."""

    instance_cache: bool = False
    _registry_instances: dict[str, Any]

    def __init_subclass__(
        cls,
        instance_cache: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init_subclass__(**kwargs)
        if not hasattr(cls, "_registry_instances"):
            cls._registry_instances = {}
            cls.instance_cache = instance_cache

    @classmethod
    def register(cls, key: str | list[str] | None = None) -> Callable[[ClassT], ClassT]:
        def decorator(subclass: ClassT) -> ClassT:
            keys = (
                key
                if isinstance(key, list)
                else [key or cls._generate_key(subclass.__name__)]
            )
            for k in keys:
                cls.register_item(k, subclass)
            return subclass

        return decorator

    @classmethod
    def get_class(cls, key: str) -> Any:
        clean_key = cls._normalize_key(key)
        if clean_key not in cls._registry_items:
            cls._ensure_loaded()

        if clean_key not in cls._registry_items:
            available = ", ".join(repr(k) for k in set(cls._registry_items.keys()))
            raise KeyError(
                f"'{key}' not found in {cls.registry_name}. Available: [{available}]"
            )
        return cls._registry_items[clean_key]

    @classmethod
    def create(cls, key: str, *args: Any, **kwargs: Any) -> Any:
        target_cls = cls.get_class(key)
        instance = target_cls(*args, **kwargs)

        if cls.instance_cache:
            cache_key = cls._make_cache_key(key, args, kwargs)
            cls._registry_instances[cache_key] = instance

        return instance

    @classmethod
    def get_or_create(cls, key: str, *args: Any, **kwargs: Any) -> Any:
        """Retrieves a cached instance if present; otherwise, instantiates and caches it."""
        clean_key = cls._normalize_key(key)
        cache_key = cls._make_cache_key(clean_key, args, kwargs)

        if cls.instance_cache and cache_key in cls._registry_instances:
            return cls._registry_instances[cache_key]

        return cls.create(key, *args, **kwargs)

    @classmethod
    def clear_registry(cls) -> None:
        """Clears registered classes and cached instances."""
        super().clear_registry()
        cls._registry_instances.clear()

    @staticmethod
    def _make_cache_key(key: str, args: tuple, kwargs: dict) -> str:
        arg_str = "_".join(str(a) for a in args)
        kwarg_str = "_".join(f"{k}={v}" for k, v in sorted(kwargs.items()))
        return "|".join(p for p in [key, arg_str, kwarg_str] if p)


class FunctionRegistry(Registry):
    """Registry specialized for functions."""

    @classmethod
    def register(
        cls, key: str | None = None
    ) -> Callable[[Callable[..., R]], Callable[..., R]]:
        def decorator(fn: Callable[..., R]) -> Callable[..., R]:
            fn_name = getattr(fn, "__name__", str(fn))
            reg_key = key or cls._generate_key(fn_name)
            cls.register_item(reg_key, fn)
            return fn

        return decorator

    @classmethod
    def invoke(cls, key: str, *args: Any, **kwargs: Any) -> Any:
        clean_key = cls._normalize_key(key)
        if clean_key not in cls._registry_items:
            cls._ensure_loaded()

        fn = cls._registry_items[clean_key]
        return fn(*args, **kwargs)
