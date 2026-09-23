# import logging
# import re
# from collections.abc import Callable
# from typing import Any, Self, TypeVar

# from autoregistry import Registry

# LOG = logging.getLogger(__name__)

# T = TypeVar("T")


# class ClassRegistry(
#     Registry,
#     recursive=True,        # Automatically searches all submodules recursively!
#     # suffix="",             # Optional: set to remove suffixes like "Stage" if desired
# ):
#     """Base registry wrapper powered by autoregistry.

#     Provides automatic inheritance-based class registration across all subpackages.
#     """

#     @classmethod
#     def get(cls, name: str) -> type[Self]:
#         """Retrieves a registered class by key, automatically searching submodules."""
#         # autoregistry handles dict subscription via __getitem__
#         try:
#             return cls[name]
#         except KeyError:
#             available = ", ".join(repr(k) for k in cls.__registry__.keys())
#             raise KeyError(
#                 f"'{name}' not found in {cls.__name__}. Available: [{available}]"
#             ) from None

#     @classmethod
#     def create(cls, name: str, *args: Any, **kwargs: Any) -> Self:
#         """Instantiates a registered class directly by key."""
#         target_cls = cls.get(name)
#         return target_cls(*args, **kwargs)

#     @classmethod
#     def keys(cls) -> list[str]:
#         """Returns list of registered keys."""
#         return list(cls.__registry__.keys())


# class DecoratorRegistry:
#     """Standalone container object for registering classes or functions via decorators.

#     Keeps registered classes completely decoupled from registry base classes, making it
#     ideal for structural duck-typing and `Protocol` interfaces.

#     Example:
#         >>> plugin_registry = DecoratorRegistry(name="Plugins")
#         >>>
#         >>> # 1. Register standalone class using default snake_case key ("audio_plugin")
#         >>> @plugin_registry.register()
#         ... class AudioPlugin:
#         ...     def execute(self) -> str:
#         ...         return "Playing audio..."
#         >>>
#         >>> # 2. Register with explicit key override
#         >>> @plugin_registry.register(name="custom_video")
#         ... class VideoPlugin:
#         ...     def execute(self) -> str:
#         ...         return "Rendering video..."
#         >>>
#         >>> # 3. Lookup using .get() or [] subscription
#         >>> plugin_cls = plugin_registry.get("audio_plugin")
#         >>> same_cls = plugin_registry["audio_plugin"]
#         >>>
#         >>> # 4. Direct instantiation helper
#         >>> video = plugin_registry.create("custom_video")
#         >>> video.execute()
#         'Rendering video...'
#         >>>
#         >>> plugin_registry.keys()
#         ['audio_plugin', 'custom_video']
#     """

#     def __init__(self, name: str = "Registry") -> None:
#         self.name = name
#         self._items: dict[str, type] = {}

#     def register(self, name: str | None = None) -> Callable[[type[T]], type[T]]:
#         """Decorator to register a class or function."""

#         def decorator(cls: type[T]) -> type[T]:
#             key = name or re.sub(r"(?<!^)(?=[A-Z])", "_", cls.__name__).lower()
#             if key in self._items:
#                 raise KeyError(
#                     f"Key collision: '{key}' is already registered in {self.name}."
#                 )
#             self._items[key] = cls
#             return cls

#         return decorator

#     def get(self, name: str) -> type:
#         """Retrieves a registered item by key."""
#         if name not in self._items:
#             available = ", ".join(repr(k) for k in self._items)
#             raise KeyError(
#                 f"'{name}' not found in {self.name}. Available: [{available}]"
#             )
#         return self._items[name]

#     def __getitem__(self, name: str) -> type:
#         """Enables dictionary subscription syntax: registry["key"]"""
#         return self.get(name)

#     def create(self, name: str, *args: Any, **kwargs: Any) -> Any:
#         """Instantiates a registered class directly by key."""
#         cls = self.get(name)
#         return cls(*args, **kwargs)

#     def keys(self) -> list[str]:
#         """Returns list of registered keys."""
#         return list(self._items.keys())
