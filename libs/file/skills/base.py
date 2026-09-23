from typing import TYPE_CHECKING, Any

from libs.metaclasses.draft import ClassRegistry

if TYPE_CHECKING:
    from libs.file.clients.base import FileSystemClient


class FileSkill(
    ClassRegistry,
    registry_name="FileSystemSkillRegistry",
    auto_key=True,
    package_paths="libs.file.skills",
):
    """Base registry class for all file system skill mixins.

    Subclasses automatically register under 'FileSystemSkillRegistry' using
    their class name or an explicit `__key__`.
    """

    # @classmethod
    # def get_mixin(cls, key_or_class: str | type) -> type:
    #     """Resolve a skill key or class into a mixin class."""
    #     if isinstance(key_or_class, type) and issubclass(key_or_class, FileSkill):
    #         return key_or_class
    #     if isinstance(key_or_class, str):
    #         return cls.get_class(key_or_class.lower())
    #     raise TypeError(f"Invalid skill reference: {key_or_class}")

    def __init__(self, fs: "FileSystemClient", **kwargs: Any):
        self.fs = fs

    def close(self) -> None:
        """Optional lifecycle cleanup hook."""
