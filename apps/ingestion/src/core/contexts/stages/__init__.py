# from collections.abc import Callable
# from typing import Any

# from .archive import ArchiveConfig, parse_archive_config
# from .extract import ExtractConfig, parse_extract_config
# from .transform import TransformConfig, parse_transform_config
# from .write import LoadConfig, parse_load_config

# StageConfig = ExtractConfig | TransformConfig | LoadConfig | ArchiveConfig

# STAGE_PARSERS = {
#     "extract": parse_extract_config,
#     "transform": parse_transform_config,
# }


# def parse_stage_config(stage: str, step: dict[str, Any], get_val: Callable) -> Any:
#     parser = STAGE_PARSERS.get(stage.casefold())
#     return parser(step, get_val) if parser else None


# __all__ = [
#     "StageConfig",
#     "ArchiveConfig",
#     "ExtractConfig",
#     "LoadConfig",
#     "TransformConfig",
#     "parse_archive_config",
#     "parse_extract_config",
#     "parse_load_config",
#     "parse_transform_config",
# ]
