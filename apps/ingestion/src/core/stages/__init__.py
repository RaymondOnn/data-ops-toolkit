from collections.abc import Callable
from typing import Any

from src.core.stages.extract.config import parse_extract_config
from src.core.stages.transform.config import parse_transform_config

from .contracts import ErrorInfo
from .types import StageConfig, StagePayload

STAGE_PARSERS = {
    "extract": parse_extract_config,
    "transform": parse_transform_config,
}


def parse_stage_config(stage: str, step: dict[str, Any], get_val: Callable) -> Any:
    parser = STAGE_PARSERS.get(stage.casefold())
    return parser(step, get_val) if parser else None


__all__ = ["ErrorInfo", "StageConfig", "StagePayload", "parse_stage_config"]
