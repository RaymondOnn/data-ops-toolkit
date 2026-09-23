from collections.abc import Callable
from typing import Any

from src.core.stages.extract.config import parse_extract_config
from src.core.stages.publish.config import parse_publish_config
from src.core.stages.transform.config import parse_transform_config
from src.core.stages.write.config import parse_write_config

from .contracts import ErrorInfo
from .types import StageConfig, StagePayload

# Define a strict unified type signature for stage parsers
StageParserFn = Callable[[dict[str, Any], dict[str, Any]], StageConfig]

STAGE_PARSERS: dict[str, StageParserFn] = {
    "extract": parse_extract_config,
    "transform": parse_transform_config,
    "write": parse_write_config,
    "publish": parse_publish_config,
}


def parse_stage_config(
    stage: str, step: dict[str, Any], context: dict[str, Any]
) -> StageConfig | None:
    parser = STAGE_PARSERS.get(stage.casefold())
    return parser(step, context) if parser else None


__all__ = ["ErrorInfo", "StageConfig", "StagePayload", "parse_stage_config"]
