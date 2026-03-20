import importlib
import re
from typing import TYPE_CHECKING, Any, cast

import structlog
from src.core.strategies.transform.transform import (
    BitmaskTransformer,
    DefaultTransformer,
    NoOpTransformer,
)

if TYPE_CHECKING:
    from src.core.strategies.transform.base import Transformer


LOG = structlog.getLogger(__name__)


class TransformFactory:
    """
    Decision: Dynamic Module Loading.
    Allows for job-specific logic (e.g., complex bitmasking for a specific vendor)
    without bloating the core engine codebase.
    """

    @staticmethod
    def get_transformer(
        transform_type: str,
        job_id: str | None = None,
        dataset_id: str | None = None,
        **kwargs: Any,
    ) -> "Transformer":
        # 1. Handle Standard Transformers
        standard_map: dict[str, Any] = {
            "default": DefaultTransformer,
            "bitmask": BitmaskTransformer,
            "noop": NoOpTransformer,
        }

        if transform_type in standard_map:
            return cast("Transformer", standard_map[transform_type]())

        # 2. Handle Custom Transformers
        if transform_type == "custom":
            if not job_id:
                LOG.warning(
                    "Custom transformer requested but no job_id provided. "
                    "Falling back to Default."
                )
                return cast("Transformer", DefaultTransformer())

            # Normalize job_id for the package path (replace hyphens with underscores)
            job_pkg = job_id.replace("-", "_")

            # Precedence:
            # 1. Dataset-level: src...custom.<job_pkg>.<dataset_id_safe>
            # 2. Job-level:     src...custom.<job_pkg>.<job_id_safe>

            lookups = []
            if dataset_id:
                lookups.append((dataset_id, "dataset"))
            lookups.append((job_id, "job"))

            for identifier, category in lookups:
                id_safe = identifier.replace("-", "_")
                # Pattern: sales_daily -> SalesDailyTransformer
                parts = re.split(r"[-_]", identifier)
                class_name = "".join(x.capitalize() for x in parts) + "Transformer"
                module_path = (
                    f"src.core.strategies.transform.custom.{job_pkg}.{id_safe}"
                )

                try:
                    module = importlib.import_module(module_path)
                    transformer_class = getattr(module, class_name)
                    LOG.info(
                        f"Loaded {category}-level transformer",
                        module=module_path,
                        class_name=class_name,
                    )
                    return cast("Transformer", transformer_class())
                except (ImportError, AttributeError):
                    continue

            LOG.warning(
                "Custom transformer requested but no specific logic found in "
                f"job-nested package '{job_pkg}'. Falling back to Default.",
                job_id=job_id,
                dataset_id=dataset_id,
            )
            return cast("Transformer", DefaultTransformer())

        # 3. Fallback
        LOG.warning(
            "Unknown transform_type. Falling back to Default.",
            transform_type=transform_type,
        )
        return cast("Transformer", DefaultTransformer())
