# """Transformer factory with dynamic registration."""

# import importlib

# from .base import (
#     CustomFunctionTransformerAdapter,
#     TransformContext,
#     Transformer,
#     TransformRegistry,
# )


# class TransformFactory:
#     """Factory for creating transform logic instances."""

#     @classmethod
#     def get(cls, context: TransformContext) -> Transformer:
#         """Get transformer instance by registered name or module path."""
#         transform_type = context.sub_step.type.casefold() if context else None
#         if not transform_type:
#             raise ValueError("Transform type must be specified in the context.")

#         # 1. Check built-in transformer registry
#         if transform_type in TransformRegistry:
#             transformer_cls = Transformer.get(transform_type)
#             return cls._instantiate(transformer_cls, context)

#         # 2. Dynamic custom class or function import
#         if transform_type in ("custom", "python"):
#             module_path = context.sub_step.module_path
#             if not module_path:
#                 raise ValueError(
#                     f"Sub-step '{context.sub_step.id}' is typed as '{transform_type}' "
#                     "but lacks 'module_path'."
#                 )
#             return cls._load_from_path(module_path, context)

#         raise ValueError(
#             f"Unknown transform type: '{transform_type}'. "
#             f"Available built-ins: {TransformRegistry.keys()}"
#         )

#     @classmethod
#     def _instantiate(
#         cls, transformer_cls: type[Transformer], context: TransformContext
#     ) -> Transformer:
#         """Instantiate transformer with context parameters."""
#         return transformer_cls(
#             config=context.sub_step,
#             sources=context.sources,
#             output_dir=context.target,
#             file_format=context.format,
#         )

#     @classmethod
#     def _load_from_path(
#         cls, module_path: str, context: TransformContext
#     ) -> Transformer:
#         """Loads a Transformer subclass or standalone Callable function from a python path."""
#         try:
#             mod_path, target_name = module_path.rsplit(".", 1)
#             module = importlib.import_module(mod_path)
#             target = getattr(module, target_name)
#         except (ValueError, ImportError, AttributeError) as e:
#             raise ImportError(
#                 f"Failed to load custom transformer from '{module_path}': {e}"
#             ) from e

#         # Target is a Transformer subclass
#         if isinstance(target, type) and issubclass(target, Transformer):
#             return cls._instantiate(target, context)

#         # Target is a standalone custom function
#         if callable(target):
#             return CustomFunctionTransformerAdapter(
#                 target_func=target,
#                 config=context.sub_step,
#                 sources=context.sources,
#                 output_dir=context.target,
#                 file_format=context.format,
#             )

#         raise TypeError(
#             f"Target '{module_path}' must be a Transformer subclass or a Callable."
#         )
