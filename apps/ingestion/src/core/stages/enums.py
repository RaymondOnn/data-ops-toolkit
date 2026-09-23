# # If you still need StageBitmask for type hints
# class StageBitmask(IntFlag):
#     """Auto-generated from Stage enum."""

#     NONE = 0

#     @classmethod
#     def generate(cls) -> None:
#         """Generate bitmask members from Stage enum."""
#         for stage in Stage:
#             setattr(cls, stage.name, stage.bitmask)

#     @classmethod
#     def all(cls) -> int:
#         """Bitmask with all stages set."""
#         return (1 << len(Stage)) - 1


# # Generate StageBitmask members
# StageBitmask.generate()
