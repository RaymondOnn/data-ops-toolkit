import msgspec


# 1. Base class setting 'stage' as the discriminator field
class BaseStageConfig(msgspec.Struct, tag_field="stage", kw_only=True):
    pass
