from .archive.config import ArchiveConfig
from .archive.enums import ArchivePayload
from .extract.config import ExtractConfig
from .extract.enums import ExtractPayload
from .publish.enums import PublishPayload
from .start.enums import StartPayload
from .transform.config import TransformConfig
from .transform.enums import TransformPayload
from .write.config import LoadConfig
from .write.enums import WritePayload

StagePayload = (
    StartPayload
    | ExtractPayload
    | TransformPayload
    | WritePayload
    | PublishPayload
    | ArchivePayload
)


StageConfig = ExtractConfig | TransformConfig | LoadConfig | ArchiveConfig
