import logging
import re
from io import BytesIO

LOG = logging.getLogger(__name__)


def repair_csv(stream: BytesIO) -> BytesIO:
    """Fixes common 'Excel-exported' CSV issues like the UTF-8 BOM."""
    content = stream.read()
    if content.startswith(b"\xef\xbb\xbf"):
        LOG.info("🛠️ Stripping BOM from CSV...")
        content = content[3:]
    return BytesIO(content)


def repair_json(stream: BytesIO, encoding: str = "utf-8") -> BytesIO:
    """Handles common 'Trailing Comma' issues in JSON arrays/objects."""
    content = stream.read().decode(encoding, errors="ignore")
    # Fix [1, 2, 3,] -> [1, 2, 3]
    clean_content = re.sub(r",\s*([\]}])", r"\1", content)
    return BytesIO(clean_content.encode("utf-8"))


def repair_xml(stream: BytesIO, encoding: str = "utf-8") -> BytesIO:
    """Removes ASCII control characters (0-31) except for tab, newline, carriage return."""
    content = stream.read().decode(encoding, errors="ignore")
    # Clean control characters but keep valid whitespace
    clean_content = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", content)
    return BytesIO(clean_content.encode("utf-8"))
