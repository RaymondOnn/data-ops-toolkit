from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional

import msgspec

from src.services.base import Service


class ReaderContext(msgspec.Struct):
    """
    Type-safe container for all ingestion parameters.
    Serializable for Ray worker distribution.
    """
    source_type: str
    target_table: Optional[str] = None  # Used by DatabaseIngest
    source_path: Optional[str] = None   # Used by FileIngest
    parallelism: int = 10
    # For any source-specific extras (e.g., API keys, custom filters)
    options: Dict[str, Any] = {}
    
    # mode: Literal["single_shot", "partitioned"]
    # work_units: List[List[str]]  # List of file groups to process
    # total_size_bytes: int

class Reader(ABC):
    @abstractmethod
    def fetch(
        self, 
        service: Service, 
        context: ReaderContext, 
        target_folder: Path
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError("Subclasses must implement this method")
    
class FileIngest(Reader):
    def get_units(self, service: Service, context: ReaderContext) -> list[str]:
        # context.source_path provides the directory or bucket to scan
        return service.list_files(context.source_path)
    
    