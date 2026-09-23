from collections.abc import Generator
from typing import TYPE_CHECKING, Any

from libs.metaclasses.draft import ClassRegistry

if TYPE_CHECKING:
    import polars as pl

    from src.services.contracts import Archive, Sink, Source

    # During type checking, Service pretends to inherit all protocols
    class _ServiceProtocolBase(Source, Sink, Archive):
        pass
else:

    class _ServiceProtocolBase:
        pass


class Service(
    ClassRegistry,
    _ServiceProtocolBase,
    registry_name="ServiceRegistry",
    auto_key=True,
    instance_cache=True,
    module_paths=[
        "src.services.file",
        "src.services.database",
    ],
):
    """
    Base class for all resilient services.
    Ensures the decorator can find the 'name' for the Registry.
    """

    name: str
    config: dict[str, Any]

    def __init__(self, name: str | None = None, **config: Any):
        """
        Initializes the service with a unique name and configuration.

        Args:
            name: Human-readable name or identifier for the service instance.
            **config: Driver-specific configuration parameters.
        """
        self.name = name or config.get("type") or self.__class__.__name__.lower()
        self.config = config

    @property
    def connector(self) -> Any:
        raise NotImplementedError

    def probe(self, target: str | None = None) -> bool:
        """
        Health probe interface for the service.

        Returns:
            bool: True if responsive and healthy, False otherwise.
        """
        return True

    # @abstractmethod
    def count_units(self, target: str, filter_condition: str | None = None) -> int:
        """
        Returns total row/item count for resource calculation or validation.

        Args:
            target: The identifier of the resource (e.g., table or path).
            filter_condition: Optional filter logic to apply to the count.

        Returns:
            int: The total number of items or rows found.
        """
        raise NotImplementedError()

    def fetch_df(self, query: str) -> Generator["pl.DataFrame", Any, None]:
        """
        Base signature for streaming DataFrames.

        Args:
            query: The SQL query or command to execute.

        Yields:
            pl.DataFrame: A batch of results.

        Raises:
            NotImplementedError: If the service does not support streaming.
        """
        raise NotImplementedError("Service does not support fetch_df()")

    def exists(self, target: str) -> bool:
        """
        Base signature for checking if an artifact/table exists.

        Args:
            target: The unique name of the artifact to check.

        Returns:
            bool: True if it exists, False otherwise.

        Raises:
            NotImplementedError: If the service does not support exists check.
        """
        raise NotImplementedError("Service does not support exists()")

    def setup_resource(self, target: str, **kwargs: Any):
        raise NotImplementedError

    def clone(self, source: Any, dest: Any) -> None:
        """
        Clones a dataset structure or data to a new identifier.

        Args:
            source: The source to clone from.
            dest: The destination to create.
        """
        raise NotImplementedError()

    def delete(self, target: str) -> None:
        """
        Drops or deletes a dataset or table.

        Args:
            target: The name of the table or object to drop.
        """
        raise NotImplementedError()
