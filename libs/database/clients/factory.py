from typing import ClassVar

from libs.database.clients.base import DBClient


class DatabaseFactory:
    _registry: ClassVar[dict[str, type[DBClient]]] = {}

    @classmethod
    def register(
        cls, client_cls: type[DBClient] | None = None, db_type: str | None = None
    ):
        """
        Supports both direct calls and use as a class decorator:

        @DatabaseFactory.register
        class PostgresClient(DBClient): ...
        """

        def decorator(subclass: type[DBClient]):
            # Auto-detect db_type from the class attribute `type` if not explicitly passed
            key = db_type or getattr(subclass, "type", None)
            if not key:
                raise ValueError(
                    f"Class {subclass.__name__} must define a 'type' attribute or specify db_type."
                )
            cls._registry[key.lower()] = subclass
            return subclass

        if client_cls is not None:
            return decorator(client_cls)
        return decorator

    @classmethod
    def get(cls, db_type: str, **kwargs) -> DBClient:
        client_cls = cls._registry.get(db_type.lower())
        if not client_cls:
            raise ValueError(
                f"No client registered for database: '{db_type}'. Registered: {list(cls._registry.keys())}"
            )
        return client_cls(**kwargs)
