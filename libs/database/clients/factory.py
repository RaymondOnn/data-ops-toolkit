# from libs.database.clients.base import DBClient, DBRegistry


# class DatabaseFactory:
#     """Factory for creating database client instances."""

#     @classmethod
#     def get(cls, db_type: str, **kwargs) -> DBClient:
#         if db_type.lower() not in DBRegistry:
#             raise ValueError(
#                 f"No client registered for database: '{db_type}'. Available: {DBRegistry.keys()}"
#             )

#         client_cls = DBClient.get_client_cls(db_type)
#         return client_cls(**kwargs)
