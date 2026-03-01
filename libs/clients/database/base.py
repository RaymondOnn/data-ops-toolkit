from abc import ABC, abstractmethod

class BaseDBClient(ABC):
    def __init__(self, pool):
        self.pool = pool

    @abstractmethod
    def fetch_dataframe(self, query: str):
        pass

    # Generic behavior shared by ALL databases
    def execute_raw(self, sql: str):
        with self.pool.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()