import dbutils
from libs.registry import get_registry



def create_pool(driver, **config):
    """Create a connection pool using DBUtils"""
    return dbutils.PooledDB(
        creator=driver,
        maxconnections=config.get('max_connections', 5),
        mincached=config.get('min_cached', 2),
        maxcached=config.get('max_cached', 5),
        blocking=config.get('blocking', True),
        **{k: v for k, v in config.items() 
           if k not in ['max_connections', 'min_cached', 'max_cached', 'blocking']}
    )
    


class DatabaseManager:
    def __init__(self, service_name: str):
        self.service_name = service_name
        self._registry = None  # Don't create it yet!

    @property
    def registry(self):
        """Standard Lazy-loading property"""
        if self._registry is None:
            self._registry = get_registry()
        return self._registry

    def can_proceed(self) -> bool:
        # This will trigger get_registry() only when needed
        status = self.registry.get_status(self.service_name)
        return status == "UP"

    def get_client(self, name, driver, client_class, **config):
        """
        Returns a specialized client (e.g., SnowflakeClient) 
        initialized with a connection pool.
        """
        if name not in self.registry:
            self.registry[name] = create_pool(driver, **config)
        
        # Wrap the pool in the requested custom behavior class
        return client_class(pool=self.registry[name])
    
    def update_registry(self, service_name, status):
        if self.registry.get(service_name) != status:
            self.registry[service_name] = status
            # You can trigger a single alert here instead of per-worker
            print(f"ALERT: Service {service_name} changed to {status}")
    
    
def decode_status(status_int: int) -> list:
    flags = {1: "INGESTED", 2: "ARCHIVED", 4: "MASKED", 8: "NOTIFIED", 16: "SYNCED"}
    return [label for bit, label in flags.items() if status_int & bit]