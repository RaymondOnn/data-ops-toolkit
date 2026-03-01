from libs.resilence.circuit_breaker import CircuitBreaker


class OracleService:
    def __init__(self, name, manager):
        self.name = name
        self.manager = manager
        self.breaker = CircuitBreaker(threshold=3) # The shared component we discussed

    def execute(self, query):
        if not self.breaker.can_attempt():
            # Automatically tell the manager we are down
            self.manager.update_registry(self.name, "DOWN")
            return None
        
        try:
            # Attempt real work...
            result = self.conn.execute(query)
            self.breaker.success()
            self.manager.update_registry(self.name, "UP")
            return result
        except Exception:
            self.breaker.trip()
            if self.breaker.state == "OPEN":
                self.manager.update_registry(self.name, "DOWN")
            raise