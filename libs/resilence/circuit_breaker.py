


from typing import Literal


class CircuitBreaker:
    def __init__(self, threshold):
        self.threshold = threshold
        self.state = "CLOSED"
        
    def can_attempt(self):
        if self.state == "OPEN":
            return False
        else:
            return True
        
    def success(self):
        self.state = "CLOSED"
        
    def trip(self):
        self.state = "OPEN"
        
    def reset(self):
        """
        Reset the circuit breaker back to its original state ("CLOSED").
        """
        self.state = "CLOSED"
        
    def state(self):
        return self.state
    
    def __repr__(self):
        return f"CircuitBreaker(threshold={self.threshold}, state={self.state})"
        
    def __bool__(self):
        return self.state == "OPEN"
    
    