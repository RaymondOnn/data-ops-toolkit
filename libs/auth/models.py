from typing import Optional

from .provider import SecretProvider



class Secret:
    """A wrapper that hides the actual secret until needed."""
    def __init__(self, secret_id: str, provider: Optional[SecretProvider] = None):
        self.secret_id = secret_id
        self.provider = provider
        self._value = None

    def resolve(self, sanitize: bool = False) -> str:
        """Fetch the actual string. Used by DB Clients right before connection."""
        if not self._value:
            if not self.provider:
                raise ValueError(f"Provider not set for secret: {self.secret_id}")
            self._value = self.provider.get_secret(self.secret_id)
            
        if sanitize:
            import urllib.parse
            self._value = urllib.parse.quote_plus(self._value)

        return str(self._value)
    
    def __repr__(self) -> str:
        # This shows up in debugger and logs
        return f"<Secret id='{self.secret_id}' (HIDDEN)>"

    def __str__(self) -> str:
        if not self._value:
            n_chars = 0
        else:   
            n_chars = len(self._value)
        
        # This shows up in print()
        return f"******** (length: {n_chars})"