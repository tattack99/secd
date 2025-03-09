from typing import Protocol, Optional

class SecretServiceProtocol(Protocol):
    def get_secret(self, namespace: str, secret_name: str, key: str) -> Optional[str]:
        """Retrieve a secret value by namespace, name, and key."""
        ...