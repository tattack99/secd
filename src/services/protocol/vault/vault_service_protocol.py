from typing import Protocol

class VaultServiceProtocol(Protocol):
    def get_service_by_vault_secret(self, secret_name: str, namespace: str) -> str:
        """Find a service FQDN by Vault secret name."""
        ...
        