from typing import Protocol

class VaultServiceProtocol(Protocol):
    def create_kubernetes_auth_role(
        self,
        role_name: str,
        service_account_name: str,
        namespace: str,
        policy: str,
        ttl: str = "1h"
    ) -> None:
        pass

    def delete_kubernetes_auth_role(self, role_name: str) -> None:
        ...