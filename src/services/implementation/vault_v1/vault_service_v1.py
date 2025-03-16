import hvac

from app.src.services.protocol.vault.vault_service_protocol import VaultServiceProtocol
from app.src.util.setup import get_settings

class VaultServiceV1(VaultServiceProtocol):
    def __init__(self):
        """Initialize the Vault client with address and token."""
        vault_addr = get_settings()['vault']['address']
        vault_token = get_settings()['vault']['token']
        self.client = hvac.Client(url=vault_addr, token=vault_token)
        if not self.client.is_authenticated():
            raise Exception("Failed to authenticate with Vault")

    def create_kubernetes_auth_role(
        self,
        role_name: str,
        service_account_name: str,
        namespace: str,
        policy: str,
        ttl: str = "1h"
    ) -> None:
        try:
            self.client.auth.kubernetes.create_role(
                name=role_name,
                bound_service_account_names=[service_account_name],
                bound_service_account_namespaces=[namespace],
                policies=[policy],
                ttl=ttl
            )
        except Exception as e:
            raise Exception(f"Failed to create Kubernetes auth role {role_name}: {str(e)}")

    def delete_kubernetes_auth_role(self, role_name: str) -> None:
        try:
            self.client.auth.kubernetes.delete_role(name=role_name)
        except Exception as e:
            raise Exception(f"Failed to delete Kubernetes auth role {role_name}: {str(e)}")