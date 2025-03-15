import hvac
from src.util.logger import log
from app.src.util.setup import get_settings

class VaultServiceV1:
    def __init__(self):
        """
        Initialize Vault client with address and root token.
        :param vault_addr: Vault server URL (e.g., "http://10.152.183.52:8200")
        :param vault_token: Vault root token for authentication
        """
        vault_addr = get_settings()['vault']['address']
        vault_token = get_settings()['vault']['token']
        self.client = hvac.Client(url=vault_addr, token=vault_token)
        if not self.client.is_authenticated():
            log("Failed to authenticate with Vault", "ERROR")
            raise Exception("Vault authentication failed")
        #log("Vault client initialized", "DEBUG")

    def enable_kubernetes_auth(self) -> None:
        """Enable Kubernetes authentication method if not already enabled."""
        try:
            auth_methods = self.client.sys.list_auth_methods()
            if 'kubernetes/' not in auth_methods:
                self.client.sys.enable_auth_method(method_type='kubernetes', path='kubernetes')
                log("Kubernetes auth method enabled", "INFO")
            else:
                log("Kubernetes auth method already enabled", "DEBUG")
        except Exception as e:
            log(f"Error enabling Kubernetes auth: {str(e)}", "ERROR")
            raise

    def configure_kubernetes_auth(self, kubernetes_host: str, ca_cert: str, token_reviewer_jwt: str) -> None:
        """Configure Kubernetes auth with host, CA cert, and token reviewer JWT."""
        try:
            self.client.sys.write_auth_config(
                path='kubernetes/config',
                config={
                    'kubernetes_host': kubernetes_host,
                    'kubernetes_ca_cert': ca_cert,
                    'token_reviewer_jwt': token_reviewer_jwt
                }
            )
            log("Kubernetes auth configured", "INFO")
        except Exception as e:
            log(f"Error configuring Kubernetes auth: {str(e)}", "ERROR")
            raise

    def write_policy(self, policy_name: str, policy: str) -> None:
        """Write a Vault policy."""
        try:
            self.client.sys.create_or_update_policy(name=policy_name, policy=policy)
            log(f"Policy {policy_name} written", "INFO")
        except Exception as e:
            log(f"Error writing policy {policy_name}: {str(e)}", "ERROR")
            raise

    def create_kubernetes_role(self, role_name: str, bound_service_account_names: str, bound_service_account_namespaces: str, policies: str, ttl: str) -> None:
        """Create a Vault role for Kubernetes auth."""
        try:
            self.client.auth.kubernetes.create_role(
                name=role_name,
                bound_service_account_names=[bound_service_account_names],
                bound_service_account_namespaces=[bound_service_account_namespaces],
                policies=[policies],
                ttl=ttl
            )
            log(f"Vault role {role_name} created", "INFO")
        except Exception as e:
            log(f"Error creating role {role_name}: {str(e)}", "ERROR")
            raise

    def enable_kv_secrets_engine(self, path: str = "secret") -> None:
        """Enable KV secrets engine at the specified path."""
        try:
            mounts = self.client.sys.list_mounted_secrets_engines()
            if f"{path}/" not in mounts:
                self.client.sys.enable_secrets_engine(backend_type='kv', path=path, options={'version': '2'})
                log(f"KV secrets engine enabled at {path}", "INFO")
            else:
                log(f"KV secrets engine already enabled at {path}", "DEBUG")
        except Exception as e:
            log(f"Error enabling KV secrets engine: {str(e)}", "ERROR")
            raise

    def put_kv_secret(self, path: str, secret_data: dict[str, str]) -> None:
        """Store a secret in the KV secrets engine."""
        try:
            self.client.secrets.kv.v2.create_or_update_secret(
                path=path,
                secret=secret_data,
                mount_point="secret"
            )
            log(f"Secret stored at {path}", "INFO")
        except Exception as e:
            log(f"Error storing secret at {path}: {str(e)}", "ERROR")
            raise