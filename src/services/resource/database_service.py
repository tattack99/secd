from keycloak import KeycloakAuthenticationError
from src.util.logger import log
from src.services.core.keycloak_service import KeycloakService
from src.services.core.kubernetes_service import KubernetesService

class DatabaseService:
    def __init__(self, keycloak_service: KeycloakService, kubernetes_service: KubernetesService):
        self.keycloak_service = keycloak_service
        self.kubernetes_service = kubernetes_service

    def get_pod_ip(self, pod_name_prefix: str) -> str:
        try:
            pod_ip = self.kubernetes_service.get_pod_ip_in_namespace("storage", pod_name_prefix)
            return pod_ip
        except Exception as e:
            log(f"Error retrieving pod IP: {str(e)}", "ERROR")
            raise Exception(f"Error retrieving pod IP: {e}")

    def validate(self, auth_header: str) -> bool:
        try:
            return self.keycloak_service.validate(auth_header=auth_header)
        except KeycloakAuthenticationError as e:
            log(f"Authentication failed: {str(e)}", "ERROR")
            raise KeycloakAuthenticationError()