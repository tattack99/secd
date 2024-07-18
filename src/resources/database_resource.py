from keycloak import KeycloakAuthenticationError
from src.services.database_service import DatabaseService
from src.services.keycloak_service import KeycloakService
from src.services.kubernetes_service import KubernetesService
from src.util.logger import log
from falcon import HTTPUnauthorized, HTTPNotFound
import falcon

class DatabaseResource:
    def __init__(
            self,
            database_service : DatabaseService,
            keycloak_service : KeycloakService,
            kubernetes_service : KubernetesService,
            ):

        self.database_service = database_service
        self.keycloak_service = keycloak_service
        self.kubernetes_service = kubernetes_service

    def on_get(self, req, resp):
        log("Handling GET /v1/database request")
        try:
            auth_header = req.get_header('Authorization')
            if not self.keycloak_service.validate(auth_header=auth_header):
                raise HTTPUnauthorized(description="Could not validate header")

            pod_ip = self.kubernetes_service.get_pod_ip_in_namespace("storage", "mysql")

            if pod_ip is None:
                raise HTTPNotFound(description="Database pod not found")

            resp.status = falcon.HTTP_200
            resp.media = {
                "message": "Access granted",
                "database_pod_ip": pod_ip
            }

        except HTTPUnauthorized as e:
            log(f"Authorization failed: {str(e)}", "ERROR")
            resp.status = falcon.HTTP_401
            resp.media = {"error": str(e)}
        except KeycloakAuthenticationError as e:
            log(f"Authentication failed: {str(e)}", "ERROR")
            resp.status = falcon.HTTP_401
            resp.media = {"error": str(e)}
        except HTTPNotFound as e:
            log(f"Database pod not found: {str(e)}", "ERROR")
            resp.status = falcon.HTTP_404
            resp.media = {"error": str(e)}
        except Exception as e:
            log(f"Unexpected error: {str(e)}", "ERROR")
            resp.status = falcon.HTTP_500
            resp.media = {"error": "Internal server error"}

