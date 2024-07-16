from keycloak import KeycloakAuthenticationError
from src.services.database_service import DatabaseService
from src.services.keycloak_service import KeycloakService
from src.util.logger import log
from falcon import HTTPUnauthorized
import falcon

class DatabaseResource:
    def __init__(
            self,
            database_service : DatabaseService,
            keycloak_service : KeycloakService
            ):

        self.database_service = database_service
        self.keycloak_service = keycloak_service

    def on_get(self, req, resp):
        log("Handling GET /v1/database request")
        try:
            auth_header = req.get_header('Authorization')
            if not auth_header:
                log("Authorization header missing", "WARNING")
                raise HTTPUnauthorized(description="Authorization header missing")

            valid_header = self.keycloak_service.validate(auth_header)
            if not valid_header:
                log("Not valid authorization header", "WARNING")
                raise HTTPUnauthorized(description="Authorization header missing")


            resp.status = falcon.HTTP_200
            resp.media = {"message": "Access granted"}
        except HTTPUnauthorized as e:
            log(f"Authorization failed: {str(e)}", "WARNING")
            resp.status = falcon.HTTP_401
            resp.media = {"error": str(e)}
        except KeycloakAuthenticationError as e:
            log(f"Authentication failed: {str(e)}", "WARNING")
            resp.status = falcon.HTTP_401
            resp.media = {"error": str(e)}
        except Exception as e:
            log(f"Unexpected error: {str(e)}", "ERROR")
            resp.status = falcon.HTTP_500
            resp.media = {"error": "Internal server error"}
