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
            # Validate token
            token = req.get_header('Authorization')
            self.keycloak_service.validate_token(token)

            # Validate keycloak user have access to database
            #database = req.get_param('database')
            #self.keycloak_service.validate_database_role(database)

            #host = self.database_service.get_database_host(database)


        except KeycloakAuthenticationError as e:
            log(f"Token validation error: {e.error_message}", "ERROR")
            raise HTTPUnauthorized(description=e.error_message)
        except Exception as e:
            log(f"Error: {e}", "ERROR")
            resp.media = {'message': 'Failed'}
            resp.status = falcon.HTTP_500

        resp.media = {'message': 'Success'}
        resp.status = falcon.HTTP_200
