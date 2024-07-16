import logging
from secure.src.services.keycloak_service import KeycloakService
import falcon

class KeycloakAuthMiddleware:
    def __init__(self, keycloak_service : KeycloakService):
        self.keycloak_service = keycloak_service

    def process_request(self, req, resp):
        logging.debug("Processing request for: %s", req.path)
        try:
            token = req.get_header('Authorization')
            self.keycloak_service.authenticate(token)
        except falcon.HTTPUnauthorized as e:
            raise falcon.HTTPUnauthorized(description="Authorization header missing")
        except Exception as e:
            raise falcon.HTTPUnauthorized(description="Token is invalid!")