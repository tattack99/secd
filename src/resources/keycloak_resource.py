
class KeycloakResource:
    def __init__(self, keycloak_service):
        self.keycloak_service = keycloak_service

    def on_get(self, req, resp):
        return {"data": "KeycloakResource"}