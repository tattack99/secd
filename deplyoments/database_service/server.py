from wsgiref.simple_server import make_server
from database_service.resources.database_resource import DatabaseResource
from database_service.resources.keycloak_resource import KeycloakResource
import falcon


class Server:
    def __init__(self):
        self.app = falcon.App()
        self.app.add_route('/database', DatabaseResource())
        self.app.add_route('/keycloak', KeycloakResource())

    def run(self):
        with make_server('', 8080, self.app) as httpd:
            print("Serving on port 8080...")
            httpd.serve_forever()

