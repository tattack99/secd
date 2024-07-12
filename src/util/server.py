import falcon
import threading
from wsgiref.simple_server import make_server
from secure.src.util.setup import load_settings
from secure.src.resources.database_resource import DatabaseResource
from secure.src.resources.keycloak_resource import KeycloakResource
from secure.src.resources.hook_resource import HookResource
from secure.src.services.database_service import DatabaseService
from secure.src.services.docker_service import DockerService
from secure.src.services.gitlab_service import GitlabService
from secure.src.services.keycloak_service import KeycloakService
from secure.src.services.kubernetes_service import KubernetesService
from secure.src.services.mysql_service import MySQLService
from secure.src.util.logger import log
from secure.src.util.daemon import Daemon

class Server:
    def __init__(self):
        log("Starting server...")
        load_settings()

        # Instantiate services
        self.keycloak_service = KeycloakService()
        self.database_service = DatabaseService()
        self.docker_service = DockerService()
        self.kubernetes_service = KubernetesService()
        self.gitlab_service = GitlabService()
        self.mysql_service = MySQLService()

        # Instantiate resources
        self.hook_resource = HookResource(
            database_service=self.database_service,
            keycloak_service=self.keycloak_service,
            gitlab_service=self.gitlab_service,
            kubernetes_service=self.kubernetes_service,
            docker_service=self.docker_service,
            mysql_service=self.mysql_service)

        self.database_resource = DatabaseResource(
            database_service=self.database_service,
            keycloak_service=self.keycloak_service)

        self.keycloak_resource = KeycloakResource(
            keycloak_service=self.keycloak_service)

        # Add routes
        self.app = falcon.App()
        self.app.add_route('/v1/hook',self.hook_resource)
        self.app.add_route('/v1/database',self.database_resource)
        self.app.add_route('/v1/keycloak',self.keycloak_resource)

    def run(self):
        try:

            # TODO: Extract these Daemon to standalone microservices
            log("Creating Daemon micrk8s_cleanup...")
            micrk8s_cleanup = Daemon(self.kubernetes_service, self.gitlab_service, self.database_service)
            micrk8s_cleanup_thread = threading.Thread(target=micrk8s_cleanup.start_microk8s_cleanup)
            micrk8s_cleanup_thread.start()

            log("Creating Daemon database_service...")
            database_service = Daemon(self.kubernetes_service, self.gitlab_service, self.database_service)
            database_service_thread = threading.Thread(target=database_service.start_database_service)
            database_service_thread.start()


        except Exception as e:
            log(f"Error starting Daemon thread: {e}", "ERROR")

        with make_server('', 8080, self.app) as httpd:
            log('Serving on port 8080...')
            httpd.serve_forever()
