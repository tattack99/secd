import falcon
import threading
from wsgiref.simple_server import make_server
from app.src.util.setup import load_settings
from app.src.util.logger import log
from app.src.resources.hook_resource import HookResource
from app.src.services.core.docker_service import DockerService
from app.src.services.core.gitlab_service import GitlabService
from app.src.services.core.keycloak_service import KeycloakService
from app.src.services.core.kubernetes_service import KubernetesService
from app.src.util.daemon import Daemon
from app.src.services.resource.hook_service import HookService
from src.services.core.kubernetes_service_v1 import KubernetesServiceV1

class Server:
    def __init__(self):
        log("Starting server....")
        load_settings()

        self.apps = []
        self.threads = []

        # Instantiate core services
        self.keycloak_service = KeycloakService()
        self.docker_service = DockerService()
        self.kubernetes_service = KubernetesService()
        self.gitlab_service = GitlabService()
        self.kubernetes_service_v1 = KubernetesServiceV1()

        # Instantiate resources services
        self.hook_service = HookService(
            keycloak_service=self.keycloak_service,
            gitlab_service=self.gitlab_service,
            kubernetes_service=self.kubernetes_service,
            docker_service=self.docker_service,
        )

        # Instantiate resources
        self.hook_resource = HookResource(
            hook_service=self.hook_service
        )

        # Create multiple apps
        self.create_app('/v1/hook', self.hook_resource, 8080)

    def create_app(self, path, resource, port):
        app = falcon.App()
        app.add_route(path, resource)
        self.apps.append((app, port))

    def serve_app(self, app, port):
        try:
            with make_server('', port, app) as httpd:
                httpd.serve_forever()
        except Exception as e:
            log(f"Error starting server: {e}", "ERROR")

    def run(self):
        log("Running server...")
        try:
            microk8s_cleanup = Daemon(self.kubernetes_service, self.gitlab_service)
            microk8s_cleanup_thread = threading.Thread(target=microk8s_cleanup.start_microk8s_cleanup)
            microk8s_cleanup_thread.start()

            # Serve each app on different ports in separate threads
            for app, port in self.apps:
                thread = threading.Thread(target=self.serve_app, args=(app, port))
                self.threads.append(thread)
                thread.start()

            # Join threads to keep the main thread running
            for thread in self.threads:
                thread.join()

        except Exception as e:
            log(f"Error starting Daemon thread: {e}", "ERROR")
