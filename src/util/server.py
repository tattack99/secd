import falcon
import threading
from wsgiref.simple_server import make_server
from secure.src.util.setup import load_settings
from secure.src.util.logger import log
from secure.src.resources.hook_resource import HookResource
from secure.src.services.core.docker_service import DockerService
from secure.src.services.core.gitlab_service import GitlabService
from secure.src.services.core.keycloak_service import KeycloakService
from secure.src.services.core.kubernetes_service import KubernetesService
from secure.src.util.daemon import Daemon
from secure.src.services.resource.hook_service import HookService

class Server:
    def __init__(self):
        log("Starting server...")
        load_settings()

        self.apps = []
        self.threads = []

        # Instantiate core services
        self.keycloak_service = KeycloakService()
        self.docker_service = DockerService()
        self.kubernetes_service = KubernetesService()
        self.gitlab_service = GitlabService()

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
        #log(f"Starting server on port {port}...")
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
