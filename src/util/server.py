import falcon
import threading
from wsgiref.simple_server import make_server
from app.src.services.implementation.kubernetes_service_v1 import KubernetesServiceV1
from app.src.services.implementation.kubernetes_v1.helm_serviceV1 import HelmServiceV1
from app.src.services.implementation.kubernetes_v1.namespace_serviceV1 import NamespaceServiceV1
from app.src.services.implementation.kubernetes_v1.persistent_volume_serviceV1 import PersistentVolumeServiceV1
from app.src.services.implementation.kubernetes_v1.pod_serviceV1 import PodServiceV1
from app.src.services.implementation.kubernetes_v1.secret_serviceV1 import SecretServiceV1
from app.src.util.setup import load_settings, get_settings
from app.src.util.logger import log
from app.src.resources.hook_resource import HookResource
from app.src.services.implementation.docker_service import DockerService
from app.src.services.implementation.gitlab_service import GitlabService
from app.src.services.implementation.keycloak_service import KeycloakService
from app.src.services.implementation.kubernetes_service import KubernetesService

from app.src.util.daemon import Daemon
from app.src.resources.hook_service import HookService
from kubernetes import client, config

class Server:
    def __init__(self):
        log("Starting server....")
        load_settings()

        self.apps = []
        self.threads = []

        self.init_kubernetesV1()

        # Instantiate core services
        self.keycloak_service = KeycloakService()
        self.docker_service = DockerService()
        self.kubernetes_service = KubernetesService()
        self.gitlab_service = GitlabService()

        # Instantiate resources services
        self.hook_service = HookService(
            keycloak_service=self.keycloak_service,
            gitlab_service=self.gitlab_service,
            kubernetes_service=self.kubernetes_service_v1,
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
            microk8s_cleanup = Daemon(self.kubernetes_service_v1, self.gitlab_service)
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
    
    def init_kubernetesV1(self):
        self.config_path = get_settings()['k8s']['configPath']
        self.config = client.Configuration()
        config.load_kube_config(config_file=self.config_path, client_configuration=self.config)
 
        self.config.verify_ssl = False

        namespace_service = NamespaceServiceV1(config=self.config)    
        pv_service = PersistentVolumeServiceV1(config=self.config)
        pod_service = PodServiceV1(config=self.config)
        secret_service = SecretServiceV1(config=self.config)
        helm_service = HelmServiceV1(config=self.config)

        self.kubernetes_service_v1 = KubernetesServiceV1(
            namespace_service=namespace_service,
            pod_service=pod_service,
            pv_service=pv_service,
            secret_service=secret_service,
            helm_service=helm_service,
        )