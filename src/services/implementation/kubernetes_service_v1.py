from typing import Optional, Dict, List
from kubernetes import client
from app.src.util.setup import get_settings
from app.src.util.logger import log
from app.src.services.protocol.kubernetes.namespace_service_protocol import NamespaceServiceProtocol
from app.src.services.protocol.kubernetes.pod_service_protocol import PodServiceProtocol
from app.src.services.protocol.kubernetes.persistent_volume_service_protocol import PersistentVolumeServiceProtocol
from app.src.services.protocol.kubernetes.secret_service_protocol import SecretServiceProtocol
from app.src.services.protocol.kubernetes.helm_service_protocol import HelmServiceProtocol
import os
import datetime

class KubernetesServiceV1:
    def __init__(
        self,
        namespace_service: NamespaceServiceProtocol,
        pod_service: PodServiceProtocol,
        pv_service: PersistentVolumeServiceProtocol,
        secret_service: SecretServiceProtocol,
        helm_service: HelmServiceProtocol,
    ):
        self.namespace_service = namespace_service
        self.pod_service = pod_service
        self.pv_service = pv_service
        self.secret_service = secret_service
        self.helm_service = helm_service
        self.config_path = get_settings()['k8s']['configPath']

    def handle_cache_dir(self, run_meta: Dict, keycloak_user_id: str, run_id: str) -> tuple[Optional[str], Optional[str]]:
        cache_dir = None
        mount_path = None
        if "cache_dir" in run_meta and run_meta["cache_dir"]:
            mount_path = run_meta.get('mount_path', '/cache')
            cache_dir = run_meta['cache_dir']
            cache_path = f"{get_settings()['path']['cachePath']}/{keycloak_user_id}/{cache_dir}"
            if not os.path.exists(cache_path):
                os.makedirs(cache_path)
                log(f"Cache directory created at: {cache_path}")
        return cache_dir, mount_path

    def create_namespace(self, user_id: str, run_id: str, run_for: int) -> None:
        run_until = datetime.datetime.now() + datetime.timedelta(hours=run_for)
        namespace_name = f"secd-{run_id}"
        labels = {"access": "database-access"}
        annotations = {"userid": user_id, "rununtil": run_until.isoformat()}
        self.namespace_service.create_namespace(namespace_name, labels, annotations)

    def cleanup_resources(self) -> List[str]:
        secd_namespaces = []

        namespaces = self.namespace_service.get_namespaces()
        for namespace in namespaces.items:
            if namespace.metadata.name.startswith("secd-"):
                secd_namespaces.append(namespace)

        run_ids = self.namespace_service.cleanup_namespaces(secd_namespaces)
        self.pv_service.cleanup_persistent_volumes(secd_namespaces)

        return run_ids

    def get_secret(self, namespace: str, secret_name: str, key: str) -> Optional[str]:
        return self.secret_service.get_secret(namespace, secret_name, key)

    def get_pod_by_helm_release(self, release_name: str, namespace: str) -> Optional[client.V1Pod]:
        return self.pod_service.get_pod_by_helm_release(release_name, namespace)

    def get_service_by_helm_release(self, release_name: str, namespace: str) -> Optional[str]:
        return self.helm_service.get_service_by_helm_release(release_name, namespace)