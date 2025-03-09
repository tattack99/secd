from kubernetes import client, config
from app.src.util.logger import log
from app.src.services.protocol.kubernetes.namespace_service_protocol import NamespaceServiceProtocol
from typing import List, Optional
import datetime

class NamespaceServiceV1(NamespaceServiceProtocol):
    def __init__(self, config: client.Configuration):   
        api_client = client.ApiClient(configuration=config)
        self.v1 = client.CoreV1Api(api_client=api_client)

    def create_namespace(self, name: str, labels: dict, annotations: dict) -> client.V1Namespace:
        namespace = client.V1Namespace(
            metadata=client.V1ObjectMeta(name=name, labels=labels, annotations=annotations)
        )
        self.v1.create_namespace(body=namespace)
        log(f"Namespace {name} created")
        return namespace

    def get_namespace(self, name: str) -> Optional[client.V1Namespace]:
        try:
            return self.v1.read_namespace(name=name)
        except client.ApiException as e:
            log(f"Failed to get namespace {name}: {e}", "ERROR")
            return None

    def list_namespaces(self) -> List[client.V1Namespace]:
        return self.v1.list_namespace().items

    def delete_namespace(self, name: str) -> None:
        try:
            log(f"Deleting namespace {name}")
            self.v1.delete_namespace(name=name)
            log(f"Namespace {name} deleted")
        except client.ApiException as e:
            log(f"Failed to delete namespace {name}: {e}", "ERROR")

    def cleanup_namespaces(self) -> List[str]:
        run_ids = []
        namespaces = self.v1.list_namespace()
        for namespace in namespaces.items:
            if self._should_cleanup_namespace(namespace):
                run_id = self._cleanup_namespace(namespace)
                run_ids.append(run_id)
        return run_ids

    # Helper methods moved from KubernetesService
    def _should_cleanup_namespace(self, namespace) -> bool:
        annotations = namespace.metadata.annotations or {}
        if 'rununtil' not in annotations:
            return False
        expired = datetime.datetime.fromisoformat(annotations['rununtil']) < datetime.datetime.now()
        completed = self._is_pod_completed(namespace.metadata.name)
        return expired or completed

    def _is_pod_completed(self, namespace_name: str) -> bool:
        pod_list = self.v1.list_namespaced_pod(namespace=namespace_name)
        if pod_list.items:
            pod = pod_list.items[0]
            return pod.status.phase in ['Succeeded', 'Failed']
        return False

    def _cleanup_namespace(self, namespace) -> str:
        run_id = namespace.metadata.name.replace("secd-", "")
        log(f"Finishing run {namespace.metadata.name} - Deleting resources")
        self.v1.delete_namespace(namespace.metadata.name)
        return run_id