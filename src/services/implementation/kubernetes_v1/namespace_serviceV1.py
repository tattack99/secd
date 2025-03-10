from kubernetes import client, config
from app.src.util.logger import log
from app.src.services.protocol.kubernetes.namespace_service_protocol import NamespaceServiceProtocol
from typing import List, Optional
import datetime

class NamespaceServiceV1(NamespaceServiceProtocol):
    def __init__(self, config: client.Configuration):   
        api_client = client.ApiClient(configuration=config)
        self.v1 = client.CoreV1Api(api_client=api_client)

    # CRUD methods
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
        
    def get_namespaces(self) -> List[client.V1Namespace]:
        try:
            return self.v1.list_namespace()
        except client.ApiException as e:
            log(f"Failed to get namespaces: {e}", "ERROR")
            return []
    
    def delete_namespace(self, name: str) -> None:
        try:
            self.v1.delete_namespace(name=name)
            log(f"Namespace {name} deleted")
        except client.ApiException as e:
            log(f"Failed to delete namespace {name}: {e}", "ERROR")


    # Service methods
    def cleanup_namespaces(self, namespaces) -> List[str]:
        run_ids = []
        for namespace in namespaces:
            if self._should_cleanup_namespace(namespace):
                log(f"Cleaning up namespace {namespace.metadata.name}")
                run_id = self._cleanup_namespace(namespace)
                run_ids.append(run_id)
        return run_ids

    # Helper methods
    def _should_cleanup_namespace(self, namespace) -> bool:
        annotations = namespace.metadata.annotations or {}
        if 'rununtil' not in annotations:
            return False
        expired = datetime.datetime.fromisoformat(annotations['rununtil']) < datetime.datetime.now()
        completed = self._is_pod_completed(namespace.metadata.name)
        return expired or completed
    
    def _cleanup_namespace(self, namespace) -> str:
        run_id = namespace.metadata.name.replace("secd-", "")
        self.v1.delete_namespace(namespace.metadata.name)
        log(f"Namespace {namespace.metadata.name} deleted")
        return run_id

    def _is_pod_completed(self, namespace_name: str) -> bool:
        pod_list = self.v1.list_namespaced_pod(namespace=namespace_name)
        if len(pod_list.items) > 0:
            pod = pod_list.items[0]
            log(f"Pod found in namespace: {namespace_name} with phase: {pod.status.phase}")
            return pod.status.phase in ['Succeeded', 'Failed']
        return False

