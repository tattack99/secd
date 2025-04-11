from typing import Protocol, List, Optional, Dict
from kubernetes.client import V1Pod

class PodServiceProtocol(Protocol):
    def create_pod(
            self,
            run_id: str,
            image: str,
            envs: Dict[str, str],
            gpu: str,
            mount_path: Optional[str],
            database: str,
            namespace: str,
            pvc_name: str
        ) -> V1Pod:
            ...

    def get_pod_by_label(self, label_selector: str, namespace: str) -> Optional[V1Pod]:
        ...

    def get_pod(self, namespace: str, name: str) -> Optional[V1Pod]:
        ...

    def list_pods(self, namespace: str) -> List[V1Pod]:
        ...

    def delete_pod(self, namespace: str, name: str) -> None:
        ...

    def create_pod_by_vault(
            self,
            run_id: str,
            image: str,
            envs: Dict[str, str],
            gpu: str,
            mount_path: Optional[str],
            database: str,
            namespace: str,
            pvc_name: str,
            vault_role: str) -> None:
        ...

    def get_pod_by_helm_release(self, release_name: str, namespace: str) -> Optional[V1Pod]:
        ...

    def get_pod_ip(self, namespace: str, pod_name_prefix: str) -> Optional[str]:
        ...
    
    def read_namespaced_pod_log(self, name: str, namespace: str, container: str, exec_command: List[str]) -> str:
        ...