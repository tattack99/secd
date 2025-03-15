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
            """Create a pod with the specified configuration.

            Args:
                run_id (str): The unique run identifier used to name the pod and related resources.
                image (str): The Docker image to use for the pod.
                envs (Dict[str, str]): Environment variables for the pod.
                gpu (str): The GPU requirement (e.g., "true" or specific GPU type).
                mount_path (Optional[str]): The mount path for an optional cache volume.
                database (str): The database label for network policy access.
                namespace (str): The Kubernetes namespace where the pod will be created.
                pvc_name (str): The name of the Persistent Volume Claim (PVC) for NFS storage.

            Returns:
                V1Pod: The created pod object.
            """
            ...

    def get_pod(self, namespace: str, name: str) -> Optional[V1Pod]:
        """Retrieve a pod by name and namespace."""
        ...

    def list_pods(self, namespace: str) -> List[V1Pod]:
        """List all pods in a namespace."""
        ...

    def delete_pod(self, namespace: str, name: str) -> None:
        """Delete a pod by name and namespace."""
        ...

    def get_pod_by_helm_release(self, release_name: str, namespace: str) -> Optional[V1Pod]:
        """Find a pod by Helm release name."""
        ...

    def get_pod_ip(self, namespace: str, pod_name_prefix: str) -> Optional[str]:
        """Get the IP address of a pod matching a name prefix."""
        ...
    
    def read_namespaced_pod_log(self, name: str, namespace: str, container: str, exec_command: List[str]) -> str:
        """Read logs from a pod."""
        ...