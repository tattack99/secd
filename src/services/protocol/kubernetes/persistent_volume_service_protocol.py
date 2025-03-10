from typing import Protocol, List, Optional, Dict
from kubernetes.client import V1PersistentVolume, V1PersistentVolumeClaim
from kubernetes import client

class PersistentVolumeServiceProtocol(Protocol):
    def create_persistent_volume(
            self,
            name: str,
            path: str,
            capacity: str = "50Gi",
            access_modes: List[str] = ["ReadWriteOnce"]
        ) -> client.V1PersistentVolume:
            ...

    def get_persistent_volume(self, name: str) -> Optional[V1PersistentVolume]:
        """Retrieve a persistent volume by name."""
        ...

    def delete_persistent_volume(self, name: str) -> None:
        """Delete a persistent volume."""
        ...

    def create_persistent_volume_claim(
            self,
            pvc_name: str,
            namespace: str,
            release_name: str,
            storage_size: str = "100Gi",
            access_modes: Optional[List[str]] = None,
            storage_class_name: str = "nfs"
        ) -> V1PersistentVolumeClaim:
            """Create a persistent volume claim with the specified parameters.

            Args:
                pvc_name (str): The name of the PVC.
                namespace (str): The namespace where the PVC will be created.
                release_name (str): The release name used to derive the volumeName.
                storage_size (str, optional): The requested storage size. Defaults to "100Gi".
                access_modes (Optional[List[str]], optional): The access modes for the PVC. Defaults to ["ReadOnlyMany"].
                storage_class_name (str, optional): The storage class name. Defaults to "nfs".

            Returns:
                V1PersistentVolumeClaim: The created PVC object.
            """
            ...

    def get_persistent_volume_claim(self, namespace: str, name: str) -> Optional[V1PersistentVolumeClaim]:
        """Retrieve a persistent volume claim."""
        ...

    def delete_persistent_volume_claim(self, namespace: str, name: str) -> None:
        """Delete a persistent volume claim."""
        ...

    def get_pv_by_helm_release(self, release_name: str) -> Optional[V1PersistentVolume]:
        """Find a persistent volume by Helm release name."""
        ...

    def cleanup_persistent_volumes(self, namespaces):
        """Clean up persistent volumes."""
        ...