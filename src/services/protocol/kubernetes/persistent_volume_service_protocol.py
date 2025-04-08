from typing import Protocol, List, Optional
from kubernetes.client import V1PersistentVolume, V1PersistentVolumeClaim
from kubernetes import client

class PersistentVolumeServiceProtocol(Protocol):

    # CRUD methods
    def create_persistent_volume(
        self,
        name: str,
        path: str,
        capacity: str = "50Gi",
        access_modes: List[str] = ["ReadWriteOnce"]
    ) -> V1PersistentVolume:
        ...

    def get_persistent_volume(self, name: str) -> Optional[V1PersistentVolume]:
        ...

    def get_pvc(self, name: str, namespace: str) -> Optional[V1PersistentVolumeClaim]:
        ...

    def delete_persistent_volume(self, name: str) -> None:
        ...

    def create_persistent_volume_claim(
        self,
        pvc_name: str,
        namespace: str,
        volume_name: str,  # Changed from release_name
        storage_size: str = "100Gi",
        access_modes: Optional[List[str]] = None,
        storage_class_name: str = "nfs"
    ) -> V1PersistentVolumeClaim:
        ...

    def get_persistent_volume_claim(self, namespace: str, name: str) -> Optional[V1PersistentVolumeClaim]:
        ...

    def delete_persistent_volume_claim(self, namespace: str, name: str) -> None:
        ...

    def cleanup_persistent_volumes(self, namespaces: List[client.V1Namespace]) -> None:
        ...

    def get_pv_by_helm_release(self, release_name: str) -> Optional[V1PersistentVolume]:
        ...