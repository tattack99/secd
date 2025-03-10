import time
from kubernetes import client, config
from app.src.util.setup import get_settings
from app.src.util.logger import log
from app.src.services.protocol.kubernetes.persistent_volume_service_protocol import PersistentVolumeServiceProtocol
from typing import List, Optional, Dict

class PersistentVolumeServiceV1(PersistentVolumeServiceProtocol):
    def __init__(self, config: client.Configuration):   
        api_client = client.ApiClient(configuration=config)
        self.v1 = client.CoreV1Api(api_client=api_client)

    def cleanup_persistent_volumes(self, namespaces):
        for namespace in namespaces:
            pvc_name_list = self._get_pvc_names(namespace.metadata.name)
            log(f"Number of PVCs in namespace {namespace.metadata.name}: {len(pvc_name_list)}")
            pv_name_list = self._get_pv_name_from_any_pvc(namespace.metadata.name)
            log(f"Number of PVs in namespace {namespace.metadata.name}: {len(pv_name_list)}")

            if pv_name_list:
                self._wait_for_pvc_deletion(namespace.metadata.name, pvc_name_list)
                self._make_pv_available(pv_name_list)



    def create_persistent_volume(
        self,
        name: str,
        path: str,
        capacity: str = "50Gi",
        access_modes: List[str] = ["ReadWriteOnce"]
    ) -> client.V1PersistentVolume:
        pv = client.V1PersistentVolume(
            metadata=client.V1ObjectMeta(name=name),
            spec=client.V1PersistentVolumeSpec(
                access_modes=access_modes,
                capacity={"storage": capacity},
                nfs=client.V1NFSVolumeSource(path=path, server='nfs.secd'),
                storage_class_name="nfs",
                persistent_volume_reclaim_policy="Retain",
                volume_mode="Filesystem",
            )
        )
        self.v1.create_persistent_volume(body=pv)
        log(f"PV {name} created")
        return pv

    def get_persistent_volume(self, name: str) -> Optional[client.V1PersistentVolume]:
        try:
            return self.v1.read_persistent_volume(name)
        except client.ApiException as e:
            log(f"Failed to get PV {name}: {e}", "ERROR")
            return None

    def delete_persistent_volume(self, name: str) -> None:
        try:
            self.v1.delete_persistent_volume(name)
            log(f"PV {name} deleted")
        except client.ApiException as e:
            log(f"Failed to delete PV {name}: {e}", "ERROR")

    def create_persistent_volume_claim(
        self,
        pvc_name: str,
        namespace: str,
        volume_name: str,  # Replace release_name with volume_name
        storage_size: str = "100Gi",
        access_modes: Optional[List[str]] = None,
        storage_class_name: str = "nfs"
    ):
        if access_modes is None:
            access_modes = ["ReadOnlyMany"]
        pvc_manifest = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {
                "name": pvc_name,
                "namespace": namespace
            },
            "spec": {
                "accessModes": access_modes,
                "resources": {
                    "requests": {
                        "storage": storage_size
                    }
                },
                "storageClassName": storage_class_name,
                "volumeName": volume_name  # Use the provided volume_name directly
            }
        }
        self.v1.create_namespaced_persistent_volume_claim(namespace=namespace, body=pvc_manifest)
        log(f"PVC {pvc_name} created in namespace {namespace}")

    def get_persistent_volume_claim(self, namespace: str, name: str) -> Optional[client.V1PersistentVolumeClaim]:
        try:
            return self.v1.read_namespaced_persistent_volume_claim(name, namespace)
        except client.ApiException as e:
            log(f"Failed to get PVC {name} in namespace {namespace}: {e}", "ERROR")
            return None

    def delete_persistent_volume_claim(self, namespace: str, name: str) -> None:
        try:
            self.v1.delete_namespaced_persistent_volume_claim(name, namespace)
            log(f"PVC {name} deleted in namespace {namespace}")
        except client.ApiException as e:
            log(f"Failed to delete PVC {name} in namespace {namespace}: {e}", "ERROR")

    def get_pv_by_helm_release(self, release_name: str) -> Optional[client.V1PersistentVolume]:
        pvs = self.v1.list_persistent_volume().items
        for pv in pvs:
            labels = pv.metadata.labels or {}
            if labels.get('release') == release_name:
                log(f"Found PV for release '{release_name}': {pv.metadata.name}")
                return pv
        return None
    

    def _wait_for_pvc_deletion(self, namespace_name: str, pvc_name_list: List[str], timeout: int = 60):
        start_time = time.time()
        while time.time() - start_time < timeout:
            all_deleted = True
            for pvc_name in pvc_name_list:
                try:
                    self.v1.read_namespaced_persistent_volume_claim(name=pvc_name, namespace=namespace_name)
                    log(f"Waiting for PVC {pvc_name} in namespace {namespace_name} to be deleted...")
                    all_deleted = False
                except client.exceptions.ApiException as e:
                    if e.status == 404:
                        log(f"PVC {pvc_name} in namespace {namespace_name} has been deleted.")
                    else:
                        log(f"Error checking PVC {pvc_name} in namespace {namespace_name}: {e}", "ERROR")
            if all_deleted:
                break
            time.sleep(5)  # Sleep for a short interval before retrying
        if not all_deleted:
            log(f"Timeout waiting for PVC deletion in namespace {namespace_name}.")

    def _make_pv_available(self, pv_name_list: List[str]):
        for pv_name in pv_name_list:
            try:
                pv = self.v1.read_persistent_volume(pv_name)
                if pv.status.phase == "Released":
                    self.v1.patch_persistent_volume(
                        name=pv_name,
                        body={"spec": {"claimRef": None}}
                    )
                    log(f"Persistent Volume {pv_name} is now available.")
                else:
                    log(f"PV {pv_name} is not in Released state. Current state: {pv.status.phase}")
            except client.exceptions.ApiException as e:
                log(f"Error releasing PV {pv_name}: {e}", "ERROR")

    def _get_pv_name_from_any_pvc(self, namespace_name: str) -> List[str]:
        pv_name_list = []
        try:
            # List all PVCs in the namespace
            pvc_list = self.v1.list_namespaced_persistent_volume_claim(namespace=namespace_name)
            for pvc in pvc_list.items:
                pv_name = pvc.spec.volume_name
                if pv_name:
                    log(f"Found PV {pv_name} for PVC {pvc.metadata.name} in namespace {namespace_name}")
                    pv_name_list.append(pv_name)
        except client.exceptions.ApiException as e:
            log(f"Failed to retrieve PV name from any PVC in namespace {namespace_name}: {e}", "ERROR")
        return pv_name_list
    
    def _get_pvc_names(self, namespace_name: str) -> List[str]:
        pvc_name_list = []
        try:
            pvc_list = self.v1.list_namespaced_persistent_volume_claim(namespace=namespace_name)
            for pvc in pvc_list.items:
                pvc_name_list.append(pvc.metadata.name)
        except client.exceptions.ApiException as e:
            log(f"Failed to retrieve PVC names in namespace {namespace_name}: {e}", "ERROR")
        return pvc_name_list
