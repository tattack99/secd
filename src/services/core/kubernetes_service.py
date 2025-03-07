import base64
import datetime
import os
import time
from kubernetes.client.rest import ApiException
from kubernetes import client, config
from typing import Dict, List, Optional
from app.src.util.setup import get_settings
from app.src.util.logger import log

class KubernetesService:
    def __init__(self) -> None:
        self.config_path = get_settings()['k8s']['configPath']
        self.config = client.Configuration()
        
        # Load kubeconfig into the custom configuration
        config.load_kube_config(config_file=self.config_path, client_configuration=self.config)
        
        # Set the correct host (adjust based on your setup)
        self.config.host = "https://192.168.0.11:16443"
        self.config.verify_ssl = False
        
        # Pass the configuration to the API client
        api_client = client.ApiClient(configuration=self.config)
        self.v1 = client.CoreV1Api(api_client=api_client)
        self.apps_v1 = client.AppsV1Api(api_client=api_client)

    def get_secret(self, namespace, secret_name, key):
        #log(f"Fetching secret {secret_name} in namespace {namespace}")
        try:
            secret = self.v1.read_namespaced_secret(secret_name, namespace)
            secret_value = secret.data[key]
            return base64.b64decode(secret_value).decode('utf-8')
        except ApiException as e:
            log(f"Failed to get secret {secret_name} in namespace {namespace}: {e}", "ERROR")
            raise Exception(f"Failed to get secret {secret_name} in namespace {namespace}: {e}")

    def get_pod(self, pod_name: str, namespace: str):
        try:
            return self.v1.read_namespaced_pod(pod_name, namespace=namespace)
        except ApiException as e:
            log(f"Failed to get pod {pod_name} in namespace {namespace}: {e}", "ERROR")
            raise Exception(f"Failed to get pod {pod_name} in namespace {namespace}: {e}")

    def get_pvc(self, pvc_name: str, namespace: str):
        try:
            return self.v1.read_namespaced_persistent_volume_claim(pvc_name, namespace=namespace)
        except ApiException as e:
            log(f"Failed to get PVC {pvc_name} in namespace {namespace}: {e}", "ERROR")
            raise Exception(f"Failed to get PVC {pvc_name} in namespace {namespace}: {e}")

    def get_pv(self, pv_name: str):
        try:
            return self.v1.read_persistent_volume(pv_name)
        except ApiException as e:
            log(f"Failed to get PV {pv_name}: {e}", "ERROR")
            raise Exception(f"Failed to get PV {pv_name}: {e}")

    def get_nfs_path(self, pod_name : str):
        pod = self.get_pod(pod_name,"storage")
        for volume in pod.spec.volumes:
            if volume.persistent_volume_claim:
                pvc_name = volume.persistent_volume_claim.claim_name
                pvc = self.get_pvc(pvc_name,"storage")
                pv = self.get_pv(pvc.spec.volume_name)
                if pv.spec.nfs:
                    return pv.spec.nfs.path
        return None

    def get_pod_by_helm_release(self, release_name: str, namespace: str):
        try:
            log(f"Searching for pod with Helm release name '{release_name}' in namespace '{namespace}'.")

            # List all pods in the specified namespace
            pods = self.v1.list_namespaced_pod(namespace=namespace)
            #log(f"Found {len(pods.items)} pods in namespace '{namespace}'.")

            for pod in pods.items:
                # Access the labels dictionary
                labels = pod.metadata.labels or {}
                #log(f"Pod '{pod.metadata.name}' has labels: {labels}")

                # Check if the pod has a label indicating it's part of the release
                release_label = labels.get('release')  # Safely access the 'release' label

                if release_label == release_name:
                    pod_name = pod.metadata.name
                    log(f"Pod '{pod_name}' matches Helm release '{release_name}'.")
                    return pod
                
            return None  # Return None if no matching pod is found

        except Exception as e:
            log(f"Failed to get pod for Helm release '{release_name}' in namespace '{namespace}': {e}", "ERROR")
            raise Exception(f"Failed to get pod for Helm release '{release_name}' in namespace '{namespace}': {e}")

    def get_service_by_helm_release(self, release_name: str, namespace: str):
        try:

            # List all services in the specified namespace
            services = self.v1.list_namespaced_service(namespace=namespace)

            for service in services.items:
                # Access the labels dictionary
                labels = service.metadata.labels or {}
                #log(f"Service '{service.metadata.name}' has labels: {labels}")

                # Check if the service has a label indicating it's part of the release
                release_label = labels.get('release')  # Safely access the 'release' label

                if release_label == release_name:
                    service_name = service.metadata.name
                    log(f"Service '{service_name}' matches Helm release '{release_name}'.")

                    # Construct the FQDN for the service
                    service_fqdn = f"{service_name}.{namespace}.svc.cluster.local"
                    return service_fqdn

            log(f"No service found for Helm release '{release_name}' in namespace '{namespace}'.", "WARNING")
            return None  # Return None if no matching service is found

        except Exception as e:
            log(f"Failed to get service for Helm release '{release_name}' in namespace '{namespace}': {e}", "ERROR")
            raise Exception(f"Failed to get service for Helm release '{release_name}' in namespace '{namespace}': {e}")


    def get_pod_by_name(self, namespace: str, pod_name: str):
        #log(f"Fetching pod '{pod_name}' in namespace '{namespace}'")
        try:
            pod = self.v1.read_namespaced_pod(pod_name, namespace)
            #log(f"Pod '{pod_name}' fetched successfully.")
            return pod
        except client.exceptions.ApiException as e:
            error_message = f"Exception when calling CoreV1Api->read_namespaced_pod: {str(e)}"
            log(error_message)
            raise RuntimeError(error_message)

    def get_pod_in_namespace(self, namespace: str) -> List[str]:
        pods = self.v1.list_namespaced_pod(namespace)
        return [pod.metadata.name for pod in pods.items]

    def get_pod_ip_in_namespace(self, namespace, pod_name_prefix):
        pods = self.v1.list_namespaced_pod(namespace)
        for pod in pods.items:
            if pod_name_prefix in pod.metadata.name:
                return pod.status.pod_ip
        return None

    def handle_cache_dir(self, run_meta, keycloak_user_id, run_id):
        cache_dir = None
        mount_path = None
        if "cache_dir" in run_meta and run_meta["cache_dir"]:
            mount_path = run_meta.get('mount_path', '/cache')

            cache_dir = run_meta['cache_dir']
            cache_path = f"{get_settings()['path']['cachePath']}/{keycloak_user_id}/{cache_dir}"

            if not os.path.exists(cache_path):
                os.makedirs(cache_path)
                log(f"Cache directory created at: {cache_path}")

            pvc_repo_path = get_settings()['k8s']['pvcPath']
            self.create_persistent_volume(run_id, f'{pvc_repo_path}/cache/{keycloak_user_id}/{cache_dir}', "cache")
        return cache_dir, mount_path

    def create_namespace(self, user_id: str, run_id: str, run_for: int):
        run_until = datetime.datetime.now() + datetime.timedelta(hours=run_for)
        namespace_name = f"secd-{run_id}"

        namespace = client.V1Namespace(
            metadata=client.V1ObjectMeta(
                name=namespace_name,
                labels={
                    "access": "database-access"  # Label for network policy
                },
                annotations={
                    "userid": user_id,
                    "rununtil": run_until.isoformat(),
                }
            )
        )

        self.v1.create_namespace(body=namespace)
        log(f"Namespace {namespace_name} created")

    def create_persistent_volume_claim(self, pvc_name, namespace, release_name, storage_size="100Gi"):
        pvc_manifest = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {
                "name": pvc_name,
                "namespace": namespace
            },
            "spec": {
                "accessModes": ["ReadOnlyMany"],  # Set read-only for executing pods
                "resources": {
                    "requests": {
                        "storage": storage_size
                    }
                },
                "storageClassName": "nfs",  # Ensure it matches the PV storage class
                "volumeName": f"pv-storage-{release_name}"  # Bind to the existing PV
            }
        }
        self.v1.create_namespaced_persistent_volume_claim(namespace=namespace, body=pvc_manifest)
        log(f"PVC {pvc_name} created in namespace {namespace}")



    def _create_volume(self, volume_name: str, claim_name: str) -> client.V1Volume:
        try:
            if not volume_name:
                raise ValueError("Volume name cannot be empty")
            if not claim_name:
                raise ValueError("Claim name cannot be empty")

            volume = client.V1Volume(
                name=volume_name,
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=claim_name,
                    read_only=True
                )
            )
            return volume
        except Exception as e:
            log(f"Error creating volume: {str(e)}", "ERROR")
            raise

    def _create_mount(self, volume_name: str, mount_path: str, read_only: bool = False) -> client.V1VolumeMount:
        try:
            if not volume_name:
                raise ValueError("Volume name cannot be empty")
            if not mount_path:
                raise ValueError("Mount path cannot be empty")

            mount = client.V1VolumeMount(
                name=volume_name,
                mount_path=mount_path,
                read_only=read_only
            )
            return mount
        except Exception as e:
            log(f"Error creating volume mount: {str(e)}", "ERROR")
            raise

    def _create_pod_spec(self, volumes: List[client.V1Volume], containers: List[client.V1Container]) -> client.V1PodSpec:
        return client.V1PodSpec(
            volumes=volumes,
            containers=containers,
            restart_policy="Never"
        )

    def _create_container(self, name: str, image: str, envs: List[client.V1EnvVar], volume_mounts: List[client.V1VolumeMount], resources: client.V1ResourceRequirements) -> client.V1Container:
        return client.V1Container(
            name=name,
            image=image,
            env=envs,
            volume_mounts=volume_mounts,
            resources=resources
        )

    def _create_pod_object(self, pod_name: str, labels: Dict[str, str], pod_spec: client.V1PodSpec) -> client.V1Pod:
        return client.V1Pod(
            metadata=client.V1ObjectMeta(name=pod_name, labels=labels),
            spec=pod_spec
        )

    def _set_resources(self, gpu: int = 0, cpu: float = 0.0, memory: float = 0.0) -> tuple[(client.V1ResourceRequirements, Dict[str, str])]:
        resources = client.V1ResourceRequirements()
        labels = {}

        if gpu > 0:
            resources.limits = resources.limits or {}
            resources.requests = resources.requests or {}
            resources.limits["nvidia.com/gpu"] = str(gpu)
            resources.requests["nvidia.com/gpu"] = str(gpu)
            labels["gpu"] = "true"

        if cpu > 0:
            resources.limits = resources.limits or {}
            resources.requests = resources.requests or {}
            resources.limits["cpu"] = str(cpu)
            resources.requests["cpu"] = str(cpu)
            labels["cpu"] = str(cpu)

        if memory > 0:
            resources.limits = resources.limits or {}
            resources.requests = resources.requests or {}
            resources.limits["memory"] = f"{memory}Gi"
            resources.requests["memory"] = f"{memory}Gi"
            labels["memory"] = f"{memory}Gi"

        return resources, labels


    def create_pod_v1(
        self,
        run_id: str,
        namespace: str,
        image: str,
        envs: Dict[str, str],
        gpu: str = "",
        mount_path: Optional[str] = None,
        database: str = "",
        pvc_name: str = "pvc-storage-nfs-1",
    ) -> None:
        try:
            pod_name: str = f"secd-{run_id}"
            k8s_envs: List[client.V1EnvVar] = [client.V1EnvVar(name=key, value=value) for key, value in envs.items()]
            resources: client.V1ResourceRequirements = client.V1ResourceRequirements()
            labels: Dict[str, str] = {
                "access": f"{database}",  # Ensure the label matches the network policy
                "run_id": run_id
            }
            volumes: List[client.V1Volume] = []
            volume_mounts: List[client.V1VolumeMount] = []

            # Use the specified PVC for NFS storage
            volumes.append(self._create_volume(pvc_name, pvc_name))
            volume_mounts.append(self._create_mount(pvc_name, "/data", read_only=True))  # Set read-only if needed

            # Create additional volumes and mounts for output and cache directories
            volumes.append(self._create_volume(f'vol-{run_id}-output', f'secd-pvc-{run_id}-output'))
            volume_mounts.append(self._create_mount(f'vol-{run_id}-output', '/output'))

            if mount_path:
                volumes.append(self._create_volume(f'vol-{run_id}-cache', f'secd-pvc-{run_id}-cache'))
                volume_mounts.append(self._create_mount(f'vol-{run_id}-cache', mount_path))

            if gpu:
                resources, gpu_labels = self._set_resources(gpu=1)
                labels.update(gpu_labels)

            container = self._create_container(pod_name, image, k8s_envs, volume_mounts, resources)
            pod_spec = self._create_pod_spec(volumes, [container])
            pod = self._create_pod_object(pod_name, labels, pod_spec)

            # Create the pod in the specified namespace
            self.v1.create_namespaced_pod(namespace=namespace, body=pod)
            log(f"Pod {pod_name} created in namespace {namespace}")

        except Exception as e:
            log(f"Error creating pod: {str(e)}", "ERROR")
            raise Exception(f"Error creating pod: {e}")



    def create_pod(self, run_id: str, image: str, envs: Dict[str, str], gpu, mount_path):
        v1 = self.v1
        pod_name = f"secd-{run_id}"

        #log(f"Creating pod: {pod_name} with image: {image}")

        k8s_envs = [client.V1EnvVar(name=key, value=value) for key, value in envs.items()]

        resources = client.V1ResourceRequirements()
        labels = {}

        if gpu:
            resources.limits = {"nvidia.com/gpu": 1}
            resources.requests = {"nvidia.com/gpu": 1}
            labels["gpu"] = "true"

        volumes = [
            client.V1Volume(
                name=f'vol-{run_id}-output',
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=f'secd-pvc-{run_id}-output'
                )
            )
        ]

        volume_mounts = [client.V1VolumeMount(
            name=f'vol-{run_id}-output',
            mount_path='/output'
        )]

        if mount_path:
            volumes.append(
                client.V1Volume(
                    name=f'vol-{run_id}-cache',
                    persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                        claim_name=f'secd-pvc-{run_id}-cache'
                    )
                )
            )
            volume_mounts.append(
                client.V1VolumeMount(
                    name=f'vol-{run_id}-cache',
                    mount_path=mount_path
                )
            )

        pod_spec = client.V1PodSpec(
            volumes=volumes,
            containers=[
                client.V1Container(
                    name=pod_name,
                    image=image,
                    env=k8s_envs,
                    volume_mounts=volume_mounts,
                    resources=resources
                )
            ],
            restart_policy="Never"
        )

        pod = client.V1Pod(
            metadata=client.V1ObjectMeta(name=pod_name, labels=labels),
            spec=pod_spec
        )

        v1.create_namespaced_pod(namespace=f"secd-{run_id}", body=pod)
        log(f"Pod {pod_name} created in namespace secd-{run_id}")

    def create_persistent_volume(self, run_id: str, path: str, type: str = "output"):
        v1 = self.v1
        pv_name = f'secd-{run_id}-{type}'
        pvc_name = f'secd-pvc-{run_id}-{type}'

        #log(f"Creating persistent volume: {pv_name} with path: {path}")

        pv = client.V1PersistentVolume(
            metadata=client.V1ObjectMeta(name=pv_name),
            spec=client.V1PersistentVolumeSpec(
                access_modes=["ReadWriteOnce"],
                capacity={"storage": "50Gi"},
                nfs=client.V1NFSVolumeSource(path=path, server='nfs.secd'),
                storage_class_name="nfs",
                persistent_volume_reclaim_policy="Retain",
                volume_mode="Filesystem"
            )
        )

        v1.create_persistent_volume(body=pv)
        log(f"PV {pv_name} created")

        pvc = client.V1PersistentVolumeClaim(
            metadata=client.V1ObjectMeta(name=pvc_name),
            spec=client.V1PersistentVolumeClaimSpec(
                access_modes=["ReadWriteOnce"],
                resources=client.V1ResourceRequirements(requests={"storage": "50Gi"}),
                storage_class_name="nfs",
                volume_name=pv_name,
                volume_mode="Filesystem"
            )
        )

        v1.create_namespaced_persistent_volume_claim(body=pvc, namespace=f"secd-{run_id}")
        log(f"PVC {pvc_name} created in namespace secd-{run_id}")

    def get_pv_by_helm_release(self, release_name: str):
        try:
            # List all persistent volumes
            pvs = self.v1.list_persistent_volume()

            # Iterate over each PV to find the one with the matching release label
            for pv in pvs.items:
                labels = pv.metadata.labels

                # Check if labels exist before accessing them
                if labels is not None:
                    # Check if the release label matches the specified release name
                    if labels.get('release') == release_name:
                        log(f"Found PV for release '{release_name}': {pv.metadata.name}")
                        return pv

            # Log a warning if no matching PV is found
            log(f"No PV found with release name '{release_name}'", "WARNING")
            return None

        except Exception as e:
            # Log and raise exception if API call fails
            log(f"Failed to get PV by Helm release '{release_name}': {e}", "ERROR")
            raise Exception(f"Failed to get PV by Helm release '{release_name}': {e}")


    def cleanup_resources(self) -> List[str]:
        log("Cleaning up resources...")
        run_ids = []

        log("Listing namespaces...")
        namespaces = self.v1.list_namespace()
        #log(f"Found {len(namespaces.items)} namespaces")

        for namespace in namespaces.items:
            if self._should_cleanup_namespace(namespace):
                run_id = self._cleanup_namespace(namespace)
                run_ids.append(run_id)

        return run_ids

    def _should_cleanup_namespace(self, namespace) -> bool:
        annotations = namespace.metadata.annotations
        if annotations is None or 'rununtil' not in annotations:
            return False

        expired = self._is_namespace_expired(annotations['rununtil'])
        completed = self._is_pod_completed(namespace.metadata.name)

        return expired or completed

    def _is_namespace_expired(self, rununtil: str) -> bool:
        return datetime.datetime.fromisoformat(rununtil) < datetime.datetime.now()

    def _get_pvc_names(self, namespace_name: str) -> List[str]:
        """
        Retrieves all PVC names in the given namespace.
        """
        pvc_name_list = []
        try:
            pvc_list = self.v1.list_namespaced_persistent_volume_claim(namespace=namespace_name)
            for pvc in pvc_list.items:
                pvc_name_list.append(pvc.metadata.name)
        except client.exceptions.ApiException as e:
            log(f"Failed to retrieve PVC names in namespace {namespace_name}: {e}", "ERROR")
        return pvc_name_list

    def _get_pv_name_from_any_pvc(self, namespace_name: str) -> List[str]:
        """
        Retrieves the PV names associated with all PVCs in the given namespace.
        """
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


    def _delete_pvc(self, namespace_name: str):
        try:
            pvc_name_list = []
            pvc_list = self.v1.list_namespaced_persistent_volume_claim(namespace=namespace_name)
            for pvc in pvc_list.items:
                self.v1.delete_namespaced_persistent_volume_claim(name=pvc.metadata.name, namespace=namespace_name)
                log(f"PVC {pvc.metadata.name} deleted in namespace {namespace_name}")
                pvc_name_list.append(pvc.metadata.name)
            return pvc_name_list
        except Exception as e:
            log(f"Failed to delete PVC in namespace {namespace_name}: {e}", "ERROR")

    def _delete_namespace(self, namespace_name: str):
        try:
            self.v1.delete_namespace(name=namespace_name)
            log(f"Namespace {namespace_name} deleted")
        except Exception as e:
            log(f"Failed to delete namespace {namespace_name}: {e}", "ERROR")

    def _delete_persistent_volume(self, namespace_name: str):
        try:
            self.v1.delete_persistent_volume(name=f'{namespace_name}-output')
            log(f"Persistent volume {namespace_name}-output deleted")
        except Exception as e:
            log(f"Failed to delete persistent volume: {e}", "ERROR")

    def _is_pod_completed(self, namespace_name: str) -> bool:
        pod_list = self.v1.list_namespaced_pod(namespace=namespace_name)
        if len(pod_list.items) > 0:
            pod = pod_list.items[0]
            log(f"Pod found in namespace: {namespace_name} with phase: {pod.status.phase}")
            return pod.status.phase == 'Succeeded' or pod.status.phase == 'Failed'
        return False

    def _cleanup_namespace(self, namespace) -> str:
        run_id = namespace.metadata.name.replace("secd-", "")
        log(f"Finishing run {namespace.metadata.name} - Deleting resources")

        self._delete_namespace(namespace.metadata.name)
        #self._delete_persistent_volume(namespace.metadata.name)

        # Get lists of PVCs and PVs
        pvc_name_list = self._get_pvc_names(namespace.metadata.name)
        pv_name_list = self._get_pv_name_from_any_pvc(namespace.metadata.name)

        if pv_name_list:
            self._wait_for_pvc_deletion(namespace.metadata.name, pvc_name_list)
            self._make_pv_available(pv_name_list)

        return run_id
