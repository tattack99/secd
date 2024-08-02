import base64
import datetime
import os
from kubernetes.client.rest import ApiException
from kubernetes import client, config
from typing import Dict, List, Optional
from secure.src.util.setup import get_settings
from secure.src.util.logger import log

class KubernetesService:
    def __init__(self) -> None:
        self.config_path = get_settings()['k8s']['configPath']
        config.load_kube_config(self.config_path)
        self.v1 = client.CoreV1Api()

    def get_secret(self, namespace, secret_name, key):
        log(f"Fetching secret {secret_name} in namespace {namespace}")
        v1 = self.v1

        secret = v1.read_namespaced_secret(secret_name, namespace)
        secret_value = secret.data[key]

        return base64.b64decode(secret_value).decode('utf-8')

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

    def get_nfs_path(self, pod_name : str) -> str:
        pod = self.get_pod(pod_name,"storage")
        for volume in pod.spec.volumes:
            if volume.persistent_volume_claim:
                pvc_name = volume.persistent_volume_claim.claim_name
                pvc = self.get_pvc(pvc_name,"storage")
                pv = self.get_pv(pvc.spec.volume_name)
                if pv.spec.nfs:
                    return pv.spec.nfs.path
        return "None"


    def get_pod_by_name(self, namespace: str, pod_name: str):
        log(f"Fetching pod '{pod_name}' in namespace '{namespace}'")
        try:
            pod = self.v1.read_namespaced_pod(pod_name, namespace)
            log(f"Pod '{pod_name}' fetched successfully.")
            return pod
        except client.exceptions.ApiException as e:
            error_message = f"Exception when calling CoreV1Api->read_namespaced_pod: {str(e)}"
            log(error_message)
            raise RuntimeError(error_message)

    def get_pod_in_namespace(self, namespace: str) -> List[str]:
        v1 = self.v1
        pods = v1.list_namespaced_pod(namespace)
        return [pod.metadata.name for pod in pods.items]

    def get_pod_ip_in_namespace(self, namespace, pod_name_prefix) -> str:
        pods = self.v1.list_namespaced_pod(namespace)
        for pod in pods.items:
            if pod_name_prefix in pod.metadata.name:
                return pod.status.pod_ip
        return ""

    def handle_cache_dir(self, run_meta, keycloak_user_id, run_id):
        log(f"Handling cache directory for run: {run_id}")
        cache_dir = None
        mount_path = None
        if "cache_dir" in run_meta and run_meta["cache_dir"]:
            mount_path = run_meta.get('mount_path', '/cache')
            log(f"Found custom mount_path: {mount_path}" if 'mount_path' in run_meta else "Using default mount_path: /cache")

            cache_dir = run_meta['cache_dir']
            cache_path = f"{get_settings()['path']['cachePath']}/{keycloak_user_id}/{cache_dir}"
            log(f"Found cache_dir: {cache_path}")

            if not os.path.exists(cache_path):
                os.makedirs(cache_path)
                log(f"Cache directory created at: {cache_path}")

            pvc_repo_path = get_settings()['k8s']['pvcPath']
            self.create_persistent_volume(run_id, f'{pvc_repo_path}/cache/{keycloak_user_id}/{cache_dir}', "cache")
            log(f"Cache PVC created for {run_id}")
        return cache_dir, mount_path

    def create_namespace(self, user_id: str, run_id: str, run_for: int):
        v1 = self.v1
        run_until = datetime.datetime.now() + datetime.timedelta(hours=run_for)
        namespace_name = f"secd-{run_id}"

        log(f"Creating namespace: {namespace_name} for user: {user_id} until: {run_until}")

        # Define the namespace object with labels
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

        # Create the namespace in the cluster
        v1.create_namespace(body=namespace)
        log(f"Namespace {namespace_name} created with label 'access=database-access'")


    def _create_volume(self, volume_name: str, claim_name: str) -> client.V1Volume:
        try:
            if not volume_name:
                raise ValueError("Volume name cannot be empty")
            if not claim_name:
                raise ValueError("Claim name cannot be empty")

            volume = client.V1Volume(
                name=volume_name,
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=claim_name
                )
            )
            return volume
        except Exception as e:
            log(f"Error creating volume: {str(e)}", "ERROR")
            raise

    def _create_mount(self, volume_name: str, mount_path: str) -> client.V1VolumeMount:
        try:
            if not volume_name:
                raise ValueError("Volume name cannot be empty")
            if not mount_path:
                raise ValueError("Mount path cannot be empty")

            mount = client.V1VolumeMount(
                name=volume_name,
                mount_path=mount_path
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


    def create_pod_v1(self, run_id: str, image: str, envs: Dict[str, str], gpu: str = "", mount_path: Optional[str] = None, database: str = "") -> None:
        try:
            pod_name: str = f"secd-{run_id}"
            k8s_envs: List[client.V1EnvVar] = [client.V1EnvVar(name=key, value=value) for key, value in envs.items()]
            resources: client.V1ResourceRequirements = client.V1ResourceRequirements()
            labels: Dict[str, str] = {
                "access": f"{database}"  # Ensure the label matches the network policy
            }
            volumes: List[client.V1Volume] = []
            volume_mounts: List[client.V1VolumeMount] = []

            log(f"Creating pod: {pod_name} with image: {image}")


            volumes.append(self._create_volume(f'vol-{run_id}-output', f'secd-pvc-{run_id}-output'))
            volume_mounts.append(self._create_mount(f'vol-{run_id}-output', '/output'))

            if mount_path:
                volumes.append(self._create_volume(f'vol-{run_id}-cache', f'secd-pvc-{run_id}-cache'))
                volume_mounts.append(self._create_mount(f'vol-{run_id}-cache', mount_path))


            if gpu:
                resources, gpu_labels = self._set_resources(gpu=1)
                labels.update(gpu_labels)

            containers = [self._create_container(pod_name, image, k8s_envs, volume_mounts, resources)]

            pod_spec = self._create_pod_spec(volumes, containers)

            pod = self._create_pod_object(pod_name, labels, pod_spec)

            self.v1.create_namespaced_pod(namespace=f"secd-{run_id}", body=pod)
            log(f"Pod {pod_name} created in namespace secd-{run_id}")

        except Exception as e:
            log(f"Error creating pod: {str(e)}", "ERROR")
            raise Exception(f"Error creating pod: {e}")


    def create_pod(self, run_id: str, image: str, envs: Dict[str, str], gpu, mount_path):
        v1 = self.v1
        pod_name = f"secd-{run_id}"

        log(f"Creating pod: {pod_name} with image: {image}")

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

        log(f"Creating persistent volume: {pv_name} with path: {path}")

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
        log(f"Persistent volume {pv_name} created")

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
        log(f"Persistent volume claim {pvc_name} created in namespace secd-{run_id}")

    def delete_by_user_id(self, user_id: str) -> List[str]:
        run_ids = []

        log(f"Deleting resources for user: {user_id}")

        namespaces = self.v1.list_namespace()
        for namespace in namespaces.items:
            annotations = namespace.metadata.annotations
            if annotations is None or 'userid' not in annotations:
                continue

            k8s_user_id = annotations.get('userid')
            if user_id == k8s_user_id:
                run_id = namespace.metadata.name.replace("secd-", "")
                log(f"Deleting namespace: {namespace.metadata.name} for user: {user_id}")

                self.v1.delete_namespace(name=namespace.metadata.name)
                self.v1.delete_persistent_volume(name=f'{namespace.metadata.name}-output')

                run_ids.append(run_id)

        log(f"Deleted resources for user: {user_id}, run_ids: {run_ids}")
        return run_ids

    def cleanup_resources(self) -> List[str]:
        run_ids = []

        namespaces = self.v1.list_namespace()
        for namespace in namespaces.items:
            if self._should_cleanup_namespace(namespace):
                run_id = self._cleanup_namespace(namespace)
                run_ids.append(run_id)

        return run_ids

    def _should_cleanup_namespace(self, namespace) -> bool:
        """Determine if a namespace should be cleaned up."""
        annotations = namespace.metadata.annotations
        if annotations is None or 'rununtil' not in annotations:
            return False

        expired = self._is_namespace_expired(annotations['rununtil'])
        completed = self._is_pod_completed(namespace.metadata.name)

        return expired or completed

    def _is_namespace_expired(self, rununtil: str) -> bool:
        """Check if the namespace is expired."""
        return datetime.datetime.fromisoformat(rununtil) < datetime.datetime.now()

    def _is_pod_completed(self, namespace_name: str) -> bool:
        """Check if the pod in the namespace is completed."""
        pod_list = self.v1.list_namespaced_pod(namespace=namespace_name)
        if len(pod_list.items) > 0:
            pod = pod_list.items[0]
            log(f"Pod found in namespace: {namespace_name} with phase: {pod.status.phase}")
            return pod.status.phase == 'Succeeded'
        return False

    def _cleanup_namespace(self, namespace) -> str:
        """Perform the cleanup of the namespace and its associated resources."""
        run_id = namespace.metadata.name.replace("secd-", "")
        log(f"Finishing run {namespace.metadata.name} - Deleting resources")

        self._delete_namespace(namespace.metadata.name)
        self._delete_persistent_volume(namespace.metadata.name)

        return run_id

    def _delete_namespace(self, namespace_name: str):
        """Delete the specified namespace."""
        try:
            self.v1.delete_namespace(name=namespace_name)
            log(f"Namespace {namespace_name} deleted")
        except Exception as e:
            log(f"Failed to delete namespace {namespace_name}: {e}", "ERROR")

    def _delete_persistent_volume(self, namespace_name: str):
        """Attempt to delete the persistent volume associated with the namespace."""
        try:
            log(f"Attempting to delete persistent volume: {namespace_name}-output")
            self.v1.delete_persistent_volume(name=f'{namespace_name}-output')
            log(f"Persistent volume {namespace_name}-output deleted")
        except Exception as e:
            log(f"Failed to delete persistent volume: {e}", "ERROR")
