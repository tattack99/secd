import base64
import datetime
import os
from kubernetes import client, config
from typing import Dict, List

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

        # Fetch the secret
        secret = v1.read_namespaced_secret(secret_name, namespace)

        # Decode the secret value
        secret_value = secret.data[key]

        return base64.b64decode(secret_value).decode('utf-8')

    def get_pod_details(self, namespace: str, pod_name: str):
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

    def get_pod_ip_in_namespace(self, namespace, pod_name_prefix):
        v1 = self.v1
        pods = v1.list_namespaced_pod(namespace)
        for pod in pods.items:
            if pod_name_prefix in pod.metadata.name:
                return pod.status.pod_ip
        return None

    def handle_cache_dir(self, run_meta, keycloak_user_id, run_id):
        log(f"Handling cache directory for run: {run_id}")
        cache_dir = mount_path = None
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

    def create_namespace(self, user_id: str, run_id: str, run_for: datetime):
        v1 = self.v1
        run_until = datetime.datetime.now() + datetime.timedelta(hours=run_for)
        namespace_name = f"secd-{run_id}"

        log(f"Creating namespace: {namespace_name} for user: {user_id} until: {run_until}")

        namespace = client.V1Namespace()
        namespace.metadata = client.V1ObjectMeta(
            name=namespace_name,
            annotations={
                "userid": user_id,
                "rununtil": run_until.isoformat(),
            }
        )
        v1.create_namespace(body=namespace)
        log(f"Namespace {namespace_name} created")


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
        v1 = self.v1
        run_ids = []

        log(f"Deleting resources for user: {user_id}")

        namespaces = v1.list_namespace()
        for namespace in namespaces.items:
            annotations = namespace.metadata.annotations
            if annotations is None or 'userid' not in annotations:
                continue

            k8s_user_id = annotations.get('userid')
            if user_id == k8s_user_id:
                run_id = namespace.metadata.name.replace("secd-", "")
                log(f"Deleting namespace: {namespace.metadata.name} for user: {user_id}")

                v1.delete_namespace(name=namespace.metadata.name)
                v1.delete_persistent_volume(name=f'{namespace.metadata.name}-output')

                run_ids.append(run_id)

        log(f"Deleted resources for user: {user_id}, run_ids: {run_ids}")
        return run_ids

    def cleanup_resources(self) -> List[str]:
        v1 = self.v1
        run_ids = []

        namespaces = v1.list_namespace()
        for namespace in namespaces.items:

            annotations = namespace.metadata.annotations
            if annotations is None or 'rununtil' not in annotations:
                continue

            rununtil = annotations.get('rununtil')
            expired = datetime.datetime.fromisoformat(rununtil) < datetime.datetime.now()

            completed = False
            pod_list = v1.list_namespaced_pod(namespace=namespace.metadata.name)
            if len(pod_list.items) > 0:
                pod = pod_list.items[0]
                log(f"Pod found in namespace: {namespace.metadata.name} with phase: {pod.status.phase}")
                if pod.status.phase == 'Succeeded':
                    completed = True

            if expired or completed:
                run_id = namespace.metadata.name.replace("secd-", "")
                log(f"Finishing run {namespace.metadata.name} - expired rununtil {rununtil} - Deleting resources, expired:{expired}, completed: {completed}")
                v1.delete_namespace(name=namespace.metadata.name)
                log(f"Namespace {namespace.metadata.name} deleted")
                try:
                    log(f"Attempting to delete persistent volume: {namespace.metadata.name}-output")
                    v1.delete_persistent_volume(name=f'{namespace.metadata.name}-output')
                    log(f"Persistent volume {namespace.metadata.name}-output deleted")
                except Exception as e:
                    log(f"Failed to delete persistent volume: {e}", "ERROR")

                run_ids.append(run_id)

        return run_ids
