import datetime
from kubernetes import client, config
from typing import Dict, List

from src.util.setup import get_settings
from src.util.logger import log


def _with_k8s():
    config_path = get_settings()['k8s']['configPath']
    config.load_kube_config(config_path)
    v1 = client.CoreV1Api()
    return v1


def create_namespace(user_id: str, run_id: str, run_for: datetime):
    v1 = _with_k8s()
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


def create_pod(run_id: str, image: str, envs: Dict[str, str], gpu, mount_path):
    v1 = _with_k8s()
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

def create_persistent_volume(run_id: str, path: str, type: str = "output"):
    v1 = _with_k8s()
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

def delete_by_user_id(user_id: str) -> List[str]:
    v1 = _with_k8s()
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

def cleanup_resources() -> List[str]:
    v1 = _with_k8s()
    run_ids = []

    #log("Starting cleanup of resources")

    namespaces = v1.list_namespace()
    for namespace in namespaces.items:
        #log(f"Checking namespace: {namespace.metadata.name}")

        annotations = namespace.metadata.annotations
        if annotations is None or 'rununtil' not in annotations:
            continue

        rununtil = annotations.get('rununtil')
        expired = datetime.datetime.fromisoformat(rununtil) < datetime.datetime.now()
        #log(f"Namespace: {namespace.metadata.name} has 'rununtil': {rununtil}, expired: {expired}")

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

    #log(f"Cleanup completed, run_ids: {run_ids}")
    return run_ids
