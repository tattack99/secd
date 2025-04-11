from kubernetes import client
from app.src.util.setup import get_settings
from app.src.util.logger import log
from app.src.services.protocol.kubernetes_services.pod_service_protocol import PodServiceProtocol
from typing import List, Optional, Dict

class PodService(PodServiceProtocol):
    def __init__(self, config: client.Configuration):   
        api_client = client.ApiClient(configuration=config)
        self.v1 = client.CoreV1Api(api_client=api_client)
    
    def get_pod_by_label(self, label_selector: str, namespace: str) -> Optional[client.V1Pod]:
        try:
            pods = self.v1.list_namespaced_pod(
                namespace=namespace,
                label_selector=label_selector
            ).items
            if pods:
                pod = pods[0]
                #log(f"Pod '{pod.metadata.name}' found with label selector '{label_selector}' in namespace '{namespace}'", "DEBUG")
                return pod
            log(f"No pod found with label selector '{label_selector}' in namespace '{namespace}'", "WARNING")
            return None
        except client.ApiException as e:
            log(f"Failed to get pod with label selector '{label_selector}' in namespace '{namespace}': {str(e)}", "ERROR")
            return None

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
        vault_role: str
    ) -> client.V1Pod:
        try:
            pod_name = f"secd-{run_id}"
            database_name = database 
            service_account_name = f"sa-{database_name}"  # e.g., sa-mysql-1
            k8s_auth_role_name = f"role-{database_name}-{namespace}"  # e.g., role-mysql-1-secd-<run_id>
            db_role_name = f"role-{database_name}"  # e.g., role-mysql-1

            #log(f"database : {database}", "DEBUG")

            # Vault annotations for credential injection
            annotations = {
                "vault.hashicorp.com/agent-inject": "true",
                "vault.hashicorp.com/role": k8s_auth_role_name,
                "vault.hashicorp.com/agent-inject-secret-dbcreds": f"database/creds/{db_role_name}",
                "vault.hashicorp.com/agent-inject-template-dbcreds": f"""
                {{{{- with secret "database/creds/{db_role_name}" -}}}}
                export DB_USER="{{{{ .Data.username }}}}"
                export DB_PASS="{{{{ .Data.password }}}}"
                {{{{- end -}}}}
                """
            }

            k8s_envs = [client.V1EnvVar(name=key, value=value) for key, value in envs.items()]
            resources = client.V1ResourceRequirements()
            labels = {"name": database_name, "run_id": run_id}
            #log(f"labels : {labels}", "DEBUG")
            volumes = []
            volume_mounts = []
            if pvc_name:
                volumes.append(self._create_volume(pvc_name, pvc_name, read_only=True))
                volume_mounts.append(self._create_mount(pvc_name, "/data", read_only=True))
            volumes.append(self._create_volume(f"vol-{run_id}-output", f"secd-pvc-{run_id}-output"))
            volume_mounts.append(self._create_mount(f"vol-{run_id}-output", "/output"))
            if mount_path:
                volumes.append(self._create_volume(f"vol-{run_id}-cache", f"secd-pvc-{run_id}-cache"))
                volume_mounts.append(self._create_mount(f"vol-{run_id}-cache", mount_path))
            if gpu:
                resources, gpu_labels = self._set_resources(gpu=1)
                labels.update(gpu_labels)
            container = self._create_container(
                pod_name, image, k8s_envs, volume_mounts, resources,
                command=["/bin/sh", "-c"],
                args=[". /vault/secrets/dbcreds && env | grep DB_ && python /app/app.py"]
            )
            pod_spec = self._create_pod_spec(volumes, [container], service_account=service_account_name)
            pod = self._create_pod_object(pod_name, labels, pod_spec, annotations=annotations)
            self.v1.create_namespaced_pod(namespace=namespace, body=pod)
            log(f"Pod {pod_name} created in namespace {namespace} with Vault v3 config")
            return pod
        except Exception as e:
            log(f"Error creating pod with Vault v3: {str(e)}", "ERROR")
            raise Exception(f"Error creating pod with Vault v3: {e}")

    # Remaining methods unchanged (read_namespaced_pod_log, _create_volume, etc.)
    def read_namespaced_pod_log(self, name: str, namespace: str, container: str, exec_command: List[str]) -> str:
        try:
            resp = self.v1.read_namespaced_pod_log(
                name=name,
                namespace=namespace,
                container=container,
                exec_command=exec_command
            )
            #log(f"Read logs from pod {name}", "DEBUG")
            return resp.strip()
        except Exception as e:
            log(f"Error reading logs from pod {name}: {str(e)}", "ERROR")
            raise Exception(f"Error reading logs from pod {name}: {e}")

    def _create_volume(self, name: str, claim_name: str, read_only: bool = False) -> client.V1Volume:
        return client.V1Volume(
            name=name,
            persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                claim_name=claim_name,
                read_only=read_only
            )
        )

    def _create_mount(self, name: str, mount_path: str, read_only: bool = False) -> client.V1VolumeMount:
        return client.V1VolumeMount(
            name=name,
            mount_path=mount_path,
            read_only=read_only
        )

    def _set_resources(self, gpu: int) -> tuple[client.V1ResourceRequirements, Dict[str, str]]:
        resources = client.V1ResourceRequirements(
            limits={"nvidia.com/gpu": str(gpu)},
            requests={"nvidia.com/gpu": str(gpu)}
        )
        labels = {"gpu": "true"}
        return resources, labels

    def _create_container(
        self,
        name: str,
        image: str,
        envs: List[client.V1EnvVar],
        mounts: List[client.V1VolumeMount],
        resources: client.V1ResourceRequirements,
        command: Optional[List[str]] = None,
        args: Optional[List[str]] = None
    ) -> client.V1Container:
        return client.V1Container(
            name=name,
            image=image,
            env=envs,
            volume_mounts=mounts,
            resources=resources,
            command=command,
            args=args
        )

    def _create_pod_spec(self, volumes: List[client.V1Volume], containers: List[client.V1Container], service_account: str = "") -> client.V1PodSpec:
        return client.V1PodSpec(
            service_account=service_account,
            volumes=volumes,
            containers=containers,
            restart_policy="Never"
        )

    def _create_pod_object(self, name: str, labels: Dict[str, str], spec: client.V1PodSpec, annotations: Optional[Dict[str, str]] = None) -> client.V1Pod:
        return client.V1Pod(
            metadata=client.V1ObjectMeta(
                name=name, 
                labels=labels, 
                annotations=annotations),
            spec=spec
        )

    def get_pod(self, namespace: str, name: str) -> Optional[client.V1Pod]:
        try:
            return self.v1.read_namespaced_pod(name, namespace)
        except client.ApiException as e:
            log(f"Failed to get pod {name} in namespace {namespace}: {e}", "ERROR")
            return None

    def list_pods(self, namespace: str) -> List[client.V1Pod]:
        return self.v1.list_namespaced_pod(namespace).items

    def delete_pod(self, namespace: str, name: str) -> None:
        try:
            self.v1.delete_namespaced_pod(name, namespace)
            log(f"Pod {name} deleted in namespace {namespace}")
        except client.ApiException as e:
            log(f"Failed to delete pod {name} in namespace {namespace}: {e}", "ERROR")

    def get_pod_by_helm_release(self, release_name: str, namespace: str) -> Optional[client.V1Pod]:
        pods = self.list_pods(namespace)
        for pod in pods:
            labels = pod.metadata.labels or {}
            if labels.get('release') == release_name:
                log(f"Pod '{pod.metadata.name}' matches Helm release '{release_name}'.")
                return pod
        return None

    def get_pod_ip(self, namespace: str, pod_name_prefix: str) -> Optional[str]:
        pods = self.list_pods(namespace)
        for pod in pods:
            if pod_name_prefix in pod.metadata.name:
                return pod.status.pod_ip
        return None