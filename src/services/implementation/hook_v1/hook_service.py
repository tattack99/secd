import datetime
import os
from typing import Dict, Tuple, Any
import uuid

from app.src.services.implementation.kubernetes_service_v1 import KubernetesServiceV1
from app.src.services.protocol.hook.hook_service_protocol import HookServiceProtocol
from src.util.logger import log
from src.util.setup import get_settings
from app.src.services.implementation.gitlab_service import GitlabService
from app.src.services.implementation.keycloak_service import KeycloakService
from app.src.services.implementation.docker_service import DockerService

# Constants
SECD_GROUP = "secd"
STORAGE_TYPE = "storage"
DATABASE_SERVICE = "database-service"
STORAGE_SIZE = "100Gi"
OUTPUT_STORAGE_SIZE = "50Gi"

class HookService(HookServiceProtocol):
    def __init__(
        self,
        gitlab_service: GitlabService,
        keycloak_service: KeycloakService,
        docker_service: DockerService,
        kubernetes_service: KubernetesServiceV1
    ):
        self.gitlab_service = gitlab_service
        self.keycloak_service = keycloak_service
        self.docker_service = docker_service
        self.kubernetes_service = kubernetes_service

    def create(self, body: Dict[str, Any]) -> None:
        temp_user_id = ""
        try:
            gitlab_user_id = self._validate_request(body)
            keycloak_user_id = self._validate_user_permissions(gitlab_user_id)
            run_id, repo_path, output_path, date = self._prepare_repository(body)
            run_meta, release_name = self._get_run_metadata(repo_path)
            self._check_user_role(keycloak_user_id, release_name)
            temp_user_id = self._setup_temp_user(keycloak_user_id)
            service_name = self._fetch_storage_service(release_name)
            image_name = self._handle_docker_operations(repo_path, run_id)
            self._setup_kubernetes(run_id, keycloak_user_id, temp_user_id, date, run_meta, service_name, image_name)
            log("Create process completed successfully", "INFO")
        except Exception as e:
            log(f"Error in create process: {str(e)}", "ERROR")
            raise
        finally:
            self._cleanup_temp_user(temp_user_id)

    def _validate_request(self, body: Dict[str, Any]) -> str:
        log("Validating request body...", "INFO")
        self.gitlab_service.validate_body(body)
        gitlab_user_id = body['user_id']
        log(f"Extracted gitlab_user_id: {gitlab_user_id}", "DEBUG")
        return gitlab_user_id

    def _validate_user_permissions(self, gitlab_user_id: str) -> str:
        log("Retrieving Keycloak user ID...", "INFO")
        keycloak_user_id = self.gitlab_service.get_idp_user_id(int(gitlab_user_id))
        log(f"Keycloak user ID: {keycloak_user_id}", "DEBUG")
        log("Checking user group membership...", "INFO")
        if not self.keycloak_service.check_user_in_group(keycloak_user_id, SECD_GROUP):
            log(f"User {keycloak_user_id} not in '{SECD_GROUP}' group", "ERROR")
            raise Exception("User is not in the group.")
        return keycloak_user_id

    def _prepare_repository(self, body: Dict[str, Any]) -> Tuple[str, str, str, str]:
        log("Preparing repository...", "INFO")
        run_id = str(uuid.uuid4()).replace('-', '')
        repo_path = f"{get_settings()['path']['repoPath']}/{run_id}"
        self.gitlab_service.clone(body["project"]["http_url"], repo_path)
        date = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        output_path = f"{repo_path}/outputs/{date}-{run_id}"
        os.makedirs(output_path)
        log(f"Prepared run_id={run_id}, repo_path={repo_path}, output_path={output_path}, date={date}", "DEBUG")
        return run_id, repo_path, output_path, date

    def _get_run_metadata(self, repo_path: str) -> Tuple[Dict[str, Any], str]:
        log("Retrieving metadata from secd.yml...", "INFO")
        run_meta = self.gitlab_service.get_metadata(f"{repo_path}/secd.yml")
        release_name = run_meta['database']
        log(f"Metadata release_name: {release_name}", "DEBUG")
        return run_meta, release_name

    def _check_user_role(self, keycloak_user_id: str, release_name: str) -> None:
        log("Verifying user role...", "INFO")
        if not self.keycloak_service.check_user_has_role(keycloak_user_id, DATABASE_SERVICE, release_name):
            log(f"User {keycloak_user_id} lacks role for {release_name}", "ERROR")
            raise Exception("User does not have the required role.")

    def _setup_temp_user(self, keycloak_user_id: str) -> str:
        log("Creating temporary user...", "INFO")
        temp_user_id = "temp_" + keycloak_user_id
        temp_user_password = "temp_password"
        temp_user_id = self.keycloak_service.create_temp_user(temp_user_id, temp_user_password)
        log(f"Temporary user ID: {temp_user_id}", "DEBUG")
        return temp_user_id

    def _fetch_storage_service(self, release_name: str) -> str:
        log("Fetching storage service...", "INFO")
        service_name = self.kubernetes_service.get_service_by_helm_release(release_name, STORAGE_TYPE)
        if not service_name:
            log(f"No service found for release {release_name}", "ERROR")
            raise Exception(f"Service not found for release {release_name}")
        return service_name

    def _handle_docker_operations(self, repo_path: str, run_id: str) -> str:
        log("Logging into Docker registry...", "INFO")
        self.docker_service.login_to_registry()
        log("Building and pushing Docker image...", "INFO")
        image_name = self.docker_service.build_and_push_image(repo_path, run_id)
        log(f"Docker image: {image_name}", "DEBUG")
        return image_name

    def _setup_kubernetes(self, run_id: str, keycloak_user_id: str, temp_user_id: str, date: str, run_meta: Dict[str, Any], service_name: str, image_name: str) -> None:
        log("Setting up Kubernetes resources...", "INFO")
        self._setup_kubernetes_resources(temp_user_id, date, run_id, run_meta)
        env_vars = self._prepare_environment_variables(service_name, run_meta['database'])
        pv = self.kubernetes_service.pv_service.get_pv_by_helm_release(run_meta['database'])
        if not pv:
            log(f"No PV found for release {run_meta['database']}", "ERROR")
            raise Exception(f"PV not found for release {run_meta['database']}")
        pvc_name = f"pvc-storage-{run_meta['database']}"
        namespace = f"secd-{run_id}"
        volume_name_nfs = f"pv-storage-{run_meta['database']}"
        log("Creating storage PVC...", "INFO")
        self.kubernetes_service.pv_service.create_persistent_volume_claim(
            pvc_name, namespace, volume_name_nfs, storage_size=STORAGE_SIZE
        )
        output_pvc_name = f"secd-pvc-{run_id}-output"
        volume_name_output = f"secd-{run_id}-output"
        log("Creating output PVC...", "INFO")
        self.kubernetes_service.pv_service.create_persistent_volume_claim(
            output_pvc_name, namespace, volume_name_output, storage_size=OUTPUT_STORAGE_SIZE, access_modes=["ReadWriteOnce"]
        )
        log("Creating Kubernetes pod...", "INFO")
        self._create_kubernetes_pod(run_id, keycloak_user_id, image_name, run_meta, env_vars, pvc_name, namespace)

    def _cleanup_temp_user(self, temp_user_id: str) -> None:
        if temp_user_id:
            log(f"Deleting temporary user {temp_user_id}...", "INFO")
            self.keycloak_service.delete_temp_user(temp_user_id)
            log(f"Temporary user {temp_user_id} deleted", "DEBUG")

    def _setup_kubernetes_resources(self, temp_user_id: str, date: str, run_id: str, run_meta: Dict[str, Any]) -> None:
        pvc_repo_path = get_settings()['k8s']['pvcPath']
        self.kubernetes_service.create_namespace(temp_user_id, run_id, run_meta['runfor'])
        pv_name = f'secd-{run_id}-output'
        self.kubernetes_service.pv_service.create_persistent_volume(pv_name, f'{pvc_repo_path}/repos/{run_id}/outputs/{date}-{run_id}')
        log(f"Kubernetes resources created for {run_id}", "DEBUG")

    def _prepare_environment_variables(self, service_name: str, release_name: str) -> Dict[str, str]:
        secret_name = f"secret-{release_name}"
        db_user = self.kubernetes_service.get_secret(STORAGE_TYPE, secret_name, "user")
        db_password = self.kubernetes_service.get_secret(STORAGE_TYPE, secret_name, "user-password")
        env_vars = {
            "DB_USER": db_user,
            "DB_PASS": db_password,
            "DB_HOST": service_name,
            "NFS_PATH": '/data',
            "OUTPUT_PATH": '/output',
            "SECD": 'PRODUCTION'
        }
        log(f"Environment variables prepared: {env_vars}", "DEBUG")
        return env_vars

    def _create_kubernetes_pod(self, run_id: str, keycloak_user_id: str, image_name: str, run_meta: Dict[str, Any], env_vars: Dict[str, str], pvc_name: str, namespace: str) -> None:
        cache_dir, mount_path = self.kubernetes_service.handle_cache_dir(run_meta, keycloak_user_id, run_id)
        release_name = run_meta['database']
        log(f"Database pod name: {release_name}", "DEBUG")
        db_pod = self.kubernetes_service.get_pod_by_helm_release(release_name=release_name, namespace=STORAGE_TYPE)
        db_label = db_pod.metadata.labels.get('database', '')

        self.kubernetes_service.pod_service.create_pod(
            run_id=run_id,
            image=image_name,
            envs=env_vars,
            gpu=run_meta['gpu'],
            mount_path=mount_path,
            database=db_label,
            namespace=namespace,
            pvc_name=pvc_name,
        )