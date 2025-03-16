import datetime
import os
import uuid
from typing import Dict, Optional, Tuple, Any
from app.src.util.logger import log
from app.src.util.setup import get_settings
from app.src.services.implementation.kubernetes_service_v1 import KubernetesServiceV1
from app.src.services.implementation.vault_v1.vault_service_v1 import VaultServiceV1
from app.src.services.protocol.hook.hook_service_protocol import HookServiceProtocol
from app.src.services.implementation.gitlab_service import GitlabService
from app.src.services.implementation.keycloak_service import KeycloakService
from app.src.services.implementation.docker_service import DockerService

SECD_GROUP = "secd"
STORAGE_TYPE = "storage"
DATABASE_SERVICE = "database-service"
STORAGE_SIZE = "100Gi"
OUTPUT_STORAGE_SIZE = "50Gi"
VAULT_ADDR = get_settings()['vault']['address']

class HookService(HookServiceProtocol):
    def __init__(
        self,
        gitlab_service: GitlabService,
        keycloak_service: KeycloakService,
        docker_service: DockerService,
        kubernetes_service: KubernetesServiceV1,
        vault_service: VaultServiceV1,
    ):
        self.gitlab_service = gitlab_service
        self.keycloak_service = keycloak_service
        self.docker_service = docker_service
        self.kubernetes_service = kubernetes_service
        self.vault_service = vault_service

    def create(self, body: Dict[str, Any]) -> None:
        run_id = str(uuid.uuid4()).replace('-', '')
        repo_path = f"{get_settings()['path']['repoPath']}/{run_id}"
        date = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        output_path = f"{repo_path}/outputs/{date}-{run_id}"

        namespace = f"secd-{run_id}"
        output_pvc_name = f"secd-pvc-{run_id}-output"
        volume_name_output = f"secd-{run_id}-output"
        temp_user_id = ""

        # Vault setup variables
        service_account_name = "karolinska-1-test"  # Hardcoded from your YAML
        vault_role_name = "karolinska-1-role"  # Hardcoded from your YAML

        try:
            gitlab_user_id, keycloak_user_id = self._validate_request_and_permissions(body)

            run_meta, release_name = self._prepare_repository_and_metadata(body["project"]["http_url"], repo_path, output_path)

            self._check_user_role(keycloak_user_id, release_name)

            temp_user_id = self._setup_temp_user(keycloak_user_id)

            service_name = self._fetch_storage_service(release_name)

            image_name = self._handle_docker_operations(repo_path, run_id)

            self._create_namespace_and_pv(temp_user_id, run_id, run_meta, date, volume_name_output)
            env_vars = self._prepare_env_vars(service_name, run_meta)
            pvc_name = self._setup_pvc(run_meta, namespace, output_pvc_name, volume_name_output)

            # Vault setup
            self.kubernetes_service.create_service_account(service_account_name, namespace)
            self.vault_service.create_kubernetes_auth_role(
                role_name=vault_role_name,
                service_account_name=service_account_name,
                namespace=namespace,
                policy="mysql-read-policy"  # Hardcoded policy name
            )

            self._create_pod(run_id, keycloak_user_id, image_name, run_meta, env_vars, pvc_name, namespace, vault_role_name)
        except Exception as e:
            log(f"Error in create process: {str(e)}", "ERROR")
            raise
        finally:
            self._cleanup_temp_user(temp_user_id)

    def _validate_request_and_permissions(self, body: Dict[str, Any]) -> Tuple[str, str]:
        self.gitlab_service.validate_body(body)
        gitlab_user_id = body['user_id']
        keycloak_user_id = self.gitlab_service.get_idp_user_id(int(gitlab_user_id))
        if not self.keycloak_service.check_user_in_group(keycloak_user_id, SECD_GROUP):
            log(f"User {keycloak_user_id} not in '{SECD_GROUP}' group", "ERROR")
            raise Exception("User is not in the group.")
        return gitlab_user_id, keycloak_user_id

    def _prepare_repository_and_metadata(self, http_url: str, repo_path: str, output_path: str) -> Tuple[Dict[str, Any], str]:
        self.gitlab_service.clone(http_url, repo_path)
        os.makedirs(output_path)
        run_meta = self.gitlab_service.get_metadata(f"{repo_path}/secd.yml")
        release_name = run_meta['database']
        return run_meta, release_name

    def _check_user_role(self, keycloak_user_id: str, release_name: str) -> None:
        if not self.keycloak_service.check_user_has_role(keycloak_user_id, DATABASE_SERVICE, release_name):
            log(f"User {keycloak_user_id} lacks role for {release_name}", "ERROR")
            raise Exception("User does not have the required role.")

    def _setup_temp_user(self, keycloak_user_id: str) -> str:
        temp_user_id = "temp_" + keycloak_user_id
        temp_user_password = "temp_password"
        return self.keycloak_service.create_temp_user(temp_user_id, temp_user_password)

    def _fetch_storage_service(self, release_name: str) -> str:
        service_name = self.kubernetes_service.get_service_by_helm_release(release_name, STORAGE_TYPE)
        if not service_name:
            log(f"No service found for release {release_name}", "ERROR")
            raise Exception(f"Service not found for release {release_name}")
        return service_name

    def _handle_docker_operations(self, repo_path: str, run_id: str) -> str:
        self.docker_service.login_to_registry()
        return self.docker_service.build_and_push_image(repo_path, run_id)

    def _create_namespace_and_pv(
        self,
        temp_user_id: str,
        run_id: str,
        run_meta: Dict[str, Any],
        date: str,
        volume_name_output: str
    ) -> None:
        pvc_repo_path = get_settings()['k8s']['pvcPath']
        self.kubernetes_service.create_namespace(temp_user_id, run_id, run_meta['runfor'])
        self.kubernetes_service.pv_service.create_persistent_volume(
            volume_name_output, f"{pvc_repo_path}/repos/{run_id}/outputs/{date}-{run_id}"
        )

    def _prepare_env_vars(self, service_name: str, run_meta: Dict[str, Any]) -> Dict[str, str]:
        secret_name = f"secret-{run_meta['database']}"
        db_user = self.kubernetes_service.get_secret(STORAGE_TYPE, secret_name, "user")
        db_password = self.kubernetes_service.get_secret(STORAGE_TYPE, secret_name, "user-password")
        if db_user is None or db_password is None:
            raise ValueError(f"Secrets not found in {secret_name}")
        return {
            "DB_USER": db_user,
            "DB_PASS": db_password,
            "DB_HOST": service_name,
            "NFS_PATH": '/data',
            "OUTPUT_PATH": '/output',
            "SECD": 'PRODUCTION'
        }

    def _setup_pvc(
        self,
        run_meta: Dict[str, Any],
        namespace: str,
        output_pvc_name: str,
        volume_name_output: str
    ) -> str:
        pv = self.kubernetes_service.pv_service.get_pv_by_helm_release(run_meta['database'])
        if not pv:
            log(f"No PV found for release {run_meta['database']}", "ERROR")
            raise Exception(f"PV not found for release {run_meta['database']}")
        pvc_name = f"pvc-storage-{run_meta['database']}" if run_meta["database_type"] == "file" else ""
        if run_meta["database_type"] == "file":
            self.kubernetes_service.pv_service.create_persistent_volume_claim(
                pvc_name, namespace, f"pv-storage-{run_meta['database']}", storage_size=STORAGE_SIZE
            )
        self.kubernetes_service.pv_service.create_persistent_volume_claim(
            output_pvc_name, namespace, volume_name_output, storage_size=OUTPUT_STORAGE_SIZE, access_modes=["ReadWriteOnce"]
        )
        return pvc_name

    def _create_pod(
        self,
        run_id: str,
        keycloak_user_id: str,
        image_name: str,
        run_meta: Dict[str, Any],
        env_vars: Dict[str, str],
        pvc_name: str,
        namespace: str,
        vault_role: str
    ) -> None:
        cache_dir, mount_path = self.kubernetes_service.handle_cache_dir(run_meta, keycloak_user_id, run_id)
        db_pod = self.kubernetes_service.get_pod_by_helm_release(release_name=run_meta['database'], namespace=STORAGE_TYPE)
        db_label = db_pod.metadata.labels.get('database', '')
        self.kubernetes_service.pod_service.create_pod_by_vault(
            run_id=run_id,
            image=image_name,
            envs=env_vars,
            gpu=run_meta['gpu'],
            mount_path=mount_path,
            database=db_label,
            namespace=namespace,
            pvc_name=pvc_name,
            vault_role=vault_role
        )

    def _cleanup_temp_user(self, temp_user_id: str) -> None:
        if temp_user_id:
            self.keycloak_service.delete_temp_user(temp_user_id)