import datetime
import os
from typing import Dict
import uuid

from src.util.logger import log
from src.util.setup import get_settings
from src.services.core.gitlab_service import GitlabService
from src.services.core.keycloak_service import KeycloakService
from src.services.core.docker_service import DockerService
from src.services.core.kubernetes_service import KubernetesService

class HookService:
    def __init__(
        self,
        gitlab_service : GitlabService,
        keycloak_service : KeycloakService,
        docker_service : DockerService,
        kubernetes_service : KubernetesService):

        self.gitlab_service = gitlab_service
        self.keycloak_service = keycloak_service
        self.docker_service = docker_service
        self.kubernetes_service = kubernetes_service

    def create(self, body):
        try:
            log("Starting create process...")
            user_have_permission = False
            self.gitlab_service.validate_body(body)
            gitlab_user_id = body['user_id']

            keycloak_user_id = self.gitlab_service.get_idp_user_id(gitlab_user_id)
            user_in_group = self.keycloak_service.check_user_in_group(keycloak_user_id, "secd") # All user must be in secd to access
            if not user_in_group:
                log("User is not in the group.")
                raise Exception("User is not in the group.")

            run_id, repo_path, output_path, date = self.prepare_repository_and_paths(body)
            run_meta = self.gitlab_service.get_metadata(f"{repo_path}/secd.yml")
            release_name = run_meta['database']

            has_role = self.keycloak_service.check_user_has_role(keycloak_user_id, "database-service", release_name)
            if not has_role:
                log(f"Keycloak user: {keycloak_user_id}, does not have the required role: {release_name}.")
                raise Exception("User does not have the required role.")


            temp_user_id, temp_user_password = self.create_temp_user(keycloak_user_id)

            service_name = self.kubernetes_service.get_service_by_helm_release(release_name, "storage")
            if not service_name:
                raise Exception(f"Service not found for release {release_name}")

            self.docker_service.login_to_registry()

            image_name = self.docker_service.build_and_push_image(repo_path, run_id)

            self.setup_kubernetes_resources(temp_user_id, date, run_id, run_meta)

            env_vars = self.prepare_environment_variables(service_name, release_name)

            pv = self.kubernetes_service.get_pv_by_helm_release(release_name)
            if not pv:
                raise Exception(f"PV not found for release {release_name}")

            pvc_name = f"pvc-storage-{release_name}"
            namespace = f"secd-{run_id}"
            self.kubernetes_service.create_persistent_volume_claim(pvc_name, namespace, release_name, storage_size="100Gi")

            self.create_kubernetes_pod(run_id, keycloak_user_id,image_name, run_meta, env_vars, pvc_name, namespace)

        except Exception as e:
            log(f"Error in create process: {str(e)}", "ERROR")

        finally:
            self.keycloak_service.delete_temp_user(temp_user_id)
            log(f"Temp user deleted: {temp_user_id}")

    def check_user_permissions(self, keycloak_user_id: str) -> bool:
        has_role = self.keycloak_service.check_user_has_role(keycloak_user_id, "database-service", "mysql_test")
        in_group = self.keycloak_service.check_user_in_group(keycloak_user_id, "secd")
        return has_role and in_group

    def prepare_repository_and_paths(self, body: Dict) -> tuple[str, str, str, str]:
        run_id = str(uuid.uuid4()).replace('-', '')
        repo_path = f"{get_settings()['path']['repoPath']}/{run_id}"
        self.gitlab_service.clone(body["project"]["http_url"], repo_path)
        date = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        output_path = f'{repo_path}/outputs/{date}-{run_id}'
        os.makedirs(output_path)
        log(f"Output path created: {output_path}")

        return run_id, repo_path, output_path, date

    def create_temp_user(self, keycloak_user_id: str) -> tuple[str, str]:
        temp_user_id = "temp_" + keycloak_user_id
        temp_user_password = "temp_password"
        temp_user_id = self.keycloak_service.create_temp_user(temp_user_id, temp_user_password)
        return temp_user_id, temp_user_password

    def setup_kubernetes_resources(self, tmp_user_id: str, date:str, run_id: str, run_meta: Dict):
        pvc_repo_path = get_settings()['k8s']['pvcPath']
        self.kubernetes_service.create_namespace(tmp_user_id, run_id, run_meta['runfor'])
        self.kubernetes_service.create_persistent_volume(run_id, f'{pvc_repo_path}/repos/{run_id}/outputs/{date}-{run_id}')
        log(f"Kubernetes resources created for {run_id}")


    def prepare_environment_variables(self, service_name: str, release_name: str) -> Dict[str, str]:
        secret_name = f"secret-{release_name}"
        db_user = self.kubernetes_service.get_secret("storage", secret_name, "user")
        db_password = self.kubernetes_service.get_secret("storage", secret_name, "user-password")
        env_vars = {
            "DB_USER": db_user,
            "DB_PASS": db_password,
            "DB_HOST": service_name,
            "NFS_PATH": '/data',
            "OUTPUT_PATH": '/output',
            "SECD": 'PRODUCTION'
        }
        log(f"Environment variables prepared: {env_vars}")
        return env_vars

    def create_kubernetes_pod(self, run_id: str, keycloak_user_id: str, image_name: str, run_meta: Dict, env_vars: Dict[str, str], pvc_name: str, namespace: str):
        cache_dir, mount_path = self.kubernetes_service.handle_cache_dir(run_meta, keycloak_user_id, run_id)
        release_name = run_meta['database']
        log(f"Database pod name: {release_name}")
        db_pod = self.kubernetes_service.get_pod_by_helm_release(release_name=release_name, namespace="storage")
        db_label = db_pod.metadata.labels.get('database', '')

        self.kubernetes_service.create_pod_v1(
            run_id=run_id,
            image=image_name,
            envs=env_vars,
            gpu=run_meta['gpu'],
            mount_path=mount_path,
            database=db_label,
            namespace=namespace,
            pvc_name=pvc_name,
        )

