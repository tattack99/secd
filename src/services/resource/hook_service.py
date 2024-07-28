import datetime
import os
from typing import Dict
import uuid
import requests

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
            self.gitlab_service.validate_body(body)

            # Get user info
            gitlab_user_id = body['user_id']
            keycloak_user_id = self.gitlab_service.get_idp_user_id(gitlab_user_id)

            # Clone repo
            run_id = str(uuid.uuid4()).replace('-', '')
            repo_path = f"{get_settings()['path']['repoPath']}/{run_id}"
            self.gitlab_service.clone(body["project"]["http_url"], repo_path)

            # Create output path
            date = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            output_path = f'{repo_path}/outputs/{date}-{run_id}'
            os.makedirs(output_path)
            log(f"Output path created: {output_path}")

            # Get metadata
            run_meta = self.gitlab_service.get_metadata(f"{repo_path}/secd.yml")
            run_for = run_meta['runfor']
            gpu = run_meta['gpu']
            database = run_meta['database']

            # Temp keycloak user
            temp_user_id = "temp_" + keycloak_user_id
            temp_user_password = "temp_password"

            # Check if user has 'mysql_test' role in 'database-service' client and is in 'secd' group
            user_have_permission = False
            if self.keycloak_service.check_user_has_role(keycloak_user_id, "database-service", "mysql_test") and self.keycloak_service.check_user_in_group(keycloak_user_id, "secd"):
                user_have_permission = True

            if user_have_permission:
                log("User has the necessary permissions.")
                temp_user_id = self.keycloak_service.create_temp_user(temp_user_id, temp_user_password)
            else:
                log("User does not have the necessary permissions.")
                return

            # Get database IP address
            token_response: Dict[str, str] = self.keycloak_service.get_access_token_username_password(temp_user_id, temp_user_password)
            token = token_response['access_token']
            headers = {
                    'Authorization': f'Bearer {token}',
                    'Database': database
                }

            database_host_response = requests.get('http://localhost:8001/v1/database', headers=headers)
            if database_host_response.status_code != 200:
                log(f"Failed to get database host: {database_host_response.text}", "ERROR")
                raise Exception("Failed to get database host")
            database_host = database_host_response.json()['database_pod_ip']
            log(f"Database host: {database_host}")

            # Get database info
            image_name = self.docker_service.build_and_push_image(repo_path, run_id)

            # Creating namespace
            pvc_repo_path = get_settings()['k8s']['pvcPath']
            self.kubernetes_service.create_namespace(temp_user_id, run_id, run_for)
            self.kubernetes_service.create_persistent_volume(run_id, f'{pvc_repo_path}/repos/{run_id}/outputs/{date}-{run_id}')
            log(f"Namespace and output volume created for {run_id}")

            # Creating pod
            cache_dir, mount_path = self.kubernetes_service.handle_cache_dir(run_meta, keycloak_user_id, run_id)
            db_pod_name = run_meta['database']
            log(f"Database pod name: {db_pod_name}")
            db_pod = self.kubernetes_service.get_pod_details("storage", db_pod_name)
            log(f"Database pod found: {db_pod.metadata.name}")

            # Use the release name to construct the secret name
            release_name = db_pod.metadata.labels['release']
            secret_name = f"secret-{release_name}"
            log(f"Constructed secret name: {secret_name}")

            db_password = self.kubernetes_service.get_secret(namespace="storage", secret_name=secret_name, key="root-password")

            self.kubernetes_service.create_pod(run_id, image_name, {
                "DB_USER": "root",
                "DB_PASS": db_password,
                "DB_HOST": database_host,
                "OUTPUT_PATH": '/output',
                "SECD": 'PRODUCTION'
            }, gpu, mount_path)
            log(f"Pod created for {run_id}")

        except Exception as e:
            log(f"Error in create process: {str(e)}", "ERROR")
        else:
            log(f"Successfully launched {run_id}")
        finally:
            if(user_have_permission):
                self.keycloak_service.delete_temp_user(temp_user_id)
                log(f"temp user deleted: {temp_user_id}")
