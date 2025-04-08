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
        vault_role_name = "mysql-1-readonly"

        try:
            gitlab_user_id, keycloak_user_id = self._validate_request_and_permissions(body)

            run_meta, database_name = self._prepare_repository_and_metadata(body["project"]["http_url"], repo_path, output_path)
            # Override database name to match hardcoded MySQL instance
            log(f"run_meta['database_name']: {run_meta['database_name']}")

            self._check_user_role(keycloak_user_id, database_name)

            temp_user_id = self._setup_temp_user(keycloak_user_id)
            service_name = self._generate_service_name(database_name,"storage")
            image_name = self._handle_docker_operations(repo_path, run_id)

            self._create_namespace_and_pv(temp_user_id, run_id, run_meta, date, volume_name_output)
            env_vars = self._prepare_env_vars(service_name, run_meta)
            pvc_name = self._setup_pvc(run_meta, namespace, output_pvc_name, volume_name_output)

            self._vault_setup(namespace=namespace)

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
        database_name = run_meta['database_name']
        return run_meta, database_name

    def _check_user_role(self, keycloak_user_id: str, database_name: str) -> None:
        if not self.keycloak_service.check_user_has_role(keycloak_user_id, DATABASE_SERVICE, database_name):
            log(f"User {keycloak_user_id} lacks role for {database_name}", "ERROR")
            raise Exception("User does not have the required role.")

    def _setup_temp_user(self, keycloak_user_id: str) -> str:
        temp_user_id = "temp_" + keycloak_user_id
        temp_user_password = "temp_password"
        return self.keycloak_service.create_temp_user(temp_user_id, temp_user_password)

    def _generate_service_name(self, name:str, namespace:str) -> str:
        return f"{name}.{namespace}.svc.cluster.local"

    def _fetch_storage_service(self, database_name: str) -> str:
        service_name = self.kubernetes_service.get_service_by_helm_release(database_name, STORAGE_TYPE)
        if not service_name:
            log(f"No service found for release {database_name}", "ERROR")
            raise Exception(f"Service not found for release {database_name}")
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
        # Hardcode DB_HOST for this specific MySQL service
        return {
            "DB_HOST": "mysql-1.storage.svc.cluster.local",
            "NFS_PATH": '/data',
            "OUTPUT_PATH": '/output',
            "SECD": 'PRODUCTION'
        }

    def _setup_pvc(
        self,
        run_meta: Dict[str, any],
        namespace: str,
        output_pvc_name: str,
        volume_name_output: str
    ) -> str:
        # Fetch the database pod using the correct label selector
        db_pod = self.kubernetes_service.pod_service.get_pod_by_label(
            label_selector=f"name={run_meta['database_name']}",  # e.g., "name=mysql-1"
            namespace=STORAGE_TYPE  # "storage"
        )
        if not db_pod:
            log(f"No pod found for database {run_meta['database_name']} in namespace {STORAGE_TYPE}", "ERROR")
            raise Exception(f"Database pod not found for {run_meta['database_name']}")

        # Extract PVC name from pod's volume spec
        pvc_name = None
        for volume in db_pod.spec.volumes:
            if volume.persistent_volume_claim:
                pvc_name = volume.persistent_volume_claim.claim_name  # e.g., "pvc-storage-mysql-1"
                break
        if not pvc_name:
            log(f"No PVC found in pod for database {run_meta['database_name']}", "ERROR")
            raise Exception("No PVC associated with the database pod")

        # Fetch the PV bound to this PVC
        pvc = self.kubernetes_service.pv_service.get_pvc(pvc_name, namespace=STORAGE_TYPE)
        if not pvc or not pvc.spec.volume_name:
            log(f"No PV bound to PVC {pvc_name} in namespace {STORAGE_TYPE}", "ERROR")
            raise Exception(f"PV not found for PVC {pvc_name}")
        pv_name = pvc.spec.volume_name  # e.g., "pv-storage-mysql-1"

        # Log the discovered PV for debugging
        log(f"Found PV {pv_name} for database {run_meta['database_name']} via pod data")

        # Proceed with existing PVC setup for output
        self.kubernetes_service.pv_service.create_persistent_volume_claim(
            output_pvc_name, namespace, volume_name_output, storage_size=OUTPUT_STORAGE_SIZE, access_modes=["ReadWriteOnce"]
        )

        # Return the database PVC name if needed, or adjust based on your logic
        return pvc_name if run_meta["database_type"] == "file" else ""


    def _create_pod(
        self,
        run_id: str,
        keycloak_user_id: str,
        image_name: str,
        run_meta: Dict[str, any],
        env_vars: Dict[str, str],
        pvc_name: str,
        namespace: str,
        vault_role: str
    ) -> None:
        cache_dir, mount_path = self.kubernetes_service.handle_cache_dir(run_meta, keycloak_user_id, run_id)
        db_pod = self.kubernetes_service.pod_service.get_pod_by_label(
            label_selector=f"name={run_meta['database_name']}",  # e.g., "name=mysql-1"
            namespace=STORAGE_TYPE  # "storage"
        )

        db_label = db_pod.metadata.labels.get('database', '')
        self.kubernetes_service.pod_service.create_pod_by_vault(  # Switch to v3
                run_id=run_id,
                image=image_name,
                envs=env_vars,
                gpu=run_meta['gpu'],
                mount_path=mount_path,
                database=db_label,
                namespace=namespace,
                pvc_name=pvc_name,
                vault_role="mysql-1-readonly"  # Match Vault role from _vault_setup
            )

    def _cleanup_temp_user(self, temp_user_id: str) -> None:
        if temp_user_id:
            self.keycloak_service.delete_temp_user(temp_user_id)
    
    def _vault_setup(self, namespace):
        # Define database details
        database_name = "mysql-1"
        database_type = "mysql"

        # Define resource names
        db_role_name = f"role-{database_name}"  # e.g., role-mysql-1 (for database creds)
        policy_name = f"policy-{database_name}"  # e.g., policy-mysql-1
        service_account_name = f"sa-{database_name}"  # e.g., sa-mysql-1 (in pod's namespace)
        k8s_auth_role_name = f"role-{database_name}-{namespace}"  # e.g., role-mysql-1-secd-<run_id>

        # Step 1: Configure Vault database connection
        connection_url_template = f"{{{{username}}}}:{{{{password}}}}@tcp(service-{database_name}.storage.svc.cluster.local:3306)/"
        allowed_roles = [db_role_name]
        username = "vault"
        password = "vaultpassword"
        self.vault_service.configure_database_connection(
            database_name=database_name,
            db_type=database_type,
            connection_url_template=connection_url_template,
            allowed_roles=allowed_roles,
            admin_username=username,
            admin_password=password
        )

        # Step 2: Create database role for temporary users
        creation_statements = [
            "CREATE USER '{{name}}'@'%' IDENTIFIED BY '{{password}}';",
            "GRANT SELECT ON *.* TO '{{name}}'@'%';"
        ]
        self.vault_service.create_database_role(
            role_name=db_role_name,
            database_name=database_name,
            creation_statements=creation_statements,
        )

        # Step 3: Create policy for accessing temporary credentials
        policy_rules = f"""
            path "database/creds/{db_role_name}" {{
                capabilities = ["read"]
            }}
        """
        self.vault_service.create_policy(
            policy_name=policy_name,
            policy_rules=policy_rules
        )

        # Step 4: Create service account in the pod's namespace
        self.kubernetes_service.create_service_account(service_account_name, namespace)

        # Step 5: Create Kubernetes auth role for the pod's namespace
        self.vault_service.create_kubernetes_auth_role(
            role_name=k8s_auth_role_name,
            service_account_name=service_account_name,
            service_account_namespace=namespace,  # Pod's namespace (e.g., secd-<run_id>)
            policy=policy_name
        )