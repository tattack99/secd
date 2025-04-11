import datetime
import os
import uuid
from typing import Dict, Optional, Tuple, Any
from app.src.util.logger import log
from app.src.util.setup import get_settings
from app.src.services.protocol.hook_service_protocol import HookServiceProtocol
from app.src.services.implementation.kubernetes_service import KubernetesService
from app.src.services.implementation.vault_service import VaultService
from app.src.services.implementation.gitlab_service import GitlabService
from app.src.services.implementation.keycloak_service import KeycloakService
from app.src.services.implementation.docker_service import DockerService

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
        kubernetes_service: KubernetesService,
        vault_service: VaultService,
    ):
        self.gitlab_service = gitlab_service
        self.keycloak_service = keycloak_service
        self.docker_service = docker_service
        self.kubernetes_service = kubernetes_service
        self.vault_service = vault_service

    def create(self, body: Dict[str, Any]):
        run_id = str(uuid.uuid4()).replace('-', '')
        repo_path = f"{get_settings()['path']['repoPath']}/{run_id}"
        date = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        output_path = f"{repo_path}/outputs/{date}-{run_id}"
        namespace = f"secd-{run_id}"
        pvc_repo_path = get_settings()['k8s']['pvcPath']
        pvc_name_output = f"secd-pvc-{run_id}-output"
        pv_name_output = f"secd-pv-{run_id}-output"

        try:

            run_meta, keycloak_user_id, image_name = self._validate(
                body = body,
                repo_path = repo_path,
                output_path = output_path,
                run_id = run_id
            )

            database_name = run_meta["database_name"]
            database_type = run_meta["database_type"]
            run_for = run_meta["runfor"]
            namespace_labels = {"name": database_name}
            service_name = f"service-{database_name}.storage.svc.cluster.local"
            env_vars = {
                "DB_HOST": service_name,
                "NFS_PATH": "/data",
                "OUTPUT_PATH": "/output",
                "SECD": "PRODUCTION"
            }

            self._create_namespace(
                user_id=keycloak_user_id,
                run_id=run_id,
                run_for=run_for,
                labels=namespace_labels,
            )
            self._create_pv(
                pv_name_output=pv_name_output,
                pvc_repo_path = pvc_repo_path,
                date = date,
                run_id = run_id
            )

            pvc_name = self._setup_pvc(
                database_name = database_name,
                database_type = database_type,
                namespace = namespace, 
                pvc_name_output = pvc_name_output, 
                pv_name_output = pv_name_output
            )

            vault_role_name = self._vault_setup(
                database_name = database_name,
                database_type = database_type,
                namespace = namespace
            )

            self._create_pod(
                run_id = run_id, 
                keycloak_user_id = keycloak_user_id, 
                image_name = image_name, 
                run_meta = run_meta, 
                env_vars = env_vars, 
                pvc_name = pvc_name, 
                namespace = namespace, 
                vault_role = vault_role_name
            )

        except Exception as e:
            log(f"Error in create process: {str(e)}", "ERROR")

    def _validate(
        self,
        body: Dict[str, Any],
        repo_path: str,
        output_path: str,
        run_id
    ):
        self.gitlab_service.validate_body(body)

        gitlab_user_id = body['user_id']
        http_url = body["project"]["http_url"]

        keycloak_user_id = self.gitlab_service.get_idp_user_id(int(gitlab_user_id))
        if not self.keycloak_service.check_user_in_group(keycloak_user_id, SECD_GROUP):
            log(f"User {keycloak_user_id} not in '{SECD_GROUP}' group", "ERROR")
            raise Exception("User is not in the group.")

        self.gitlab_service.clone(http_url, repo_path)
        os.makedirs(output_path)

        run_meta = self.gitlab_service.get_metadata(f"{repo_path}/secd.yml")
        database_name = run_meta['database_name']

        if not self.keycloak_service.check_user_has_role(keycloak_user_id, DATABASE_SERVICE, database_name):
            log(f"User {keycloak_user_id} lacks role for {database_name}", "ERROR")
            raise Exception("User does not have the required role.")

        self.docker_service.login_to_registry()
        image_name = self.docker_service.build_and_push_image(repo_path, run_id)
        
        return run_meta, keycloak_user_id, image_name


    def _create_pv(
        self,
        pv_name_output:str,
        pvc_repo_path:str,
        date:str,
        run_id:str,
    ):
        self.kubernetes_service.pv_service.create_persistent_volume(
            pv_name_output, 
            f"{pvc_repo_path}/repos/{run_id}/outputs/{date}-{run_id}"
        )

    def _create_namespace(
        self,
        user_id:str,
        run_id:str,
        run_for:int,
        labels:str,
    ):
        self.kubernetes_service.create_namespace(
            user_id = user_id, 
            run_id = run_id, 
            run_for = run_for,
            labels = labels,
        )

    def _setup_pvc(
        self,
        database_name:str, 
        database_type:str,
        namespace: str,
        pvc_name_output: str,
        pv_name_output: str
    ) -> str:
        # Fetch the database pod using the correct label selector
        db_pod = self.kubernetes_service.pod_service.get_pod_by_label(
            label_selector=f"name={database_name}",  # e.g., "name=mysql-1"
            namespace=STORAGE_TYPE
        )
        if not db_pod:
            log(f"No pod found for database {database_name} in namespace {STORAGE_TYPE}", "ERROR")
            raise Exception(f"Database pod not found for {database_name}")

        # Extract PVC name from pod's volume spec
        pvc_name = None
        for volume in db_pod.spec.volumes:
            if volume.persistent_volume_claim:
                pvc_name = volume.persistent_volume_claim.claim_name  # e.g., "pvc-storage-mysql-1"
                break
        if not pvc_name:
            log(f"No PVC found in pod for database {database_name}", "ERROR")
            raise Exception("No PVC associated with the database pod")

        # Fetch the PV bound to this PVC
        pvc = self.kubernetes_service.pv_service.get_pvc(pvc_name, namespace=STORAGE_TYPE)
        if not pvc or not pvc.spec.volume_name:
            log(f"No PV bound to PVC {pvc_name} in namespace {STORAGE_TYPE}", "ERROR")
            raise Exception(f"PV not found for PVC {pvc_name}")
        pv_name = pvc.spec.volume_name  # e.g., "pv-storage-mysql-1"

        # Log the discovered PV for debugging
        log(f"Found PV {pv_name} for database {database_name} via pod data")

        # Proceed with existing PVC setup for output
        self.kubernetes_service.pv_service.create_persistent_volume_claim(
            pvc_name_output, namespace, pv_name_output, storage_size=OUTPUT_STORAGE_SIZE, access_modes=["ReadWriteOnce"]
        )

        # Return the database PVC name if needed, or adjust based on your logic
        return pvc_name if database_type == "file" else ""


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
    ):
        cache_dir, mount_path = self.kubernetes_service.handle_cache_dir(run_meta, keycloak_user_id, run_id)
        db_pod = self.kubernetes_service.pod_service.get_pod_by_label(
            label_selector=f"name={run_meta['database_name']}",
            namespace=STORAGE_TYPE 
        )

        db_label = db_pod.metadata.labels.get('name')
        self.kubernetes_service.pod_service.create_pod_by_vault(
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

    def _vault_setup(
        self, 
        database_name:str, 
        database_type:str,
        namespace:str
    ) -> str:
        # Define database details
        #database_name = "mysql-1"
        #database_type = "mysql"

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
        return db_role_name