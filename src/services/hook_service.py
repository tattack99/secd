import datetime
import os
import uuid
from typing import Dict, Any
from app.src.util.logger import log
from app.src.util.setup import get_settings
from app.src.services.kubernetes_service import KubernetesService
from app.src.services.vault_service import VaultService
from app.src.services.gitlab_service import GitlabService
from app.src.services.keycloak_service import KeycloakService
from app.src.services.docker_service import DockerService

SECD_GROUP = "secd"
STORAGE_TYPE = "storage"
DATABASE_SERVICE = "database-service"
STORAGE_SIZE = "100Gi"
OUTPUT_STORAGE_SIZE = "50Gi"

class HookService():
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

        self.run_id = "run_id"
        self.namespace = "namespace"
        self.date = datetime.datetime.now()
        self.repo_path = "repo_path"
        self.output_path = "output_path"
        self.pvc_repo_path = "pvc_repo_path"
        self.pvc_name_output= "pvc_name_output"
        self.pv_name_output= "pv_name_output"

        self.run_meta = any
        self.keycloak_user_id = any
        self.image_name = any
        self.run_for = any
        self.namespace_labels = any
        self.database_name = any
        self.database_type = any
        self.env_vars = any
        self.pvc_name = any
        self.vault_role_name = any


    def create(self, body: Dict[str, Any]):
        self.run_id = str(uuid.uuid4()).replace('-', '')
        self.namespace = f"secd-{self.run_id}"
        self.date = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        self.repo_path = f"{get_settings()['path']['repoPath']}/{self.run_id}"
        self.output_path = f"{self.repo_path}/outputs/{self.date}-{self.run_id}"
        self.pvc_repo_path = get_settings()['k8s']['pvcPath']
        self.pvc_name_output = f"secd-pvc-{self.run_id}-output"
        self.pv_name_output = f"secd-pv-{self.run_id}-output"

        try:

            self._validate(body)

            self.database_name = self.run_meta["database_name"]
            self.database_type = self.run_meta["database_type"]
            self.run_for = self.run_meta["runfor"]
            self.namespace_labels = {"name": self.database_name}
            service_name = f"service-{self.database_name}.storage.svc.cluster.local"
            self.env_vars = {
                "DB_HOST": service_name,
                "NFS_PATH": "/data",
                "OUTPUT_PATH": "/output",
                "SECD": "PRODUCTION"
            }

            # TODO: FIX file type
            if self.database_type == "file": 
                self._database_is_file()

            # TODO: FIX mysql
            if self.database_type == "mysql": 
                self._database_is_mysql()
                

            else:
                log(f"self.database_type not implemented: {self.database_type}", "WARNING")

        except Exception as e:
            log(f"Error in create process: {str(e)}", "ERROR")

    def _database_is_mysql(self):
        self._create_namespace()
        self._create_pv()
        self._setup_pvc()
        self._vault_setup()
        self._create_pod_by_vault()


    def _database_is_file(self):
        # I think we can just create a pod that mount to the file location
        # This is to test that the pod get access to the nfs
        # After the pod can print out the data from there
        # We can add credentials verification after the pod can read data from mount location

        # Create self.namespace for pod to run in
        self._create_namespace()
        
        # Create pv to save run of executing pod
        self._create_pv()
        
        # Do something with similar to: /home/cloud/secd/charts/test/nfs-pod-mount/nfs-pod-test.yaml
        # This pod is able to access data from the nfs

    def _validate(self, body: Dict[str, Any]):

        self.gitlab_service.validate_body(body)

        gitlab_user_id = body['user_id']
        http_url = body["project"]["http_url"]

        self.keycloak_user_id = self.gitlab_service.get_idp_user_id(int(gitlab_user_id))
        if not self.keycloak_service.check_user_in_group(self.keycloak_user_id, SECD_GROUP):
            log(f"User {self.keycloak_user_id} not in '{SECD_GROUP}' group", "ERROR")
            raise Exception("User is not in the group.")

        self.gitlab_service.clone(http_url, self.repo_path)
        os.makedirs(self.output_path)

        self.run_meta = self.gitlab_service.get_metadata(f"{self.repo_path}/secd.yml")
        self.database_name = self.run_meta['database_name']

        if not self.keycloak_service.check_user_has_role(self.keycloak_user_id, DATABASE_SERVICE, self.database_name):
            log(f"User {self.keycloak_user_id} lacks role for {self.database_name}", "ERROR")
            raise Exception("User does not have the required role.")

        self.docker_service.login_to_registry()
        self.image_name = self.docker_service.build_and_push_image(self.repo_path, self.run_id)
        


    def _create_pv(self):
        self.kubernetes_service.pv_service.create_persistent_volume(
            name = self.pv_name_output, 
            path = f"{self.pvc_repo_path}/repos/{self.run_id}/outputs/{self.date}-{self.run_id}"
        )

    def _create_namespace(self):
        self.kubernetes_service.create_namespace(
            user_id = self.keycloak_user_id, 
            run_id = self.run_id, 
            run_for = self.run_for,
            labels = self.namespace_labels,
        )

    def _setup_pvc(self) -> str:
        # Fetch the database pod using the correct label selector
        db_pod = self.kubernetes_service.pod_service.get_pod_by_label(
            label_selector=f"name={self.database_name}",  # e.g., "name=mysql-1"
            namespace=STORAGE_TYPE
        )
        if not db_pod:
            log(f"No pod found for database {self.database_name} in namespace {STORAGE_TYPE}", "ERROR")
            raise Exception(f"Database pod not found for {self.database_name}")

        # Extract PVC name from database pod's volume spec
        self.pvc_name = None
        for volume in db_pod.spec.volumes:
            if volume.persistent_volume_claim:
                self.pvc_name = volume.persistent_volume_claim.claim_name  # e.g., "pvc-storage-mysql-1"
                break
        if not self.pvc_name:
            log(f"No PVC found in pod for database {self.database_name}", "ERROR")
            raise Exception("No PVC associated with the database pod")

        # Fetch the PV bound to this PVC
        pvc = self.kubernetes_service.pv_service.get_pvc(self.pvc_name, namespace=STORAGE_TYPE)
        if not pvc or not pvc.spec.volume_name:
            log(f"No PV bound to PVC {self.pvc_name} in namespace {STORAGE_TYPE}", "ERROR")
            raise Exception(f"PV not found for PVC {self.pvc_name}")
        pv_name = pvc.spec.volume_name  # e.g., "pv-storage-mysql-1"

        # Log the discovered PV for debugging
        log(f"Found PV {pv_name} for database {self.database_name} via pod data")

        # Proceed with existing PVC setup for output
        self.kubernetes_service.pv_service.create_persistent_volume_claim(
            self.pvc_name_output, 
            self.namespace, 
            self.pv_name_output, 
            storage_size=OUTPUT_STORAGE_SIZE, 
            access_modes=["ReadWriteOnce"]
        )

        log(f"database_type: {self.database_type}", "DEBUG")
        if self.database_type == "file": 
            return self.pvc_name
        else:
            self.pvc_name = None
            return ""


    def _create_pod_by_vault(self):

        cache_dir, mount_path = self.kubernetes_service.handle_cache_dir(self.run_meta, self.keycloak_user_id, self.run_id)
        db_pod = self.kubernetes_service.pod_service.get_pod_by_label(
            label_selector=f"name={self.run_meta['database_name']}",
            namespace=STORAGE_TYPE 
        )

        db_label = db_pod.metadata.labels.get('name')
        log(f"self.pvc_name: {self.pvc_name}","DEBUG" )
        self.kubernetes_service.pod_service.create_pod_by_vault(
                run_id=self.run_id,
                image=self.image_name,
                envs=self.env_vars,
                gpu=self.run_meta['gpu'],
                mount_path=mount_path,
                database=db_label,
                namespace=self.namespace,
                pvc_name=self.pvc_name,
                vault_role= self.vault_role_name  # Match Vault role from _vault_setup
            )

    def _vault_setup(self) -> str:
        # Define database details
        #self.database_name = "mysql-1"
        #self.database_type = "mysql"

        # Define resource names
        self.vault_role_name = f"role-{self.database_name}"  # e.g., role-mysql-1 (for database creds)
        policy_name = f"policy-{self.database_name}"  # e.g., policy-mysql-1
        service_account_name = f"sa-{self.database_name}"  # e.g., sa-mysql-1 (in pod's self.namespace)
        k8s_auth_role_name = f"role-{self.database_name}-{self.namespace}"  # e.g., role-mysql-1-secd-<self.run_id>

        # Step 1: Configure Vault database connection
        connection_url_template = f"{{{{username}}}}:{{{{password}}}}@tcp(service-{self.database_name}.storage.svc.cluster.local:3306)/"
        allowed_roles = [self.vault_role_name]
        username = "vault"
        password = "vaultpassword"
        self.vault_service.configure_database_connection(
            database_name=self.database_name,
            db_type=self.database_type,
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
            role_name=self.vault_role_name,
            database_name=self.database_name,
            creation_statements=creation_statements,
        )

        # Step 3: Create policy for accessing temporary credentials
        policy_rules = f"""
            path "database/creds/{self.vault_role_name}" {{
                capabilities = ["read"]
            }}
        """
        self.vault_service.create_policy(
            policy_name=policy_name,
            policy_rules=policy_rules
        )

        # Step 4: Create service account in the pod's self.namespace
        self.kubernetes_service.create_service_account(service_account_name, self.namespace)

        # Step 5: Create Kubernetes auth role for the pod's self.namespace
        self.vault_service.create_kubernetes_auth_role(
            role_name=k8s_auth_role_name,
            service_account_name=service_account_name,
            service_account_namespace=self.namespace,  # Pod's self.namespace (e.g., secd-<self.run_id>)
            policy=policy_name
        )
        return self.vault_role_name