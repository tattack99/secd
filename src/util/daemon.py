import time
import secure.src.services.kubernetes_service as kubernetes_service
import secure.src.services.gitlab_service as gitlab_service

from secure.src.services.database_service import DatabaseService
from secure.src.services.kubernetes_service import KubernetesService
from secure.src.services.gitlab_service import GitlabService

from secure.src.util.logger import log

class Daemon:
    def __init__(
            self,
            kubernetes_service : KubernetesService,
            gitlab_service : GitlabService,
            database_service : DatabaseService):
        
        self.kubernetes_service = kubernetes_service
        self.gitlab_service = gitlab_service
        self.database_service = database_service


    def start_microk8s_cleanup(self):
        log("Starting microk8s clean up service...")
        while True:
            try:
                cleaned_run_ids = self.kubernetes_service.cleanup_resources()

                for run_id in cleaned_run_ids:
                    log(f"Finishing run {run_id} - expired rununtil - Pushing results")
                    self.gitlab_service.push_results(run_id)
                    log(f"Finishing run {run_id} - Finished and cleaned up")
            except Exception as e:
                log(f"Error in Daemon run loop: {e}", "ERROR")

            time.sleep(5)

    def start_database_service(self):
        log("Starting database service...")
        try:
            print("start_database_service")
        except Exception as e:
            log(f"Error in database manage loop: {e}", "ERROR")
