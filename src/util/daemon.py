import time
import src.services.k8s_service as k8s_service
import src.services.gitlab_service as gitlab_service

from src.services.database_service import DatabaseService
from src.util.logger import log

class Daemon:
    def start_microk8s_cleanup(self):
        log("Starting microk8s clean up service...")
        while True:
            try:
                cleaned_run_ids = k8s_service.cleanup_resources()

                for run_id in cleaned_run_ids:
                    log(f"Finishing run {run_id} - expired rununtil - Pushing results")
                    gitlab_service.push_results(run_id)
                    log(f"Finishing run {run_id} - Finished and cleaned up")
            except Exception as e:
                log(f"Error in Daemon run loop: {e}", "ERROR")

            time.sleep(5)

    def start_database_service(self):
            log("Starting database service...")
            try:
                database_service = DatabaseService()
                database_service.run()

            except Exception as e:
                log(f"Error in database manage loop: {e}", "ERROR")
