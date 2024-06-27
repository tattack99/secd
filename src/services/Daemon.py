import time
import src.services.k8s_service as k8s_service
import src.services.gitlab_service as gitlab_service
from src.services.logger import log

class Daemon:
    def __init__(self):
        pass

    def run(self):
        log("Starting daemon...")
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
