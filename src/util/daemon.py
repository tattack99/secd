import time
import urllib3
from app.src.services.implementation.gitlab_service import GitlabService
from app.src.services.implementation.kubernetes_service_v1 import KubernetesServiceV1
from app.src.util.logger import log





class Daemon:
    def __init__(
            self,
            kubernetes_service : KubernetesServiceV1,
            gitlab_service : GitlabService):
        urllib3.disable_warnings()
        self.kubernetes_service = kubernetes_service
        self.gitlab_service = gitlab_service


    def start_microk8s_cleanup(self):
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
