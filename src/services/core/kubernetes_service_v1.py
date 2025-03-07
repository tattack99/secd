from kubernetes.client.rest import ApiException
from kubernetes import client, config
from typing import Dict, List, Optional
from app.src.util.setup import get_settings
from app.src.util.logger import log
from src.services.core.protocol.kubernetes_service_protocol import KubernetesServiceProtocol

class KubernetesServiceV1(KubernetesServiceProtocol):
    def __init__(self) -> None:
        log("Instantiating Kubernetes service v1")
        self.config_path = get_settings()['k8s']['configPath']
        config.load_kube_config(self.config_path)
        self.v1 = client.CoreV1Api()
        log("Kubernetes service v1 instantiated")

    def create_namespace(self) -> None:
        raise NotImplementedError

    def read_namespace(self) -> None:
        raise NotImplementedError

    def update_namespace(self) -> None:
        raise NotImplementedError

    def delete_namespace(self) -> None:
        raise NotImplementedError

