from app.src.services.protocol.kubernetes.service_account_service_protocol import ServiceAccountServiceProtocol
from kubernetes import client
from app.src.util.logger import log


class ServiceAccountServiceV1(ServiceAccountServiceProtocol):
    def __init__(self, config: client.Configuration):   
        api_client = client.ApiClient(configuration=config)
        self.v1 = client.CoreV1Api(api_client=api_client)

    def create_service_account(self, name: str, namespace: str) -> None:
        sa = client.V1ServiceAccount(
            metadata=client.V1ObjectMeta(name=name, namespace=namespace)
        )
        self.v1.create_namespaced_service_account(namespace=namespace, body=sa)
        log(f"Created Service Account {name} in namespace {namespace}", "DEBUG")