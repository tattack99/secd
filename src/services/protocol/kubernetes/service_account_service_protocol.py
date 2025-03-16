from typing import List, Protocol
from kubernetes import client

class ServiceAccountServiceProtocol(Protocol):
    def create_service_account(self, name: str, namespace: str) -> None:
        ...
    
    def delete_service_account(self, name: str, namespace: str) -> None:    
        ...
    
    def cleanup_service_accounts(self, namespaces: List[client.V1Namespace]) -> None:
        ...