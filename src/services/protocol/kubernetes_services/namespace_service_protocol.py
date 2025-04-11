from typing import Protocol, List, Optional
from kubernetes.client import V1Namespace

class NamespaceServiceProtocol(Protocol):
    def create_namespace(self, name: str, labels: dict, annotations: dict) -> V1Namespace:
        ...

    def get_namespace(self, name: str) -> Optional[V1Namespace]:
        ...

    def get_namespaces(self) -> List[V1Namespace]:
        ...

    def delete_namespace(self, name: str) -> None:
        ...

    def cleanup_namespaces(self, namespaces) -> List[str]:
        ...