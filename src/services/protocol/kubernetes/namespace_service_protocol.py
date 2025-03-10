from typing import Protocol, List, Optional
from kubernetes.client import V1Namespace

class NamespaceServiceProtocol(Protocol):
    def create_namespace(self, name: str, labels: dict, annotations: dict) -> V1Namespace:
        """Create a namespace with given name, labels, and annotations."""
        ...

    def get_namespace(self, name: str) -> Optional[V1Namespace]:
        """Retrieve a namespace by name."""
        ...

    def get_namespaces(self) -> List[V1Namespace]:
        """List all namespaces."""
        ...

    def delete_namespace(self, name: str) -> None:
        """Delete a namespace by name."""
        ...

    def cleanup_namespaces(self, namespaces) -> List[str]:
        """Clean up namespaces based on expiration or completion status."""
        ...