from typing import Protocol, Optional

class HelmServiceProtocol(Protocol):
    def get_service_by_helm_release(self, release_name: str, namespace: str) -> Optional[str]:
        ...