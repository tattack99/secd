from typing import Protocol

class ServiceAccountServiceProtocol(Protocol):
    def create_service_account(self, name: str, namespace: str) -> None:
        ...