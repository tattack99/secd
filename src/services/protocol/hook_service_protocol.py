class HookServiceProtocol:
    def __init__(self, hook_service):
        self.hook_service = hook_service

    def create(self, body):
        ...