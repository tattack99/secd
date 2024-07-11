


class DatabaseResource:
    def __init__(self, database_service):
        self.database_service = database_service

    def on_get(self, req, resp):
        return {"data": "DatabaseResource"}
