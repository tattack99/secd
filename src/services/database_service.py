

class DatabaseService:
    def __init__(self):
         pass


"""
class DatabaseResource:
    def on_get(self, req, resp):
        log("Handling GET /v1/database request")
        token = req.get_header('Authorization')

        if not token:
            log("Authorization token missing", "ERROR")
            raise HTTPUnauthorized(description='Authorization token required')

        if not keycloak_service.validate_token(token):
            log("Invalid authorization token", "ERROR")
            raise HTTPUnauthorized(description='Invalid authorization token')

        resp.media = {'message': 'Success'}
        resp.status = falcon.HTTP_200
"""
def get_database_host(database : str) -> str:
    if(database == "mysql") : return '192.168.1.1'
    return ""