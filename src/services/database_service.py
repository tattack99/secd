import falcon
from wsgiref.simple_server import make_server
from src.util.logger import log
import src.services.keycloak_service as keycloak_service

class DatabaseService:
    def __init__(self):
         self.micro_service = falcon.App()
         self.micro_service.add_route('/v1/database', DatabaseResource())

    def run(self):
            log("Managing database resources...")
            with make_server('', 8082, self.micro_service) as httpd:
                log('Database service running on port 8082...')
                httpd.serve_forever()

class DatabaseResource:
    def on_get(self, req, resp):
        log("GET /v1/database")
        resp.media = {"database": "mysql"}

    def on_post(self, req, resp):
        log("POST /v1/database")
        data = req.media
        database = data['database']
        host = get_database_host(database)
        resp.media = {"host": host}

    def on_put(self, req, resp):
        log("PUT /v1/database")
        data = req.media
        database = data['database']
        host = get_database_host(database)
        resp.media = {"host": host}

    def on_delete(self, req, resp):
        log("DELETE /v1/database")
        data = req.media
        database = data['database']
        host = get_database_host(database)
        resp.media = {"host": host}

def get_database_host(database : str) -> str:
    if(database == "mysql") : return '192.168.1.1'
    return ""