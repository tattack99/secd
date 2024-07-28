from src.services.resource.database_service import DatabaseService
from src.util.logger import log
from falcon import HTTPUnauthorized, HTTPNotFound
from keycloak import KeycloakAuthenticationError
import falcon

class DatabaseResource:
    def __init__(self, database_service: DatabaseService):
        self.database_service = database_service

    def on_get(self, req, resp):
        try:
            log("GET /v1/database request")
            auth_header = self.get_auth_header(req)
            database_header = self.get_database_header(req)

            if not self.database_service.validate(auth_header):
                raise falcon.HTTPUnauthorized(description="Could not validate header")

            pod_ip = self.database_service.get_pod_ip(database_header)
            if pod_ip is None:
                raise falcon.HTTPNotFound(description="Database pod not found")

            self.set_response(resp, falcon.HTTP_200, "Access granted", pod_ip)
        except HTTPUnauthorized as e:
            self.handle_error(resp, falcon.HTTP_401, str(e))
        except KeycloakAuthenticationError as e:
            self.handle_error(resp, falcon.HTTP_401, str(e))
        except HTTPNotFound as e:
            self.handle_error(resp, falcon.HTTP_404, str(e))
        except Exception as e:
            self.handle_error(resp, falcon.HTTP_500, "Internal server error", str(e))

    def get_auth_header(self, req):
        auth_header = req.get_header('Authorization')
        if not auth_header:
            raise HTTPUnauthorized(description="Authorization header missing")
        return auth_header

    def get_database_header(self, req):
        database_header = req.get_header('Database')
        if not database_header:
            raise HTTPNotFound(description="Header does not contain database")
        return database_header

    def set_response(self, resp, status, message, pod_ip=None):
        resp.status = status
        resp.media = {"message": message}
        if pod_ip:
            resp.media["database_pod_ip"] = pod_ip

    def handle_error(self, resp, status, error_message, log_message=None):
        if log_message:
            log(f"Error: {log_message}", "ERROR")
        else:
            log(f"Error: {error_message}", "ERROR")
        resp.status = status
        resp.media = {"error": error_message}