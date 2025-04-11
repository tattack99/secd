import falcon
import json
import threading
import gitlab

from app.src.services.protocol.hook_service_protocol import HookServiceProtocol
from app.src.util.logger import log
from app.src.util.setup import get_settings

class Hook:
    def __init__(self, hook_service: HookServiceProtocol):
        self.hook_service = hook_service

    def on_post(self, req, resp):
        #log("POST /v1/hook request")
        try:
            self.validate_event_token(req)
            body = self.parse_request_body(req)
            threading.Thread(target=self.hook_service.create(body)).start()
            self.set_response(resp, falcon.HTTP_200, "success")
        except Exception as e:
            self.handle_error(resp, falcon.HTTP_500, "Internal server error", e)

    def parse_request_body(self, req):
        try:
            body_raw = req.bounded_stream.read()
            if not body_raw:
                log("Missing body in request", "ERROR")
                raise falcon.HTTPBadRequest(title='Bad request', description='Missing body')
            body = json.loads(body_raw)
            return body
        except json.JSONDecodeError as e:
            log(f"Invalid body: {str(e)}", "ERROR")

    def validate_event_token(self, req):
        event = req.get_header('X-Gitlab-Event')
        if event not in ['Push Hook', 'System Hook']:
            log(f"Invalid X-Gitlab-Event header: {event}", "ERROR")
            raise gitlab.GitlabHeadError(error_message='Invalid X-Gitlab-Event header', response_code=400)

        token = req.get_header('X-Gitlab-Token')
        if token != get_settings()['gitlab']['secret']:
            log("Unauthorized access: Invalid token", "ERROR")
            raise gitlab.GitlabAuthenticationError(error_message='Unauthorized access: Invalid token', response_code=401)

    def set_response(self, resp, status, message):
        resp.status = status
        resp.media = {"status": message}

    def handle_error(self, resp, status, error_message, exception=None):
        if exception:
            log(f"Error: {str(exception)}", "ERROR")
        else:
            log(f"Error: {error_message}", "ERROR")
        resp.status = status
        resp.media = {"error": error_message}
