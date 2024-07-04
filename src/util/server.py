import falcon
import threading
from wsgiref.simple_server import make_server

from src.util.logger import log
from src.util.hook_resource import HookResource
from src.util.daemon import Daemon

class Server:
    def __init__(self):
        self.app = falcon.App()
        self.app.add_route('/v1/hook', HookResource())

    def run(self):
        try:
            log("Creating Daemon micrk8s_cleanup...")
            micrk8s_cleanup = Daemon()
            micrk8s_cleanup_thread = threading.Thread(target=micrk8s_cleanup.start_microk8s_cleanup)
            micrk8s_cleanup_thread.start()

            log("Creating Daemon database_service...")
            database_service = Daemon()
            database_service_thread = threading.Thread(target=database_service.start_database_service)
            database_service_thread.start()


        except Exception as e:
            log(f"Error starting Daemon thread: {e}", "ERROR")

        with make_server('', 8080, self.app) as httpd:
            log('Serving on port 8080...')
            httpd.serve_forever()
