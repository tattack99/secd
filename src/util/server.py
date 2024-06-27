import falcon
import threading
from wsgiref.simple_server import make_server

from src.util.logger import log
from src.util.HookResource import HookResource
from src.util.Daemon import Daemon

class Server:
    def __init__(self):
        self.app = falcon.App()
        self.app.add_route('/v1/hook', HookResource())

    def run(self):
        try:
            log("Creating Daemon instance...")
            daemon = Daemon()

            log("Starting Daemon thread...")
            daemon_thread = threading.Thread(target=daemon.run)
            daemon_thread.start()

        except Exception as e:
            log(f"Error starting Daemon thread: {e}", "ERROR")

        with make_server('', 8080, self.app) as httpd:
            log('Serving on port 8080...')
            httpd.serve_forever()
