from src.services.setup import load_settings
from src.services.logger import log
from src.util.server import Server

if __name__ == '__main__':
    try:
        log('Loading settings...')
        load_settings()

        log("Starting up...")
        server = Server()
        server.run()

    except KeyboardInterrupt:
        pass
    except Exception as e:
        log(f'Error: {e}', "ERROR")
    finally:
        log('Shutting down...')
