import docker
from src.util.setup import get_settings
from src.util.logger import log

class DockerService:
    def __init__(self):
        self.client = docker.from_env()
        self.reg_settings = get_settings()["registry"]

    def _with_docker(self):
        return self.client

    def build_image(self, repo_path, image_name):
        log(f"Building image {image_name} at {repo_path}")
        client = self._with_docker()
        try:
            client.images.build(path=repo_path, tag=image_name)
            log(f"Image {image_name} built successfully")
        except Exception as e:
            log(f"Error building image {image_name}: {str(e)}", "ERROR")
            raise Exception(f"Error building image {image_name}: {e}")

    def push_and_remove_image(self, image_name):
        log(f"Pushing image {image_name}")
        client = self._with_docker()
        url = self.reg_settings.get("url")
        username = self.reg_settings.get("username")
        password = self.reg_settings.get("password")

        try:
            client.login(username=username, password=password, registry=url)
            log(f"Logged in to registry {url} successfully")
        except Exception as e:
            log(f"Error in login to registry {url}: {str(e)}", "ERROR")
            raise Exception(f"Error in login to registry {url}: {e}")

        try:
            client.images.push(image_name)
            log(f"Image {image_name} pushed successfully")
        except Exception as e:
            log(f"Error pushing image {image_name}: {str(e)}", "ERROR")
            raise Exception(f"Error pushing image {image_name}: {e}")

        try:
            client.images.remove(image_name)
            log(f"Image {image_name} removed successfully")
        except Exception as e:
            log(f"Error removing image {image_name}: {str(e)}", "ERROR")
            pass

        self.remove_dangling()

    def remove_dangling(self):
        client = self._with_docker()
        for image in client.images.list():
            if not image.tags:
                client.images.remove(image.id)

