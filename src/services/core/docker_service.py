import docker
import docker.errors
from secure.src.util.setup import get_settings
from secure.src.util.logger import log

class DockerService:
    def __init__(self) -> None:
        self.client = docker.from_env()
        self.reg_settings = get_settings()['registry']

    def build_image(self, repo_path, image_name):
        try:
            log(f"Building image {image_name} at {repo_path}")
            self.client.images.build(path=repo_path, tag=image_name)
            log(f"Image {image_name} built successfully")
        except docker.errors.BuildError as e:
            log(f"Build error for image {image_name}: {str(e)}", "ERROR")
            raise Exception(f"Build error for image {image_name}: {e}")
        except docker.errors.APIError as e:
            log(f"Docker API error while building image {image_name}: {str(e)}", "ERROR")
            raise Exception(f"Docker API error while building image {image_name}: {e}")
        except Exception as e:
            log(f"Unexpected error building image {image_name}: {str(e)}", "ERROR")
            raise Exception(f"Unexpected error building image {image_name}: {e}")

    def build_and_push_image(self, repo_path, run_id):
        image_name = self.generate_image_name(run_id)
        log(f"Image name: {image_name}")
        try:
            self.build_image(repo_path, image_name)
            self.push_and_remove_image(image_name)
            log(f"Image {image_name} built and pushed")
            return image_name
        except Exception as e:
            log(f"Failed to build and push image {image_name}: {str(e)}", "ERROR")
            raise Exception(f"Failed to build and push image {image_name}: {e}")

    def generate_image_name(self, run_id):
        try:
            return f"{self.reg_settings['url']}/{self.reg_settings['project']}/{run_id}"
        except KeyError as e:
            log(f"Missing registry setting: {str(e)}", "ERROR")
            raise Exception(f"Missing registry setting: {e}")

    def login_to_registry(self):
        try:
            url = self.reg_settings.get("url")
            username = self.reg_settings.get("username")
            password = self.reg_settings.get("password")
            self.client.login(username=username, password=password, registry=url)
            log(f"Logged in to registry {url} successfully")
        except docker.errors.APIError as e:
            log(f"Docker API error while logging into registry {url}: {str(e)}", "ERROR")
            raise Exception(f"Docker API error while logging into registry {url}: {e}")
        except Exception as e:
            log(f"Unexpected error logging into registry {url}: {str(e)}", "ERROR")
            raise Exception(f"Unexpected error logging into registry {url}: {e}")

    def push_image(self, image_name):
        try:
            self.client.images.push(image_name)
            log(f"Image {image_name} pushed successfully")
        except docker.errors.APIError as e:
            log(f"Docker API error while pushing image {image_name}: {str(e)}", "ERROR")
            raise Exception(f"Docker API error while pushing image {image_name}: {e}")
        except Exception as e:
            log(f"Unexpected error pushing image {image_name}: {str(e)}", "ERROR")
            raise Exception(f"Unexpected error pushing image {image_name}: {e}")

    def remove_image(self, image_name):
        try:
            self.client.images.remove(image_name)
            log(f"Image {image_name} removed successfully")
        except docker.errors.APIError as e:
            log(f"Docker API error while removing image {image_name}: {str(e)}", "ERROR")
            raise Exception(f"Docker API error while removing image {image_name}: {e}")
        except Exception as e:
            log(f"Unexpected error removing image {image_name}: {str(e)}", "ERROR")
            pass

    def remove_dangling(self):
        try:
            for image in self.client.images.list():
                if not image.tags:
                    self.client.images.remove(image.id)
        except docker.errors.APIError as e:
            log(f"Docker API error while removing dangling images: {str(e)}", "ERROR")
        except Exception as e:
            log(f"Unexpected error while removing dangling images: {str(e)}", "ERROR")

    def push_and_remove_image(self, image_name):
        try:
            self.login_to_registry()
            self.push_image(image_name)
            self.remove_image(image_name)
            self.remove_dangling()
        except Exception as e:
            log(f"Failed to push and remove image {image_name}: {str(e)}", "ERROR")
            raise Exception(f"Failed to push and remove image {image_name}: {e}")
