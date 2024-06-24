import docker
from src.setup import get_settings
from src.logger import log


def _with_docker():
    client = docker.from_env()
    return client


def build_image(repo_path, image_name):
    log(f"Building image {image_name} at {repo_path}") # 1
    client = _with_docker()
    try:
        client.images.build(path=repo_path, tag=image_name)
        log(f"Image {image_name} built successfully")
    except Exception as e:
        log(f"Error building image {image_name}: {str(e)}", "ERROR")
        raise Exception(f"Error building image {image_name}: {e}")


def push_and_remove_image(image_name):
    log(f"Pushing image {image_name}") # 2
    client = _with_docker()
    regSettings = get_settings()["registry"]

    url = regSettings.get("url")
    username = regSettings.get("username")
    password = regSettings.get("password")

    client = _with_docker()
    try:
        client.login(username=username, password=password, registry=url)
        log(f"Logged in to registry {url} successfully")
    except Exception as e:
        log(f"Error in login to registry {url}: {str(e)}", "ERROR")
        raise Exception(f"Error in login to registry {image_name}: {e}")

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

    remove_dangling()


def remove_dangling():
    client = _with_docker()

    for image in client.images.list():
        if image.tags == None:
            client.images.remove(image.id)
