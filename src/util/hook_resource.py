import datetime
import os
import uuid
import falcon
import json
import threading

from cerberus import Validator
from src.util.logger import log
from src.util.setup import get_settings
import src.services.gitlab_service as gitlab_service
import src.services.keycloak_service as keycloak_service
import src.services.mysql_service as mysql_service
import src.services.docker_service as docker_service
import src.services.k8s_service as k8s_service
import src.services.db_service as db_service

# Helper functions
def validate_event_token(event, req):
    if event not in ['Push Hook', 'System Hook']:
        log(f"Invalid X-Gitlab-Event header: {event}", "ERROR")
        raise falcon.HTTPBadRequest(
            title='Bad request',
            description='Invalid X-Gitlab-Event header'
        )
    if req.get_header('X-Gitlab-Token') != get_settings()['gitlab']['secret']:
        log("Unauthorized access: Invalid token", "ERROR")
        raise falcon.HTTPUnauthorized(
            title='Unauthorized',
            description='Invalid token'
        )

def parse_request_body(req):
    body_raw = req.bounded_stream.read()
    if not body_raw:
        log("Missing body in request", "ERROR")
        raise falcon.HTTPBadRequest(
            title='Bad request',
            description='Missing body'
        )

    try:
        body = json.loads(body_raw)
    except json.JSONDecodeError as e:
        log(f"Invalid body: {str(e)}", "ERROR")
        raise falcon.HTTPBadRequest(
            title='Bad request',
            description='Invalid body'
        )

    log(f"Request body: {body}")
    return body

def validate_body_schema(body):
    schema = {
    'event_name': {'type': 'string'},
    'ref': {'type': 'string'},
    'user_id': {'type': 'integer'},
    'project_id': {'type': 'integer'},
    'project': {
        'type': 'dict',
        'schema': {
            'http_url': {'type': 'string'},
        }
    },
    'commits': {
        'type': 'list',
        'schema': {
            'type': 'dict',
            'schema': {
                'id': {'type': 'string'},
            }
        }
    }
    }
    v = Validator(schema)
    v.allow_unknown = True
    if not v.validate(body):
        log(f"Invalid body: {v.errors}", "ERROR")
        raise falcon.HTTPBadRequest(
            title='Bad request',
            description=f'Invalid body: {v.errors}'
        )

def validate_event_name(body):
    if body['event_name'] != 'push':
        log(f"Invalid event_name: {body['event_name']}", "ERROR")
        raise falcon.HTTPBadRequest(
            title='Bad request',
            description=f'Invalid event_name: {body["event_name"]}'
        )

def validate_commit_branch(body):
    if body['ref'] != 'refs/heads/main':
        log(f"Commit is not from main branch: {body['ref']}", "ERROR")
        raise falcon.HTTPBadRequest(
            title='Bad request',
            description=f'Commit is not from main branch: {body["ref"]}'
        )

def validate_commits_signature(body):
    log(f"Found {len(body['commits'])} commits - {body['project']['path_with_namespace']}")
    for push_commit in body['commits']:
        signature = gitlab_service.get_signature(body['project_id'], push_commit['id'])
        if signature is None:
            log(f"No signature found for commit {push_commit['id']}", "ERROR")
            raise falcon.HTTPBadRequest(
                title='Bad request',
                description=f'No signature found for commit {push_commit["id"]}'
            )

        if signature['verification_status'] != 'verified':
            log(f"Found signature, but it is not verified: {signature['verification_status']}", "ERROR")
            raise falcon.HTTPBadRequest(
                title='Bad request',
                description=f'Signature not verified for commit {push_commit["id"]}'
            )

    log(f"All {len(body['commits'])} commits have a verified signature")

def validate_dockerfile_presence(body):
    if not gitlab_service.has_file_in_repo(body['project_id'], 'Dockerfile', body['ref']):
        log(f"No Dockerfile found in project {body['project_id']}", "ERROR")
        raise falcon.HTTPBadRequest(
            title='Bad request',
            description=f'No Dockerfile found in project {body["project_id"]}'
        )

def validate_body(body):
    validate_body_schema(body)
    validate_event_name(body)
    validate_commit_branch(body)
    validate_commits_signature(body)
    validate_dockerfile_presence(body)

def handle_database_info(run_meta, keycloak_groups):
    db_host = db_service.get_database_host(run_meta['db_type'])
    db_user = db_pass = None
    if(run_meta['db_type'] == "mysql"):
        mysql_groups = [group['path'][len('/mysql_'):] for group in keycloak_groups if group['path'].startswith('/mysql_')]
        log(f"MySQL groups: {mysql_groups}")
        db_user, db_pass = mysql_service.create_mysql_user(mysql_groups, run_meta['db_database'])
        log(f"MySQL user created: {db_user}")
    return db_host, db_user, db_pass

def build_and_push_image(repo_path, run_id):
    reg_settings = get_settings()['registry']
    image_name = f"{reg_settings['url']}/{reg_settings['project']}/{run_id}"
    log(f"image name: {image_name}")
    docker_service.build_image(repo_path, image_name)
    docker_service.push_and_remove_image(image_name)
    log(f"Image {image_name} built and pushed")
    return image_name

def handle_cache_dir(run_meta, keycloak_user_id, run_id):
    cache_dir = mount_path = None
    if "cache_dir" in run_meta and run_meta["cache_dir"]:
        mount_path = run_meta.get('mount_path', '/cache')
        log(f"Found custom mount_path: {mount_path}" if 'mount_path' in run_meta else "Using default mount_path: /cache")

        cache_dir = run_meta['cache_dir']
        cache_path = f"{get_settings()['path']['cachePath']}/{keycloak_user_id}/{cache_dir}"
        log(f"Found cache_dir: {cache_path}")

        if not os.path.exists(cache_path):
            os.makedirs(cache_path)
            log(f"Cache directory created at: {cache_path}")

        pvc_repo_path = get_settings()['k8s']['pvcPath']
        k8s_service.create_persistent_volume(run_id, f'{pvc_repo_path}/cache/{keycloak_user_id}/{cache_dir}', "cache")
        log(f"Cache PVC created for {run_id}")
    return cache_dir, mount_path

def create_pod(run_id, image_name, db_user, db_pass, db_host, db_database, gpu, mount_path):
    k8s_service.create_pod(run_id, image_name, {
        "DB_USER": db_user,
        "DB_PASS": db_pass,
        "DB_HOST": db_host,
        "DB_DATABASE": db_database,
        "OUTPUT_PATH": '/output',
        "SECD": 'PRODUCTION'
    }, gpu, mount_path)
    log(f"Pod created for {run_id}")

def create(body):
    try:
        log("Starting create process...")

        # Get user info
        gitlab_user_id = body['user_id']
        keycloak_user_id = gitlab_service.get_idp_user_id(gitlab_user_id)

        # Clone repo
        run_id = str(uuid.uuid4()).replace('-', '')
        repo_path = f"{get_settings()['path']['repoPath']}/{run_id}"
        gitlab_service.clone(body["project"]["http_url"], repo_path)

        # Create output path
        date = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_path = f'{repo_path}/outputs/{date}-{run_id}'
        os.makedirs(output_path)
        log(f"Output path created: {output_path}")

        # Get metadata
        run_meta = gitlab_service.get_metadata(f"{repo_path}/secd.yml")
        run_for = run_meta['runfor']
        gpu = run_meta['gpu']
        db_database = run_meta['db_database'] # TODO: This will specify which database that also exists in keycloak

        # Check if user has 'mysql_test' role in 'database-service' client and is in 'secd' group
        user_have_permission = False
        if keycloak_service.check_user_has_role(keycloak_user_id, "database-service", "mysql_test") and keycloak_service.check_user_in_group(keycloak_user_id, "secd"):
            user_have_permission = True

        if user_have_permission:
            log("User has the necessary permissions.")
            temp_user_id = keycloak_service.create_temp_user("temp_" + keycloak_user_id, "temp_password")
        else:
            log("User does not have the necessary permissions.")
            return

        # Get database info
        image_name = build_and_push_image(repo_path, run_id)

        # Creating namespace
        pvc_repo_path = get_settings()['k8s']['pvcPath']
        k8s_service.create_namespace(temp_user_id, run_id, run_for)
        k8s_service.create_persistent_volume(run_id, f'{pvc_repo_path}/repos/{run_id}/outputs/{date}-{run_id}')
        log(f"Namespace and output volume created for {run_id}")

        # Creating pod
        cache_dir, mount_path = handle_cache_dir(run_meta, keycloak_user_id, run_id)

        k8s_service.create_pod(run_id, image_name, {
            "DB_USER": get_settings()['db']['mysql']['username'],
            "DB_PASS": get_settings()['db']['mysql']['password'],
            "DB_HOST": get_settings()['db']['mysql']['host'],
            "DB_DATABASE": db_database,
            "OUTPUT_PATH": '/output',
            "SECD": 'PRODUCTION'
        }, gpu, mount_path)
        log(f"Pod created for {run_id}")

    except Exception as e:
        log(f"Error in create process: {str(e)}", "ERROR")
    else:
        log(f"Successfully launched {run_id}")
    finally:
        if(user_have_permission):
            keycloak_service.delete_temp_user(temp_user_id)
            log(f"temp user deleted: {temp_user_id}")


 # HookResource class
class HookResource:
    def on_post(self, req, resp):
        log("Starting hook process...")

        event = req.get_header('X-Gitlab-Event')
        validate_event_token(event,req)
        log(f"Received event: {event}")

        body = parse_request_body(req)
        validate_body(body)
        log(f"Request body: {body}")

        resp.status = falcon.HTTP_200
        resp.media = {"status": "success"}
        log("Hook processing completed successfully")

        threading.Thread(target=create(body=body)).start()

        resp.status = falcon.HTTP_200
