import sys
import os
import uuid
from keycloak import KeycloakAdmin, KeycloakGetError
import jwt


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

import secure.src.services.keycloak_service as keycloak_service
from secure.src.util.setup import load_settings
from secure.src.util.logger import log


os.environ['CONFIG_FILE'] = '/home/cloud/secd/config.yml'
load_settings()
keycloak_service._with_keycloak_client()

def decode_token(token: str):
    try:
        decoded = jwt.decode(token, options={"verify_signature": False})
        log(f'Token contents: {decoded}', "DEBUG")
        return decoded
    except jwt.DecodeError as e:
        log(f'Error decoding token: {e}', "ERROR")
        return None


def create_temp_user(username: str, password: str) -> str:
    try:
        user_id = keycloak_service.create_temp_user(username, password)
    except KeycloakGetError as e:
        log(f'Error creating user {username}. Details: {e}', "ERROR")
        return None

    return user_id

def delete_temp_user(user_id: str) -> bool:
    try:
        keycloak_service.delete_temp_user(user_id)
    except KeycloakGetError as e:
        log(f'Error deleting user {user_id}. Details: {e}', "ERROR")
        return False

    return True

def assign_role_to_user(user_id: str, client_id: str, role: str) -> bool:
    client = keycloak_service._with_keycloak_client()

    # Investigate the token
    token = client.connection.token['access_token']
    decoded_token = jwt.decode(token, options={"verify_signature": False})
    log(f'Token contents: {decoded_token}', "DEBUG")

    # Fetch the internal client ID for the given client name
    try:
        clients = client.get_clients()
    except KeycloakGetError as e:
        log(f'Error fetching clients: {e.response_code}, Details: {e.response_body}', "ERROR")
        return False

    internal_client_id = next((c['id'] for c in clients if c['clientId'] == client_id), None)

    if internal_client_id is None:
        log(f'Error: Client {client_id} not found', "ERROR")
        return False

    # Fetch the client IDs
    client_ids = [c['id'] for c in clients]
    log(f"Client IDs: {client_ids}")

    # Fetch the roles for the client to ensure the role exists
    try:
        roles = client.get_client_roles(client_id=internal_client_id)
        log(f"Roles for client {client_id}: {roles}")
    except KeycloakGetError as e:
        log(f'Error fetching roles for client {client_id}. Details: {e}', "ERROR")
        return False

    # Find the role representation by name
    role_representation = next((r for r in roles if r['name'] == role), None)

    if role_representation is None:
        log(f'Error: Role {role} not found for client {client_id}', "ERROR")
        return False

    try:
        client.assign_client_role(user_id=user_id, client_id=internal_client_id, roles=[role_representation])
    except KeycloakPostError as e:
        log(f'Error assigning role {role} to user {user_id}. Details: {e}', "ERROR")
        return False

    return True


# Test the functions
if __name__ == "__main__":
    username = "testuser"
    password = "testpassword"

    user_id = create_temp_user(username, password)
    if user_id:
        log(f"User {username} created with ID: {user_id}")
        role = "Gitlab Access"
        client_id = "gitlab"

        if assign_role_to_user(user_id, client_id, role):
            log(f"Role {role} assigned to user ID: {user_id}")

        #if delete_temp_user(user_id):
        #    log(f"User ID: {user_id} deleted")