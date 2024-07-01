from uuid import UUID
from keycloak import KeycloakGetError, KeycloakAdmin, KeycloakPostError
from typing import List
from secure.src.util.setup import get_settings
from secure.src.util.logger import log

def _with_keycloak_client() -> KeycloakAdmin:
    kcSettings = get_settings()['keycloak']
    client = KeycloakAdmin(
        server_url=kcSettings['url'],
        username=kcSettings['username'],
        password=kcSettings['password'],
        realm_name=kcSettings['realm'],
        verify=True
    )
    return client

def get_keycloak_user_groups(keycloak_user_id: str) -> List[str]:
    client = _with_keycloak_client()

    try:
        groups = client.get_user_groups(keycloak_user_id)
    except KeycloakGetError as e:
        log(f'User {keycloak_user_id} not found. Details: {e}', "ERROR")
        return None

    return groups

def create_temp_user(username: str, password: str) -> str:
    client = _with_keycloak_client()

    UserRepresentation = {
        "username": username,
        "enabled": True,
        "credentials": [{"type": "password", "value": password, "temporary": False}],
        "firstName": "Temp",
        "lastName": "User",
        "email": f"{username}@example.com",
        "emailVerified": False
    }

    try:
        user_id = client.create_user(UserRepresentation)
    except KeycloakGetError as e:
        log(f'Error creating user {username}. Details: {e}', "ERROR")
        return None

    return user_id

def delete_temp_user(user_id: str) -> bool:
    client = _with_keycloak_client()

    try:
        client.delete_user(user_id)
    except KeycloakGetError as e:
        log(f'Error deleting user {user_id}. Details: {e}', "ERROR")
        return False

    return True

def assign_role_to_user(user_id: str, client_id: str, role: str) -> bool:
    client = _with_keycloak_client()

    try:
        clients = client.get_clients()
    except KeycloakGetError as e:
        log(f'Error fetching clients: {e.response_code}, Details: {e.response_body}', "ERROR")
        return False

    internal_client_id = next((c['id'] for c in clients if c['clientId'] == client_id), None)
    try:
        roles = client.get_client_roles(client_id=internal_client_id)
        log(f"Roles for client {client_id}: {roles}")
    except KeycloakGetError as e:
        log(f'Error fetching roles for client {client_id}. Details: {e}', "ERROR")
        return False

    role_representation = next((r for r in roles if r['name'] == role), None)
    if role_representation is None:
        log(f'Error: Role {role} not found for client {client_id}', "ERROR")
        return False


    try:
        client.assign_client_role(user_id=user_id, client_id=client_id, roles=[role_representation])
    except KeycloakPostError as e:
        log(f'Error assigning role {role} to user {user_id}. Details: {e}', "ERROR")
        return False

    return True
