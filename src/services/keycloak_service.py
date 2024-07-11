from keycloak import KeycloakAuthenticationError, KeycloakGetError, KeycloakAdmin, KeycloakOpenIDConnection, KeycloakPostError, KeycloakOpenID
from typing import Dict, List
from secure.src.util.setup import get_settings
from secure.src.util.logger import log


class KeycloakService:
    def __init__(self):
        self.kc_settings = get_settings()['keycloak']
        keycloak_connection = KeycloakOpenIDConnection(
            server_url=self.kc_settings['url'],
            username=self.kc_settings['username'],
            password=self.kc_settings['password'],
            realm_name=self.kc_settings['realm'],
            client_id=self.kc_settings['admin-cli']['client_id'],
            verify=True)
        self.keycloak_admin = KeycloakAdmin(connection=keycloak_connection)

        self.keycloak_openid = KeycloakOpenID(
            server_url=self.kc_settings['url'],
            realm_name=self.kc_settings['realm'],
            client_id=self.kc_settings['database-service']['client_id'],
            client_secret_key=self.kc_settings['database-service']['client_secret'],
        )

    def validate_token(self, token):
        try:
            log(f"Validating token: {token}")
            userinfo = self.keycloak_openid.introspect(token)
            log(f"Token introspection response: {userinfo}")
            if userinfo['active']:
                log("Token is active")
                return True
            else:
                log("Token is inactive")
                return False
        except KeycloakAuthenticationError as e:
            log(f"Token validation error: {str(e)}", "ERROR")
            return False

    def get_access_token(self, username, password, grant_type='password'):
        try:
            log(f"Requesting access token with username: {username}, grant_type: {grant_type}")
            token_response = self.keycloak_openid.token(username=username, password=password, grant_type=grant_type)
            log(f"Access token response: {token_response}")
            return token_response
        except Exception as e:
            log(f"Error obtaining access token: {str(e)}", "ERROR")
            return None
    def check_user_in_group(self, user_id, group):
        return False

    def check_user_has_role(self, user_id, client_id, role):
        return False

# Instantiate the KeycloakService
keycloak_service = KeycloakService()

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

def validate_token(token):
    return keycloak_service.validate_token(token)

def get_access_token(username, password, grant_type):
    return keycloak_service.get_access_token(username,password,grant_type)

def get_user_groups(keycloak_user_id: str) -> List[str]:
    client = _with_keycloak_client()

    try:
        groups = client.get_user_groups(keycloak_user_id)
    except KeycloakGetError as e:
        log(f'User {keycloak_user_id} not found. Details: {e}', "ERROR")
        return None

    return groups

def get_user_realm_roles(user_id: str) -> Dict[str, any]:
    client = _with_keycloak_client()
    try:
        roles = client.get_realm_roles_of_user(user_id)
    except KeycloakGetError as e:
        log(f'Error fetching realm roles for user {user_id}. Details: {e}', "ERROR")
        return None
    return roles

def get_user_client_roles(user_id: str, client_id: str) -> Dict[str, any]:
    client = _with_keycloak_client()
    try:
        # Fetch clients to find the correct client ID
        clients = client.get_clients()
        client_internal_id = next((c['id'] for c in clients if c['clientId'] == client_id), None)

        if not client_internal_id:
            log(f'Client with clientId {client_id} not found', "ERROR")
            return None

        roles = client.get_client_roles_of_user(user_id=user_id, client_id=client_internal_id)
    except KeycloakGetError as e:
        log(f'Error fetching client roles for user {user_id} in client {client_id}. Details: {e}', "ERROR")
        return None
    return roles

def get_user_groups(user_id: str) -> List[Dict[str, any]]:
    client = _with_keycloak_client()
    try:
        groups = client.get_user_groups(user_id=user_id)
    except KeycloakGetError as e:
        log(f'Error fetching groups for user {user_id}. Details: {e}', "ERROR")
        return []
    return groups

def get_user_client_roles(user_id: str, client_id: str) -> List[Dict[str, any]]:
    client = _with_keycloak_client()
    try:
        clients = client.get_clients()
        client_internal_id = next((c['id'] for c in clients if c['clientId'] == client_id), None)

        if not client_internal_id:
            log(f'Client with clientId {client_id} not found', "ERROR")
            return []

        roles = client.get_client_roles_of_user(user_id=user_id, client_id=client_internal_id)
    except KeycloakGetError as e:
        log(f'Error fetching client roles for user {user_id} in client {client_id}. Details: {e}', "ERROR")
        return []
    return roles

def check_user_in_group(user_id: str, group_name: str) -> bool:
    groups = get_user_groups(user_id)
    group_names = [group['name'] for group in groups]
    log(f"User groups: {group_names}")
    if group_name in group_names:
        log(f"User is part of the '{group_name}' group.")
        return True
    else:
        log(f"User is not part of the '{group_name}' group.")
        return False

def check_user_has_role(user_id: str, client_id: str, role_name: str) -> bool:
    roles = get_user_client_roles(user_id, client_id)
    role_names = [role['name'] for role in roles]
    log(f"User roles: {role_names}")
    if role_name in role_names:
        log(f"User has access to '{role_name}' role.")
        return True
    else:
        log(f"User does not have the '{role_name}' role.")
        return False


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
    if not internal_client_id:
        log(f'Error: Client {client_id} not found', "ERROR")
        return False

    try:
        roles = client.get_client_roles(client_id=internal_client_id)
    except KeycloakGetError as e:
        log(f'Error fetching roles for client {client_id}. Details: {e}', "ERROR")
        return False

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
