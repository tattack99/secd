import sys
import os
from typing import Any, Dict
import pytest
import requests

os.environ['CONFIG_FILE'] = '/home/cloud/secd/config/config.yml'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

from secure.src.services.keycloak_service import KeycloakService
from secure.src.util.setup import get_settings

BASE_URL = 'http://localhost:8001/v1/database'
KEYCLOAK_URL = "https://iam.secd/realms/cloud/protocol/openid-connect/token"
CLIENT_ID = get_settings()['keycloak']['database-service']['client_id']
CLIENT_SECRET = get_settings()['keycloak']['database-service']['client_secret']
GRANT_TYPE = "client_credentials"
ROLE = "mysql_test"
USERNAME = "temp_user"
PASSWORD = "temp_password"

@pytest.fixture(scope='module')
def setup_keycloak_user():
    keycloak_service = KeycloakService()

    user_id = keycloak_service.create_temp_user(USERNAME, PASSWORD)
    if user_id:
        print(f"User {USERNAME} created with ID: {user_id}")

        if keycloak_service.assign_role_to_user(user_id, CLIENT_ID, ROLE):
            print(f"Role {ROLE} assigned to user ID: {user_id}")

        yield user_id

        if keycloak_service.delete_temp_user(user_id):
            print(f"User ID: {user_id} deleted")
    else:
        raise Exception("Failed to create user")

def test_get_database_no_header():
    response = requests.get(BASE_URL)
    print("Database service response status code:", response.status_code)
    print("Database service response headers:", response.headers)
    print("Database service response body:", response.text)
    assert 401 == response.status_code, f"Expected 401 but got {response.status_code}"


def test_get_database_incorrect_auth():
    keycloak_service = KeycloakService()
    token = keycloak_service.get_access_token_username_password(USERNAME, PASSWORD)
    headers = {'Authorization': f'Bearer {token} invalid'}

    response = requests.get(BASE_URL, headers=headers)
    print("Database service response status code:", response.status_code)
    print("Database service response headers:", response.headers)
    print("Database service response body:", response.text)
    assert 401 == response.status_code, f"Expected 401 but got {response.status_code}"

def test_get_database_correct_auth():
    keycloak_service = KeycloakService()
    token_response: Dict[str, str] = keycloak_service.get_access_token_username_password(USERNAME, PASSWORD)
    token = token_response['access_token']
    headers = {'Authorization': f'Bearer {token}'}

    print(f'headers: {headers}')
    response = requests.get(BASE_URL, headers=headers)
    print("Database service response status code:", response.status_code)
    print("Database service response headers:", response.headers)
    print("Database service response body:", response.text)
    assert 200 == response.status_code, f"Expected 200 but got {response.status_code}"

