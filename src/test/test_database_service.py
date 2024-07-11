import sys
import os
import pytest
import requests

os.environ['CONFIG_FILE'] = '/home/cloud/secd/config/config.yml'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

import secure.src.services.keycloak_service as keycloak_service
from secure.src.util.setup import get_settings

BASE_URL = 'https://gitlab.secd/v1/database'
KEYCLOAK_URL = "https://iam.secd/realms/cloud/protocol/openid-connect/token"
CLIENT_ID = get_settings()['keycloak']['database-service']['client_id']
CLIENT_SECRET = get_settings()['keycloak']['database-service']['client_secret']
GRANT_TYPE = "client_credentials"
ROLE = "mysql_test"
USERNAME = "temp_user"
PASSWORD = "temp_password"



@pytest.fixture(scope='module')
def setup_keycloak_user():
    # Create test user
    user_id = keycloak_service.create_temp_user(USERNAME, PASSWORD)
    if user_id:
        print(f"User {USERNAME} created with ID: {user_id}")

        # Assign role to user
        if keycloak_service.assign_role_to_user(user_id, CLIENT_ID, ROLE):
            print(f"Role {ROLE} assigned to user ID: {user_id}")

        # Yield control to the test
        yield user_id

        # Clean up after the test completes
        if keycloak_service.delete_temp_user(user_id):
            print(f"User ID: {user_id} deleted")
    else:
        raise Exception("Failed to create user")

def get_access_token(username, password):
    response = requests.post(KEYCLOAK_URL, data={
        "grant_type": GRANT_TYPE,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "username": username,
        "password": password
    })
    print("Keycloak response status code:", response.status_code)
    print("Keycloak response body:", response.text)
    if response.status_code != 200:
        response.raise_for_status()
    return response.json().get("access_token")

def test_get_database_authorized(setup_keycloak_user):
    token = keycloak_service.get_access_token(USERNAME, PASSWORD, GRANT_TYPE)
    headers = {'Authorization': f'Bearer {token}'}
    print(f"Authorization header: {headers}")
    response = requests.get(BASE_URL, headers=headers)
    print("Database service response status code:", response.status_code)
    print("Database service response headers:", response.headers)
    print("Database service response body:", response.text)
    assert 200 == response.status_code, f"Expected 200 but got {response.status_code}"

def test_get_database_unauthorized():
    headers = {'Authorization': 'Bearer invalid'}
    print(f"Authorization header: {headers}")
    response = requests.get(BASE_URL, headers=headers)
    print("Database service response status code:", response.status_code)
    print("Database service response headers:", response.headers)
    print("Database service response body:", response.text)
    assert 401 == response.status_code, f"Expected 401 but got {response.status_code}"


def test_post_database():
    pass

def test_put_database():
    pass

def test_delete_database():
    pass
