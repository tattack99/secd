import sys
import os
import uuid
from keycloak import KeycloakAdmin, KeycloakGetError, KeycloakPostError
import jwt

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

import secure.src.services.keycloak_service as keycloak_service
from secure.src.util.setup import load_settings
from secure.src.util.logger import log

load_settings()

# Test the functions
def test_integration_set_role_user():
    username = "testuser"
    password = "testpassword"

    # create test user
    user_id = keycloak_service.create_temp_user(username, password)
    if user_id:
        log(f"User {username} created with ID: {user_id}")
        role = "Gitlab Access"
        client_id = "gitlab"

        # set role to user
        if keycloak_service.assign_role_to_user(user_id, client_id, role):
            log(f"Role {role} assigned to user ID: {user_id}")

        # clean up
        if keycloak_service.delete_temp_user(user_id):
            log(f"User ID: {user_id} deleted")