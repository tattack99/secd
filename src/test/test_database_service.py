import sys
import os
import requests

os.environ['CONFIG_FILE'] = '/home/cloud/secd/config/config.yml'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

BASE_URL = 'http://localhost:8082/v1/database'

def test_get_database():
    response = requests.get(BASE_URL)
    assert response.status_code == 200
    assert response.json() == {"database": "mysql"}

def test_post_database():
    data = {"database": "mysql"}
    response = requests.post(BASE_URL, json=data)
    assert response.status_code == 200
    assert response.json() == {"host": "192.168.1.1"}

def test_put_database():
    data = {"database": "mysql"}
    response = requests.put(BASE_URL, json=data)
    assert response.status_code == 200
    assert response.json() == {"host": "192.168.1.1"}

def test_delete_database():
    data = {"database": "mysql"}
    response = requests.delete(BASE_URL, json=data)
    assert response.status_code == 200
    assert response.json() == {"host": "192.168.1.1"}