import yaml
import os
from cerberus import Validator
from dotenv import load_dotenv
from src.util.logger import log

# Load environment variables from .env file if it exists
load_dotenv()
settings = {}

def get_settings():
    """Get settings from config file"""
    global settings
    if settings == {}:
        load_settings()
    return settings

def load_settings(config_path=None):
    """Load settings from config file"""
    log('Loading settings...')

    # If config_path is not provided, use the environment variable or default path
    if config_path is None:
        config_path = os.getenv("CONFIG_FILE")
        if not config_path:
            raise Exception("CONFIG_FILE environment variable is not set and no config_path provided")

    if not os.path.exists(config_path):
        raise Exception(f'Config file not found: {config_path}')

    with open(config_path, 'r') as yaml_file:
        loaded_yaml = yaml.load(yaml_file, Loader=yaml.FullLoader)

    # match against schema
    schema_path = "/home/cloud/secd/secure/config/settings-schema.yml"
    if not os.path.exists(schema_path):
        raise Exception(f'Schema file not found: {schema_path}')

    with open(schema_path, 'r') as schema_file:
        schema = yaml.load(schema_file, Loader=yaml.FullLoader)

    v = Validator(schema)
    if not v.validate(loaded_yaml):
        raise Exception(f'Invalid config file: {v.errors}')

    global settings
    settings = loaded_yaml
