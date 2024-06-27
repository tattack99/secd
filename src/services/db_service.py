from typing import List, Tuple, Dict

def get_database_host(database : str) -> str:
    if(database == "mysql") : return '192.168.1.1'
    return ""
