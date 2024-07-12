

class DatabaseService:
    def __init__(self):
         pass


    def get_database_host(self, database : str) -> str:
        if(database == "mysql") : return '192.168.1.1'
        return ""