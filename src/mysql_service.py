import time
import mysql.connector
import uuid

from typing import List, Tuple
from src.setup import get_settings


def _with_mysql_client() -> mysql.connector.MySQLConnection:
    msqlSettings = get_settings()['db']['mysql']
    client = mysql.connector.connect(
        host=msqlSettings['host'],
        user=msqlSettings['username'],
        password=msqlSettings['password'],
    )

    return client


def create_mysql_user(groups: List[str], database: str) -> Tuple[str, str]:
    client = _with_mysql_client()  # Assuming this function provides a MySQL connection
    cursor = client.cursor()

    # Generate unique user and password
    db_user = str(uuid.uuid4()).replace('-', '')
    db_pass = str(uuid.uuid4()).replace('-', '')

    try:
        # Drop user if exists
        cursor.execute(f"DROP USER IF EXISTS '{db_user}';")
        client.commit()  # Commit the transaction

        # Create new user
        cursor.execute(f"CREATE USER '{db_user}'@'%' IDENTIFIED BY '{db_pass}';")
        client.commit()  # Commit the transaction

        # Create roles and grant read-only permissions
        for group in groups:
            cursor.execute(f"CREATE ROLE IF NOT EXISTS '{group}';")
            cursor.execute(f"GRANT SELECT ON {database}.* TO '{group}';")
            cursor.execute(f"GRANT '{group}' TO '{db_user}'@'%';")
        client.commit()  # Commit the transaction after granting roles

        # Set default roles for the user if any groups are specified
        if groups:
            cursor.execute(f"ALTER USER '{db_user}'@'%' DEFAULT ROLE {', '.join(groups)};")
            client.commit()  # Commit the transaction

        # Directly grant SELECT permissions to the user
        cursor.execute(f"GRANT SELECT ON {database}.* TO '{db_user}'@'%';")
        client.commit()  # Commit the transactio

        return db_user, db_pass
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        client.rollback()  # Rollback in case of any error
        raise
    finally:
        cursor.close()
        client.close()


def delete_mysql_user(db_user: str):
    client = _with_mysql_client()
    client.execute(f"drop user if exists '{db_user}';")

