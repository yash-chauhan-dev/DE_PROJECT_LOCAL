import mysql.connector
from resources.dev import config


def get_mysql_connection():
    connection = mysql.connector.connect(
        host=config.host,
        user=config.properties["user"],
        password=config.properties["password"],
        database=config.database_name
    )
    return connection
