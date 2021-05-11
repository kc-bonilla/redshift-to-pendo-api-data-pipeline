"""This module establishes the connection with our Redshift Warehouse"""
import psycopg2
from singer.logger import get_logger

LOGGER = get_logger()


def select_all(conn, query):
    """creates cursor for psycopg2 to run select statements"""
    cur = conn.cursor()
    cur.execute(query)
    column_specs = cur.fetchall()
    cur.close()
    return column_specs


def open_connection(config):
    """uses config args to open connection to Redshift"""
    host = config.get('host'),
    port = config.get('port'),
    dbname = config.get('dbname'),
    user = config.get('user'),
    password = config.get('password')
    connection = psycopg2.connect(
        host=host[0],
        port=port[0],
        dbname=dbname[0],
        user=user[0],
        password=password)
    LOGGER.info("Connected to Redshift via psycopg2")
    return connection
