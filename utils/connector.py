import mysql.connector

def fetch_cursor(env):
    connection = fetch_connection(env)
    return [connection.cursor(dictionary=True), connection]

def fetch_connection(env):
    return mysql.connector.connect(
        host=env['HOST'],
        user=env['USER'],
        password=env['PASSWORD'],
        database=env['DATABASE'],
        port=env['PORT']
    )

def close_connection(connection):
    connection.close()