from databaseconverter.db.databases.DatabaseConnector import DatabaseConnector
import psycopg2


class Postgres(DatabaseConnector):
    def __init__(self, config):
        self.conn = None
        self.config = config

    #def get_connection(self):
    #    self.conn = mysql.connector.connect(**self.config)

    def open_connection(self):
        self.conn = psycopg2.connect(**self.config)
        return self.conn

    def close_connection(self):
        if self.conn.is_connected():
            self.conn.cursor().close()
            self.conn.close()
