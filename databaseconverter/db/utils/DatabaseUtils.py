from databaseconverter.db.databases.Mysql import Mysql
from databaseconverter.db.databases.Postgres import Postgres


class DatabaseUtils:
    def __init__(self, db, host, user, password, database, port):
        self.db = db
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.config = {
            'host': self.host,
            'user': self.user,
            'password': self.password,
            'database': self.database,
            'port': self.port
        }
        # 'host': 'localhost',
        # 'user': 'anderson',
        # 'password': 'root',
        # 'database': 'locadora'

    def get_connection(self):
        db_obj = None
        match self.db:
            case 'mysql':
                db_obj = Mysql(self.config)
            case 'postgres':
                db_obj = Postgres(self.config)
            case 'oracle':
                print('oracle')
            case 'sqlserver':
                print('sqlserver')
            case _:
                raise Exception('Database not supported.')
        return db_obj

    def get_all_tables_sql(self):
        command = ''
        match self.db:
            case 'mysql':
                command = 'show tables'
            case 'postgres':
                command = 'SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\';'
            case 'oracle':
                command = ''
            case 'sqlserver':
                command = ''
            case _:
                raise Exception('Database not supported.')
        return command

    def get_table_create_sql(self, table_name):
        command = ''
        match self.db:
            case 'mysql':
                command = f"SHOW CREATE TABLE {table_name}"
                # command = "SELECT CONCAT('CREATE TABLE ', GROUP_CONCAT(column_definition SEPARATOR ', '), ';') AS create_table_statement " \
                #          "FROM (SELECT CONCAT(column_name, ' ', column_type) AS column_definition FROM INFORMATION_SCHEMA.COLUMNS " \
                #          "WHERE table_name = '{table_name}') AS columns_definition;"
            case 'postgres':
                command = "SELECT 'CREATE TABLE ' || table_name || ' (' || string_agg(column_name || ' ' || data_type || " \
                          "CASE WHEN character_maximum_length IS NOT NULL THEN '(' || character_maximum_length || ')'" \
                          "ELSE '' END, ', ') || ');'" \
                          f"FROM information_schema.columns WHERE table_name = '{table_name}'" \
                          "GROUP BY table_name;"
            case 'oracle':
                command = ''
            case 'sqlserver':
                command = ''
            case _:
                raise Exception('Database not supported.')
        return command

    def adapt_data_type(self):
        command = ''
        match self.db:
            case 'mysql':
                command = f"SHOW CREATE TABLE {table_name}"
                # command = "SELECT CONCAT('CREATE TABLE ', GROUP_CONCAT(column_definition SEPARATOR ', '), ';') AS create_table_statement " \
                #          "FROM (SELECT CONCAT(column_name, ' ', column_type) AS column_definition FROM INFORMATION_SCHEMA.COLUMNS " \
                #          "WHERE table_name = '{table_name}') AS columns_definition;"
            case 'postgres':
                command = "SELECT 'CREATE TABLE ' || table_name || ' (' || string_agg(column_name || ' ' || data_type || " \
                          "CASE WHEN character_maximum_length IS NOT NULL THEN '(' || character_maximum_length || ')'" \
                          "ELSE '' END, ', ') || ');'" \
                          f"FROM information_schema.columns WHERE table_name = '{table_name}'" \
                          "GROUP BY table_name;"
            case 'oracle':
                command = ''
            case 'sqlserver':
                command = ''
            case _:
                raise Exception('Banco de dados não suportado.')
        return command
