import mysql.connector
from databaseconverter.db.utils.DatabaseUtils import DatabaseUtils

# Configurações do banco de dados de origem
origem_config = {
    'host': 'localhost',
    'user': 'anderson',
    'password': 'root',
    'database': 'locadora',
    'port': '3306'
}

# Configurações do banco de dados de destino
destino_config = {
    'host': 'localhost',
    'user': 'anderson',
    'password': 'root',
    'database': 'clone',
    'port': '3306'
}


try:
    # Conexão com o banco de dados de origem
    #origem_conn = mysql.connector.connect(**origem_config)
    #origem_cursor = origem_conn.cursor()
    du_origin = DatabaseUtils(db='postgres', host='192.168.1.33', user='postgres', password='postgres', database='lar',
                              port=5433)
    origin_conn = du_origin.get_connection().open_connection()
    origin_cursor = origin_conn.cursor()

    # Conexão com o banco de dados de destino
    #destino_conn = mysql.connector.connect(**destino_config)
    #destino_cursor = destino_conn.cursor()
    du_destiny = DatabaseUtils(db='mysql', host='localhost', user='anderson', password='root', database='clone',
                              port=3306)
    destiny_conn = du_destiny.get_connection().open_connection()
    destiny_cursor = destiny_conn.cursor()

    # Obtém as tabelas do banco de dados de origem
    origin_cursor.execute(du_origin.get_all_tables_sql())
    tabelas = origin_cursor.fetchall()

    for tabela in tabelas:
        tabela = tabela[0]
        # Obtém a estrutura da tabela da origem
        origin_cursor.execute(du_origin.get_table_create_sql(tabela))
        create_table_query = origin_cursor.fetchone()[0]
        print(du_destiny.get_table_create_sql(tabela))
        print(create_table_query)

        # Cria a tabela na base de dados de destino
        destiny_cursor.execute(create_table_query)
        print(f"Tabela {tabela} copiada com sucesso!")

    # Commit das alterações no banco de dados de destino
    destiny_conn.commit()

#select DBMS_METADATA.GET_DDL('TABLE','USUARIO','SIM3G_FLORA_2018_01_23') from DUAL;
except mysql.connector.Error as e:
    print("Erro:", e)

finally:
    # Fecha as conexões
    if 'origem_conn' in locals() and origin_cursor.is_connected():
        origin_cursor.close()
        origin_conn.close()
    if 'destiny_conn' in locals() and destiny_conn.is_connected():
        destiny_cursor.close()
        destiny_conn.close()