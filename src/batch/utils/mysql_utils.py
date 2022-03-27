import pymysql
from functools import reduce
from pymongo import MongoClient

###########################################################################################################
###################################    MYSQL UTILS METHODS    #############################################

def configure_mysql_database(conn_dict, database):
    commands = dict(
            create_database = f"CREATE DATABASE IF NOT EXISTS {database};",
            use_database= f"use {database};",
            grant_permissions = f"GRANT ALL PRIVILEGES ON {database}.* to '{conn_dict['user']}'@'%';"
    )
    connection = pymysql.connect(**conn_dict)
    cursor = connection.cursor()
    [cursor.execute(sql_command) for sql_command in commands.values()]
    return

def create_mysql_table(conn_dict, database, table, schema):
    return dict(
        create_kafka_ingestion_db = f"CREATE DATABASE IF NOT EXISTS {database};",
        use_kafka_ingestion_db= f"use {database};",
        grant_permissions = f"GRANT ALL PRIVILEGES ON {database}.* to '{conn_dict['user']}'@'%';",
        drop_fake_user_table = f"DROP TABLE IF EXISTS {table};",
        create_fake_user_table = f""" CREATE TABLE IF NOT EXISTS {table} ({table}_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, """ + \
                                 f"""{reduce(lambda a, b: f"{a}, {b}", [f"{k} {v}" for k, v in schema.items()])})""")

def populate_mysql_db(conn_dict, database, table, schema, method_data_generator, method_params, num_rows):

    connection = pymysql.connect(**conn_dict)
    cursor = connection.cursor()
    configure_mysql_database(conn_dict, database)

    commands_create_table = create_mysql_table(conn_dict, database, table, schema)
    [cursor.execute(sql_command) for sql_command in commands_create_table.values()]
    
    for i in range(num_rows):
        real_time_data = method_data_generator(**method_params)
        columns, values = (list(real_time_data.keys()), list(real_time_data.values()))
        columns =  reduce(lambda a, b: f"{a}, {b}", [f"`{i}`" for i in real_time_data.keys()])
        formato = reduce(lambda a, b: f"{a}, {b}",['%s' for i in range(len(values))])
        sql_command = f"INSERT INTO `{table}` ({columns}) VALUES ({formato})"
        cursor.execute(sql_command, values)
        connection.commit()
    connection.close()
