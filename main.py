from fileinput import filename
from typing import List
from xmlrpc.client import Boolean
import psycopg2
import datetime
import os
from pathlib import Path
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2.errors as perrs
import csv
import json


def get_server_connection(
    username: str,
    password: str
):
    return psycopg2.connect(f"user='{username}' password='{password}'");


def get_db_connection(
    username: str,
    password: str,
    database_name: str
):
    return psycopg2.connect(f"dbname='{database_name}' user='{username}' password='{password}'");


# def create_db(
#     username: str,
#     password: str,
#     database_name: str
# ) -> object:
#     # Connect to PostgreSQL DBMS
#     server_connection = get_server_connection(
#         username=username,
#         password=password
#     )

#     server_connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT);
#     cursor = server_connection.cursor();

#     # Create table statement
#     sql_create_db = f"create database \"{database_name}\";"

#     # Create db
#     try:
#         cursor.execute(sql_create_db);
#     except perrs.DuplicateDatabase as ex:
#         print(f"Database '{database_name}' already exists.")

#     # Get db connection
#     return get_db_connection(
#         username=username,
#         password=password,
#         database_name=database_name
#     )


# def other_stuff():
#     ###################################
#     with open("data/holdings_map.json", "r") as f:
#         holdings_mappings = json.load(f)
#         # print(holdings_mappings)


#     with open(filepath, "r") as f:
#         csv_reader =  csv.DictReader(f, delimiter=delimiter)
#         # print(csv_reader.fieldnames)
#         from data.holdings_model import Holding
#         cursor = connection.cursor()
#         # cursor.execute(sql_create_db);
#         c = 0
#         for row in csv_reader:
#             # print(row.items())
#             # d = dict((holdings_mappings[key], f"\"{value}\"") for (key, value) in row.items() if value != "")
#             d = dict((holdings_mappings[key], wrap_in_quotes(value=value)) for (key, value) in row.items())

#             # dsan = {k: v for k, v in d.items() if v is not None}

#             sql = f"insert into tmp_ ({','.join(d.keys())}) values ({','.join(d.values())});"
#             print(sql)

#             h = Holding(
#                 **d
#             )

#             cursor.execute(sql)
#             c += 1
#             if c % 50000 == 0:
#                 print(c)
#                 connection.commit()
#             # print(d)

#             if c > 1:
#                 return
#         return

#     ###################################

# def mappings_stuff():
#      # Fields in mapping but not in file:

#     mapped_source_column_names = [c["source_fieldname"] for c in mappings]
#     mapped_columns_missing_in_file = [c for c in mapped_source_column_names if c not in column_names]

#     if len(mapped_columns_missing_in_file) > 0:
#         raise Exception(f"Mapped columns missing in file: {mapped_columns_missing_in_file}")

#     # Fields in file but not in mapping: Ignore
#     unmapped_columns_in_file = [c for c in column_names if c not in mapped_source_column_names]
#     if len(unmapped_columns_in_file) > 0:
#         print(f"Unmapped columns will be ignored: {unmapped_columns_in_file}")

#     mapped_column_names = [c for c in column_names if c in mapped_source_column_names]


def create_table(
   database_connection,
   table_name: str,
   column_names: List[str],
   drop_table_if_exists: Boolean = True
):
    # Get cursor object from the database connection
    cursor = database_connection.cursor()

    # Drop target table if it already exists
    if drop_table_if_exists:
        sql_drop_existing = f"drop table if exists \"{table_name}\""
        cursor.execute(sql_drop_existing)
        database_connection.commit()
        print(f"Dropped table [{table_name}].")

    # Create table statement
    cols = [f"\"{c}\"  varchar(256)" for c in column_names]
    cols.insert(0, "id BIGSERIAL PRIMARY KEY NOT NULL")     # Add id column
    sql_create_table = ", \n".join(cols)
    sql_create_table = f"create table {table_name} \n (\n{sql_create_table}\n);"
    # print(sql_create_table)

    # Actually create table
    cursor.execute(sql_create_table)
    database_connection.commit()
    print(f"Created table [{table_name}].")


def get_table_metadata(filepath: str, delimiter: str = ","):
    with open(filepath, "r") as f:
        csv_reader =  csv.DictReader(f, delimiter=delimiter)
        print(csv_reader.fieldnames)
        return csv_reader.fieldnames


def get_list_as_quoted_string(items: List[str]):
    return ",".join([f"\"{c}\"" for c in items])


def copy_data_into_db(
    connection,
    filepath: str,
    table_name: str,
    column_names: List[str],
    delimiter: str = ","
):
    with open(filepath, "r") as f:
        # Skip header
        f.readline()

        # Copy to db
        print(f"Copying file '{filepath}' into db...")
        cur = connection.cursor()
        cur.copy_from(f, table_name, columns=column_names, sep=delimiter)
        connection.commit()
        connection.close()
        print("Done.")

    ## Could add id to file line by line first
    ## Doesn't retain order of rows in file
    # with open(filepath, "r") as f:
    #     sql = f"COPY {table_name} ({get_list_as_quoted_string(column_names)}) FROM STDIN WITH CSV HEADER DELIMITER AS '{delimiter}'"
    #     print(sql)
    #     cur = connection.cursor()
    #     cur.copy_expert(sql=sql, file=f)
    #     connection.commit()
    #     cur.close()


def copy_file_into_db_sql(
    connection,
    filepath: str,
    delimiter: str = ",",
    table_name: str = None
):
    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        return

    if not table_name:
        table_name = "tmp_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")

    column_names = get_table_metadata(filepath=filepath, delimiter=delimiter)

    create_table(
        database_connection=connection,
        table_name=table_name,
        column_names=column_names,
        drop_table_if_exists=True
    )

    copy_data_into_db(
        connection=connection,
        filepath=filepath,
        table_name=table_name,
        column_names=column_names,
        delimiter=delimiter
    )


def move_data_from_tmp_to_table():
    pass


def get_insert_columns_clause(
    columns: List[str],
    table: str
) -> str:
    return f"insert into {wrap_in_quotes(table, True)} ({','.join([wrap_in_quotes(c, double_quote=True) for c in columns])})"


def get_insert_values_clause(item_dict: dict):
    return "(" + ",".join([wrap_in_quotes(str(v), False) if v else 'NULL' for v in item_dict.values()]) +")"


def copy_file_into_db_py(
    connection,
    filepath: str,
    delimiter: str = ",",
    mappings: dict = None,
    table_name: str = None
):
    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        return

    if not table_name:
        table_name = "tmp_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")

    column_names = get_table_metadata(filepath=filepath, delimiter=delimiter)

    create_table(
        database_connection=connection,
        table_name=table_name,
        column_names=column_names,
        drop_table_if_exists=True
    )

    # Pydantic

    with open(filepath, 'r') as file:
        csv_reader =  csv.DictReader(file, delimiter=delimiter)

        cursor = connection.cursor()
        from data.holdings_model import Holding

        c = 0
        insert_columns_clause = get_insert_columns_clause(columns=column_names, table=table_name)
        insert_values_sql = []

        for row in csv_reader:
            c += 1
            # print(row.items())
            d = dict((mappings[key], none_empty(value=value)) for (key, value) in row.items())
            # print(d)
            m = Holding(
                **d
            )
            insert_values_clause = get_insert_values_clause(m.dict())
            insert_values_sql.append(insert_values_clause)

            # TODO: Leftover rows
            if c % 50000 == 0:
                exec_insert_sql(
                    database_connection=connection,
                    cursor=cursor,
                    insert_columns_clause=insert_columns_clause,
                    insert_values_sql=insert_values_sql
                )
                print(f"Processed: {c} rows")
                insert_values_sql= []

        exec_insert_sql(
            database_connection=connection,
            cursor=cursor,
            insert_columns_clause=insert_columns_clause,
            insert_values_sql=insert_values_sql
        )

        cursor.close()

def exec_insert_sql(
    database_connection,
    cursor,
    insert_columns_clause: str,
    insert_values_sql: List[str]
):
    sql_insert = f"{insert_columns_clause} values {','.join(insert_values_sql)};"
    r = cursor.execute(sql_insert);
    database_connection.commit()
    print(f"Added {len(insert_values_sql)} rows.")
    # print(sql_insert)


def load_json_from_file(filepath: str) -> dict:
    with open(filepath, "r") as f:
        return json.load(f)


def wrap_in_quotes(value: str, double_quote=False):
    if not double_quote:
        return f"'{value}'" if value else 'NULL'
    else:
        return f"\"{value}\"" if value else 'NULL'


def none_empty(value: str):
    return value if value else None


def main():
    # Create DB
    connection = create_db(
        username="postgres",
        password="pwn123#",
        database_name="sqletltest"
    )

    ## Use bulk copy to get data in.
    # delimiter = "\t"
    # filepath = '/home/pavdwest/Downloads/statpro/misc/SDHExporter-Holdings.csv'
    # copy_file_into_db_sql(
    #     connection=connection,
    #     filepath=filepath,
    #     delimiter="\t",
    #     table_name="tmp_"
    # )


    # Use pydantic models to import data
    delimiter = "\t"
    filepath = '/home/pavdwest/Downloads/statpro/misc/SDHExporter-Holdings.csv'
    # filepath = 'data/holdings.csv'
    mappings = load_json_from_file("data/holdings_map.json")
    copy_file_into_db_py(
        connection=connection,
        filepath=filepath,
        delimiter="\t",
        mappings=mappings,
        table_name="tmp_"
    )


if __name__ == '__main__':
    main()
